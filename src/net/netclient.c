#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "utils/list/list_func.h"
#include "utils/guc/guc.h"
#include "utils/uuid/uuid.h"
#include "queue/queue.h"
#include "storage/file_buffer.h"
#include "net/netmsg/netmsg.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netclient.h"

/* Reset status, timeout, close descriptor, clean packets memory, set iompbase and iompops etc */
void netclient_reset(netclient* netclient)
{
    netclient->protocoltype = NETCLIENT_PROTOCOLTYPE_IPTCP;
    netclient->hbtimeout = 0;
    netclient->timeout = 0;
    netclient->status = NETCLIENTCONN_STATUS_NOP;
    if (netclient->fd != -1)
    {
        osal_file_close(netclient->fd);
        netclient->fd = -1;
    }
    netclient->pos = 0;
    netclient->ops = netiomp_init(NETIOMP_TYPE_POLL);
    netclient->ops->reset(netclient->base);
    netclient->base->timeout = NET_POLLTIMEOUT;
    queue_destroy(netclient->rpackets, netpacket_destroyvoid);
    netclient->rpackets = queue_init();
    queue_destroy(netclient->wpackets, netpacket_destroyvoid);
    netclient->wpackets = queue_init();
    netclient->callback = netclient_default_packets_handler;
}

void netclient_setprotocoltype(netclient* netclient, netclient_protocoltype protocoltype)
{
    netclient->protocoltype = protocoltype;
}

/* Set netclient timeout */
void netclient_settimeout(netclient* netclient, int timeout)
{
    if (NULL == netclient)
    {
        return;
    }

    netclient->timeout = timeout;
    return;
}

/* Set netclient hbtimeout*/
void netclient_sethbtimeout(netclient* netclient, int hbtimeout)
{
    if (NULL == netclient)
    {
        return;
    }

    netclient->hbtimeout = hbtimeout;
    return;
}

/* Set netclient svrhost */
void netclient_setsvrhost(netclient* netclient, char* host)
{
    if (NULL == netclient)
    {
        return;
    }

    rmemset1(netclient->svrhost, 0, '\0', 128);

    if (NULL != host)
    {
        rmemcpy1(netclient->svrhost, 0, host, strlen(host));
    }
    return;
}

/* Set netclient svrport */
void netclient_setsvrport(netclient* netclient, char* port)
{
    if (NULL == netclient)
    {
        return;
    }

    rmemset1(netclient->svrport, 0, '\0', 128);

    if (NULL != port)
    {
        rmemcpy1(netclient->svrhost, 0, port, strlen(port));
    }
    return;
}

/* Connect to server */
bool netclient_conn(netclient* netclient)
{
    bool               bret = false;
    int                iret = 0;
    int                yes = 1;
    int                domain = AF_INET;
    int                addrlen = 0;
    int                keep_alive = 0;
    int                idle = 0;
    int                interval = 0;
    int                count = 0;
    int                timeout = 0;
    struct sockaddr*   connaddr = NULL;
    struct sockaddr_in addrin;
    struct sockaddr_un addrun;
    char               unixdoamin[512] = {0};

    keep_alive = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVE);
    idle = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_IDLE);
    interval = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_INTERVAL);
    count = guc_getConfigOptionInt(CFG_KEY_TCP_KEEPALIVES_COUNT);
    timeout = guc_getConfigOptionInt(CFG_KEY_TCP_USER_TIMEOUT);

    /* Get address corresponding to host */
    rmemset1(&addrin, 0, 0, sizeof(struct sockaddr_in));

    if (NETCLIENT_PROTOCOLTYPE_IPTCP == netclient->protocoltype)
    {
        bret = osal_host2sockaddr(
            &addrin, netclient->svrhost, netclient->svrport, domain, SOCK_STREAM, IPPROTO_TCP, 1);
        if (false == bret)
        {
            elog(RLOG_WARNING, "can not get addr info, %s", strerror(errno));
            return false;
        }
        connaddr = (struct sockaddr*)&addrin;
        addrlen = sizeof(struct sockaddr_in);
    }
    else if (NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN == netclient->protocoltype)
    {
        domain = AF_LOCAL;
        snprintf(unixdoamin,
                 512,
                 "%s/%s%s",
                 RMANAGER_UNIXDOMAINDIR,
                 RMANAGER_UNIXDOMAINPREFIX,
                 netclient->svrport);
        if (sizeof(addrun.sun_path) <= strlen(unixdoamin))
        {
            elog(RLOG_WARNING, "unix domain dir too long, %s", strerror(errno));
            return false;
        }
        memset(addrun.sun_path, '\0', sizeof(addrun.sun_path));
        memcpy(addrun.sun_path, unixdoamin, strlen(unixdoamin));
        addrun.sun_family = domain;
        connaddr = (struct sockaddr*)&addrun;
        addrlen = sizeof(struct sockaddr_un);
    }

    /* Create TCP descriptor */
    netclient->fd = osal_socket(domain, SOCK_STREAM, 0);
    if (-1 == netclient->fd)
    {
        return false;
    }

    /* Disable TCP_NODELAY */
    osal_setsockopt(netclient->fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int));

    /* Set to non-blocking mode */
    osal_setunblock(netclient->fd);

    if (keep_alive)
    {
        osal_setsockopt(
            netclient->fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&keep_alive, sizeof(keep_alive));

        osal_setsockopt(netclient->fd, IPPROTO_TCP, TCP_KEEPIDLE, (char*)&idle, sizeof(idle));

        osal_setsockopt(
            netclient->fd, IPPROTO_TCP, TCP_KEEPINTVL, (char*)&interval, sizeof(interval));

        osal_setsockopt(netclient->fd, IPPROTO_TCP, TCP_KEEPCNT, (char*)&count, sizeof(count));

        osal_setsockopt(
            netclient->fd, IPPROTO_TCP, TCP_USER_TIMEOUT, (char*)&timeout, sizeof(timeout));
    }

    /* Connect to target */
    iret = osal_connect(netclient->fd, (struct sockaddr*)connaddr, addrlen);
    if (-1 == iret)
    {
        /* Check if current status is connecting */
        if (errno == EINPROGRESS)
        {
            netclient->status = NETCLIENTCONN_STATUS_INPROCESS;
            return true;
        }

        netclient->status = NETCLIENTCONN_STATUS_NOP;
        elog(RLOG_WARNING, "connect error, %s", strerror(errno));
        osal_file_close(netclient->fd);
        netclient->fd = -1;
        return false;
    }

    netclient->status = NETCLIENTCONN_STATUS_INPROCESS;
    return true;
}

/* Used to check if connected to target, when status is INPROCESS, check if can convert to CONNECTED
 */
bool netclient_isconnect(netclient* netclient)
{
    uint16    flag = POLLOUT;
    int       iret = 0;
    int       value = 0;
    int       timeout = 0;
    socklen_t len = sizeof(int);

    while (1)
    {
        if (true == g_gotsigterm)
        {
            return false;
        }

        netclient->ops->reset(netclient->base);
        netclient->pos = netclient->ops->add(netclient->base, netclient->fd, flag);

        timeout = netclient->base->timeout;
        netclient->base->timeout = 10000;
        iret = netclient->ops->iomp(netclient->base);
        if (-1 == iret)
        {
            /* Check if error is caused by signal, if so continue monitoring */
            if (errno == EINTR)
            {
                continue;
            }
            elog(RLOG_WARNING, "can't connect");
            osal_file_close(netclient->fd);
            netclient->fd = -1;
            return false;
        }

        /* Check status, see if connection is normal */
        if (0 == iret)
        {
            /* Timeout, close descriptor */
            elog(RLOG_WARNING, "connect timeout");
            osal_file_close(netclient->fd);
            netclient->fd = -1;
            return false;
        }
        netclient->base->timeout = timeout;

        iret = osal_getsockopt(netclient->fd, SOL_SOCKET, SO_ERROR, &value, &len);
        if (-1 == iret)
        {
            /* Close connection */
            elog(RLOG_WARNING, "osal_getsockopt error, %s", strerror(errno));
            osal_file_close(netclient->fd);
            netclient->fd = -1;
            return false;
        }

        if (0 != value)
        {
            elog(RLOG_WARNING, "osal_getsockopt value error, %s", strerror(value));
            osal_file_close(netclient->fd);
            netclient->fd = -1;
            return false;
        }

        break;
    }

    netclient->status = NETCLIENTCONN_STATUS_CONNECTED;
    return true;
}

/*
 * Try to connect to server
 *  conn
 *  sleep(1)
 *  is conn ?
 *
 *  true    Connected
 *  false   Not connected
 */
bool netclient_tryconn(netclient* netclient)
{
    /* Connect to target */
    if (false == netclient_conn(netclient))
    {
        netclient->status = NETCLIENTCONN_STATUS_NOP;
        return false;
    }

    /* Check if current status is INPROCESS */
    if (NETCLIENTCONN_STATUS_INPROCESS == netclient->status)
    {
        /* Check descriptor status, reset status if there is error */
        if (false == netclient_isconnect(netclient))
        {
            /* Connection failed */
            elog(RLOG_WARNING, "connect timeout error, %s", strerror(errno));
            netclient->status = NETCLIENTCONN_STATUS_NOP;
            return false;
        }
    }
    return true;
}

/* Receive data */
static bool netclient_recv(netclient* netclient)
{
    bool       bhead = false;
    int        rlen = 0;
    int        readlen = 0;
    int        msglen = 0;
    uint8*     cptr = NULL;
    netpacket* npacket = NULL;
    uint8      hdr[8] = {0};

    if (NULL == netclient)
    {
        return false;
    }

    /*
     * 1、Get last packet from read queue
     *  1.1 Complete packet, apply for a new packet and mount to read queue
     *  1.2 Incomplete packet, use this packet
     * 2、Queue is empty
     *  Apply for a new packet and mount to read queue
     */
    if (false == queue_isnull(netclient->rpackets))
    {
        npacket = (netpacket*)netclient->rpackets->tail->data;
        if (npacket->offset == npacket->used)
        {
            npacket = NULL;
        }
    }

    if (NULL == npacket)
    {
        npacket = netpacket_init();
        if (NULL == npacket)
        {
            elog(RLOG_WARNING, "net client read out of memory");
            return false;
        }
        bhead = true;
        rlen = readlen = 8;
        cptr = hdr;
        if (false == queue_put(netclient->rpackets, npacket))
        {
            elog(RLOG_WARNING, "net client add packet to read queue error");
            netpacket_destroy(npacket);
            return false;
        }
    }
    else
    {
        /* Read data */
        rlen = readlen = npacket->used - npacket->offset;
        cptr = npacket->data + npacket->offset;
    }

    if (false == osal_net_read(netclient->fd, cptr, &rlen))
    {
        elog(RLOG_WARNING, "net client read data error");
        return false;
    }

    if (false == bhead)
    {
        npacket->offset += readlen;
        return true;
    }

    rmemcpy1(&msglen, 0, cptr, 4);
    msglen = r_ntoh32(msglen);
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "net client netpacket data init out of memory");
        return false;
    }
    rmemcpy0(npacket->data, 0, cptr, readlen);
    npacket->used = msglen;
    npacket->max = msglen;
    npacket->offset = readlen;
    return true;
}

/* Send data */
static bool netclient_send(netclient* netclient)
{
    /*
     * Send data
     *  1、Get packet to send from queue
     *  2、Send data
     */
    int        timeout = 0;
    int        sendlen = 0;
    uint8*     cptr = NULL;
    netpacket* npacket = NULL;
    if (NULL == netclient)
    {
        return true;
    }

    if (true == queue_isnull(netclient->wpackets))
    {
        return true;
    }

    npacket = queue_get(netclient->wpackets, &timeout);
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "net client send get packet from queue error");
        return false;
    }

    cptr = npacket->data + npacket->offset;
    sendlen = npacket->used - npacket->offset;
    if (false == osal_net_write(netclient->fd, cptr, sendlen))
    {
        elog(RLOG_WARNING, "net client send packet error");
        netpacket_destroy(npacket);
        return false;
    }

    netpacket_destroy(npacket);
    return true;
}

/* Create connection and send data */
bool netclient_senddata(
    netclient_protocoltype ptype, char* host, char* port, uint8* data, int datalen)
{
    bool       bret = true;
    int        intervaltimeout = 0;
    int        interval = 10000;
    netpacket* npacket = NULL;
    netclient  netclient = {0};

    netclient.fd = -1;
    netclient.base = NULL;
    netclient.rpackets = NULL;
    netclient.wpackets = NULL;

    if (NULL != host)
    {
        sprintf(netclient.svrhost, "%s", host);
    }
    sprintf(netclient.svrport, "%s", port);

    /* Set network model to use */
    netclient.ops = netiomp_init(NETIOMP_TYPE_POLL);

    /* Apply for base information for subsequent descriptor processing */
    if (false == netclient.ops->create(&netclient.base))
    {
        bret = false;
        goto netclient_senddata_done;
    }

    /* Set type */
    netclient_setprotocoltype(&netclient, ptype);
    netclient_sethbtimeout(&netclient, NET_HBTIME);
    netclient_settimeout(&netclient, 0);

    netclient.base->timeout = NET_POLLTIMEOUT;
    netclient.status = NETCLIENTCONN_STATUS_NOP;
    netclient.wpackets = queue_init();
    if (NULL == netclient.wpackets)
    {
        bret = false;
        goto netclient_senddata_done;
    }

    /* Build packet */
    netclient.rpackets = queue_init();
    if (NULL == netclient.rpackets)
    {
        bret = false;
        goto netclient_senddata_done;
    }
    netclient.callback = NULL;

    npacket = netpacket_init();
    if (NULL == npacket)
    {
        bret = false;
        goto netclient_senddata_done;
    }
    npacket->data = netpacket_data_init(datalen);
    if (NULL == npacket->data)
    {
        bret = false;
        goto netclient_senddata_done;
    }
    rmemcpy0(npacket->data, 0, data, datalen);
    npacket->used = datalen;
    npacket->offset = 0;

    /* Connect to xmanager */
    if (false == netclient_tryconn(&netclient))
    {
        bret = false;
        elog(RLOG_WARNING, "can not connect server");
        goto netclient_senddata_done;
    }

    if (NETCLIENTCONN_STATUS_CONNECTED != netclient.status)
    {
        bret = false;
        elog(RLOG_WARNING, "connect server error");
        goto netclient_senddata_done;
    }

    if (false == queue_put(netclient.wpackets, npacket))
    {
        bret = false;
        goto netclient_senddata_done;
    }
    npacket = NULL;

    while (intervaltimeout < interval)
    {
        if (false == netclient_desc2(&netclient))
        {
            bret = false;
            elog(RLOG_WARNING, "metric capture iomp desc error");
            goto netclient_senddata_done;
        }

        if (true == queue_isnull(netclient.wpackets))
        {
            sleep(1);
            goto netclient_senddata_done;
        }
        intervaltimeout += NET_POLLTIMEOUT;
    }

    elog(RLOG_WARNING, "send data to server timeout");
    bret = false;
netclient_senddata_done:
    if (NULL != npacket)
    {
        netpacket_destroy(npacket);
        npacket = NULL;
    }
    netclient_destroy(&netclient);
    return bret;
}

/* Create listen event and wait for event trigger, process triggered event */
bool netclient_desc(netclient* netclient)
{
    int        iret = 0;
    int        event = 0;  /* Event type */
    int        revent = 0; /* Triggered event */

    /* Data length recorded in message, default set to header length */
    uint32     msglen = NETMSG_TYPE_HDR_SIZE;

    uint8      head[NETMSG_TYPE_HDR_SIZE] = {0};
    uint8*     ruptr = NULL;
    netpacket* netpacket = NULL;

    /* FOR DEBUG BEGIN */
    uint8*     uptr = NULL;
    uint32     debugmsgtype = 0;
    uint32     debugmsglen = 0;

    /* FOR DEBUG END */

    /* Get data */
    /*
     * 1、Create listen event
     * 2、Check if listen event triggered
     * 3、Different processing logic based on different protocol types
     */
    /* Reset event listening */
    netclient->ops->reset(netclient->base);
    event |= POLLIN;

    if (false == netclient_wpacketisnull(netclient))
    {
        /* Reset event listening */
        event |= POLLOUT;
    }

    /* Add listen event */
    netclient->pos = netclient->ops->add(netclient->base, netclient->fd, event);

    /* Call iomp port */
    iret = netclient->ops->iomp(netclient->base);
    if (-1 == iret)
    {
        if (errno == EINTR)
        {
            return true;
        }
        return false;
    }

    if (0 == iret)
    {
        /* Timeout, accumulate hbtime and timeout then continue */
        netclient->timeout += netclient->base->timeout;
        netclient->hbtimeout += netclient->base->timeout;
        return true;
    }

    /* Message triggered, check triggered event type */
    revent = netclient->ops->getevent(netclient->base, netclient->pos);

    /* Whether there is data to read */
    if (POLLIN == (revent & POLLIN))
    {
        /* Read data and process */
        /* Reset counter */
        netclient->timeout = 0;

        if (netclient_rpacketisnull(netclient))
        {
            uint32 msgtype = 0;
            /* Get all data */
            ruptr = head;
            if (false == osal_net_read(netclient->fd, ruptr, (int*)&msglen))
            {
                /* Read data failed, exit */
                if (0 == msglen)
                {
                    elog(RLOG_WARNING, "osal_close sock, %s", strerror(errno));
                }
                else
                {
                    elog(RLOG_WARNING, "read error, errno:%d, %s", errno, strerror(errno));
                }
                return false;
            }

            /* Convert length and get */
            msgtype = CONCAT(get, 32bit)(&ruptr);
            msglen = CONCAT(get, 32bit)(&ruptr);

            elog(RLOG_DEBUG, "type: %u, len: %u", msgtype, msglen);

            netpacket = netpacket_init();
            netpacket->max = MAXALIGN(msglen);
            netpacket->offset = NETMSG_TYPE_HDR_SIZE;
            netpacket->used = msglen;
            netpacket->data = rmalloc0(msglen);
            if (NULL == netpacket->data)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(netpacket->data, 0, '\0', msglen);

            rmemcpy0(netpacket->data, 0, head, NETMSG_TYPE_HDR_SIZE);

            msglen -= NETMSG_TYPE_HDR_SIZE;

            if (0 != msglen)
            {
                ruptr = netpacket->data + NETMSG_TYPE_HDR_SIZE;

                /* Get all data */
                if (false == osal_net_read(netclient->fd, ruptr, (int*)&msglen))
                {
                    /* Read data failed, exit */
                    if (0 == msglen)
                    {
                        elog(RLOG_WARNING, "osal_close sock");
                    }
                    else
                    {
                        elog(RLOG_WARNING, "read error, %s", strerror(errno));
                    }
                    netpacket_destroy(netpacket);
                    return false;
                }
            }
            netclient->callback(netclient, netpacket);
            netpacket_destroy(netpacket);
        }
    }

    if (POLLOUT == (POLLOUT & revent))
    {
        /* Check if triggered */
        /* Check if there is data to write */

        netpacket = queue_get(netclient->wpackets, NULL);
        if (NULL == netpacket)
        {
            return true;
        }

        /* Send data */
        /* FOR DEBUG BEGIN */
        uptr = netpacket->data;
        debugmsgtype = get32bit(&uptr);
        debugmsglen = get32bit(&uptr);
        elog(RLOG_DEBUG, "send msgtype:%u, msglen:%u", debugmsgtype, debugmsglen);
        /* FOR DEBUG END */

        if (false == osal_net_write(netclient->fd, netpacket->data, netpacket->used))
        {
            /* Send data failed, close connection */
            elog(RLOG_WARNING, "write data 2 error, %s", strerror(errno));
            netpacket_destroy(netpacket);
            return false;
        }
        netpacket_destroy(netpacket);
        netpacket = NULL;
    }

    if ((POLLIN != (revent & POLLIN)) && (POLLOUT != (POLLOUT & revent)))
    {
        elog(RLOG_WARNING, "unknown event, %d", revent);
        return false;
    }

    return true;
}

/*
 * Create listen event and wait for event trigger, receive or send data, only receive or send, no
 * business processing
 */
bool netclient_desc2(netclient* netclient)
{
    int iret = 0;
    int event = 0;  /* Event type */
    int revent = 0; /* Triggered event */

    /* Get data */
    /*
     * 1、Create listen event
     * 2、Check if listen event triggered
     * 3、Different processing logic based on different protocol types
     */
    /* Reset event listening */
    netclient->ops->reset(netclient->base);

    /* Create listen event */
    event |= POLLIN;
    if (false == netclient_wpacketisnull(netclient))
    {
        /* Write event */
        event |= POLLOUT;
    }

    /* Add listen event */
    netclient->pos = netclient->ops->add(netclient->base, netclient->fd, event);

    /* Call iomp port */
    iret = netclient->ops->iomp(netclient->base);
    if (-1 == iret)
    {
        if (errno == EINTR)
        {
            return true;
        }
        elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
        return false;
    }

    if (0 == iret)
    {
        /* Timeout, accumulate hbtime and timeout then continue */
        netclient->hbtimeout += netclient->base->timeout;
        return true;
    }

    /* Message triggered, check triggered event type */
    revent = netclient->ops->getevent(netclient->base, netclient->pos);
    if (0 == revent)
    {
        /* No event triggered */
        return true;
    }

    if (POLLIN == (revent & POLLIN))
    {
        /* Receive data */
        if (false == netclient_recv(netclient))
        {
            elog(RLOG_WARNING, "net pool recv error");
            return false;
        }
    }

    if (POLLOUT == (revent & POLLOUT))
    {
        /* Send data */
        if (false == netclient_send(netclient))
        {
            elog(RLOG_WARNING, "net pool send error");
            return false;
        }
    }

    if (POLLHUP == revent || POLLERR == revent)
    {
        elog(RLOG_WARNING, "iomp pollhup/pollerr error, %s", strerror(errno));
        return false;
    }

    return true;
}

bool netclient_addwpacket(netclient* netclient, void* packet)
{
    bool result = false;
    if (NULL != netclient->wpackets)
    {
        result = queue_put(netclient->wpackets, packet);
    }
    return result;
}

bool netclient_wpacketisnull(netclient* netclient)
{
    bool result = false;

    if (NULL == netclient->wpackets->head)
    {
        result = true;
    }

    return result;
}

bool netclient_rpacketisnull(netclient* netclient)
{
    bool result = false;

    if (NULL == netclient->rpackets->head)
    {
        result = true;
    }

    return result;
}

/*  Callback function to process received information */
bool netclient_default_packets_handler(void* netclient_ptr, netpacket* netpacket)
{
    uint8*     uptr = NULL;
    uint32     msgtype = NETMSG_TYPE_NOP;
    netclient* client_state = NULL;
    client_state = (netclient*)netclient_ptr;

    uptr = netpacket->data;

    msgtype = CONCAT(get, 32bit)(&uptr);

    if (false == netmsg((void*)client_state, msgtype, netpacket->data))
    {
        client_state->status = NETCLIENTCONN_STATUS_NOP;
        return false;
    }

    return true;
}

/*
 * Clean up descriptor/queue
 * Set connection status to not connected
 */
void netclient_clear(netclient* netclient)
{
    if (NULL == netclient)
    {
        return;
    }

    if (-1 != netclient->fd)
    {
        elog(RLOG_WARNING, "netclient->fd:%d", netclient->fd);
        osal_close(netclient->fd);
        netclient->fd = -1;
        netclient->status = NETCLIENTCONN_STATUS_NOP;
    }

    if (NULL != netclient->wpackets)
    {
        queue_clear(netclient->wpackets, netpacket_destroyvoid);
    }

    if (NULL != netclient->rpackets)
    {
        queue_clear(netclient->rpackets, netpacket_destroyvoid);
    }
}

/* Resource cleanup */
void netclient_destroy(netclient* netclient)
{
    if (NULL == netclient)
    {
        return;
    }

    if (-1 != netclient->fd)
    {
        elog(RLOG_WARNING, "netclient->fd:%d", netclient->fd);
        osal_close(netclient->fd);
        netclient->fd = -1;
    }

    if (NULL != netclient->base)
    {
        netclient->ops->free(netclient->base);
        netclient->base = NULL;
    }

    if (NULL != netclient->wpackets)
    {
        queue_destroy(netclient->wpackets, netpacket_destroyvoid);
    }

    if (NULL != netclient->rpackets)
    {
        queue_destroy(netclient->rpackets, netpacket_destroyvoid);
    }
}