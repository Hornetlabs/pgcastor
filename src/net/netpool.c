#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"

/*----------------net pool entry operatiion begin------------------*/
netpoolentry* netpoolentry_init(void)
{
    netpoolentry* npoolentry = NULL;

    npoolentry = rmalloc0(sizeof(netpoolentry));
    if (NULL == npoolentry)
    {
        elog(RLOG_WARNING, "net pool entry init error, out of memory");
        return NULL;
    }
    rmemset0(npoolentry, 0, '\0', sizeof(netpoolentry));

    npoolentry->fd = -1;
    npoolentry->stat = NETPOOLENTRY_STAT_NOP;
    npoolentry->host = NULL;
    npoolentry->port = NULL;
    npoolentry->rpackets = queue_init();
    if (NULL == npoolentry->rpackets)
    {
        rfree(npoolentry);
        return NULL;
    }

    npoolentry->wpackets = queue_init();
    if (NULL == npoolentry->wpackets)
    {
        queue_destroy(npoolentry->wpackets, NULL);
        rfree(npoolentry);
        return NULL;
    }
    return npoolentry;
}

/* Set descriptor */
void netpoolentry_setfd(netpoolentry* npoolentry, int fd)
{
    if (NULL == npoolentry)
    {
        return;
    }
    npoolentry->fd = fd;
    npoolentry->stat = NETPOOLENTRY_STAT_OK;
}

/* Set host information */
bool netpoolentry_sethost(netpoolentry* npoolentry, char* host)
{
    int len = 0;
    if (NULL == npoolentry || NULL == host)
    {
        return true;
    }

    if (NULL != npoolentry->host)
    {
        rfree(npoolentry->host);
        npoolentry->host = NULL;
    }

    len = strlen(host);
    len++;
    npoolentry->host = rmalloc0(len);
    if (NULL == npoolentry->host)
    {
        return false;
    }
    rmemset0(npoolentry->host, 0, '\0', len);
    len--;
    rmemcpy0(npoolentry->host, 0, host, len);
    return true;
}

/* Set port information */
bool netpoolentry_setport(netpoolentry* npoolentry, char* port)
{
    int len = 0;
    if (NULL == npoolentry || NULL == port)
    {
        return true;
    }

    if (NULL != npoolentry->port)
    {
        rfree(npoolentry->port);
        npoolentry->port = NULL;
    }

    len = strlen(port);
    len++;
    npoolentry->port = rmalloc0(len);
    if (NULL == npoolentry->port)
    {
        return false;
    }
    rmemset0(npoolentry->port, 0, '\0', len);
    len--;
    rmemcpy0(npoolentry->port, 0, port, len);
    return true;
}

void netpoolentry_destroy(netpoolentry* npoolentry)
{
    if (NULL == npoolentry)
    {
        return;
    }

    if (-1 != npoolentry->fd)
    {
        close(npoolentry->fd);
        npoolentry->fd = -1;
    }

    if (NULL != npoolentry->host)
    {
        rfree(npoolentry->host);
        npoolentry->host = NULL;
    }

    if (NULL != npoolentry->port)
    {
        rfree(npoolentry->port);
        npoolentry->port = NULL;
    }

    queue_destroy(npoolentry->rpackets, netpacket_destroyvoid);
    queue_destroy(npoolentry->wpackets, netpacket_destroyvoid);
    rfree(npoolentry);
}

/*----------------net pool entry operatiion   end------------------*/

netpool* netpool_init(void)
{
    int      index = 0;
    netpool* npool = NULL;

    npool = rmalloc0(sizeof(netpool));
    if (NULL == npool)
    {
        elog(RLOG_WARNING, "net pool init error");
        return NULL;
    }
    rmemset0(npool, 0, '\0', sizeof(netpool));

    npool->fdmax = NETPOOL_DEFAULTFDSIZE;
    npool->fds = rmalloc0(sizeof(void*) * npool->fdmax);
    if (NULL == npool->fds)
    {
        elog(RLOG_ERROR, "net server reset out of memory");
        return NULL;
    }
    rmemset0(npool->fds, 0, '\0', sizeof(void*) * npool->fdmax);

    npool->pos = rmalloc0(sizeof(int) * npool->fdmax);
    if (NULL == npool->pos)
    {
        elog(RLOG_ERROR, "net pool reset out of memory");
        return NULL;
    }
    rmemset0(npool->pos, 0, '\0', sizeof(int) * npool->fdmax);

    npool->errorfds = rmalloc0(sizeof(int) * npool->fdmax);
    if (NULL == npool->errorfds)
    {
        elog(RLOG_ERROR, "net pool reset out of memory");
        return NULL;
    }
    rmemset0(npool->errorfds, 0, '\0', sizeof(int) * npool->fdmax);

    for (index = 0; index < npool->fdmax; index++)
    {
        npool->fds[index] = NULL;
        npool->pos[index] = -1;
        npool->errorfds[index] = -1;
    }

    /* Apply for base information for subsequent descriptor processing */
    npool->ops = netiomp_init(NETIOMP_TYPE_POLL);
    if (false == npool->ops->create(&npool->base))
    {
        elog(RLOG_ERROR, "net pool reset NETIOMP_TYPE_POLL create error");
        return NULL;
    }
    npool->ops->reset(npool->base);
    npool->base->timeout = NET_POLLTIMEOUT;

    return npool;
}

/* Add */
bool netpool_add(netpool* npool, netpoolentry* entry)
{
    /*
     * 1. Search for unused slot in original fds
     * 2. If not found, allocate new space
     */
    int            index = 0;
    int            fdmax = 0;
    int*           pos = NULL;
    int*           errorfds = NULL;
    netpoolentry** pentrys = NULL;

    for (index = 0; index < npool->fdmax; index++)
    {
        if (NULL != npool->fds[index])
        {
            continue;
        }

        npool->fds[index] = entry;
        if (index == npool->fdcnt)
        {
            npool->fdcnt++;
        }
        return true;
    }

    fdmax = (npool->fdmax * 2);
    pentrys = (netpoolentry**)rmalloc0(sizeof(void*) * fdmax);
    if (NULL == pentrys)
    {
        elog(RLOG_WARNING, "net pool add entry error, out of memory");
        return false;
    }
    rmemset0(pentrys, 0, '\0', sizeof(void*) * fdmax);

    pos = rmalloc0(sizeof(int) * fdmax);
    if (NULL == pos)
    {
        elog(RLOG_WARNING, "net pool add entry error, out of memory");
        rfree(pentrys);
        return false;
    }
    rmemset0(pos, 0, '\0', sizeof(int) * fdmax);

    errorfds = rmalloc0(sizeof(int) * fdmax);
    if (NULL == errorfds)
    {
        elog(RLOG_WARNING, "net pool add entry error, out of memeory");
        rfree(pentrys);
        rfree(pos);
        return false;
    }
    rmemset0(errorfds, 0, '\0', sizeof(int) * fdmax);

    for (index = 0; index < npool->fdmax; index++)
    {
        pentrys[index] = npool->fds[index];
        pos[index] = npool->pos[index];
        errorfds[index] = -1;
    }

    for (; index < fdmax; index++)
    {
        pentrys[index] = NULL;
        pos[index] = -1;
        errorfds[index] = -1;
    }

    rfree(npool->fds);
    rfree(npool->pos);
    rfree(npool->errorfds);
    npool->fds = pentrys;
    npool->pos = pos;
    npool->errorfds = errorfds;
    npool->fds[npool->fdcnt] = entry;
    npool->fdcnt++;
    return true;
}

/* Delete */
void netpool_del(netpool* npool, int fd)
{
    int index = 0;
    int fdcnt = 0;

    fdcnt = npool->fdcnt;
    for (index = 0; index < fdcnt; index++)
    {
        if (NULL == npool->fds[index])
        {
            continue;
        }

        if (npool->fds[index]->fd != fd)
        {
            continue;
        }

        netpoolentry_destroy(npool->fds[index]);
        npool->fds[index] = NULL;
        npool->pos[index] = -1;
    }
}

/* Get entry from netpool by fd */
netpoolentry* netpool_getentrybyfd(netpool* npool, int fd)
{
    int index = 0;

    for (index = 0; index < npool->fdcnt; index++)
    {
        if (NULL == npool->fds[index])
        {
            continue;
        }

        if (npool->fds[index]->fd != fd)
        {
            continue;
        }

        return npool->fds[index];
    }

    return NULL;
}

/* Receive data */
static bool netpool_recv(netpoolentry* npoolentry)
{
    bool       bhead = false;
    int        rlen = 0;
    int        readlen = 0;
    int        msglen = 0;
    uint8*     cptr = NULL;
    netpacket* npacket = NULL;
    uint8      hdr[8] = {0};
    if (NULL == npoolentry)
    {
        return false;
    }

    /*
     * 1. Get last packet from read queue
     *  1.1 Complete packet, apply for a new packet and mount to read queue
     *  1.2 Incomplete packet, use this packet
     * 2. Queue is empty
     *  Apply for a new packet and mount to read queue
     */
    if (false == queue_isnull(npoolentry->rpackets))
    {
        npacket = (netpacket*)npoolentry->rpackets->tail->data;
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
            elog(RLOG_WARNING, "net pool read out of memory");
            return false;
        }
        bhead = true;
        rlen = readlen = 8;
        cptr = hdr;
        if (false == queue_put(npoolentry->rpackets, npacket))
        {
            elog(RLOG_WARNING, "net pool add packet to read queue error");
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

    if (false == osal_net_read(npoolentry->fd, cptr, &rlen))
    {
        elog(RLOG_WARNING, "net pool read data error");
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
        elog(RLOG_WARNING, "net pool netpacket data init out of memory, msglen:%d", msglen);
        return false;
    }
    rmemcpy0(npacket->data, 0, cptr, readlen);
    npacket->used = msglen;
    npacket->max = msglen;
    npacket->offset = readlen;
    return true;
}

/* Send data */
static bool netpool_send(netpoolentry* npoolentry)
{
    /*
     * Send data
     *  1. Get packet to send from queue
     *  2. Send data
     */
    int        timeout = 0;
    int        sendlen = 0;
    uint8*     cptr = NULL;
    netpacket* npacket = NULL;
    if (NULL == npoolentry)
    {
        return true;
    }

    if (true == queue_isnull(npoolentry->wpackets))
    {
        return true;
    }

    npacket = queue_get(npoolentry->wpackets, &timeout);
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "net pool send get packet from queue error");
        return false;
    }

    cptr = npacket->data + npacket->offset;
    sendlen = npacket->used - npacket->offset;
    if (false == osal_net_write(npoolentry->fd, cptr, sendlen))
    {
        elog(RLOG_WARNING, "net pool send packet error, %s", strerror(errno));
        netpacket_destroy(npacket);
        return false;
    }

    netpacket_destroy(npacket);
    return true;
}

/* Create event and receive descriptor, call callback function after trigger */
bool netpool_desc(netpool* npool, int* cnt, int** perrorfds)
{
    uint16 event = 0;
    uint16 revent = 0;
    int    iret = 0;
    int    index = 0;

    if (NULL == npool)
    {
        return false;
    }

    /* Add valid descriptors to listen queue */
    *cnt = 0;
    *perrorfds = npool->errorfds;
    npool->ops->reset(npool->base);
    for (index = 0; index < npool->fdcnt; index++)
    {
        if (NULL == npool->fds[index])
        {
            npool->pos[index] = -1;
            continue;
        }

        /* In state of closing after sending, no longer read data */
        event = 0;
        if (NETPOOLENTRY_STAT_CLOSEUTILWPACKETNULL != npool->fds[index]->stat)
        {
            event = POLLIN;
        }
        if (false == queue_isnull(npool->fds[index]->wpackets))
        {
            event |= POLLOUT;
        }
        npool->pos[index] = npool->ops->add(npool->base, npool->fds[index]->fd, event);
    }

    /* Call iomp port */
    iret = npool->ops->iomp(npool->base);
    if (-1 == iret)
    {
        /* Check if error is caused by signal, if so continue monitoring */
        if (errno == EINTR)
        {
            return true;
        }
        elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
        return false;
    }

    /* No event triggered, return */
    if (0 == iret)
    {
        return true;
    }

    for (index = 0; index < npool->fdcnt; index++)
    {
        revent = 0;
        if (-1 == npool->pos[index])
        {
            continue;
        }

        /* Get triggered event */
        revent = npool->ops->getevent(npool->base, npool->pos[index]);
        if (0 == revent)
        {
            /* No event triggered */
            continue;
        }

        if (POLLIN == (revent & POLLIN))
        {
            /* Receive data */
            if (false == netpool_recv(npool->fds[index]))
            {
                elog(RLOG_WARNING, "net pool recv error");
                npool->errorfds[*cnt] = npool->fds[index]->fd;
                (*cnt)++;
                netpoolentry_destroy(npool->fds[index]);
                npool->fds[index] = NULL;
                continue;
            }
        }

        if (POLLOUT == (revent & POLLOUT))
        {
            /* Send data */
            if (false == netpool_send(npool->fds[index]))
            {
                elog(RLOG_WARNING, "net pool send error");
                npool->errorfds[*cnt] = npool->fds[index]->fd;
                (*cnt)++;
                netpoolentry_destroy(npool->fds[index]);
                npool->fds[index] = NULL;
                continue;
            }
        }

        if (POLLHUP == revent || POLLERR == revent)
        {
            elog(RLOG_WARNING, "iomp pollhup/pollerr error, %s", strerror(errno));
            npool->errorfds[*cnt] = npool->fds[index]->fd;
            (*cnt)++;
            netpoolentry_destroy(npool->fds[index]);
            npool->fds[index] = NULL;
            continue;
        }
    }

    return true;
}

/* Destroy */
void netpool_destroy(netpool* npool)
{
    int index = 0;
    if (NULL == npool)
    {
        return;
    }

    if (NULL != npool->fds)
    {
        for (index = 0; index < npool->fdmax; index++)
        {
            if (NULL == npool->fds[index])
            {
                continue;
            }
            netpoolentry_destroy(npool->fds[index]);
            npool->fds[index] = NULL;
        }
        rfree(npool->fds);
        npool->fds = NULL;
    }

    if (NULL != npool->pos)
    {
        rfree(npool->pos);
        npool->pos = NULL;
    }

    if (NULL != npool->errorfds)
    {
        rfree(npool->errorfds);
        npool->errorfds = NULL;
    }

    if (NULL != npool->base)
    {
        npool->ops->free(npool->base);
        npool->base = NULL;
    }
    rfree(npool);
}
