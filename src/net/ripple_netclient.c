#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/list/list_func.h"
#include "utils/guc/guc.h"
#include "utils/uuid/ripple_uuid.h"
#include "queue/ripple_queue.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"


/* 重置状态、超时时间、关闭描述符、清理packets内存、设置 iompbase和iompops等 */
void ripple_netclient_reset(ripple_netclient* netclient)
{
    netclient->protocoltype = RIPPLE_NETCLIENT_PROTOCOLTYPE_IPTCP;
    netclient->hbtimeout = 0;
    netclient->timeout = 0;
    netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
    if (netclient->fd != -1)
    {
        FileClose(netclient->fd);
        netclient->fd = -1;
    }
    netclient->pos = 0;
    netclient->ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);
    netclient->ops->reset(netclient->base);
    netclient->base->timeout = RIPPLE_NET_POLLTIMEOUT;
    ripple_queue_destroy(netclient->rpackets, ripple_netpacket_destroyvoid);
    netclient->rpackets = ripple_queue_init();
    ripple_queue_destroy(netclient->wpackets, ripple_netpacket_destroyvoid);
    netclient->wpackets = ripple_queue_init();
    netclient->callback = ripple_netclient_packets_handler;
}

void ripple_netclient_setprotocoltype(ripple_netclient* netclient, ripple_netclient_protocoltype protocoltype)
{
    netclient->protocoltype = protocoltype;
}


/* 设置netclient timeout */
void ripple_netclient_settimeout(ripple_netclient* netclient, int timeout)
{
    if (NULL == netclient)
    {
        return;
    }
    
    netclient->timeout = timeout;
    return;
}

/* 设置netclient hbtimeout*/
void ripple_netclient_sethbtimeout(ripple_netclient* netclient, int hbtimeout)
{
    if (NULL == netclient)
    {
        return;
    }
    
    netclient->hbtimeout = hbtimeout;
    return;
}

/* 设置netclient svrhost */
void ripple_netclient_setsvrhost(ripple_netclient* netclient, char* host)
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

/* 设置netclient svrport */
void ripple_netclient_setsvrport(ripple_netclient* netclient, char* port)
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

/* 连接服务端 */
bool ripple_netclient_conn(ripple_netclient* netclient)
{
    bool bret = false;
    int iret = 0;
    int yes = 1;
    int domain = AF_INET;
    int addrlen = 0;
    int keep_alive = 0;
    int idle = 0;
    int interval = 0;
    int count = 0;
    int timeout = 0;
    struct sockaddr* connaddr = NULL;
    struct sockaddr_in addrin;
    struct sockaddr_un addrun;
    char unixdoamin[512] = { 0 };

    keep_alive = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TCP_KEEPALIVE);
    idle = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TCP_KEEPALIVES_IDLE);
    interval = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TCP_KEEPALIVES_INTERVAL);
    count = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TCP_KEEPALIVES_COUNT);
    timeout = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TCP_USER_TIMEOUT);

    /* 获取host对应的地址 */
    rmemset1(&addrin, 0, 0, sizeof(struct sockaddr_in));

    if (RIPPLE_NETCLIENT_PROTOCOLTYPE_IPTCP == netclient->protocoltype)
    {
        bret = ripple_host2sockaddr(&addrin,
                                    netclient->svrhost,
                                    netclient->svrport,
                                    domain,
                                    SOCK_STREAM,
                                    IPPROTO_TCP,
                                    1);
        if (false == bret)
        {
            elog(RLOG_WARNING, "can not get addr info, %s", strerror(errno));
            return false;
        }
        connaddr = (struct sockaddr*)&addrin;
        addrlen = sizeof(struct sockaddr_in);
    }
    else if (RIPPLE_NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN == netclient->protocoltype)
    {
        domain = AF_LOCAL;
        snprintf(unixdoamin, 512, "%s/%s%s", RMANAGER_UNIXDOMAINDIR, RMANAGER_UNIXDOMAINPREFIX, netclient->svrport);
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

    /* 创建TCP描述符 */
    netclient->fd = ripple_socket(domain, SOCK_STREAM, 0);
    if (-1 == netclient->fd)
    {
        return false;
    }

    /* 禁用 TCP_NODELAY */
    ripple_setsockopt(netclient->fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int));

    /* 设置为 非阻塞模式 */
    ripple_setunblock(netclient->fd);

    if (keep_alive)
    {
        ripple_setsockopt(netclient->fd, SOL_SOCKET, SO_KEEPALIVE,
                                        (char *) &keep_alive, sizeof(keep_alive));

        ripple_setsockopt(netclient->fd, IPPROTO_TCP, TCP_KEEPIDLE,
                    (char *) &idle, sizeof(idle));

        ripple_setsockopt(netclient->fd, IPPROTO_TCP, TCP_KEEPINTVL,
                    (char *) &interval, sizeof(interval));

        ripple_setsockopt(netclient->fd, IPPROTO_TCP, TCP_KEEPCNT,
                    (char *) &count, sizeof(count));

        ripple_setsockopt(netclient->fd, IPPROTO_TCP, TCP_USER_TIMEOUT,
                    (char *) &timeout, sizeof(timeout));
    }

    /* 连接目标端 */
    iret = ripple_connect(netclient->fd, (struct sockaddr*)connaddr, addrlen);
    if (-1 == iret)
    {
        /* 查看当前的状态是否为连接中 */
        if(errno == EINPROGRESS)
        {
            netclient->status = RIPPLE_NETCLIENTCONN_STATUS_INPROCESS;
            return true;
        }

        netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
        elog(RLOG_WARNING, "connect error, %s", strerror(errno));
        FileClose(netclient->fd);
        netclient->fd = -1;
        return false;
    }

    netclient->status = RIPPLE_NETCLIENTCONN_STATUS_INPROCESS;
    return true;
}

/* 用于查看是否连接上目标端, 当状态为 INPROCESS 时,检测是否可以转化状态为 CONNECTED */
bool ripple_netclient_isconnect(ripple_netclient* netclient)
{
    uint16 flag = POLLOUT;
    int iret = 0;
    int value = 0;
    int timeout = 0;
    socklen_t len = sizeof(int);

    while(1)
    {

        if(true == g_gotsigterm)
        {
            return false;
        }

        netclient->ops->reset(netclient->base);
        netclient->pos = netclient->ops->add(netclient->base, netclient->fd, flag);

        timeout = netclient->base->timeout;
        netclient->base->timeout = 10000;
        iret = netclient->ops->iomp(netclient->base);
        if(-1 == iret)
        {
            /* 查看错误是否为信号引起的，若为信号引起那么继续监测 */
            if(errno == EINTR)
            {
                continue;
            }
            elog(RLOG_WARNING, "can't connect");
            FileClose(netclient->fd);
            netclient->fd = -1;
            return false;
        }

        /* 检测状态，查看是否连接正常 */
        if(0 == iret)
        {
            /* 超时了，关闭描述符 */
            elog(RLOG_WARNING, "connect timeout");
            FileClose(netclient->fd);
            netclient->fd = -1;
            return false;
        }
        netclient->base->timeout = timeout;

        iret = ripple_getsockopt(netclient->fd, SOL_SOCKET, SO_ERROR, &value, &len);
        if(-1 == iret)
        {
            /* 关闭连接 */
            elog(RLOG_WARNING, "getsockopt error, %s", strerror(errno));
            FileClose(netclient->fd);
            netclient->fd = -1;
            return false;
        }

        if(0 != value)
        {
            elog(RLOG_WARNING, "getsockopt value error, %s", strerror(value));
            FileClose(netclient->fd);
            netclient->fd = -1;
            return false;
        }

        break;
    }

    netclient->status = RIPPLE_NETCLIENTCONN_STATUS_CONNECTED;
    return true;
}

/* 
 * 尝试连接服务端
 *  conn
 *  sleep(1)
 *  is conn ?
 * 
 *  true    连接上
 *  false   未连接上
 */
bool ripple_netclient_tryconn(ripple_netclient* netclient)
{
    /* 连接目标端 */
    if (false == ripple_netclient_conn(netclient))
    {
        netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
        return false;
    }

    /* 查看当前的状态是否为 INPROCESS */
    if (RIPPLE_NETCLIENTCONN_STATUS_INPROCESS == netclient->status)
    {
        /* 查看描述符的状态，有错误那么重置状态 */
        if (false == ripple_netclient_isconnect(netclient))
        {
            /* 连接失败 */
            elog(RLOG_WARNING, "connect timeout error, %s", strerror(errno));
            netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
            return false;
        }
    }
    return true;
}

/* 接收数据 */
static bool ripple_netclient_recv(ripple_netclient* netclient)
{
    bool bhead                  = false;
    int rlen                    = 0;
    int readlen                 = 0;
    int msglen                  = 0;
    uint8* cptr                 = NULL;
    ripple_netpacket* npacket   = NULL;
    uint8 hdr[8]                 = { 0 };

    if (NULL == netclient)
    {
        return false;
    }

    /*
     * 1、在读队列中获取最后一包
     *  1.1 完整的 packet, 申请一个新的 packet 挂载到 读队列 中
     *  1.2 不完整的 packet, 使用该 packet
     * 2、队列为空
     *  申请一个新 packet 挂载到 读队列中
     */
    if (false == ripple_queue_isnull(netclient->rpackets))
    {
        npacket = (ripple_netpacket*)netclient->rpackets->tail->data;
        if (npacket->offset == npacket->used)
        {
            npacket = NULL;
        }
    }

    if (NULL == npacket)
    {
        npacket = ripple_netpacket_init();
        if (NULL == npacket)
        {
            elog(RLOG_WARNING, "net client read out of memory");
            return false;
        }
        bhead = true;
        rlen = readlen = 8;
        cptr = hdr;
        if (false == ripple_queue_put(netclient->rpackets, npacket))
        {
            elog(RLOG_WARNING, "net client add packet to read queue error");
            ripple_netpacket_destroy(npacket);
            return false;
        }
    }
    else
    {
        /* 读取数据 */
        rlen = readlen = npacket->used - npacket->offset;
        cptr = npacket->data + npacket->offset;
    }

    if (false == ripple_net_read(netclient->fd, cptr, &rlen))
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
    npacket->data = ripple_netpacket_data_init(msglen);
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


/* 发送数据 */
static bool ripple_netclient_send(ripple_netclient* netclient)
{
    /* 
     * 发送数据
     *  1、在队列中获取待发送的包
     *  2、发送数据
     */
    int timeout                 = 0;
    int sendlen                 = 0;
    uint8* cptr                 = NULL;
    ripple_netpacket* npacket   = NULL;
    if (NULL == netclient)
    {
        return true;
    }

    if (true == ripple_queue_isnull(netclient->wpackets))
    {
        return true;
    }

    npacket = ripple_queue_get(netclient->wpackets, &timeout);
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "net client send get packet from queue error");
        return false;
    }

    cptr = npacket->data + npacket->offset;
    sendlen = npacket->used - npacket->offset;
    if (false == ripple_net_write(netclient->fd, cptr, sendlen))
    {
        elog(RLOG_WARNING, "net client send packet error");
        ripple_netpacket_destroy(npacket);
        return false;
    }

    ripple_netpacket_destroy(npacket);
    return true;
}


/* 创建连接并发送数据 */
bool ripple_netclient_senddata(ripple_netclient_protocoltype ptype,
                               char* host,
                               char* port,
                               uint8* data,
                               int datalen)
{
    bool bret                               = true;
    int intervaltimeout                     = 0;
    int interval                            = 10000;
    ripple_netpacket* npacket               = NULL;
    ripple_netclient netclient              = { 0 };

    netclient.fd = -1;
    netclient.base = NULL;
    netclient.rpackets = NULL;
    netclient.wpackets = NULL;

    if (NULL != host)
    {
        sprintf(netclient.svrhost, "%s", host);
    }
    sprintf(netclient.svrport, "%s", port);

    /* 设置使用的网络模型 */
    netclient.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);

    /* 申请 base 信息，用于后续的描述符处理 */
    if (false == netclient.ops->create(&netclient.base))
    {
        bret = false;
        goto ripple_netclient_senddata_done;
    }

    /* 设置类型 */
    ripple_netclient_setprotocoltype(&netclient, ptype);
    ripple_netclient_sethbtimeout(&netclient, RIPPLE_NET_HBTIME);
    ripple_netclient_settimeout(&netclient, 0);

    netclient.base->timeout = RIPPLE_NET_POLLTIMEOUT;
    netclient.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
    netclient.wpackets = ripple_queue_init();
    if (NULL == netclient.wpackets)
    {
        bret = false;
        goto ripple_netclient_senddata_done;
    }

    /* 组建包 */
    netclient.rpackets = ripple_queue_init();
    if (NULL == netclient.rpackets)
    {
        bret = false;
        goto ripple_netclient_senddata_done;
    }
    netclient.callback = NULL;

    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        bret = false;
        goto ripple_netclient_senddata_done;
    }
    npacket->data = ripple_netpacket_data_init(datalen);
    if (NULL == npacket->data)
    {
        bret = false;
        goto ripple_netclient_senddata_done;
    }
    rmemcpy0(npacket->data, 0, data, datalen);
    npacket->used = datalen;
    npacket->offset = 0;

    /* 连接 xmanager */
    if (false == ripple_netclient_tryconn(&netclient))
    {
        bret = false;
        elog(RLOG_WARNING, "can not connect server");
        goto ripple_netclient_senddata_done;
    }

    if (RIPPLE_NETCLIENTCONN_STATUS_CONNECTED != netclient.status)
    {
        bret = false;
        elog(RLOG_WARNING, "connect server error");
        goto ripple_netclient_senddata_done;
    }

    if (false == ripple_queue_put(netclient.wpackets, npacket))
    {
        bret = false;
        goto ripple_netclient_senddata_done;
    }
    npacket = NULL;

    while (intervaltimeout < interval)
    {
        if (false == ripple_netclient_desc2(&netclient))
        {
            bret = false;
            elog(RLOG_WARNING, "metric capture iomp desc error");
            goto ripple_netclient_senddata_done;
        }

        if (true == ripple_queue_isnull(netclient.wpackets))
        {
            sleep(1);
            goto ripple_netclient_senddata_done;
        }
        intervaltimeout += RIPPLE_NET_POLLTIMEOUT;
    }

    elog(RLOG_WARNING, "send data to server timeout");
    bret = false;
ripple_netclient_senddata_done:
    if (NULL != npacket)
    {
        ripple_netpacket_destroy(npacket);
        npacket = NULL;
    }
    ripple_netclient_destroy(&netclient);
    return bret;
}

/* 创建监听事件并等待事件触发,处理触发的事件 */
bool ripple_netclient_desc(ripple_netclient* netclient)
{
    int iret = 0;
    int event = 0;                                  /* 事件类型 */
    int revent = 0;                                 /* 触发的事件 */

    /* 消息中记录的数据长度,默认设置为头长度 */
    uint32 msglen = RIPPLE_NETMSG_TYPE_HDR_SIZE;

    uint8 head[RIPPLE_NETMSG_TYPE_HDR_SIZE] = { 0 };
    uint8* ruptr = NULL;
    ripple_netpacket* netpacket = NULL;

    /* FOR DEBUG BEGIN */
    uint8* uptr = NULL;
    uint32 debugmsgtype = 0;
    uint32 debugmsglen = 0;

    /* FOR DEBUG END */

    /* 获取数据 */
    /* 
    * 1、创建监听事件
    * 2、检测监听事件是否触发
    * 3、根据不同的协议类型走不同的处理逻辑
    */
    /* 重置事件监听 */
    netclient->ops->reset(netclient->base);
    event |= POLLIN;

    if (false == ripple_netclient_wpacketisnull(netclient))
    {
        /* 重置事件监听 */
        event |= POLLOUT;
    }

    /* 添加监听事件 */
    netclient->pos = netclient->ops->add(netclient->base, netclient->fd, event);

    /* 调用iomp端口 */
    iret = netclient->ops->iomp(netclient->base);
    if(-1 == iret)
    {
        if(errno == EINTR)
        {
            return true;
        }
        return false;
    }

    if(0 == iret)
    {
        /* 超时了, 累加hbtime和timeout 那么继续 */
        netclient->timeout += netclient->base->timeout;
        netclient->hbtimeout += netclient->base->timeout;
        return true;
    }

    /* 有消息触发，那么看看触发的事件类型 */
    revent = netclient->ops->getevent(netclient->base, netclient->pos);

    /* 是否有数据需要读 */
    if(POLLIN == (revent&POLLIN))
    {
        /* 读取数据并处理 */
        /* 重置计数器 */
        netclient->timeout = 0;

        if (ripple_netclient_rpacketisnull(netclient))
        {
            uint32 msgtype = 0;
            /* 取所有数据 */
            ruptr = head;
            if(false == ripple_net_read(netclient->fd, ruptr, (int*)&msglen))
            {
                /* 读取数据失败,退出 */
                if(0 == msglen)
                {
                    elog(RLOG_WARNING, "close sock, %s", strerror(errno));
                }
                else
                {
                    elog(RLOG_WARNING, "read error, errno:%d, %s", errno, strerror(errno));
                }
                return false;
            }

            /* 换算长度并获取 */
            msgtype = RIPPLE_CONCAT(get, 32bit)(&ruptr);
            msglen = RIPPLE_CONCAT(get, 32bit)(&ruptr);

            elog(RLOG_DEBUG, "type: %u, len: %u", msgtype, msglen);

            netpacket = ripple_netpacket_init();
            netpacket->max = RIPPLE_MAXALIGN(msglen);
            netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
            netpacket->used = msglen;
            netpacket->data = rmalloc0(msglen);
            if (NULL == netpacket->data)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
            }
            rmemset0(netpacket->data, 0, '\0', msglen);

            rmemcpy0(netpacket->data, 0, head, RIPPLE_NETMSG_TYPE_HDR_SIZE);

            msglen -= RIPPLE_NETMSG_TYPE_HDR_SIZE;

            if (0 != msglen)
            {
                ruptr = netpacket->data + RIPPLE_NETMSG_TYPE_HDR_SIZE;

                /* 取所有数据 */
                if(false == ripple_net_read(netclient->fd, ruptr, (int*)&msglen))
                {
                    /* 读取数据失败,退出 */
                    if(0 == msglen)
                    {
                        elog(RLOG_WARNING, "close sock");
                    }
                    else
                    {
                        elog(RLOG_WARNING, "read error, %s", strerror(errno));
                    }
                    ripple_netpacket_destroy(netpacket);
                    return false;
                }
                
            }
            netclient->callback(netclient, netpacket);
            ripple_netpacket_destroy(netpacket);
        }
    }

    if(POLLOUT == (POLLOUT&revent))
    {
        /* 查看是否触发 */
        /* 查看是否有数据需要写 */

        netpacket = ripple_queue_get(netclient->wpackets, NULL);
        if (NULL == netpacket)
        {
            return true;
        }

        /* 发送数据 */
        /* FOR DEBUG BEGIN */
        uptr = netpacket->data;
        debugmsgtype = get32bit(&uptr);
        debugmsglen = get32bit(&uptr);
        elog(RLOG_DEBUG, "send msgtype:%u, msglen:%u", debugmsgtype, debugmsglen);
        /* FOR DEBUG END */

        if(false == ripple_net_write(netclient->fd, netpacket->data, netpacket->used))
        {
            /* 发送数据失败，关闭连接 */
            elog(RLOG_WARNING, "write data 2 error, %s", strerror(errno));
            ripple_netpacket_destroy(netpacket);
            return false;
        }
        ripple_netpacket_destroy(netpacket);
        netpacket = NULL;
    }

    if((POLLIN != (revent&POLLIN)) && (POLLOUT != (POLLOUT&revent)))
    {
        elog(RLOG_WARNING, "unknown event, %d", revent);
        return false;
    }

    return true;

}

/*
 * 创建监听事件并等待事件触发, 接收或发送数据,仅接收或发送, 不做业务处理
*/
bool ripple_netclient_desc2(ripple_netclient* netclient)
{
    int iret = 0;
    int event = 0;                                  /* 事件类型 */
    int revent = 0;                                 /* 触发的事件 */

    /* 获取数据 */
    /* 
    * 1、创建监听事件
    * 2、检测监听事件是否触发
    * 3、根据不同的协议类型走不同的处理逻辑
    */
    /* 重置事件监听 */
    netclient->ops->reset(netclient->base);

    /* 创建监听事件 */
    event |= POLLIN;
    if (false == ripple_netclient_wpacketisnull(netclient))
    {
        /* 写事件 */
        event |= POLLOUT;
    }

    /* 添加监听事件 */
    netclient->pos = netclient->ops->add(netclient->base, netclient->fd, event);

    /* 调用iomp端口 */
    iret = netclient->ops->iomp(netclient->base);
    if(-1 == iret)
    {
        if(errno == EINTR)
        {
            return true;
        }
        elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
        return false;
    }

    if(0 == iret)
    {
        /* 超时了, 累加hbtime和timeout 那么继续 */
        netclient->hbtimeout += netclient->base->timeout;
        return true;
    }

    /* 有消息触发，那么看看触发的事件类型 */
    revent = netclient->ops->getevent(netclient->base, netclient->pos);
    if (0 == revent)
    {
        /* 没有事件触发 */
        return true;
    }

    if (POLLIN == (revent&POLLIN))
    {
        /* 接收数据 */
        if (false == ripple_netclient_recv(netclient))
        {
            elog(RLOG_WARNING, "net pool recv error");
            return false;
        }
    }

    if (POLLOUT == (revent&POLLOUT))
    {
        /* 发送数据 */
        if (false == ripple_netclient_send(netclient))
        {
            elog(RLOG_WARNING, "net pool send error");
            return false;
        }
    }

    if (POLLHUP == revent
        || POLLERR == revent)
    {
        elog(RLOG_WARNING, "iomp pollhup/pollerr error, %s", strerror(errno));
        return false;
    }

    return true;
}


bool ripple_netclient_addwpacket(ripple_netclient* netclient, void* packet)
{
    bool result = false;
    if (NULL != netclient->wpackets)
    {
        result = ripple_queue_put(netclient->wpackets, packet);
    }
    return result;
}

bool ripple_netclient_wpacketisnull(ripple_netclient* netclient)
{
    bool result = false;

    if(NULL == netclient->wpackets->head)
    {
        result = true;
    }

    return result;
}

bool ripple_netclient_rpacketisnull(ripple_netclient* netclient)
{
    bool result = false;

    if(NULL == netclient->rpackets->head)
    {
        result = true;
    }

    return result;
}

/*  回调函数处理接收到的信息 */
bool ripple_netclient_packets_handler(void* netclient, ripple_netpacket* netpacket)
{
    uint8* uptr = NULL;
    uint32 msgtype = RIPPLE_NETMSG_TYPE_NOP;
    ripple_netclient* clientstate = NULL;
    clientstate = (ripple_netclient*)netclient;

    uptr = netpacket->data;

    msgtype = RIPPLE_CONCAT(get, 32bit)(&uptr);

    if(false == ripple_netmsg((void*)clientstate, msgtype, netpacket->data))
    {
        clientstate->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
        return false;
    }

    return true;

}

/*
 * 清理描述符/队列
 * 设置连接状态为未连接
*/
void ripple_netclient_clear(ripple_netclient* netclient)
{
    if(NULL == netclient)
    {
        return;
    }

    if(-1 != netclient->fd)
    {
        elog(RLOG_WARNING, "netclient->fd:%d", netclient->fd);
        ripple_close(netclient->fd);
        netclient->fd = -1;
        netclient->status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
    }

    if (NULL != netclient->wpackets)
    {
        ripple_queue_clear(netclient->wpackets, ripple_netpacket_destroyvoid);
    }

    if (NULL != netclient->rpackets)
    {
        ripple_queue_clear(netclient->rpackets, ripple_netpacket_destroyvoid);
    }
}

/* 资源回收 */
void ripple_netclient_destroy(ripple_netclient* netclient)
{
    if(NULL == netclient)
    {
        return;
    }

    if(-1 != netclient->fd)
    {
        elog(RLOG_WARNING, "netclient->fd:%d", netclient->fd);
        ripple_close(netclient->fd);
        netclient->fd = -1;
    }

    if(NULL != netclient->base)
    {
        netclient->ops->free(netclient->base);
        netclient->base = NULL;
    }

    if (NULL != netclient->wpackets)
    {
        ripple_queue_destroy(netclient->wpackets, ripple_netpacket_destroyvoid);
    }

    if (NULL != netclient->rpackets)
    {
        ripple_queue_destroy(netclient->rpackets, ripple_netpacket_destroyvoid);
    }
}