#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netpool.h"

/*----------------net pool entry operatiion begin------------------*/
ripple_netpoolentry* ripple_netpoolentry_init(void)
{
    ripple_netpoolentry* npoolentry = NULL;

    npoolentry = rmalloc0(sizeof(ripple_netpoolentry));
    if (NULL == npoolentry)
    {
        elog(RLOG_WARNING, "net pool entry init error, out of memory");
        return NULL;
    }
    rmemset0(npoolentry, 0, '\0', sizeof(ripple_netpoolentry));

    npoolentry->fd = -1;
    npoolentry->stat = RIPPLE_NETPOOLENTRY_STAT_NOP;
    npoolentry->host = NULL;
    npoolentry->port = NULL;
    npoolentry->rpackets = ripple_queue_init();
    if (NULL == npoolentry->rpackets)
    {
        rfree(npoolentry);
        return NULL;
    }

    npoolentry->wpackets = ripple_queue_init();
    if (NULL == npoolentry->wpackets)
    {
        ripple_queue_destroy(npoolentry->wpackets, NULL);
        rfree(npoolentry);
        return NULL;
    }
    return npoolentry;
}

/* 描述符设置 */
void ripple_netpoolentry_setfd(ripple_netpoolentry* npoolentry, int fd)
{
    if (NULL == npoolentry)
    {
        return;
    }
    npoolentry->fd = fd;
    npoolentry->stat = RIPPLE_NETPOOLENTRY_STAT_OK;
}

/* 设置主机信息 */
bool ripple_netpoolentry_sethost(ripple_netpoolentry* npoolentry, char* host)
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

/* 设置端口信息 */
bool ripple_netpoolentry_setport(ripple_netpoolentry* npoolentry, char* port)
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

void ripple_netpoolentry_destroy(ripple_netpoolentry* npoolentry)
{
    if (NULL == npoolentry)
    {
        return;
    }

    if (-1 != npoolentry->fd)
    {
        ripple_close(npoolentry->fd);
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

    ripple_queue_destroy(npoolentry->rpackets, ripple_netpacket_destroyvoid);
    ripple_queue_destroy(npoolentry->wpackets, ripple_netpacket_destroyvoid);
    rfree(npoolentry);
}

/*----------------net pool entry operatiion   end------------------*/

ripple_netpool* ripple_netpool_init(void)
{
    int index = 0;
    ripple_netpool* npool = NULL;

    npool = rmalloc0(sizeof(ripple_netpool));
    if (NULL == npool)
    {
        elog(RLOG_WARNING, "net pool init error");
        return NULL;
    }
    rmemset0(npool, 0, '\0', sizeof(ripple_netpool));

    npool->fdmax = RIPPLE_NETPOOL_DEFAULTFDSIZE;
    npool->fds = rmalloc0(sizeof(void*)*npool->fdmax);
    if (NULL == npool->fds)
    {
        elog(RLOG_ERROR, "net server reset out of memory");
        return NULL;
    }
    rmemset0(npool->fds, 0, '\0', sizeof(void*)*npool->fdmax);

    npool->pos = rmalloc0(sizeof(int)*npool->fdmax);
    if (NULL == npool->pos)
    {
        elog(RLOG_ERROR, "net pool reset out of memory");
        return NULL;
    }
    rmemset0(npool->pos, 0, '\0', sizeof(int)*npool->fdmax);

    npool->errorfds = rmalloc0(sizeof(int)*npool->fdmax);
    if (NULL == npool->errorfds)
    {
        elog(RLOG_ERROR, "net pool reset out of memory");
        return NULL;
    }
    rmemset0(npool->errorfds, 0, '\0', sizeof(int)*npool->fdmax);

    for (index = 0; index < npool->fdmax; index++)
    {
        npool->fds[index] = NULL;
        npool->pos[index] = -1;
        npool->errorfds[index] = -1;
    }

    /* 申请 base 信息，用于后续的描述符处理 */
    npool->ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);
    if(false == npool->ops->create(&npool->base))
    {
        elog(RLOG_ERROR, "net pool reset RIPPLE_NETIOMP_TYPE_POLL create error");
        return NULL;
    }
    npool->ops->reset(npool->base);
    npool->base->timeout = RIPPLE_NET_POLLTIMEOUT;

    return npool;
}

/* 添加 */
bool ripple_netpool_add(ripple_netpool* npool, ripple_netpoolentry* entry)
{
    /*
     * 1、在原 fds 中查照未使用的槽
     * 2、未找到那么新分配空间
     */
    int index = 0;
    int fdmax = 0;
    int* pos = NULL;
    int* errorfds = NULL;
    ripple_netpoolentry** pentrys = NULL;

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
    pentrys = (ripple_netpoolentry**)rmalloc0(sizeof(void*)*fdmax);
    if (NULL == pentrys)
    {
        elog(RLOG_WARNING, "net pool add entry error, out of memory");
        return false;
    }
    rmemset0(pentrys, 0, '\0', sizeof(void*)*fdmax);
    
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

/* 删除 */
void ripple_netpool_del(ripple_netpool* npool, int fd)
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

        ripple_netpoolentry_destroy(npool->fds[index]);
        npool->fds[index] = NULL;
        npool->pos[index] = -1;
    }
}

/* 根据 fd 在 netpool 中获取 entry */
ripple_netpoolentry* ripple_netpool_getentrybyfd(ripple_netpool* npool, int fd)
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

/* 接收数据 */
static bool ripple_netpool_recv(ripple_netpoolentry* npoolentry)
{
    bool bhead                  = false;
    int rlen                    = 0;
    int readlen                 = 0;
    int msglen                  = 0;
    uint8* cptr                 = NULL;
    ripple_netpacket* npacket   = NULL;
    uint8 hdr[8]                 = { 0 };
    if (NULL == npoolentry)
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
    if (false == ripple_queue_isnull(npoolentry->rpackets))
    {
        npacket = (ripple_netpacket*)npoolentry->rpackets->tail->data;
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
            elog(RLOG_WARNING, "net pool read out of memory");
            return false;
        }
        bhead = true;
        rlen = readlen = 8;
        cptr = hdr;
        if (false == ripple_queue_put(npoolentry->rpackets, npacket))
        {
            elog(RLOG_WARNING, "net pool add packet to read queue error");
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

    if (false == ripple_net_read(npoolentry->fd, cptr, &rlen))
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
    npacket->data = ripple_netpacket_data_init(msglen);
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

/* 发送数据 */
static bool ripple_netpool_send(ripple_netpoolentry* npoolentry)
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
    if (NULL == npoolentry)
    {
        return true;
    }

    if (true == ripple_queue_isnull(npoolentry->wpackets))
    {
        return true;
    }

    npacket = ripple_queue_get(npoolentry->wpackets, &timeout);
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "net pool send get packet from queue error");
        return false;
    }

    cptr = npacket->data + npacket->offset;
    sendlen = npacket->used - npacket->offset;
    if (false == ripple_net_write(npoolentry->fd, cptr, sendlen))
    {
        elog(RLOG_WARNING, "net pool send packet error, %s", strerror(errno));
        ripple_netpacket_destroy(npacket);
        return false;
    }

    ripple_netpacket_destroy(npacket);
    return true;
}

/* 创建事件并接收描述符,等待触发后调用回掉函数处理 */
bool ripple_netpool_desc(ripple_netpool* npool, int* cnt, int** perrorfds)
{
    uint16 event            = 0;
    uint16 revent           = 0;
    int iret                = 0;
    int index               = 0;

    if(NULL == npool)
    {
        return false;
    }

    /* 将有效的描述符加入到监听队列中 */
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

        /* 处于发送完就关闭的状态, 那么不再读取数据 */
        event = 0;
        if (RIPPLE_NETPOOLENTRY_STAT_CLOSEUTILWPACKETNULL != npool->fds[index]->stat)
        {
            event = POLLIN;
        }
        if (false == ripple_queue_isnull(npool->fds[index]->wpackets))
        {
            event |= POLLOUT;
        }
        npool->pos[index] = npool->ops->add(npool->base, npool->fds[index]->fd, event);
    }

    /* 调用iomp端口 */
    iret = npool->ops->iomp(npool->base);
    if(-1 == iret)
    {
        /* 查看错误是否为信号引起的，若为信号引起那么继续监测 */
        if(errno == EINTR)
        {
            return true;
        }
        elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
        return false;
    }

    /* 没有事件触发, 返回 */
    if(0 == iret)
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

        /* 获取触发的事件 */
        revent = npool->ops->getevent(npool->base, npool->pos[index]);
        if (0 == revent)
        {
            /* 没有事件触发 */
            continue;
        }


        if (POLLIN == (revent&POLLIN))
        {
            /* 接收数据 */
            if (false == ripple_netpool_recv(npool->fds[index]))
            {
                elog(RLOG_WARNING, "net pool recv error");
                npool->errorfds[*cnt] = npool->fds[index]->fd;
                (*cnt)++;
                ripple_netpoolentry_destroy(npool->fds[index]);
                npool->fds[index] = NULL;
                continue;
            }
        }

        if (POLLOUT == (revent&POLLOUT))
        {
            /* 发送数据 */
            if (false == ripple_netpool_send(npool->fds[index]))
            {
                elog(RLOG_WARNING, "net pool send error");
                npool->errorfds[*cnt] = npool->fds[index]->fd;
                (*cnt)++;
                ripple_netpoolentry_destroy(npool->fds[index]);
                npool->fds[index] = NULL;
                continue;
            }
        }

        if (POLLHUP == revent
            || POLLERR == revent)
        {
            elog(RLOG_WARNING, "iomp pollhup/pollerr error, %s", strerror(errno));
            npool->errorfds[*cnt] = npool->fds[index]->fd;
            (*cnt)++;
            ripple_netpoolentry_destroy(npool->fds[index]);
            npool->fds[index] = NULL;
            continue;
        }
    }

    return true;
}

/* 销毁 */
void ripple_netpool_destroy(ripple_netpool* npool)
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
            ripple_netpoolentry_destroy(npool->fds[index]);
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

    if(NULL != npool->base)
    {
        npool->ops->free(npool->base);
        npool->base = NULL;
    }
    rfree(npool);
}

