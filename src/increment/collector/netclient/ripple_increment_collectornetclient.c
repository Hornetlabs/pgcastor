#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/list/list_func.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "storage/ripple_file_buffer.h"
#include "misc/ripple_misc_stat.h"
#include "threads/ripple_threads.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"
#include "metric/collector/ripple_metric_collector.h"

/* 空间申请 */
void* ripple_increment_collectornetclientalloc(rsocket sock)
{
    ripple_increment_collectornetclient* nodesvr = NULL;

    nodesvr = (ripple_increment_collectornetclient*)rmalloc0(sizeof(ripple_increment_collectornetclient));
    if(NULL == nodesvr)
    {
        return NULL;
    }
    rmemset0(nodesvr, 0, '\0', sizeof(ripple_increment_collectornetclient));
    nodesvr->sock = sock;
    return (void*)nodesvr;
}


void* ripple_collectornetclient_pumpincreament_alloc(void)
{
    ripple_collectornetclient_increment* increamentstate = NULL;

    increamentstate = (ripple_collectornetclient_increment*)rmalloc0(sizeof(ripple_collectornetclient_increment));
    if(NULL == increamentstate)
    {
        return NULL;
    }
    rmemset0(increamentstate, 0, '\0', sizeof(ripple_collectornetclient_increment));

    increamentstate->fileid = 0;
    increamentstate->blknum = 1;

    return (void*)increamentstate;
}

void* ripple_collectornetclient_pumprefreshsharding_alloc(void)
{
    char* url = NULL;
    ripple_collectornetclient_refreshsharding* refreshstate = NULL;

    refreshstate = (ripple_collectornetclient_refreshsharding*)rmalloc0(sizeof(ripple_collectornetclient_refreshsharding));
    if(NULL == refreshstate)
    {
        return NULL;
    }
    rmemset0(refreshstate, 0, '\0', sizeof(ripple_collectornetclient_refreshsharding));
    refreshstate->upload = false;
    rmemset1(refreshstate->refresh_path, 0, 0, MAXPGPATH);
    sprintf(refreshstate->refresh_path, "%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA), RIPPLE_REFRESH_REFRESH);
    
    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    /* 不配置url不上传文件 */
    if (!(url == NULL || url[0] == '\0'))
    {
        refreshstate->upload = true;
    }

    return (void*)refreshstate;
}

ripple_collectornetclient_onlinerefreshinc* ripple_collectornetclient_onlinerefreshinc_alloc(void)
{
    char* url = NULL;
    ripple_collectornetclient_onlinerefreshinc* increamentstate = NULL;

    increamentstate = (ripple_collectornetclient_onlinerefreshinc*)rmalloc0(sizeof(ripple_collectornetclient_onlinerefreshinc));
    if(NULL == increamentstate)
    {
        return NULL;
    }
    rmemset0(increamentstate, 0, '\0', sizeof(ripple_collectornetclient_onlinerefreshinc));
 
    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    /* 不配置url不上传文件 */
    if (!(url == NULL || url[0] == '\0'))
    {
        increamentstate->upload = true;
    }
    increamentstate->fileid = 0;
    increamentstate->fd = -1;
    increamentstate->blknum = 1;

    return increamentstate;
}

ripple_collectornetclient_onlinerefreshsharding* ripple_collectornetclient_onlinerefreshsharding_alloc(void)
{
    char* url = NULL;
    ripple_collectornetclient_onlinerefreshsharding* refreshstate = NULL;

    refreshstate = (ripple_collectornetclient_onlinerefreshsharding*)rmalloc0(sizeof(ripple_collectornetclient_onlinerefreshsharding));
    if(NULL == refreshstate)
    {
        return NULL;
    }
    rmemset0(refreshstate, 0, '\0', sizeof(ripple_collectornetclient_onlinerefreshsharding));
    rmemset1(refreshstate->refresh_path, 0, 0, MAXPGPATH);

    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    /* 不配置url不上传文件 */
    if (!(url == NULL || url[0] == '\0'))
    {
        refreshstate->upload = true;
    }

    return refreshstate;
}

ripple_collectornetclient_bigtxn* ripple_collectornetclient_bigtxn_alloc(void)
{
    char* url = NULL;
    ripple_collectornetclient_bigtxn* bigtxnstate = NULL;

    bigtxnstate = (ripple_collectornetclient_bigtxn*)rmalloc0(sizeof(ripple_collectornetclient_bigtxn));
    if(NULL == bigtxnstate)
    {
        return NULL;
    }
    rmemset0(bigtxnstate, 0, '\0', sizeof(ripple_collectornetclient_bigtxn));
    rmemset1(bigtxnstate->trailpath, 0, 0, MAXPGPATH);
    bigtxnstate->fd = -1;
    bigtxnstate->xid = InvalidFullTransactionId;
    bigtxnstate->blknum = 1;
    bigtxnstate->fileid = 0;

    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    /* 不配置url不上传文件 */
    if (!(url == NULL || url[0] == '\0'))
    {
        bigtxnstate->upload = true;
    }

    return bigtxnstate;
}

static void ripple_increment_collectornetclient_collector_getflushfileid(ripple_increment_collectornetclient_state* nodesvrstate, uint64* pfileid, uint64* cfileid)
{
    ripple_collectorincrementstate_privdatacallback* callback = NULL;

    /* 使用回调函数获取 collector中的filebuffers */
    callback = (ripple_collectorincrementstate_privdatacallback*)nodesvrstate->callback;

    callback->writestate_fileid_get(nodesvrstate->privdata, nodesvrstate->clientname ,pfileid, cfileid);
}


/* 释放 */
void ripple_increment_collectornetclient_free(void* privdata)
{
    ripple_increment_collectornetclient* nodesvr = NULL;

    nodesvr = (ripple_increment_collectornetclient*)privdata;

    if (NULL == nodesvr)
    {
        return;
    }

    rfree(nodesvr);
}

/* 清理数据 */
static void ripple_increment_collectornetclient_statefree(ripple_increment_collectornetclient_state* nodesvrstate)
{
    if(NULL == nodesvrstate)
    {
        return;
    }

    if(NULL != nodesvrstate->base)
    {
        nodesvrstate->ops->free(nodesvrstate->base);
        nodesvrstate->base = NULL;
    }

    if(NULL != nodesvrstate->callback)
    {
        rfree(nodesvrstate->callback);
        nodesvrstate->callback = NULL;
    }

    if(NULL != nodesvrstate->data)
    {
        rfree(nodesvrstate->data);
        nodesvrstate->data = NULL;
    }

    rfree(nodesvrstate);
}

/* 解析 trail 文件主函数 */
void* ripple_increment_collectornetclient_main(void *args)
{
    bool bralloc                                            = false;            /* 重新分配空间 */
    bool rheader                                            = true;             /* 读取头标识， false 代表此处读取的内容为body */
    int iret = 0;
    int event                                               = 0;

    /* 消息类型 */
    uint32 msgtype                                          = 0;

    /* 消息中记录的数据长度,默认设置为头长度 */
    uint32 msglen                                           = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    uint32 buffersize                                       = 0;
    uint64 pfileid                                          = 0;
    uint64 cfileid                                          = 0;

    uint8* uptr                                             = NULL;             /* 用于组装心跳 */
    uint8* ruptr                                            = NULL;             /* 读取数据的指针 */
    uint8* buffer                                           = NULL;             /* 待发送数据缓存区 */
    ripple_thrnode* thrnode                                 = NULL;
    ripple_increment_collectornetclient* nodesvr            = NULL;
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;

    uint8   hb[RIPPLE_NETMSG_TYPE_C2P_HB_SIZE] = { 0 };

    /* 获取节点信息 */
    thrnode = (ripple_thrnode*)args;
    nodesvr = (ripple_increment_collectornetclient*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment collector netsvr exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 申请空间 */
    nodesvrstate = (ripple_increment_collectornetclient_state*)rmalloc0(sizeof(ripple_increment_collectornetclient_state));
    if(NULL == nodesvrstate)
    {
        elog(RLOG_WARNING, "increment_collectornetclient_state malloc out of memory");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        goto ripple_increment_collectornetclient_main_error;
    }
    rmemset0(nodesvrstate, 0, '\0', sizeof(ripple_increment_collectornetclient_state));
    nodesvrstate->fd = nodesvr->sock;
    nodesvrstate->privdata = nodesvr->privdata;

    /* 初始化 */
    nodesvrstate->ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);

    /* 申请 base 信息，用于后续的描述符处理 */
    if(false == nodesvrstate->ops->create(&nodesvrstate->base))
    {
        elog(RLOG_WARNING, "RIPPLE_NETIOMP_TYPE_POLL create error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        goto ripple_increment_collectornetclient_main_error;
    }
    nodesvrstate->base->timeout = RIPPLE_NET_POLLTIMEOUT;

    /*
     * 预先使用 64K + RIPPLE_NETMSG_TYPE_P2C_DATA_FIXSIZE
     * 在当前的设计中, RIPPLE_NETMSG_TYPE_P2C_DATA_FIXSIZE + 64K(最大)
     */
    buffersize = (RIPPLE_FILE_BUFFER_SIZE + RIPPLE_NETMSG_TYPE_P2C_DATA_FIXSIZE);
    buffer = rmalloc0(buffersize);
    if(NULL == buffer)
    {
        elog(RLOG_WARNING, "out of memory");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        goto ripple_increment_collectornetclient_main_error;
    }
    rmemset0(buffer, 0, '\0', buffersize);

    uptr = hb;
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_HB);
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_HB_SIZE);

    /* 将数据读取到此缓存中 */
    ruptr = buffer;
    while(1)
    {
        /*
         * 1、获取数据，并根据数据的类型做处理
         * 2、查看是否需要发送 hb 包,并检测是否超时
         */
        /* 查看是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        /* 
         * 1、创建监听事件
         * 2、检测监听事件是否触发
         * 3、根据不同的协议类型走不同的处理逻辑
         */
        /* 重置事件监听 */
        nodesvrstate->ops->reset(nodesvrstate->base);
        event |= POLLIN;

        /* 添加监听事件 */
        nodesvrstate->pos = nodesvrstate->ops->add(nodesvrstate->base, nodesvrstate->fd, event);

        /* 调用iomp端口 */
        iret = nodesvrstate->ops->iomp(nodesvrstate->base);
        
        if(-1 == iret)
        {
            /* 查看错误是否为信号引起的，若为信号引起那么继续监测 */
            if(errno == EINTR)
            {
                continue;
            }
            elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            goto ripple_increment_collectornetclient_main_error;
        }

        nodesvrstate->timeout += RIPPLE_NET_POLLTIMEOUT;
        nodesvrstate->hbtimeout += RIPPLE_NET_POLLTIMEOUT;

        if(0 == iret)
        {
            /* 超时了, 那么继续 */
            goto ripple_increment_collectornetclient_main_timeout;
        }

        /* 有消息触发，那么看看触发的事件类型 */
        event = nodesvrstate->ops->getevent(nodesvrstate->base, nodesvrstate->pos);

        /*
         * 检测事件类型，当为 POLLUP 或者 POLLERROR 时，那么说明出现了错误，退出
         */
        if(POLLHUP == (event&POLLHUP)
            || POLLERR == (event&POLLERR))
        {
            elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            goto ripple_increment_collectornetclient_main_error;
        }

        /* 查看是否有数据需要写 */
        if(POLLOUT == (POLLOUT&event))
        {
            /* 写数据 */
            if(false == ripple_net_write(nodesvrstate->fd, hb, RIPPLE_NETMSG_TYPE_C2P_HB_SIZE))
            {
                /* 发送数据失败，关闭连接 */
                elog(RLOG_WARNING, "write RIPPLE_NETMSG_TYPE_C2P_HB error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_increment_collectornetclient_main_error;
            }
            nodesvrstate->hbtimeout = 0;
        }

        /* 是否有数据需要读 */
        if(POLLIN == (event&POLLIN))
        {
            /* 读取数据并处理 */
            /* 重置计数器 */
            nodesvrstate->timeout = 0;

            /* 先读取指定长度 */
            if(false == ripple_net_read(nodesvrstate->fd, ruptr, (int*)&msglen))
            {
                if(0 == msglen)
                {
                    /* 已经关闭 */
                    elog(RLOG_WARNING,
                         "pump closed sock, collectornetclient wille be exit,errno:%d, %s",
                         errno,
                         strerror(errno));
                }
                else
                {
                    /* 读取数据失败,退出 */
                    elog(RLOG_WARNING, "read error,msglen:%u, %s", msglen, strerror(errno));
                }
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_increment_collectornetclient_main_error;
            }

            if(true == rheader)
            {
                rheader = false;

                /* 换算长度并获取 */
                msgtype = RIPPLE_CONCAT(get, 32bit)(&ruptr);
                msglen = RIPPLE_CONCAT(get, 32bit)(&ruptr);

                elog(RLOG_DEBUG, "collector msgtype:%u, msglen:%u", 
                                        msgtype, msglen);
                /* 查看空间是否足够，不足那么重新分配 */
                while(buffersize < msglen)
                {
                    buffersize += RIPPLE_FILE_BUFFER_SIZE;
                    bralloc = true;
                }

                /* 检测长度，避免导致因为申请的内存过大而出现 core */
                if((RIPPLE_MB2BYTE(RIPPLE_FILE_BUFFER_MAXSIZE)) < buffersize)
                {
                    elog(RLOG_WARNING, 
                                        "pump 2 collector msglen too big, please check logic, msgtype:%u, msglen:%u, %d", 
                                        msgtype, msglen, buffersize);
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    goto ripple_increment_collectornetclient_main_error;
                }

                /* 重新分配内存 */
                if(true == bralloc)
                {
                    buffer = rrealloc0(buffer, buffersize);
                    if(NULL == buffer)
                    {
                        elog(RLOG_WARNING, "pump 2 collector msglen too big, please check logic");
                        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                        goto ripple_increment_collectornetclient_main_error;
                    }
                    bralloc = false;
                    ruptr = buffer;
                    ruptr += RIPPLE_NETMSG_TYPE_HDR_SIZE;
                }

                /* 再次循环读取数据 */
                msglen -= RIPPLE_NETMSG_TYPE_HDR_SIZE;

                if(0 == msglen)
                {
                    /* 证明没有body，那么走处理逻辑 */
                    if(false == ripple_netmsg(nodesvrstate, msgtype, buffer))
                    {
                        elog(RLOG_WARNING, "pump 2 collector ripple_netmsg error");
                        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                        goto ripple_increment_collectornetclient_main_error;
                    }

                    /* 重置读取的标识 */
                    rheader = true;
                    msglen = RIPPLE_NETMSG_TYPE_HDR_SIZE;
                    ruptr = buffer;
                }
            }
            else
            {
                /* 根据类型处理 */
                if(false == ripple_netmsg(nodesvrstate, msgtype, buffer))
                {
                    elog(RLOG_WARNING, "pump 2 collector ripple_netmsg error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    goto ripple_increment_collectornetclient_main_error;
                }

                /* 重置读取的标识 */
                rheader = true;
                msglen = RIPPLE_NETMSG_TYPE_HDR_SIZE;
                ruptr = buffer;
            }
        }
        else
        {
            nodesvrstate->timeout += RIPPLE_NET_POLLTIMEOUT;
        }

ripple_increment_collectornetclient_main_timeout:
        /* 清空事件 */
        event = 0;


        /* 查看计数器，计算是否需要发送心跳包 */
        if(RIPPLE_NET_COLLECTOR_HBTIME == nodesvrstate->hbtimeout)
        {
            /* 设置事件 */
            event |= POLLOUT;

            /* 写数据, 心跳包 */
            uptr = (hb + RIPPLE_NETMSG_TYPE_HDR_SIZE);
            /* 
             * TODO 
             *  获取写入到 trail 文件中对应的 pump 端的 Trail 文件编号/文件偏移
             *  collector 写入的 trail 文件编号
             */
            if (RIPPLE_NETIDENTITY_TYPE_PUMPINCREAMENT == nodesvrstate->type)
            {
                RIPPLE_CONCAT(put, 8bit)(&uptr, RIPPLE_NETIDENTITY_TYPE_PUMPINCREAMENT);
                ripple_increment_collectornetclient_collector_getflushfileid(nodesvrstate, &pfileid, &cfileid);
                elog(RLOG_DEBUG, "pfileid:%lu, cfileid:%lu", pfileid, cfileid);

                RIPPLE_CONCAT(put, 64bit)(&uptr, pfileid);
                RIPPLE_CONCAT(put, 64bit)(&uptr, cfileid);
            }
            else
            {
                RIPPLE_CONCAT(put, 8bit)(&uptr, RIPPLE_NETIDENTITY_TYPE_PUMPREFRESHARDING);
                RIPPLE_CONCAT(put, 64bit)(&uptr, 0);
                RIPPLE_CONCAT(put, 64bit)(&uptr, 0);
            }
            
            nodesvrstate->hbtimeout = 0;
        }
    }

ripple_increment_collectornetclient_main_error:

    elog(RLOG_INFO, "nodesvr exit jobname: %s", nodesvrstate->clientname);
    if(-1 != nodesvrstate->fd)
    {
        ripple_close(nodesvrstate->fd);
        nodesvrstate->fd = -1;
    }

    if(NULL != buffer)
    {
        rfree(buffer);
    }

    if(NULL != nodesvrstate)
    {
        ripple_increment_collectornetclient_statefree(nodesvrstate);
    }
    ripple_pthread_exit(NULL);
    return NULL;
}
