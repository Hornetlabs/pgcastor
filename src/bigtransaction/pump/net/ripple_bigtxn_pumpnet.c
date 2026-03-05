#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"
#include "bigtransaction/pump/net/ripple_bigtxn_pumpnet.h"

/* 网络故障设置大事务管理线程状态为reset */
static void ripple_bigtxn_pumpnet_mgrstat_setreset(ripple_increment_pumpnetstate* clientstate)
{
    /* 设置大事务管理线程状态为reset */
    clientstate->callback.bigtxn_mgrstat_setreset(clientstate->privdata);
    return ;
}

/* 检查大事务管理线程状态 */
static bool ripple_bigtxn_pumpnet_mgrstat_check(ripple_increment_pumpnetstate* clientstate)
{
    return clientstate->callback.bigtxn_mgrstat_isreset(clientstate->privdata);
}

/* 创建 data 数据包挂载到 wpackets 上 */
static void ripple_bigtxn_pumpnet_wpacketsadd_data(ripple_netclient* netclient, ripple_file_buffer* fbuffer)
{
    uint32 wmsglen = 0;
    uint8* wuptr = NULL;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_netpacket* netpacket = NULL;

    finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

    wmsglen = RIPPLE_NETMSG_TYPE_P2C_BIGTXN_DATA_FIXSIZE;
    wmsglen += fbuffer->start;

    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(wmsglen);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = wmsglen;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 组装数据并发送 */
    wuptr = netpacket->data;

    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_BIGTXN_DATA);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, wmsglen);

    /* pump fileid */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, finfo->fileid);

    /* collector offset */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE));
    elog(RLOG_DEBUG, "finfo->fileid:%lu, finfo->blknum:%lu, fbuffer->start:%lu",
                        finfo->fileid,
                        finfo->blknum,
                        fbuffer->start);

    /* 写入数据 */
    rmemcpy0(netpacket->data, RIPPLE_NETMSG_TYPE_P2C_BIGTXN_DATA_FIXSIZE, fbuffer->data, fbuffer->start);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

/* 创建 p2c identity 数据包 */
static bool ripple_bigtxn_pumpnet_p2cidentity(ripple_netclient* netclient)
{
    uint32 namelen = 0;
    uint32 identity_size = 0;
    uint8* wuptr = NULL;
    char* jobname = NULL;
    ripple_netpacket* netpacket = NULL;

    jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);
    if(NULL != jobname && '\0' != jobname[0])
    {
        namelen = strlen(jobname);
    }

    identity_size = (RIPPLE_NETMSG_TYPE_P2C_IDENTITY_SIZE + namelen);
    netpacket = ripple_netpacket_init();
    if (NULL == netpacket)
    {
        elog(RLOG_WARNING, "init big transaction identity packet error");
        goto ripple_bigtxn_pumpnet_p2cidentity_error;
    }
    netpacket->max = RIPPLE_MAXALIGN(identity_size);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = identity_size;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);
    if (NULL == netpacket->data)
    {
        elog(RLOG_WARNING, "init big transaction data error");
        goto ripple_bigtxn_pumpnet_p2cidentity_error;
    }

    /* 发送请求，获取标识 */
    wuptr = netpacket->data;
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_IDENTITY);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, identity_size);
    RIPPLE_CONCAT(put, 8bit)(&wuptr, RIPPLE_NETIDENTITY_TYPE_BIGTRANSACTION);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, namelen);
    if (0 != namelen)
    {
        rmemcpy1(wuptr, 0, jobname, strlen(jobname));
    }

    if (false == ripple_netclient_addwpacket(netclient, (void*)netpacket))
    {
        elog(RLOG_WARNING, "big transaction add identity packet error");
        goto ripple_bigtxn_pumpnet_p2cidentity_error;
    }
    return true;

ripple_bigtxn_pumpnet_p2cidentity_error:
    ripple_netpacket_destroy(netpacket);
    return false;
}

/* 创建 bigtxn begin 数据包挂载到 wpackets 上 */
static void ripple_bigtxn_pumpnet_bigtxn_begin(ripple_netclient* netclient, FullTransactionId xid)
{
    uint32 wmsglen = 0;
    uint8* wuptr = NULL;
    ripple_netpacket* netpacket = NULL;

    wmsglen = RIPPLE_NETMSG_TYPE_P2C_BIGTXN_BEGIN_FIXSIZE;

    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(wmsglen);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = wmsglen;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 组装数据并发送 */
    wuptr = netpacket->data;

    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_BIGTXN_BEGIN);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, wmsglen);

    /* xid */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, xid);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

/* 创建 bigtxn end 数据包挂载到 wpackets 上 */
static void ripple_bigtxn_pumpnet_bigtxn_end(ripple_netclient* netclient)
{
    uint32 wmsglen = 0;
    uint8* wuptr = NULL;
    ripple_netpacket* netpacket = NULL;

    wmsglen = RIPPLE_NETMSG_TYPE_P2C_BIGTXN_END_FIXSIZE;

    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(wmsglen);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = wmsglen;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 组装数据并发送 */
    wuptr = netpacket->data;

    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_BIGTXN_END);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, wmsglen);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

/* 网络客户端 */
void *ripple_bigtxn_pumpnet_main(void *args)
{
    bool end_bigtxn                                 = false;
    int timeout                                     = 0;
    ripple_thrnode* thrnode                         = NULL;
    ripple_file_buffer* fbuffer                     = NULL;
    ripple_netclient* netclient                     = NULL;
    ripple_bigtxn_pumpnet *pumpnet                  = NULL;
    ripple_increment_pumpnetstate* clientstate      = NULL;

    thrnode = (ripple_thrnode *)args;

    pumpnet = (ripple_bigtxn_pumpnet*)thrnode->data;

    clientstate = (ripple_increment_pumpnetstate*)pumpnet->clientstate;
    netclient = (ripple_netclient*)clientstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump bigtxn net stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while (clientstate->state == RIPPLE_INCREMENT_PUMPNETSTATE_STATE_RESET)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 连接服务端 */
        if(RIPPLE_NETCLIENTCONN_STATUS_NOP == clientstate->base.status)
        {
            /* 连接目标端 */
            if(false == ripple_netclient_tryconn(netclient))
            {
                elog(RLOG_INFO, "can not connect collector");
                sleep(1);
                continue;
            }
            elog(RLOG_INFO, "connect collecotr success");
        }

        /* 创建 p2cidentity 数据包挂载到 wpackets 上 */
        if (false == ripple_bigtxn_pumpnet_p2cidentity(netclient))
        {
            elog(RLOG_WARNING, "add big transaction identity packet error");
            sleep(1);
            continue;
        }

        /* 发送begin */
        ripple_bigtxn_pumpnet_bigtxn_begin(netclient, pumpnet->xid);

        while (true)
        {
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                goto ripple_bigtxn_pumpnet_main_done;
            }

            /* 消息处理 */
            if(false == ripple_netclient_desc(netclient))
            {
                clientstate->base.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
                clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_RESET;
                ripple_bigtxn_pumpnet_mgrstat_setreset(clientstate);
                ripple_netclient_reset(netclient);
                end_bigtxn = false;
                break;
            }

            if (true == ripple_bigtxn_pumpnet_mgrstat_check(clientstate))
            {
                usleep(500000);
                continue;
            }

            if (ripple_queue_isnull(netclient->wpackets) && true == end_bigtxn)
            {
                /* 队列为空表示数据已经发送出去了, end_onlinerefresh 用于标识最后一个块在队列中 */
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                goto ripple_bigtxn_pumpnet_main_done;
            }

            if (clientstate->state != RIPPLE_INCREMENT_PUMPNETSTATE_STATE_IDLE 
                && !ripple_netclient_wpacketisnull(netclient))
            {
                continue;
            }

            /* 获取数据 */
            fbuffer = ripple_file_buffer_waitflush_get(clientstate->txn2filebuffer, &timeout);
            if(NULL == fbuffer)
            {
                /* 没有超时 */
                if (RIPPLE_ERROR_TIMEOUT != timeout)
                {
                    /* 处理失败, 退出 */
                    elog(RLOG_WARNING, "get file buffer error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    goto ripple_bigtxn_pumpnet_main_done;
                }

                /* 超时没有获取到数据,继续等待获取 */
                clientstate->base.hbtimeout += 1000;
                clientstate->base.timeout += 1000;
            }
            else
            {
                /* 添加数据到 wpackets */
                ripple_bigtxn_pumpnet_wpacketsadd_data(netclient, fbuffer);
                if (RIPPLE_FILE_BUFFER_FLAG_BIGTXNEND ==( fbuffer->flag & RIPPLE_FILE_BUFFER_FLAG_BIGTXNEND))
                {
                    /* 有大事务结束标记发送 bigtxn end*/
                    end_bigtxn = true;
                    ripple_bigtxn_pumpnet_bigtxn_end(netclient);
                }

                ripple_file_buffer_free(clientstate->txn2filebuffer, fbuffer);
                clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_POLLOUT;
                clientstate->base.hbtimeout = 0;
            }

            /* 心跳包组装并发送 */
            if(RIPPLE_NET_PUMP_HBTIME <= clientstate->base.hbtimeout)
            {
                ripple_netclient_wpacketsadd_hb(netclient);
                clientstate->base.hbtimeout = 0;
            }
        }
    }
ripple_bigtxn_pumpnet_main_done:

    ripple_pthread_exit(NULL);
    return NULL;
}

ripple_bigtxn_pumpnet* ripple_bigtxn_pumpnet_init(void)
{
    ripple_bigtxn_pumpnet* netstate = NULL;

    netstate = (ripple_bigtxn_pumpnet*)rmalloc0(sizeof(ripple_bigtxn_pumpnet));
    if(NULL == netstate)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(netstate, 0, 0, sizeof(ripple_bigtxn_pumpnet));

    netstate->xid = InvalidFullTransactionId;
    netstate->clientstate = ripple_increment_pumpnet_init();

    return netstate;
}

/* 资源回收 */
void ripple_bigtxn_pumpnet_free(void *args)
{
    ripple_bigtxn_pumpnet* netstate = NULL;

    netstate = (ripple_bigtxn_pumpnet*)args;

    if (NULL == netstate)
    {
        return;
    }
    
    if (netstate->clientstate)
    {
        ripple_increment_pumpnet_destroy(netstate->clientstate);
    }

    rfree(netstate);

    return;

}
