#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "onlinerefresh/pump/netincrement/ripple_onlinerefresh_pumpnet.h"

/* 网络故障设置大事务管理线程状态为reset */
static void ripple_onlinerefreshn_pumpnet_mgrsetreset(ripple_onlinerefresh_pumpnetstate* clientstate)
{
    /* 设置大事务管理线程状态为reset */
    clientstate->callback.onlinerefresh_setreset(clientstate->privdata);
    return ;
}


/* 创建 p2cidentity 数据包挂载到 wpackets 上 */
static bool ripple_onlinerefresh_pumpnet_p2cidentity(ripple_netclient* netclient, uint8* no)
{
    uint32 namelen                  = 0;
    uint32 identity_size            = 0;
    uint8* wuptr                    = NULL;
    char* jobname                   = NULL;
    ripple_netpacket* netpacket     = NULL;

    jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);
    if (NULL != jobname && '\0' != jobname[0])
    {
        namelen = strlen(jobname);
    }

    identity_size = (RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_IDENTITY_SIZE + namelen);
    netpacket = ripple_netpacket_init();
    if (NULL == netpacket)
    {
        elog(RLOG_WARNING, "onlinefresh increment init identity packet error");
        goto ripple_onlinerefresh_pumpnet_p2cidentity_error;
    }
    netpacket->max = RIPPLE_MAXALIGN(identity_size);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = identity_size;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);
    if (NULL == netpacket->data)
    {
        elog(RLOG_WARNING, "onlinefresh increment init identity data error");
        goto ripple_onlinerefresh_pumpnet_p2cidentity_error;
    }

    /* 发送请求，获取标识 */
    wuptr = netpacket->data;
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_IDENTITY);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, identity_size);
    RIPPLE_CONCAT(put, 8bit)(&wuptr, RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_INC);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, namelen);
    if (0 != namelen)
    {
        rmemcpy1(wuptr, 0, jobname, strlen(jobname));
        wuptr += namelen;
    }
    rmemcpy1(wuptr, 0, no, RIPPLE_UUID_LEN);

    if (false == ripple_netclient_addwpacket(netclient, (void*)netpacket))
    {
        elog(RLOG_WARNING, "add onlinefresh increment identity packet error");
        goto ripple_onlinerefresh_pumpnet_p2cidentity_error;
    }

    return true;
ripple_onlinerefresh_pumpnet_p2cidentity_error:

    ripple_netpacket_destroy(netpacket);
    return false;
}

/* 创建 data 数据包挂载到 wpackets 上 */
static void ripple_onlinerefresh_pumpnetstate_wpacketsadd_data(ripple_netclient* netclient, ripple_file_buffer* fbuffer)
{
    uint32 wmsglen = 0;
    uint8* wuptr = NULL;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_netpacket* netpacket = NULL;

    finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

    wmsglen = RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_INC_DATA_SIZE;
    wmsglen += fbuffer->start;

    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(wmsglen);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = wmsglen;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 组装数据并发送 */
    wuptr = netpacket->data;

    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_INC_DATA);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, wmsglen);

    /* redolsn */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, fbuffer->extra.chkpoint.redolsn.wal.lsn);

    /* restartlsn */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, fbuffer->extra.rewind.restartlsn.wal.lsn);

    /* confirmlsn */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, fbuffer->extra.rewind.confirmlsn.wal.lsn);

    /* pump fileid */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, fbuffer->extra.chkpoint.segno.trail.fileid);

    /* collector fileid */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, finfo->fileid);

    /* collector blknum */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, finfo->blknum);

    /* collector offset */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE) + fbuffer->start);
    elog(RLOG_DEBUG, "finfo->fileid:%lu, finfo->blknum:%lu, fbuffer->start:%lu",
                        finfo->fileid,
                        finfo->blknum,
                        fbuffer->start);
    if (RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESHEND == (RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESHEND&fbuffer->flag))
    {
        RIPPLE_CONCAT(put, 8bit)(&wuptr, true);
    }
    else
    {
        RIPPLE_CONCAT(put, 8bit)(&wuptr, false);
    }

    /* 写入数据 */
    rmemcpy0(netpacket->data, RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_INC_DATA_SIZE, fbuffer->data, fbuffer->start);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
}

/* 网络客户端 */
void* ripple_onlinerefresh_pumpnet_main(void* args)
{
    bool end_onlinerefresh = false;
    int timeout = 0;
    ripple_file_buffer* fbuffer = NULL;
    ripple_thrnode *thrnode = NULL;
    ripple_task_onlinerefreshpumpnet* write = NULL;
    ripple_onlinerefresh_pumpnetstate* clientstate = NULL;
    ripple_netclient* netclient = NULL;

    thrnode = (ripple_thrnode*)args;
    write = (ripple_task_onlinerefreshpumpnet* )thrnode->data;
    clientstate = write->netstate;
    netclient = (ripple_netclient*)clientstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh pump incrment net stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while (clientstate->state == RIPPLE_INCREMENT_PUMPNETSTATE_STATE_RESET)
    {
        /* 退出 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 连接服务端 */
        if(RIPPLE_NETCLIENTCONN_STATUS_NOP == clientstate->base.status)
        {
            if(false == ripple_netclient_tryconn(netclient))
            {
                elog(RLOG_INFO, "can not connect collector");
                sleep(1);
                continue;
            }
            elog(RLOG_INFO, "connect collecotr success");
        }

        clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_IDLE;

        /* 创建 p2cidentity 数据包挂载到 wpackets 上 */
        if (false == ripple_onlinerefresh_pumpnet_p2cidentity(netclient, clientstate->onlinerefresh.data))
        {
            elog(RLOG_INFO, "add onlinerefresh increment identity error");
            sleep(1);
            continue;
        }

        while (true)
        {
            /* 退出 */
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                goto ripple_onlinerefresh_pumpnet_main_done;
            }

            /* 消息处理 */
            if(false == ripple_netclient_desc(netclient))
            {
                clientstate->base.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
                clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_RESET;
                ripple_onlinerefreshn_pumpnet_mgrsetreset(clientstate);
                ripple_netclient_reset(netclient);
                break;
            }

            if (ripple_queue_isnull(netclient->wpackets) && true == end_onlinerefresh)
            {
                /* 队列为空表示数据已经发送出去了, end_onlinerefresh 用于标识最后一个块在队列中 */
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                goto ripple_onlinerefresh_pumpnet_main_done;
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
                /* 没有超时，接收到了退出信号 */
                if (RIPPLE_ERROR_TIMEOUT != timeout)
                {
                    /* 处理失败, 退出 */
                    elog(RLOG_WARNING, "get file buffer error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    goto ripple_onlinerefresh_pumpnet_main_done;
                }
            
                /* 超时没有获取到数据,继续等待获取 */
                clientstate->base.hbtimeout += 1000;
                clientstate->base.timeout += 1000;
            }
            else
            {
                /* 添加数据到 wpackets */
                ripple_onlinerefresh_pumpnetstate_wpacketsadd_data(netclient, fbuffer);
                if (RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESHEND == (RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESHEND&fbuffer->flag))
                {
                    end_onlinerefresh = true;
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

ripple_onlinerefresh_pumpnet_main_done:
    ripple_pthread_exit(NULL);

    return NULL;
}

/* 初始化操作 */
ripple_onlinerefresh_pumpnetstate* ripple_onlinerefresh_pumpnetstate_init(void)
{
    char* host = NULL;
    ripple_onlinerefresh_pumpnetstate* clientstate = NULL;
    clientstate = (ripple_onlinerefresh_pumpnetstate*)rmalloc1(sizeof(ripple_onlinerefresh_pumpnetstate));
    if(NULL == clientstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(clientstate, 0, '\0', sizeof(ripple_onlinerefresh_pumpnetstate));
    clientstate->recpos.trail.fileid = 0;
    clientstate->recpos.trail.offset = 0;
    clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_RESET;
    clientstate->base.fd = -1;

    /* 设置类型 */
    ripple_netclient_setprotocoltype(&clientstate->base, RIPPLE_NETCLIENT_PROTOCOLTYPE_IPTCP);

    /* 获取监听主机 */
    host = guc_getConfigOption(RIPPLE_CFG_KEY_HOST);
    rmemcpy1(clientstate->base.svrhost, 0, host, (strlen(host)>16) ? 16 : strlen(host));
    if(NULL == clientstate->base.svrhost
        || '\0' == clientstate->base.svrhost[0])
    {
        elog(RLOG_ERROR, "please configure host config options");
    }

    /* 获取监听端口 */
    sprintf(clientstate->base.svrport, "%d", guc_getConfigOptionInt(RIPPLE_CFG_KEY_PORT));
    if(0 == clientstate->base.svrport)
    {
        elog(RLOG_ERROR, "please configure port config options");
    }

    rmemcpy1(clientstate->base.szport, 0, clientstate->base.svrport, 128);

    elog(RLOG_DEBUG, "clientstate->base.port:%d, %s", clientstate->base.svrport, clientstate->base.szport);

    /* 设置使用的网络模型 */
    clientstate->base.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);

    /* 申请 base 信息，用于后续的描述符处理 */
    if(false == clientstate->base.ops->create(&clientstate->base.base))
    {
        elog(RLOG_ERROR, "RIPPLE_NETIOMP_TYPE_POLL create error");
    }
    clientstate->base.hbtimeout = RIPPLE_NET_PUMP_HBTIME;
    clientstate->base.timeout = 0;
    clientstate->base.base->timeout = RIPPLE_NET_POLLTIMEOUT;
    clientstate->base.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
    clientstate->base.wpackets = ripple_queue_init();
    clientstate->base.rpackets = ripple_queue_init();
    clientstate->base.callback = ripple_netclient_packets_handler;

    return clientstate;
}

/* 网络发送初始化 */
ripple_task_onlinerefreshpumpnet* ripple_onlinerefresh_pumpwrite_init(void)
{
    ripple_task_onlinerefreshpumpnet* pumpwrite = NULL;

    pumpwrite = (ripple_task_onlinerefreshpumpnet*)rmalloc0(sizeof(ripple_task_onlinerefreshpumpnet));
    if(NULL == pumpwrite)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(pumpwrite, 0, 0, sizeof(ripple_task_onlinerefreshpumpnet));
    pumpwrite->netstate = ripple_onlinerefresh_pumpnetstate_init();
    return pumpwrite;
}

/* 资源回收 */
void ripple_onlinerefresh_pumpnetstate_destroy(ripple_onlinerefresh_pumpnetstate* clientstate)
{
    if(NULL == clientstate)
    {
        return;
    }

    ripple_netclient_destroy((ripple_netclient*)&clientstate->base);

    rfree(clientstate);
    clientstate = NULL;
}

void ripple_onlinerefresh_pumpnet_free(void* args)
{
    ripple_task_onlinerefreshpumpnet* pumpwrite = NULL;

    pumpwrite = (ripple_task_onlinerefreshpumpnet*)args;

    if (pumpwrite->netstate)
    {
        ripple_onlinerefresh_pumpnetstate_destroy(pumpwrite->netstate);
    }

    rfree(pumpwrite);
}
