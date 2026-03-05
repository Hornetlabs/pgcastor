#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"

/* 设置状态 */
void ripple_increment_pumpnet_set_status(ripple_increment_pumpnetstate* clientstate, int state)
{
    if (NULL == clientstate)
    {
        return;
    }
    clientstate->state = state;
}

/* 创建 p2c identity 数据包 */
static bool ripple_increment_pumpnet_p2cidentity(ripple_netclient* netclient)
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
        elog(RLOG_WARNING, "init increment identity packet error");
        goto ripple_increment_pumpnet_p2cidentity_error;
    }
    netpacket->max = RIPPLE_MAXALIGN(identity_size);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = identity_size;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);
    if (NULL == netpacket->data)
    {
        elog(RLOG_WARNING, "init increment identity data error");
        goto ripple_increment_pumpnet_p2cidentity_error;
    }

    /* 发送请求，获取标识 */
    wuptr = netpacket->data;
    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_IDENTITY);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, identity_size);
    RIPPLE_CONCAT(put, 8bit)(&wuptr, RIPPLE_NETIDENTITY_TYPE_PUMPINCREAMENT);
    RIPPLE_CONCAT(put, 32bit)(&wuptr, namelen);
    if (0 != namelen)
    {
        rmemcpy1(wuptr, 0, jobname, strlen(jobname));
    }

    if (false == ripple_netclient_addwpacket(netclient, (void*)netpacket))
    {
        elog(RLOG_WARNING, "increment add identity packet error");
        goto ripple_increment_pumpnet_p2cidentity_error;
    }
    return true;

ripple_increment_pumpnet_p2cidentity_error:
    ripple_netpacket_destroy(netpacket);
    return false;
}

/* 创建 data 数据包挂载到 wpackets 上 */
static void ripple_increment_pumpnet_wpacketsadd_data(ripple_netclient* netclient, ripple_file_buffer* fbuffer)
{
    uint32 wmsglen = 0;
    uint64 trailoffset = 0;
    uint8* wuptr = NULL;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_netpacket* netpacket = NULL;
    ripple_increment_pumpnetstate* clientstate = NULL;

    clientstate = (ripple_increment_pumpnetstate*)netclient;

    finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

    wmsglen = RIPPLE_NETMSG_TYPE_P2C_DATA_FIXSIZE;
    wmsglen += fbuffer->start;

    netpacket = ripple_netpacket_init();
    netpacket->max = RIPPLE_MAXALIGN(wmsglen);
    netpacket->offset = RIPPLE_NETMSG_TYPE_HDR_SIZE;
    netpacket->used = wmsglen;
    netpacket->data = ripple_netpacket_data_init(netpacket->max);

    /* 组装数据并发送 */
    wuptr = netpacket->data;

    RIPPLE_CONCAT(put, 32bit)(&wuptr, RIPPLE_NETMSG_TYPE_P2C_DATA);
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
    trailoffset = ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE) + fbuffer->start;
    RIPPLE_CONCAT(put, 64bit)(&wuptr, trailoffset);
    elog(RLOG_DEBUG, "finfo->fileid:%lu, finfo->blknum:%lu, fbuffer->start:%lu",
                        finfo->fileid,
                        finfo->blknum,
                        fbuffer->start);

    /* timestamp */
    RIPPLE_CONCAT(put, 64bit)(&wuptr, fbuffer->extra.timestamp);

    /* 写入数据 */
    rmemcpy0(netpacket->data, RIPPLE_NETMSG_TYPE_P2C_DATA_FIXSIZE, fbuffer->data, fbuffer->start);

    ripple_netclient_addwpacket(netclient, (void*)netpacket);
    clientstate->callback.setmetricsendlsn(clientstate->privdata, fbuffer->extra.rewind.confirmlsn.wal.lsn);
    clientstate->callback.setmetricsendtrailno(clientstate->privdata, finfo->fileid);
    clientstate->callback.setmetricsendtrailstart(clientstate->privdata, trailoffset);
    clientstate->callback.setmetricsendtimestamp(clientstate->privdata, fbuffer->extra.timestamp);
}

static bool ripple_increment_pumpnet_check_status(ripple_increment_pumpnetstate* clientstate)
{
    if (clientstate->state >= RIPPLE_INCREMENT_PUMPNETSTATE_STATE_IDLE)
    {
        return true;
    }
    else if(RIPPLE_INCREMENT_PUMPNETSTATE_STATE_IDENTITY == clientstate->state)
    {
        /* 尝试清理缓存 */
        riple_file_buffer_clean_waitflush(clientstate->txn2filebuffer);
    }
    else if (RIPPLE_INCREMENT_PUMPNETSTATE_STATE_READY == clientstate->state)
    {
        clientstate->callback.serialstate_state_set(clientstate->privdata, RIPPLE_PUMP_STATUS_SERIAL_WORK);
        ripple_increment_pumpnet_set_status(clientstate, RIPPLE_INCREMENT_PUMPNETSTATE_STATE_IDLE);
    }
    return false;
}

/* 网络客户端 */
void* ripple_increment_pumpnet_main(void *args)
{
    int timeout = 0;
    ripple_file_buffer* fbuffer = NULL;
    ripple_thrnode* thrnode = NULL;
    ripple_increment_pumpnetstate* clientstate = NULL;
    ripple_netclient* netclient = NULL;

    thrnode = (ripple_thrnode*)args;

    clientstate = (ripple_increment_pumpnetstate* )thrnode->data;
    netclient = (ripple_netclient*)clientstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump increment net stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
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
            if(false == ripple_netclient_tryconn(netclient))
            {
                elog(RLOG_INFO, "can not connect collector");
                sleep(1);
                continue;
            }
            elog(RLOG_INFO, "connect collecotr success");
        }

        /* 创建 p2cidentity 数据包挂载到 wpackets 上 */
        if (false == ripple_increment_pumpnet_p2cidentity(netclient))
        {
            elog(RLOG_WARNING, "add increment identity packet error");
            sleep(1);
            continue;
        }

        while (true)
        {
            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                goto ripple_increment_pumpnet_main_done;
            }

            /* 消息处理 */
            if(false == ripple_netclient_desc(netclient))
            {
                clientstate->base.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
                clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_RESET;
                ripple_netclient_reset(netclient);
                break;
            }

            /* 状态检查 */
            if (!ripple_increment_pumpnet_check_status(clientstate))
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
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
                    goto ripple_increment_pumpnet_main_done;
                }
                
                /* 超时没有获取到数据,继续等待获取 */
                clientstate->base.hbtimeout += 1000;
                clientstate->base.timeout += 1000;
            }
            else
            {
                /* 添加数据到 wpackets */
                ripple_increment_pumpnet_wpacketsadd_data(netclient, fbuffer);
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

ripple_increment_pumpnet_main_done:

    ripple_pthread_exit(NULL);
    return NULL;
}

/* 初始化操作 */
ripple_increment_pumpnetstate* ripple_increment_pumpnet_init(void)
{
    char* host = NULL;
    ripple_increment_pumpnetstate* clientstate = NULL;
    clientstate = (ripple_increment_pumpnetstate*)rmalloc1(sizeof(ripple_increment_pumpnetstate));
    if (NULL == clientstate)
    {
        elog(RLOG_WARNING, "pump increment init error, out of memory");
        return NULL;
    }
    rmemset0(clientstate, 0, '\0', sizeof(ripple_increment_pumpnetstate));
    clientstate->recpos.trail.fileid = 0;
    clientstate->recpos.trail.offset = 0;
    clientstate->state = RIPPLE_INCREMENT_PUMPNETSTATE_STATE_RESET;
    clientstate->base.fd = -1;

    /* 获取监听主机 */
    host = guc_getConfigOption(RIPPLE_CFG_KEY_HOST);
    rmemcpy1(clientstate->base.svrhost, 0, host, (strlen(host)>16) ? 16 : strlen(host));
    if (NULL == clientstate->base.svrhost
        || '\0' == clientstate->base.svrhost[0])
    {
        elog(RLOG_WARNING, "please configure host config options");
        return NULL;
    }

    /* 设置类型 */
    ripple_netclient_setprotocoltype(&clientstate->base, RIPPLE_NETCLIENT_PROTOCOLTYPE_IPTCP);

    /* 获取监听端口 */
    sprintf(clientstate->base.svrport, "%d", guc_getConfigOptionInt(RIPPLE_CFG_KEY_PORT));
    if (0 == clientstate->base.svrport)
    {
        elog(RLOG_WARNING, "please configure port config options");
        return NULL;
    }

    rmemcpy1(clientstate->base.szport, 0, clientstate->base.svrport, 128);

    elog(RLOG_DEBUG, "clientstate->base.port:%s, %s", clientstate->base.svrport, clientstate->base.szport);

    /* 设置使用的网络模型 */
    clientstate->base.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);

    /* 申请 base 信息，用于后续的描述符处理 */
    if (false == clientstate->base.ops->create(&clientstate->base.base))
    {
        elog(RLOG_WARNING, "RIPPLE_NETIOMP_TYPE_POLL create error");
        return NULL;
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

/* 资源回收 */
void ripple_increment_pumpnet_destroy(ripple_increment_pumpnetstate* clientstate)
{
    if(NULL == clientstate)
    {
        return;
    }

    ripple_netclient_destroy((ripple_netclient*)&clientstate->base);

    rfree(clientstate);
    clientstate = NULL;
}
