#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "metric/collector/ripple_metric_collector.h"

/* 解析网络包 */
static bool ripple_metric_collector_parsenetpacket(ripple_metric_collector_state* mcollector, ripple_netpacket* npacket)
{
    int msgtype     = 0;
    uint8* uptr     = NULL;

    /* 根据 msgtype 做分发 */
    uptr = npacket->data;
    uptr += 8;

    rmemcpy1(&msgtype, 0, uptr, 4);
    msgtype = r_ntoh32(msgtype);

    if (RIPPLE_XMANAGER_MSG_IDENTITYCMD != msgtype
        && RIPPLE_XMANAGER_MSG_COLLECTORINCREMENT != msgtype
        && RIPPLE_XMANAGER_MSG_COLLECTORONLINEREFRESH != msgtype
        && RIPPLE_XMANAGER_MSG_COLLECTORBIGTXN)
    {
        elog(RLOG_WARNING, "collector metric got unknown msg type from xmanager:%d", msgtype);
        return false;
    }

    return true;
}

/* 尝试解析解析包 */
static bool ripple_metric_collector_tryparsepacket(ripple_metric_collector_state* mcollector, ripple_netclient* netclient)
{
    ripple_netpacket* npacket       = NULL;

    while (1)
    {
        npacket = ripple_queue_tryget(netclient->rpackets);
        if (NULL == npacket)
        {
            return true;
        }

        if (npacket->offset != npacket->used)
        {
            if (false == ripple_queue_puthead(netclient->rpackets, npacket))
            {
                elog(RLOG_WARNING, "put uncomplete packet to read queue error");
                return false;
            }
            return true;
        }

        if (false == ripple_metric_collector_parsenetpacket(mcollector, npacket))
        {
            elog(RLOG_WARNING, "collector metric parse net packet error");
            ripple_netpacket_destroy(npacket);
            return false;
        }

        ripple_netpacket_destroy(npacket);
    }
    return true;
}

/* 增量消息 */
static bool ripple_metric_collector_assembleincrementpacket(ripple_metric_collector_state* mcollector, ripple_netclient* netclient)
{
    int len                                     = 0;
    int ivalue                                  = 0;
    XLogRecPtr lsnvalue                         = 0;
    ListCell* lc                                = NULL;
    uint8* uptr                                 = NULL;
    char* jobname                               = NULL;
    ripple_netpacket* npacket                   = NULL;
    ripple_metric_collectornode* collectornode  = NULL;

    if (NULL == mcollector->pumps || 0 == mcollector->pumps->length)
    {
        return true;
    }
    

    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble packet init packet error, out of memory");
        return false;
    }
    jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);

    /* msglen/crc32/msgtype/pumpcnt */
    len = 16;

    /* jobnamelen */
    len += 4;

    /* jobname */
    len += strlen(jobname);

    /* 计算长度 */
    foreach(lc, mcollector->pumps)
    {
        collectornode = (ripple_metric_collectornode*)lfirst(lc);

        /* pumpnamelen */
        len += 4;

        /* pumpname */
        len += strlen(jobname);

        /* recvlsn/flushlsn */
        len += 16;

        /* recvtrailno/recvtrailstart/flushtrailno/flushtrailstart/recvtimestamp/flushtimestamp */
        len += 48;

    }

    npacket->used = len;
    npacket->data = ripple_netpacket_data_init(len);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "assemble packet init packet data error, out of memory");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    uptr = npacket->data;

    /* 长度 */
    ivalue = len;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 暂不处理 */
    uptr += 4;

    /* msgtype */
    ivalue = RIPPLE_XMANAGER_MSG_COLLECTORINCREMENT;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    ivalue = mcollector->pumps->length;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobnamelen */
    ivalue = strlen(jobname);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobname */
    rmemcpy1(uptr, 0, jobname, strlen(jobname));
    uptr += strlen(jobname);

    foreach(lc, mcollector->pumps)
    {
        collectornode = (ripple_metric_collectornode *)lfirst(lc);

        /* pumpnamelen */
        ivalue = strlen(collectornode->jobname);
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;

        /* pumpname */
        rmemcpy1(uptr, 0, collectornode->jobname, strlen(collectornode->jobname));
        uptr += strlen(collectornode->jobname);

        /* recvlsn */
        lsnvalue = collectornode->recvlsn;
        lsnvalue = r_hton64(lsnvalue);
        rmemcpy1(uptr, 0, &lsnvalue, 8);
        uptr += 8;

        /* flushlsn */
        lsnvalue = collectornode->flushlsn;
        lsnvalue = r_hton64(lsnvalue);
        rmemcpy1(uptr, 0, &lsnvalue, 8);
        uptr += 8;

        /* recvtrailno */
        lsnvalue = collectornode->recvtrailno;
        lsnvalue = r_hton64(lsnvalue);
        rmemcpy1(uptr, 0, &lsnvalue, 8);
        uptr += 8;

        /* recvtrailstart */
        lsnvalue = collectornode->recvtrailstart;
        lsnvalue = r_hton64(lsnvalue);
        rmemcpy1(uptr, 0, &lsnvalue, 8);
        uptr += 8;

        /* flushtrailno */
        lsnvalue = collectornode->flushtrailno;
        lsnvalue = r_hton64(lsnvalue);
        rmemcpy1(uptr, 0, &lsnvalue, 8);
        uptr += 8;

        /* flushtrailstart */
        lsnvalue = collectornode->flushtrailstart;
        lsnvalue = r_hton64(lsnvalue);
        rmemcpy1(uptr, 0, &lsnvalue, 8);
        uptr += 8;

        /* recvtimestamp */
        lsnvalue = collectornode->recvtimestamp;
        lsnvalue = r_hton64(lsnvalue);
        rmemcpy1(uptr, 0, &lsnvalue, 8);
        uptr += 8;

        /* flushtimestamp */
        lsnvalue = collectornode->flushtimestamp;
        lsnvalue = r_hton64(lsnvalue);
        rmemcpy1(uptr, 0, &lsnvalue, 8);
        uptr += 8;
    }

    if (false == ripple_queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble collector increment metric put packet to write packet error");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* 组装 identity 消息 */
static bool ripple_metric_collector_assembleidentitypacket(ripple_metric_collector_state* mcollector, ripple_netclient* netclient)
{
    int8 result                 = 0;
    int len                     = 0;
    int ivalue                  = 0;
    uint8* uptr                 = NULL;
    char* jobname               = NULL;
    char* data                  = NULL;
    ripple_netpacket* npacket   = NULL;

    jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);
    data = guc_getdata();

    /*
     * 内容
     *  jobname + data dir + config dir
     */
    /* len + crc32 + msgtype + jobtype */
    len = 4 + 4 + 4 + 4;

    /* jobname len */
    len += 4;

    /* jobname */
    len += strlen(jobname);

    /* cmdtype */
    len += 4;

    /* flag */
    len += 1;

    /* data len */
    len += 4;

    /* data */
    len += strlen(data);

    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble identity msg init net packet error , out of memory");
        return false;
    }
    npacket->used = len;

    npacket->data = ripple_netpacket_data_init(len);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "assemble identity msg init net packet data error , out of memory");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    uptr = npacket->data;

    /* total len */
    ivalue = len;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* msgtype */
    ivalue = RIPPLE_XMANAGER_MSG_IDENTITYCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobtype */
    ivalue = RIPPLE_XMANAGER_METRICNODETYPE_COLLECTOR;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobnamelen */
    ivalue = strlen(jobname);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobname */
    rmemcpy1(uptr, 0, jobname, strlen(jobname));
    uptr += strlen(jobname);

    /* command start */
    ivalue = RIPPLE_XMANAGER_MSG_STARTCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    rmemcpy1(uptr, 0, &result, 1);
    uptr += 1;

    /* data len */
    ivalue = strlen(data);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* data */
    rmemcpy1(uptr, 0, data, strlen(data));
    uptr += strlen(data);

    if (false == ripple_queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble collector identity put packet to write packet error");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* 组建消息包 */
static bool ripple_metric_collector_assemblepacket(ripple_metric_collector_state* mcollector, ripple_netclient* netclient)
{
    if (false == ripple_metric_collector_assembleincrementpacket(mcollector, netclient))
    {
        elog(RLOG_WARNING, "metric collector assemble increment packet error");
        return false;
    }
    return true;
}



/* 状态主线程 */
void* ripple_metric_collector_main(void *args)
{
    int fd                                      = -1;
    int port                                    = 0;
    int interval                                = 5000;
    int intervaltime                            = 0;
    uint32 offset                               = 0;
    ListCell* lc                                = NULL;
    ripple_thrnode* thrnode                     = NULL;
    ripple_metric_collector_state* mcollector   = NULL;
    ripple_metric_collectornode* mcollectornode = NULL;
    ripple_netclient netclient                  = { 0 };

    thrnode = (ripple_thrnode*)args;

    mcollector = (ripple_metric_collector_state*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment collector metric stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    port = guc_getConfigOptionInt(RIPPLE_CFG_KEY_XMANAGER_PORT);
    if (0 == port)
    {
        sprintf(netclient.svrport, "%s", RMANAGER_PORT);
    }
    else
    {
        sprintf(netclient.svrport, "%d", port);
    }
    elog(RLOG_DEBUG, "collector metric port:%s", netclient.svrport);

    /* 设置使用的网络模型 */
    netclient.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);

    /* 申请 base 信息，用于后续的描述符处理 */
    if (false == netclient.ops->create(&netclient.base))
    {
        elog(RLOG_WARNING, "collector metric main iomp module error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_netclient_destroy(&netclient);
        ripple_pthread_exit(NULL);
    }

    /* 设置类型 */
    ripple_netclient_setprotocoltype(&netclient, RIPPLE_NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN);

    ripple_netclient_sethbtimeout(&netclient, RIPPLE_NET_PUMP_HBTIME);
    ripple_netclient_settimeout(&netclient, 0);

    netclient.base->timeout = RIPPLE_NET_POLLTIMEOUT;
    netclient.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
    netclient.wpackets = ripple_queue_init();
    if (NULL == netclient.wpackets)
    {
        elog(RLOG_WARNING, "collector metric main init send queue error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_netclient_destroy(&netclient);
        ripple_pthread_exit(NULL);
    }
    netclient.rpackets = ripple_queue_init();
    if (NULL == netclient.rpackets)
    {
        elog(RLOG_WARNING, "collector metric main init recv queue error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_netclient_destroy(&netclient);
        ripple_pthread_exit(NULL);
    }
    netclient.callback = NULL;

    while(1)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 解析器 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 尝试连接xmanager */
        if (RIPPLE_NETCLIENTCONN_STATUS_NOP == netclient.status)
        {
            /* 连接 xmanager */
            if (false == ripple_netclient_tryconn(&netclient))
            {
                elog(RLOG_WARNING, "can not connect xmanager");
            }

            if (RIPPLE_NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
            {
                if (false == ripple_metric_collector_assembleidentitypacket(mcollector, &netclient))
                {
                    elog(RLOG_WARNING, "metric collector assemble identity packet error");
                    ripple_netclient_clear(&netclient);
                }
            }
        }

                /* 未超时且没有数据需要发送 */
        if (intervaltime <= interval 
            && true == ripple_queue_isnull(netclient.wpackets)
            && true == ripple_queue_isnull(netclient.rpackets))
        {
            intervaltime += 1000;
            sleep(1);
            continue;
        }

        if (RIPPLE_NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
        {
            if (false == ripple_netclient_desc2(&netclient))
            {
                elog(RLOG_WARNING, "metric collector iomp desc error");
                ripple_netclient_clear(&netclient);
            }
            else if (false == ripple_metric_collector_tryparsepacket(mcollector, &netclient))
            {
                /* 清理队列，关闭描述符 */
                elog(RLOG_WARNING, "metric collector parse packet error");
                ripple_netclient_clear(&netclient);
            }
        }

        if (intervaltime <= interval)
        {
            continue;
        }
        intervaltime = 0;

        /* 在连接状态下构建发送包 */
        if (RIPPLE_NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
        {
            if (false == ripple_metric_collector_assemblepacket(mcollector, &netclient))
            {
                /* 清理队列，关闭描述符 */
                elog(RLOG_WARNING, "metric collector assemble packet error");
                ripple_netclient_clear(&netclient);
            }
        }

        /* 将数据落盘 */
        fd = BasicOpenFile(RIPPLE_COLLECTOR_STATUS_FILE_TEMP,
                                O_RDWR | O_CREAT | RIPPLE_BINARY);
        if(-1 == fd)
        {
            elog(RLOG_WARNING, "open file:collector/%s error, %s", RIPPLE_COLLECTOR_STATUS_FILE_TEMP, strerror(errno));
        }

        offset = 0;
        foreach(lc, mcollector->pumps)
        {
            mcollectornode = (ripple_metric_collectornode*)lfirst(lc);
            FilePWrite(fd, (char*)mcollectornode, sizeof(ripple_metric_collectornode), offset);
            offset += sizeof(ripple_metric_collectornode);

            elog(RLOG_INFO, "-------- XSYNCH Collector Job: %s --------", mcollectornode->jobname);
            elog(RLOG_INFO, "XSYNCH Collector RecvLSN:          %X/%X", (uint32)(mcollectornode->recvlsn >> 32), (uint32)(mcollectornode->recvlsn));
            elog(RLOG_INFO, "XSYNCH Collector FLushLSN:         %X/%X", (uint32)(mcollectornode->flushlsn >> 32), (uint32)(mcollectornode->flushlsn));
            elog(RLOG_INFO, "XSYNCH Collector RecvTrail:        %lX/%lX", mcollectornode->recvtrailno, mcollectornode->recvtrailstart);
            elog(RLOG_INFO, "XSYNCH Collector FlushTrail:       %lX/%lX", mcollectornode->flushtrailno, mcollectornode->flushtrailstart);
            elog(RLOG_INFO, "XSYNCH Collector RecvTimestamp:    %lu", mcollectornode->recvtimestamp);
            elog(RLOG_INFO, "XSYNCH Collector FlushTimestamp:   %lu", mcollectornode->flushtimestamp);
        }

        FileClose(fd);

        /* 重命名 */
        if (durable_rename(RIPPLE_COLLECTOR_STATUS_FILE_TEMP, RIPPLE_COLLECTOR_STATUS_FILE, RLOG_WARNING) != 0)
        {
            elog(RLOG_WARNING, "Error renaming collector file %s 2 %s", RIPPLE_COLLECTOR_STATUS_FILE_TEMP, RIPPLE_COLLECTOR_STATUS_FILE);
        }
    }

    ripple_netclient_destroy(&netclient);
    ripple_pthread_exit(NULL);
    return NULL;
}

ripple_metric_collectornode* ripple_metric_collectornode_init(char* name)
{
    ripple_metric_collectornode* collectornode = NULL;

    collectornode = rmalloc0(sizeof(ripple_metric_collectornode));
    if(NULL == collectornode)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(collectornode, 0, '\0', sizeof(ripple_metric_collectornode));

    rmemset0(collectornode->jobname, 0, '\0', 128);

    snprintf(collectornode->jobname, 128, "%s", name);
    collectornode->recvlsn = InvalidXLogRecPtr;
    collectornode->flushlsn = InvalidXLogRecPtr;
    collectornode->recvtrailno = 0;
    collectornode->recvtrailstart = 0;
    collectornode->flushtrailno = 0; 
    collectornode->flushtrailstart = 0;
    collectornode->recvtimestamp = 0;
    collectornode->flushtimestamp = 0;

    return collectornode;
}

ripple_metric_collector_state* ripple_metric_collector_init(void)
{
    ripple_metric_collector_state* state = NULL;
    state = rmalloc0(sizeof(ripple_metric_collector_state));
    if(NULL == state)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(state, 0, '\0', sizeof(ripple_metric_collector_state));
    state->pumps = NULL;

    return state;
}

/* 缓存清理 */
void ripple_metric_collector_destroy(void* args)
{
    ripple_metric_collector_state* state = NULL;

    state = (ripple_metric_collector_state*)args;
    
    if(NULL == state)
    {
        return;
    }

    if (state->pumps)
    {
        list_free_deep(state->pumps);
    }

    rfree(state);
    return;
}
