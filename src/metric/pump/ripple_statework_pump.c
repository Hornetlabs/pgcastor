#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "metric/pump/ripple_statework_pump.h"


/* 解析网络包 */
static bool ripple_metric_pump_parsenetpacket(ripple_state_pump_state* mpump, ripple_netpacket* npacket)
{
    int msgtype     = 0;
    uint8* uptr     = NULL;

    /* 根据 msgtype 做分发 */
    uptr = npacket->data;
    uptr += 8;

    rmemcpy1(&msgtype, 0, uptr, 4);
    msgtype = r_ntoh32(msgtype);

    if (RIPPLE_XMANAGER_MSG_IDENTITYCMD != msgtype
        && RIPPLE_XMANAGER_MSG_PUMPINCREMENT != msgtype
        && RIPPLE_XMANAGER_MSG_PUMPONLINEREFRESH != msgtype
        && RIPPLE_XMANAGER_MSG_PUMPBIGTXN)
    {
        elog(RLOG_WARNING, "pump metric got unknown msg type from xmanager:%d", msgtype);
        return false;
    }

    return true;
}

/* 尝试解析解析包 */
static bool ripple_metric_pump_tryparsepacket(ripple_state_pump_state* mpump, ripple_netclient* netclient)
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

        if (false == ripple_metric_pump_parsenetpacket(mpump, npacket))
        {
            elog(RLOG_WARNING, "pump metric parse net packet error");
            ripple_netpacket_destroy(npacket);
            return false;
        }

        ripple_netpacket_destroy(npacket);
    }
    return true;
}

/* 增量消息 */
static bool ripple_metric_pump_assembleincrementpacket(ripple_state_pump_state* mpump, ripple_netclient* netclient)
{
    int len                     = 0;
    int ivalue                  = 0;
    XLogRecPtr lsnvalue         = 0;
    uint8* uptr                 = NULL;
    char* jobname               = NULL;
    ripple_netpacket* npacket   = NULL;

    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble packet init packet error, out of memory");
        return false;
    }
    jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);

    /* msglen/crc32/msgtype */
    len = 12;

    /* jobnamelen */
    len += 4;

    /* jobname */
    len += strlen(jobname);

    /* loadlsn/sendlsn */
    len += 16;

    /* loadtrailno/loadtrailstart/sendtrailno/sendtrailstart/loadtimestamp/sendtimestamp */
    len += 48;

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
    ivalue = RIPPLE_XMANAGER_MSG_PUMPINCREMENT;
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

    /* loadlsn */
    lsnvalue = mpump->loadlsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* sendlsn */
    lsnvalue = mpump->sendlsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* loadtrailno */
    lsnvalue = mpump->loadtrailno;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* loadtrailstart */
    lsnvalue = mpump->loadtrailstart;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* sendtrailno */
    lsnvalue = mpump->sendtrailno;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* sendtrailstart */
    lsnvalue = mpump->sendtrailstart;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* loadtimestamp */
    lsnvalue = mpump->loadtimestamp;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* sendtimestamp */
    lsnvalue = mpump->sendtimestamp;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    if (false == ripple_queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble pump increment metric put packet to write packet error");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* 组装 identity 消息 */
static bool ripple_metric_pump_assembleidentitypacket(ripple_state_pump_state* mpump, ripple_netclient* netclient)
{
    int8 result                 = 0;
    int len                     = 0;
    int ivalue                  = 0;
    uint8* uptr                 = NULL;
    char* jobname               = NULL;
    char* data                  = NULL;
    char* traildir              = NULL;
    ripple_netpacket* npacket   = NULL;

    jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);
    traildir = guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR);
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

    /* trail len */
    len += 4;

    /* trail */
    len += strlen(traildir);

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
    ivalue = RIPPLE_XMANAGER_METRICNODETYPE_PUMP;
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

    /* traildir len */
    ivalue = strlen(traildir);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* traildir */
    rmemcpy1(uptr, 0, traildir, strlen(traildir));
    uptr += strlen(traildir);

    if (false == ripple_queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble pump identity put packet to write packet error");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* 组建消息包 */
static bool ripple_metric_pump_assemblepacket(ripple_state_pump_state* mpump, ripple_netclient* netclient)
{
    if (false == ripple_metric_pump_assembleincrementpacket(mpump, netclient))
    {
        elog(RLOG_WARNING, "metric pump assemble increment packet error");
        return false;
    }
    return true;
}

/* 状态主线程 */
void* ripple_state_pump_main(void *args)
{
    int fd                          = -1;
    int port                        = 0;
    int interval                    = 5000;
    int intervaltime                = 0;
    XLogRecPtr loadlsn              = InvalidXLogRecPtr;
    XLogRecPtr sendlsn              = InvalidXLogRecPtr;
    uint64 loadtrailno              = 0;
    uint64 loadtrailstart           = 0;
    uint64 sendtrailno              = 0;
    uint64 sendtrailstart           = 0;
    uint64 loadtimestamp            = 0;
    uint64 sendtimestamp            = 0;
    ripple_thrnode* thrnode         = NULL;
    ripple_state_pump_state* mpump  = NULL;
    ripple_netclient netclient      = { 0 };

    thrnode = (ripple_thrnode*)args;

    mpump = (ripple_state_pump_state*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump metric exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
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
    elog(RLOG_DEBUG, "pump metric port:%s", netclient.svrport);

    /* 设置使用的网络模型 */
    netclient.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);

    /* 申请 base 信息，用于后续的描述符处理 */
    if (false == netclient.ops->create(&netclient.base))
    {
        elog(RLOG_WARNING, "pump metric main iomp module error");
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
        elog(RLOG_WARNING, "pump metric main init send queue error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_netclient_destroy(&netclient);
        ripple_pthread_exit(NULL);
    }
    netclient.rpackets = ripple_queue_init();
    if (NULL == netclient.rpackets)
    {
        elog(RLOG_WARNING, "pump metric main init recv queue error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_netclient_destroy(&netclient);
        ripple_pthread_exit(NULL);
    }
    netclient.callback = NULL;

    while(1)
    {
        /* 
         * 首先判断是否接收到退出信号
         *  对于子管理线程，收到 TERM 信号有两种场景:
         *  1、子管理线程的上级常驻线程退出
         *  2、接收到了退出标识
         * 
         * 上述两种场景, 都不需要子管理线程设置工作线程为 FREE 状态
         */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
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
                if (false == ripple_metric_pump_assembleidentitypacket(mpump, &netclient))
                {
                    elog(RLOG_WARNING, "metric pump assemble identity packet error");
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
                elog(RLOG_WARNING, "metric pump iomp desc error");
                ripple_netclient_clear(&netclient);
            }
            else if (false == ripple_metric_pump_tryparsepacket(mpump, &netclient))
            {
                /* 清理队列，关闭描述符 */
                elog(RLOG_WARNING, "metric pump parse packet error");
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
            if (false == ripple_metric_pump_assemblepacket(mpump, &netclient))
            {
                /* 清理队列，关闭描述符 */
                elog(RLOG_WARNING, "metric pump assemble packet error");
                ripple_netclient_clear(&netclient);
            }
        }

        if (loadlsn != mpump->loadlsn 
            || sendlsn != mpump->sendlsn
            || loadtrailno != mpump->loadtrailno
            || loadtrailstart != mpump->loadtrailstart
            || sendtrailno != mpump->sendtrailno
            || sendtrailstart != mpump->sendtrailstart
            || loadtimestamp != mpump->loadtimestamp
            || sendtimestamp != mpump->sendtimestamp)
        {
            loadlsn = mpump->loadlsn;
            sendlsn = mpump->sendlsn;
            loadtrailno = mpump->loadtrailno;
            loadtrailstart = mpump->loadtrailstart;
            sendtrailno = mpump->sendtrailno;
            sendtrailstart = mpump->sendtrailstart;
            loadtimestamp = mpump->loadtimestamp;
            sendtimestamp = mpump->sendtimestamp;

            elog(RLOG_INFO, "XSYNCH Pump LoadLSN:           %X/%X", (uint32)(mpump->loadlsn >> 32), (uint32)(mpump->loadlsn));
            elog(RLOG_INFO, "XSYNCH Pump SendLSN:           %X/%X", (uint32)(mpump->sendlsn >> 32), (uint32)(mpump->sendlsn));
            elog(RLOG_INFO, "XSYNCH Pump LoadTrail:         %lX/%lX", mpump->loadtrailno, mpump->loadtrailstart);
            elog(RLOG_INFO, "XSYNCH Pump SendTrail:         %lX/%lX", mpump->sendtrailno, mpump->sendtrailstart);
            elog(RLOG_INFO, "XSYNCH Pump LoadTimestamp:     %lu", mpump->loadtimestamp);
            elog(RLOG_INFO, "XSYNCH Pump SendTimestamp:     %lu", mpump->sendtimestamp);

            /* 将数据落盘 */
            fd = BasicOpenFile(RIPPLE_PUMP_STATUS_FILE_TEMP,
                                    O_RDWR | O_CREAT | RIPPLE_BINARY);
            if(-1 == fd)
            {
                elog(RLOG_WARNING, "open file:pump/%s error, %s", RIPPLE_PUMP_STATUS_FILE_TEMP, strerror(errno));
            }
            FileWrite(fd, (char*)mpump, sizeof(ripple_state_pump_state));

            FileClose(fd);

            /* 重命名 */
            if (durable_rename(RIPPLE_PUMP_STATUS_FILE_TEMP, RIPPLE_PUMP_STATUS_FILE, RLOG_WARNING) != 0) 
            {
                elog(RLOG_WARNING, "Error renaming file:%s, %s", RIPPLE_PUMP_STATUS_FILE, strerror(errno));
            }
        }

    }

    ripple_netclient_destroy(&netclient);
    ripple_pthread_exit(NULL);
    return NULL;
}

ripple_state_pump_state* ripple_state_pump_init(void)
{
    ripple_state_pump_state* state = NULL;

    state = rmalloc0(sizeof(ripple_state_pump_state));
    if(NULL == state)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(state, 0, '\0', sizeof(ripple_state_pump_state));

    state->loadlsn = InvalidXLogRecPtr;
    state->sendlsn = InvalidXLogRecPtr;
    state->loadtrailno = 0;
    state->loadtrailstart = 0;
    state->sendtrailno = 0;
    state->sendtrailstart = 0;
    state->loadtimestamp = 0;
    state->sendtimestamp = 0;

    return state;

}

/* 缓存清理 */
void ripple_state_pump_destroy(ripple_state_pump_state* state)
{
    if(NULL == state)
    {
        return;
    }

    rfree(state);
    state = NULL;
}
