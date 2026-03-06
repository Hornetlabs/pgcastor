#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "command/ripple_cmd.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "refresh/ripple_refresh_tables.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "metric/capture/ripple_metric_capture.h"

static bool ripple_metric_capture_onlinerefresh(ripple_metric_capture* mcapture, ripple_netpacket* npacket)
{
    int ivalue                          = 0;
    int index                           = 0;
    int tablecnt                        = 0;
    ripple_refresh_table* rtable        = NULL;
    ripple_refresh_tables* rtables      = NULL;
    uint8* uptr                         = NULL;

    uptr = npacket->data;

    /* 跳过 msglen + crc32 + cmdtype */
    uptr += 12;

    /* 总行数 */
    rmemcpy1(&tablecnt, 0, uptr, 4);
    tablecnt = r_ntoh32(tablecnt);
    uptr += 4;

    rtables = ripple_refresh_tables_init();
    if (NULL == rtables)
    {
        elog(RLOG_WARNING, "capture onlinerefresh out of memory");
        return false;
    }

    for (index = 0; index < tablecnt; index++)
    {
        rtable = ripple_refresh_table_init();
        if (NULL == rtable)
        {
            elog(RLOG_WARNING, "capture onlinerefresh malloc refreshtable, out of memory");
            goto ripple_metric_capture_onlinerefresh_error;
        }

        ripple_refresh_tables_add(rtable, rtables);

        /* schema */
        rmemcpy1(&ivalue, 0, uptr, 4);
        ivalue = r_ntoh32(ivalue);
        uptr += 4;
        ivalue += 1;

        rtable->schema = rmalloc0(ivalue);
        if (NULL == rtable->schema)
        {
            elog(RLOG_WARNING, "capture onlinerefresh malloc refreshtable schema, out of memory");
            goto ripple_metric_capture_onlinerefresh_error;
        }
        rmemset0(rtable->schema, 0, '\0', ivalue);
        ivalue -= 1;
        rmemcpy0(rtable->schema, 0, uptr, ivalue);
        uptr += ivalue;

        /* table */
        rmemcpy1(&ivalue, 0, uptr, 4);
        ivalue = r_ntoh32(ivalue);
        uptr += 4;
        ivalue += 1;

        rtable->table = rmalloc0(ivalue);
        if (NULL == rtable->table)
        {
            elog(RLOG_WARNING, "capture onlinerefresh malloc refreshtable table, out of memory");
            goto ripple_metric_capture_onlinerefresh_error;
        }
        rmemset0(rtable->table, 0, '\0', ivalue);
        ivalue -= 1;
        rmemcpy0(rtable->table, 0, uptr, ivalue);
        uptr += ivalue;
    }

    mcapture->addonlinerefresh(mcapture->privdata, (void*)rtables);
    return true;

ripple_metric_capture_onlinerefresh_error:

    if (NULL != rtables)
    {
        ripple_refresh_freetables(rtables);
    }
    return false;
}

/* 解析网络包 */
static bool ripple_metric_capture_parsenetpacket(ripple_metric_capture* mcapture, ripple_netpacket* npacket)
{
    int msgtype     = 0;
    uint8* uptr     = NULL;

    /* 根据 msgtype 做分发 */
    uptr = npacket->data;
    uptr += 8;

    rmemcpy1(&msgtype, 0, uptr, 4);
    msgtype = r_ntoh32(msgtype);

    if (RIPPLE_XMANAGER_MSG_IDENTITYCMD != msgtype
        && RIPPLE_XMANAGER_MSG_CAPTUREINCREMENT != msgtype
        && RIPPLE_XMANAGER_MSG_CAPTUREREFRESH != msgtype
        && RIPPLE_XMANAGER_MSG_CAPTUREBIGTXN)
    {
        elog(RLOG_WARNING, "capture metric got unknown msg type from xmanager:%d", msgtype);
        return false;
    }

    if (RIPPLE_XMANAGER_MSG_CAPTUREREFRESH == msgtype)
    {
        /* 解析数据库包, 生成 onlinerefresh 文件 */
        ripple_metric_capture_onlinerefresh(mcapture, npacket);
    }

    return true;
}

/* 尝试解析解析包 */
static bool ripple_metric_capture_tryparsepacket(ripple_metric_capture* mcapture, ripple_netclient* netclient)
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

        if (false == ripple_metric_capture_parsenetpacket(mcapture, npacket))
        {
            elog(RLOG_WARNING, "capture metric parse net packet error");
            ripple_netpacket_destroy(npacket);
            return false;
        }

        ripple_netpacket_destroy(npacket);
    }
    return true;
}

/* 增量消息 */
static bool ripple_metric_capture_assembleincrementpacket(ripple_metric_capture* mcapture, ripple_netclient* netclient)
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

    /* redolsn/restartlsn/confirmlsn/loadlsn/parselsn/flushlsn */
    len += 48;

    /* trailno/trailstart/parsetimestamp/flushtimestamp */
    len += 32;

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
    ivalue = RIPPLE_XMANAGER_MSG_CAPTUREINCREMENT;
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

    /* redolsn */
    lsnvalue = mcapture->redolsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* restartlsn */
    lsnvalue = mcapture->restartlsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* confirmlsn */
    lsnvalue = mcapture->confirmlsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* loadlsn */
    lsnvalue = mcapture->loadlsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* parselsn */
    lsnvalue = mcapture->parselsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* flushlsn */
    lsnvalue = mcapture->flushlsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* trailno */
    lsnvalue = mcapture->trailno;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* trailstart */
    lsnvalue = mcapture->trailstart;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* parsetimestamp */
    lsnvalue = mcapture->parsetimestamp;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* flushtimestamp */
    lsnvalue = mcapture->flushtimestamp;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    if (false == ripple_queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble capture increment metric put packet to write packet error");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* 组装 identity 消息 */
static bool ripple_metric_capture_assembleidentitypacket(ripple_metric_capture* mcapture, ripple_netclient* netclient)
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
    ivalue = RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE;
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
        elog(RLOG_WARNING, "assemble capture identity put packet to write packet error");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* 组建消息包 */
static bool ripple_metric_capture_assemblepacket(ripple_metric_capture* mcapture, ripple_netclient* netclient)
{
    if (false == ripple_metric_capture_assembleincrementpacket(mcapture, netclient))
    {
        elog(RLOG_WARNING, "metric capture assemble increment packet error");
        return false;
    }
    return true;
}

/* 添加待发送包 */
void ripple_metric_capture_addpackets(ripple_metric_capture* mcapture, ripple_netpacket* npacket)
{
    /* 获取锁 */
    ripple_thread_lock(&mcapture->dlpacketslock);
    mcapture->dlpackets = dlist_put(mcapture->dlpackets, npacket);
    ripple_thread_unlock(&mcapture->dlpacketslock);
}

/* 状态主线程 */
void* ripple_metric_capture_main(void *args)
{
    int fd = -1;
    int port = 0;
    int interval                                = 5000;
    int intervaltime                            = 0;
    uint64 trailno                              = 0;
    uint64 trailstart                           = 0;
    uint64 parsetimestamp                       = 0;
    uint64 flushtimestamp                       = 0;
    XLogRecPtr redolsn                          = InvalidXLogRecPtr;
    XLogRecPtr restartlsn                       = InvalidXLogRecPtr;
    XLogRecPtr confirmlsn                       = InvalidXLogRecPtr;
    XLogRecPtr loadlsn                          = InvalidXLogRecPtr;
    XLogRecPtr parselsn                         = InvalidXLogRecPtr;
    XLogRecPtr flushlsn                         = InvalidXLogRecPtr;
    dlistnode* dlnode                           = NULL;
    ripple_thrnode* thrnode                     = NULL;
    ripple_metric_capture* mcapture             = NULL;
    ripple_netclient netclient                  = { 0 };

    thrnode = (ripple_thrnode*)args;
    mcapture = (ripple_metric_capture*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment capture metric stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
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
    elog(RLOG_DEBUG, "capture metric port:%s", netclient.svrport);

    /* 设置使用的网络模型 */
    netclient.ops = ripple_netiomp_init(RIPPLE_NETIOMP_TYPE_POLL);

    /* 申请 base 信息，用于后续的描述符处理 */
    if (false == netclient.ops->create(&netclient.base))
    {
        elog(RLOG_WARNING, "capture metric main iomp module error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_netclient_destroy(&netclient);
        ripple_pthread_exit(NULL);
    }

    /* 设置类型 */
    ripple_netclient_setprotocoltype(&netclient, RIPPLE_NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN);

    ripple_netclient_sethbtimeout(&netclient, RIPPLE_NET_HBTIME);
    ripple_netclient_settimeout(&netclient, 0);

    netclient.base->timeout = RIPPLE_NET_POLLTIMEOUT;
    netclient.status = RIPPLE_NETCLIENTCONN_STATUS_NOP;
    netclient.wpackets = ripple_queue_init();
    if (NULL == netclient.wpackets)
    {
        elog(RLOG_WARNING, "capture metric main init send queue error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_netclient_destroy(&netclient);
        ripple_pthread_exit(NULL);
    }
    netclient.rpackets = ripple_queue_init();
    if (NULL == netclient.rpackets)
    {
        elog(RLOG_WARNING, "capture metric main init recv queue error");
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
                if (false == ripple_metric_capture_assembleidentitypacket(mcapture, &netclient))
                {
                    elog(RLOG_WARNING, "metric capture assemble identity packet error");
                    ripple_netclient_clear(&netclient);
                }
            }
        }

        /* 获取锁 */
        ripple_thread_lock(&mcapture->dlpacketslock);
        /* 合并 packets */
        if (false == dlist_isnull(mcapture->dlpackets))
        {
           for (dlnode = mcapture->dlpackets->head; NULL != dlnode; dlnode = dlnode->next)
           {
                ripple_queue_put(netclient.wpackets, dlnode->value);
           }

           dlist_free(mcapture->dlpackets, NULL);
           mcapture->dlpackets = NULL;
        }
        ripple_thread_unlock(&mcapture->dlpacketslock);

        /* 未超时且没有数据需要发送 */
        if (intervaltime <= interval)
        {
            if (RIPPLE_NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
            {
                if (false == ripple_netclient_desc2(&netclient))
                {
                    elog(RLOG_WARNING, "metric capture iomp desc error");
                    ripple_netclient_clear(&netclient);
                }

                if (false == ripple_metric_capture_tryparsepacket(mcapture, &netclient))
                {
                    /* 清理队列，关闭描述符 */
                    elog(RLOG_WARNING, "metric capture parse packet error");
                    ripple_netclient_clear(&netclient);
                }
                intervaltime += netclient.base->timeout;
            }
            else
            {
                intervaltime += 1000;
                sleep(1);
            }
            continue;
        }

        if (RIPPLE_NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
        {
            if (false == ripple_netclient_desc2(&netclient))
            {
                elog(RLOG_WARNING, "metric capture iomp desc error");
                ripple_netclient_clear(&netclient);
            }

            if (false == ripple_metric_capture_tryparsepacket(mcapture, &netclient))
            {
                /* 清理队列，关闭描述符 */
                elog(RLOG_WARNING, "metric capture parse packet error");
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
            if (false == ripple_metric_capture_assemblepacket(mcapture, &netclient))
            {
                /* 清理队列，关闭描述符 */
                elog(RLOG_WARNING, "metric capture assemble packet error");
                ripple_netclient_clear(&netclient);
            }
        }

        if (redolsn != mcapture->redolsn 
            || restartlsn != mcapture->restartlsn
            || confirmlsn != mcapture->confirmlsn
            || loadlsn != mcapture->loadlsn
            || parselsn != mcapture->parselsn
            || flushlsn != mcapture->flushlsn
            || parsetimestamp != mcapture->parsetimestamp
            || flushtimestamp != mcapture->flushtimestamp
            || trailno != mcapture->trailno
            || trailstart != mcapture->trailstart)
        {
            redolsn = mcapture->redolsn;
            restartlsn = mcapture->restartlsn;
            confirmlsn = mcapture->confirmlsn;
            loadlsn = mcapture->loadlsn;
            parselsn = mcapture->parselsn;
            flushlsn = mcapture->flushlsn;
            parsetimestamp = mcapture->parsetimestamp;
            flushtimestamp = mcapture->flushtimestamp;
            trailno = mcapture->trailno;
            trailstart = mcapture->trailstart;

            elog(RLOG_INFO, "XSYNCH Capture RedoLSN:            %X/%X", (uint32)(mcapture->redolsn >> 32), (uint32)(mcapture->redolsn));
            elog(RLOG_INFO, "XSYNCH Capture RestartLSN:         %X/%X", (uint32)(mcapture->restartlsn >> 32), (uint32)(mcapture->restartlsn));
            elog(RLOG_INFO, "XSYNCH Capture ConfirmLSN:         %X/%X", (uint32)(mcapture->confirmlsn >> 32), (uint32)(mcapture->confirmlsn));
            elog(RLOG_INFO, "XSYNCH Capture LoadLSN:            %X/%X", (uint32)(mcapture->loadlsn >> 32), (uint32)(mcapture->loadlsn));
            elog(RLOG_INFO, "XSYNCH Capture ParseLSN:           %X/%X", (uint32)(mcapture->parselsn >> 32), (uint32)(mcapture->parselsn));
            elog(RLOG_INFO, "XSYNCH Capture FlushLSN:           %X/%X", (uint32)(mcapture->flushlsn >> 32), (uint32)(mcapture->flushlsn));
            elog(RLOG_INFO, "XSYNCH Capture ParseTimestamp:     %lu", mcapture->parsetimestamp);
            elog(RLOG_INFO, "XSYNCH Capture FlushTimestamp:     %lu", mcapture->flushtimestamp);
            elog(RLOG_INFO, "XSYNCH Capture Trail:              %lX/%lX", mcapture->trailno, mcapture->trailstart);

            /* 将数据落盘 */
            fd = BasicOpenFile(RIPPLE_CAPTURE_STATUS_FILE_TEMP,
                                    O_RDWR | O_CREAT | RIPPLE_BINARY);
            if(-1 == fd)
            {
                elog(RLOG_WARNING, "open file:capture/%s error, %s", RIPPLE_CAPTURE_STATUS_FILE_TEMP, strerror(errno));
                continue;
            }
            FileWrite(fd, (char*)mcapture, sizeof(ripple_metric_capture));

            FileClose(fd);

            /* 重命名 */
            if (durable_rename(RIPPLE_CAPTURE_STATUS_FILE_TEMP, RIPPLE_CAPTURE_STATUS_FILE, RLOG_WARNING) != 0)
            {
                elog(RLOG_WARNING, "Error renaming capture file %s 2 %s", RIPPLE_CAPTURE_STATUS_FILE_TEMP, RIPPLE_CAPTURE_STATUS_FILE);
            }
        }
    }

    ripple_netclient_destroy(&netclient);
    ripple_pthread_exit(NULL);
    return NULL;
}

/* 初始化状态结构 */
ripple_metric_capture* ripple_metric_capture_init(void)
{
    ripple_metric_capture* mcapture = NULL;
    mcapture = rmalloc0(sizeof(ripple_metric_capture));
    if(NULL == mcapture)
    {
        elog(RLOG_WARNING, "metric capture init error, out of memory");
        return NULL;
    }
    rmemset0(mcapture, 0, '\0', sizeof(ripple_metric_capture));

    mcapture->redolsn = InvalidXLogRecPtr;
    mcapture->restartlsn = InvalidXLogRecPtr;
    mcapture->confirmlsn = InvalidXLogRecPtr;
    mcapture->loadlsn = InvalidXLogRecPtr;
    mcapture->parselsn = InvalidXLogRecPtr;
    mcapture->flushlsn = InvalidXLogRecPtr;
    mcapture->trailno = 0;
    mcapture->trailstart = 0;
    mcapture->flushtimestamp = 0;
    mcapture->parsetimestamp = 0;
    mcapture->dlpackets = NULL;
    ripple_thread_mutex_init(&mcapture->dlpacketslock, NULL);
    return mcapture;
}

/* 缓存清理 */
void ripple_metric_capture_destroy(ripple_metric_capture* mcapture)
{
    if(NULL == mcapture)
    {
        return;
    }

    ripple_thread_mutex_destroy(&mcapture->dlpacketslock);
    dlist_free(mcapture->dlpackets, ripple_netpacket_destroyvoid);

    rfree(mcapture);
    mcapture = NULL;
}

