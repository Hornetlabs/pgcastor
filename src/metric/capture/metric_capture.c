#include "app_incl.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "command/cmd.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "refresh/refresh_tables.h"
#include "net/netiomp/netiomp.h"
#include "net/netpacket/netpacket.h"
#include "net/netclient.h"
#include "xmanager/xmanager_msg.h"
#include "metric/capture/metric_capture.h"

static bool metric_capture_onlinerefresh(metric_capture* mcapture, netpacket* npacket)
{
    int             ivalue = 0;
    int             index = 0;
    int             tablecnt = 0;
    refresh_table*  rtable = NULL;
    refresh_tables* rtables = NULL;
    uint8*          uptr = NULL;

    uptr = npacket->data;

    /* Skip msglen + crc32 + cmdtype */
    uptr += 12;

    /* Total row count */
    rmemcpy1(&tablecnt, 0, uptr, 4);
    tablecnt = r_ntoh32(tablecnt);
    uptr += 4;

    rtables = refresh_tables_init();
    if (NULL == rtables)
    {
        elog(RLOG_WARNING, "capture onlinerefresh out of memory");
        return false;
    }

    for (index = 0; index < tablecnt; index++)
    {
        rtable = refresh_table_init();
        if (NULL == rtable)
        {
            elog(RLOG_WARNING, "capture onlinerefresh malloc refreshtable, out of memory");
            goto metric_capture_onlinerefresh_error;
        }

        refresh_tables_add(rtable, rtables);

        /* schema */
        rmemcpy1(&ivalue, 0, uptr, 4);
        ivalue = r_ntoh32(ivalue);
        uptr += 4;
        ivalue += 1;

        rtable->schema = rmalloc0(ivalue);
        if (NULL == rtable->schema)
        {
            elog(RLOG_WARNING, "capture onlinerefresh malloc refreshtable schema, out of memory");
            goto metric_capture_onlinerefresh_error;
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
            goto metric_capture_onlinerefresh_error;
        }
        rmemset0(rtable->table, 0, '\0', ivalue);
        ivalue -= 1;
        rmemcpy0(rtable->table, 0, uptr, ivalue);
        uptr += ivalue;
    }

    mcapture->addonlinerefresh(mcapture->privdata, (void*)rtables);
    return true;

metric_capture_onlinerefresh_error:

    if (NULL != rtables)
    {
        refresh_freetables(rtables);
    }
    return false;
}

/* Parse network packet */
static bool metric_capture_parsenetpacket(metric_capture* mcapture, netpacket* npacket)
{
    int    msgtype = 0;
    uint8* uptr = NULL;

    /* Dispatch based on msgtype */
    uptr = npacket->data;
    uptr += 8;

    rmemcpy1(&msgtype, 0, uptr, 4);
    msgtype = r_ntoh32(msgtype);

    if (XMANAGER_MSG_IDENTITYCMD != msgtype && XMANAGER_MSG_CAPTUREINCREMENT != msgtype &&
        XMANAGER_MSG_CAPTUREREFRESH != msgtype && XMANAGER_MSG_CAPTUREBIGTXN)
    {
        elog(RLOG_WARNING, "capture metric got unknown msg type from xmanager:%d", msgtype);
        return false;
    }

    if (XMANAGER_MSG_CAPTUREREFRESH == msgtype)
    {
        /* Parse database packet, generate onlinerefresh file */
        metric_capture_onlinerefresh(mcapture, npacket);
    }

    return true;
}

/* Attempt to parse packets */
static bool metric_capture_tryparsepacket(metric_capture* mcapture, netclient* netclient)
{
    netpacket* npacket = NULL;

    while (1)
    {
        npacket = queue_tryget(netclient->rpackets);
        if (NULL == npacket)
        {
            return true;
        }

        if (npacket->offset != npacket->used)
        {
            if (false == queue_puthead(netclient->rpackets, npacket))
            {
                elog(RLOG_WARNING, "put uncomplete packet to read queue error");
                return false;
            }
            return true;
        }

        if (false == metric_capture_parsenetpacket(mcapture, npacket))
        {
            elog(RLOG_WARNING, "capture metric parse net packet error");
            netpacket_destroy(npacket);
            return false;
        }

        netpacket_destroy(npacket);
    }
    return true;
}

/* Incremental message */
static bool metric_capture_assembleincrementpacket(metric_capture* mcapture, netclient* netclient)
{
    int        len = 0;
    int        ivalue = 0;
    XLogRecPtr lsnvalue = 0;
    uint8*     uptr = NULL;
    char*      jobname = NULL;
    netpacket* npacket = NULL;

    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble packet init packet error, out of memory");
        return false;
    }
    jobname = guc_getConfigOption(CFG_KEY_JOBNAME);

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
    npacket->data = netpacket_data_init(len);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "assemble packet init packet data error, out of memory");
        netpacket_destroy(npacket);
        return false;
    }
    uptr = npacket->data;

    /* Length */
    ivalue = len;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 not processed for now */
    uptr += 4;

    /* msgtype */
    ivalue = XMANAGER_MSG_CAPTUREINCREMENT;
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

    if (false == queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble capture increment metric put packet to write packet error");
        netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* Assemble identity message */
static bool metric_capture_assembleidentitypacket(metric_capture* mcapture, netclient* netclient)
{
    int8       result = 0;
    int        len = 0;
    int        ivalue = 0;
    uint8*     uptr = NULL;
    char*      jobname = NULL;
    char*      data = NULL;
    netpacket* npacket = NULL;

    jobname = guc_getConfigOption(CFG_KEY_JOBNAME);
    data = guc_getdata();

    /*
     * Content
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

    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble identity msg init net packet error , out of memory");
        return false;
    }
    npacket->used = len;

    npacket->data = netpacket_data_init(len);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "assemble identity msg init net packet data error , out of memory");
        netpacket_destroy(npacket);
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
    ivalue = XMANAGER_MSG_IDENTITYCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobtype */
    ivalue = XMANAGER_METRICNODETYPE_CAPTURE;
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
    ivalue = XMANAGER_MSG_STARTCMD;
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

    if (false == queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble capture identity put packet to write packet error");
        netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* Assemble message packet */
static bool metric_capture_assemblepacket(metric_capture* mcapture, netclient* netclient)
{
    if (false == metric_capture_assembleincrementpacket(mcapture, netclient))
    {
        elog(RLOG_WARNING, "metric capture assemble increment packet error");
        return false;
    }
    return true;
}

/* Add packets to be sent */
void metric_capture_addpackets(metric_capture* mcapture, netpacket* npacket)
{
    /* Acquire lock */
    osal_thread_lock(&mcapture->dlpacketslock);
    mcapture->dlpackets = dlist_put(mcapture->dlpackets, npacket);
    osal_thread_unlock(&mcapture->dlpacketslock);
}

/* Status main thread */
void* metric_capture_main(void* args)
{
    int             fd = -1;
    int             port = 0;
    int             interval = 5000;
    int             intervaltime = 0;
    uint64          trailno = 0;
    uint64          trailstart = 0;
    uint64          parsetimestamp = 0;
    uint64          flushtimestamp = 0;
    XLogRecPtr      redolsn = InvalidXLogRecPtr;
    XLogRecPtr      restartlsn = InvalidXLogRecPtr;
    XLogRecPtr      confirmlsn = InvalidXLogRecPtr;
    XLogRecPtr      loadlsn = InvalidXLogRecPtr;
    XLogRecPtr      parselsn = InvalidXLogRecPtr;
    XLogRecPtr      flushlsn = InvalidXLogRecPtr;
    dlistnode*      dlnode = NULL;
    thrnode*        thr_node = NULL;
    metric_capture* mcapture = NULL;
    netclient       netclient = {0};

    thr_node = (thrnode*)args;
    mcapture = (metric_capture*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "increment capture metric stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    port = guc_getConfigOptionInt(CFG_KEY_XMANAGER_PORT);
    if (0 == port)
    {
        sprintf(netclient.svrport, "%s", RMANAGER_PORT);
    }
    else
    {
        sprintf(netclient.svrport, "%d", port);
    }
    elog(RLOG_DEBUG, "capture metric port:%s", netclient.svrport);

    /* Set the network model used */
    netclient.ops = netiomp_init(NETIOMP_TYPE_POLL);

    /* Apply for base information for subsequent descriptor processing */
    if (false == netclient.ops->create(&netclient.base))
    {
        elog(RLOG_WARNING, "capture metric main iomp module error");
        thr_node->stat = THRNODE_STAT_ABORT;
        netclient_destroy(&netclient);
        pthread_exit(NULL);
    }

    /* Set type */
    netclient_setprotocoltype(&netclient, NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN);

    netclient_sethbtimeout(&netclient, NET_HBTIME);
    netclient_settimeout(&netclient, 0);

    netclient.base->timeout = NET_POLLTIMEOUT;
    netclient.status = NETCLIENTCONN_STATUS_NOP;
    netclient.wpackets = queue_init();
    if (NULL == netclient.wpackets)
    {
        elog(RLOG_WARNING, "capture metric main init send queue error");
        thr_node->stat = THRNODE_STAT_ABORT;
        netclient_destroy(&netclient);
        pthread_exit(NULL);
    }
    netclient.rpackets = queue_init();
    if (NULL == netclient.rpackets)
    {
        elog(RLOG_WARNING, "capture metric main init recv queue error");
        thr_node->stat = THRNODE_STAT_ABORT;
        netclient_destroy(&netclient);
        pthread_exit(NULL);
    }
    netclient.callback = NULL;

    while (1)
    {
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Parser */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Attempt to connect to xmanager */
        if (NETCLIENTCONN_STATUS_NOP == netclient.status)
        {
            /* Connect to xmanager */
            if (false == netclient_tryconn(&netclient))
            {
                elog(RLOG_WARNING, "can not connect xmanager");
            }

            if (NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
            {
                if (false == metric_capture_assembleidentitypacket(mcapture, &netclient))
                {
                    elog(RLOG_WARNING, "metric capture assemble identity packet error");
                    netclient_clear(&netclient);
                }
            }
        }

        /* Acquire lock */
        osal_thread_lock(&mcapture->dlpacketslock);
        /* Merge packets */
        if (false == dlist_isnull(mcapture->dlpackets))
        {
            for (dlnode = mcapture->dlpackets->head; NULL != dlnode; dlnode = dlnode->next)
            {
                queue_put(netclient.wpackets, dlnode->value);
            }

            dlist_free(mcapture->dlpackets, NULL);
            mcapture->dlpackets = NULL;
        }
        osal_thread_unlock(&mcapture->dlpacketslock);

        /* Not timed out and no data needs to be sent */
        if (intervaltime <= interval)
        {
            if (NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
            {
                if (false == netclient_desc2(&netclient))
                {
                    elog(RLOG_WARNING, "metric capture iomp desc error");
                    netclient_clear(&netclient);
                }

                if (false == metric_capture_tryparsepacket(mcapture, &netclient))
                {
                    /* Clear queue, close descriptor */
                    elog(RLOG_WARNING, "metric capture parse packet error");
                    netclient_clear(&netclient);
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

        if (NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
        {
            if (false == netclient_desc2(&netclient))
            {
                elog(RLOG_WARNING, "metric capture iomp desc error");
                netclient_clear(&netclient);
            }

            if (false == metric_capture_tryparsepacket(mcapture, &netclient))
            {
                /* Clear queue, close descriptor */
                elog(RLOG_WARNING, "metric capture parse packet error");
                netclient_clear(&netclient);
            }
        }

        if (intervaltime <= interval)
        {
            continue;
        }
        intervaltime = 0;

        /* Build send packet in connected state */
        if (NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
        {
            if (false == metric_capture_assemblepacket(mcapture, &netclient))
            {
                /* Clear queue, close descriptor */
                elog(RLOG_WARNING, "metric capture assemble packet error");
                netclient_clear(&netclient);
            }
        }

        if (redolsn != mcapture->redolsn || restartlsn != mcapture->restartlsn || confirmlsn != mcapture->confirmlsn ||
            loadlsn != mcapture->loadlsn || parselsn != mcapture->parselsn || flushlsn != mcapture->flushlsn ||
            parsetimestamp != mcapture->parsetimestamp || flushtimestamp != mcapture->flushtimestamp ||
            trailno != mcapture->trailno || trailstart != mcapture->trailstart)
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

            elog(RLOG_INFO,
                 "PGCASTOR Capture RedoLSN:            %X/%X",
                 (uint32)(mcapture->redolsn >> 32),
                 (uint32)(mcapture->redolsn));
            elog(RLOG_INFO,
                 "PGCASTOR Capture RestartLSN:         %X/%X",
                 (uint32)(mcapture->restartlsn >> 32),
                 (uint32)(mcapture->restartlsn));
            elog(RLOG_INFO,
                 "PGCASTOR Capture ConfirmLSN:         %X/%X",
                 (uint32)(mcapture->confirmlsn >> 32),
                 (uint32)(mcapture->confirmlsn));
            elog(RLOG_INFO,
                 "PGCASTOR Capture LoadLSN:            %X/%X",
                 (uint32)(mcapture->loadlsn >> 32),
                 (uint32)(mcapture->loadlsn));
            elog(RLOG_INFO,
                 "PGCASTOR Capture ParseLSN:           %X/%X",
                 (uint32)(mcapture->parselsn >> 32),
                 (uint32)(mcapture->parselsn));
            elog(RLOG_INFO,
                 "PGCASTOR Capture FlushLSN:           %X/%X",
                 (uint32)(mcapture->flushlsn >> 32),
                 (uint32)(mcapture->flushlsn));
            elog(RLOG_INFO, "PGCASTOR Capture ParseTimestamp:     %lu", mcapture->parsetimestamp);
            elog(RLOG_INFO, "PGCASTOR Capture FlushTimestamp:     %lu", mcapture->flushtimestamp);
            elog(RLOG_INFO, "PGCASTOR Capture Trail:              %lX/%lX", mcapture->trailno, mcapture->trailstart);

            /* Persist data to disk */
            fd = osal_basic_open_file(CAPTURE_STATUS_FILE_TEMP, O_RDWR | O_CREAT | BINARY);
            if (-1 == fd)
            {
                elog(RLOG_WARNING, "open file:capture/%s error, %s", CAPTURE_STATUS_FILE_TEMP, strerror(errno));
                continue;
            }
            osal_file_write(fd, (char*)mcapture, sizeof(metric_capture));

            osal_file_close(fd);

            /* Rename */
            if (osal_durable_rename(CAPTURE_STATUS_FILE_TEMP, CAPTURE_STATUS_FILE, RLOG_WARNING) != 0)
            {
                elog(RLOG_WARNING,
                     "Error renaming capture file %s 2 %s",
                     CAPTURE_STATUS_FILE_TEMP,
                     CAPTURE_STATUS_FILE);
            }
        }
    }

    netclient_destroy(&netclient);
    pthread_exit(NULL);
    return NULL;
}

/* Initialize status structure */
metric_capture* metric_capture_init(void)
{
    metric_capture* mcapture = NULL;
    mcapture = rmalloc0(sizeof(metric_capture));
    if (NULL == mcapture)
    {
        elog(RLOG_WARNING, "metric capture init error, out of memory");
        return NULL;
    }
    rmemset0(mcapture, 0, '\0', sizeof(metric_capture));

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
    osal_thread_mutex_init(&mcapture->dlpacketslock, NULL);
    return mcapture;
}

/* Cache cleanup */
void metric_capture_destroy(metric_capture* mcapture)
{
    if (NULL == mcapture)
    {
        return;
    }

    osal_thread_mutex_destroy(&mcapture->dlpacketslock);
    dlist_free(mcapture->dlpackets, netpacket_destroyvoid);

    rfree(mcapture);
    mcapture = NULL;
}
