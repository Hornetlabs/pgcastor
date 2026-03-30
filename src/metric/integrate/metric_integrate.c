#include "app_incl.h"
#include "port/thread/thread.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netpacket/netpacket.h"
#include "net/netclient.h"
#include "xmanager/xmanager_msg.h"
#include "metric/integrate/metric_integrate.h"

/* Parse network packet */
static bool metric_integrate_parsenetpacket(metric_integrate* mintegrate, netpacket* npacket)
{
    int    msgtype = 0;
    uint8* uptr = NULL;

    /* Dispatch based on msgtype */
    uptr = npacket->data;
    uptr += 8;

    rmemcpy1(&msgtype, 0, uptr, 4);
    msgtype = r_ntoh32(msgtype);

    if (XMANAGER_MSG_IDENTITYCMD != msgtype && XMANAGER_MSG_INTEGRATEINCREMENT != msgtype &&
        XMANAGER_MSG_INTEGRATEONLINEREFRESH != msgtype && XMANAGER_MSG_INTEGRATEBIGTXN)
    {
        elog(RLOG_WARNING, "integrate metric got unknown msg type from xmanager:%d", msgtype);
        return false;
    }

    return true;
}

/* Attempt to parse packets */
static bool metric_integrate_tryparsepacket(metric_integrate* mintegrate, netclient* netclient)
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

        if (false == metric_integrate_parsenetpacket(mintegrate, npacket))
        {
            elog(RLOG_WARNING, "integrate metric parse net packet error");
            netpacket_destroy(npacket);
            return false;
        }

        netpacket_destroy(npacket);
    }
    return true;
}

/* Incremental message */
static bool metric_integrate_assembleincrementpacket(metric_integrate* mintegrate, netclient* netclient)
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

    /* loadlsn/synclsn */
    len += 16;

    /* loadtrailno/loadtrailstart/synctrailno/synctrailstart/loadtimestamp/synctimestamp */
    len += 48;

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
    ivalue = XMANAGER_MSG_INTEGRATEINCREMENT;
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

    /* load lsn */
    lsnvalue = mintegrate->loadlsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* synclsn */
    lsnvalue = mintegrate->synclsn;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* loadtrailno */
    lsnvalue = mintegrate->loadtrailno;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* loadtrailstart */
    lsnvalue = mintegrate->loadtrailstart;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* synctrailno */
    lsnvalue = mintegrate->synctrailno;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* synctrailstart */
    lsnvalue = mintegrate->synctrailstart;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* loadtimestamp */
    lsnvalue = mintegrate->loadtimestamp;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    /* synctimestamp */
    lsnvalue = mintegrate->synctimestamp;
    lsnvalue = r_hton64(lsnvalue);
    rmemcpy1(uptr, 0, &lsnvalue, 8);
    uptr += 8;

    if (false == queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble integrate increment metric put packet to write packet error");
        netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* Assemble identity message */
static bool metric_integrate_assembleidentitypacket(metric_integrate* mintegrate, netclient* netclient)
{
    int8       result = 0;
    int        len = 0;
    int        ivalue = 0;
    uint8*     uptr = NULL;
    char*      jobname = NULL;
    char*      data = NULL;
    char*      traildir = NULL;
    netpacket* npacket = NULL;

    jobname = guc_getConfigOption(CFG_KEY_JOBNAME);
    traildir = guc_getConfigOption(CFG_KEY_TRAIL_DIR);
    data = guc_getdata();

    /*
     * Content
     *  jobname + data dir + config dir + trail dir
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
    ivalue = XMANAGER_METRICNODETYPE_INTEGRATE;
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

    /* traildir len */
    ivalue = strlen(traildir);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* traildir */
    rmemcpy1(uptr, 0, traildir, strlen(traildir));
    uptr += strlen(traildir);

    if (false == queue_put(netclient->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble integrate identity put packet to write packet error");
        netpacket_destroy(npacket);
        return false;
    }
    return true;
}

/* Assemble message packet */
static bool metric_integrate_assemblepacket(metric_integrate* mintegrate, netclient* netclient)
{
    if (false == metric_integrate_assembleincrementpacket(mintegrate, netclient))
    {
        elog(RLOG_WARNING, "metric integrate assemble increment packet error");
        return false;
    }
    return true;
}

/* Status main thread */
void* metric_integrate_main(void* args)
{
    int               fd = -1;
    int               port = 0;
    int               interval = 5000;
    int               intervaltime = 0;
    XLogRecPtr        loadlsn = InvalidXLogRecPtr;
    XLogRecPtr        synclsn = InvalidXLogRecPtr;
    uint64            loadtrailno = 0;
    uint64            loadtrailstart = 0;
    uint64            synctrailno = 0;
    uint64            synctrailstart = 0;
    uint64            loadtimestamp = 0;
    uint64            synctimestamp = 0;
    thrnode*          thr_node = NULL;
    metric_integrate* mintegrate = NULL;
    netclient         netclient = {0};

    thr_node = (thrnode*)args;
    mintegrate = (metric_integrate*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "increment integrate metric stat exception, expected state is THRNODE_STAT_STARTING");
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
    elog(RLOG_DEBUG, "integrate metric port:%s", netclient.svrport);

    /* Set the network model used */
    netclient.ops = netiomp_init(NETIOMP_TYPE_POLL);

    /* Apply for base information for subsequent descriptor processing */
    if (false == netclient.ops->create(&netclient.base))
    {
        elog(RLOG_WARNING, "integrate metric main iomp module error");
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
        elog(RLOG_WARNING, "integrate metric main init send queue error");
        thr_node->stat = THRNODE_STAT_ABORT;
        netclient_destroy(&netclient);
        pthread_exit(NULL);
    }
    netclient.rpackets = queue_init();
    if (NULL == netclient.rpackets)
    {
        elog(RLOG_WARNING, "integrate metric main init recv queue error");
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
                if (false == metric_integrate_assembleidentitypacket(mintegrate, &netclient))
                {
                    elog(RLOG_WARNING, "metric integrate assemble identity packet error");
                    netclient_clear(&netclient);
                }
            }
        }

        /* Not timed out and no data needs to be sent */
        if (intervaltime <= interval && true == queue_isnull(netclient.wpackets) &&
            true == queue_isnull(netclient.rpackets))
        {
            intervaltime += 1000;
            sleep(1);
            continue;
        }

        if (NETCLIENTCONN_STATUS_CONNECTED == netclient.status)
        {
            if (false == netclient_desc2(&netclient))
            {
                elog(RLOG_WARNING, "metric integrate iomp desc error");
                netclient_clear(&netclient);
            }
            else if (false == metric_integrate_tryparsepacket(mintegrate, &netclient))
            {
                /* Clear queue, close descriptor */
                elog(RLOG_WARNING, "metric integrate parse packet error");
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
            if (false == metric_integrate_assemblepacket(mintegrate, &netclient))
            {
                /* Clear queue, close descriptor */
                elog(RLOG_WARNING, "metric integrate assemble packet error");
                netclient_clear(&netclient);
            }
        }

        if (loadlsn != mintegrate->loadlsn || synclsn != mintegrate->synclsn ||
            loadtrailno != mintegrate->loadtrailno || loadtrailstart != mintegrate->loadtrailstart ||
            synctrailno != mintegrate->synctrailno || synctrailstart != mintegrate->synctrailstart ||
            loadtimestamp != mintegrate->loadtimestamp || synctimestamp != mintegrate->synctimestamp)
        {
            loadlsn = mintegrate->loadlsn;
            synclsn = mintegrate->synclsn;
            loadtrailno = mintegrate->loadtrailno;
            loadtrailstart = mintegrate->loadtrailstart;
            synctrailno = mintegrate->synctrailno;
            synctrailstart = mintegrate->synctrailstart;
            loadtimestamp = mintegrate->loadtimestamp;
            synctimestamp = mintegrate->synctimestamp;

            elog(RLOG_INFO,
                 "PGCASTOR Integrate LoadLSN:              %X/%X",
                 (uint32)(mintegrate->loadlsn >> 32),
                 (uint32)(mintegrate->loadlsn));
            elog(RLOG_INFO,
                 "PGCASTOR Integrate SyncLSN:              %X/%X",
                 (uint32)(mintegrate->synclsn >> 32),
                 (uint32)(mintegrate->synclsn));
            elog(RLOG_INFO,
                 "PGCASTOR Integrate LoadTrail:            %lX/%lX",
                 mintegrate->loadtrailno,
                 mintegrate->loadtrailstart);
            elog(RLOG_INFO,
                 "PGCASTOR Integrate SyncTrail:            %lX/%lX",
                 mintegrate->synctrailno,
                 mintegrate->synctrailstart);
            elog(RLOG_INFO, "PGCASTOR Integrate LoadTimestamp:        %lu", mintegrate->loadtimestamp);
            elog(RLOG_INFO, "PGCASTOR Integrate SyncTimestamp:        %lu", mintegrate->synctimestamp);

            /* Persist data to disk */
            fd = osal_basic_open_file(INTEGRATE_STATUS_FILE_TEMP, O_RDWR | O_CREAT | BINARY);
            if (-1 == fd)
            {
                elog(RLOG_WARNING, "open file:integrate/%s error, %s", INTEGRATE_STATUS_FILE_TEMP, strerror(errno));
            }
            osal_file_write(fd, (char*)mintegrate, sizeof(metric_integrate));

            osal_file_close(fd);

            /* Rename */
            if (osal_durable_rename(INTEGRATE_STATUS_FILE_TEMP, INTEGRATE_STATUS_FILE, RLOG_WARNING) != 0)
            {
                elog(RLOG_WARNING,
                     "Error renaming integrate file %s 2 %s",
                     INTEGRATE_STATUS_FILE_TEMP,
                     INTEGRATE_STATUS_FILE);
            }
        }
    }

    netclient_destroy(&netclient);
    pthread_exit(NULL);
    return NULL;
}

metric_integrate* metric_integrate_init(void)
{
    metric_integrate* mintegrate = NULL;

    mintegrate = rmalloc0(sizeof(metric_integrate));
    if (NULL == mintegrate)
    {
        elog(RLOG_WARNING, "metric integrate init out of memory");
        return NULL;
    }
    rmemset0(mintegrate, 0, '\0', sizeof(metric_integrate));

    mintegrate->loadlsn = InvalidXLogRecPtr;
    mintegrate->synclsn = InvalidXLogRecPtr;
    mintegrate->loadtrailno = 0;
    mintegrate->loadtrailstart = 0;
    mintegrate->synctrailno = 0;
    mintegrate->synctrailstart = 0;
    mintegrate->loadtimestamp = 0;
    mintegrate->synctimestamp = 0;

    return mintegrate;
}

/* Cache cleanup */
void metric_integrate_destroy(void* args)
{
    metric_integrate* mintegrate = NULL;

    mintegrate = (metric_integrate*)args;

    if (NULL == mintegrate)
    {
        return;
    }

    rfree(mintegrate);
    return;
}
