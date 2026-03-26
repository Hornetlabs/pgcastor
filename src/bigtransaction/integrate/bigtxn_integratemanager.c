#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/mpage/mpage.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/conn/conn.h"
#include "utils/string/stringinfo.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "sync/sync.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "parser/trail/parsertrail.h"
#include "rebuild/rebuild.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"
#include "increment/integrate/split/increment_integratesplittrail.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratefilterdataset.h"
#include "onlinerefresh/onlinerefresh_persist.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/integrate/split/bigtxn_integratesplittrail.h"
#include "bigtransaction/integrate/parser/bigtxn_integrateparsertrail.h"
#include "bigtransaction/integrate/sync/bigtxn_integratesync.h"
#include "bigtransaction/integrate/rebuild/bigtxn_integraterebuild.h"
#include "bigtransaction/integrate/bigtxn_integratemanager.h"
#include "increment/integrate/sync/increment_integratesync.h"
#include "increment/integrate/rebuild/increment_integraterebuild.h"
#include "metric/integrate/metric_integrate.h"
#include "increment/integrate/increment_integrate.h"

typedef enum BIGTXN_INTEGRATE_STAT
{
    BIGTXN_INTEGRATE_STAT_JOBNOP = 0x00,
    BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING, /* Worker thread starting */
    BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE      /* Waiting for worker thread to complete */
} bigtxn_integrate_stat;

/* Set state */
void bigtxn_integratemanager_stat_set(bigtxn_integratemanager* bigtxnmgr, int stat)
{
    bigtxnmgr->stat = stat;
}

/* Add incremental data to sync status table */
static bool bigtxn_integratemanager_setsynctable(bigtxn_integratemanager* bigtxnmgr,
                                                 thrnode*                 thrnode)
{
    char*      conninfo = NULL;
    char*      catalog_schema = NULL;
    PGconn*    conn = NULL;
    PGresult*  res = NULL;
    StringInfo sql = NULL;

    conninfo = guc_getConfigOption(CFG_KEY_URL);

bigtxn_integratemanager_setsynctableretry:
    sleep(1);
    if (THRNODE_STAT_TERM == thrnode->stat)
    {
        return false;
    }

    conn = conn_get(conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        goto bigtxn_integratemanager_setsynctableretry;
    }

    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(
        sql,
        "INSERT INTO \"%s\".\"%s\" \n"
        "(name, type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) \n"
        "VALUES (\'%s-%lu\', %hd, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (name) DO UPDATE SET \n"
        "(type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) =  \n"
        "(EXCLUDED.type, EXCLUDED.stat, EXCLUDED.rewind_fileid, EXCLUDED.rewind_offset, "
        "EXCLUDED.emit_fileid, EXCLUDED.emit_offset, EXCLUDED.xid, EXCLUDED.lsn); ",
        catalog_schema,
        SYNC_STATUSTABLE_NAME,
        STORAGE_BIG_TRANSACTION_DIR,
        bigtxnmgr->xid,
        3);
    res = conn_exec(conn, sql->data);
    if (NULL == res)
    {
        elog(RLOG_WARNING, "Execute commit failed");
        deleteStringInfo(sql);
        ;
        goto bigtxn_integratemanager_setsynctableretry;
    }
    PQclear(res);
    PQfinish(conn);
    deleteStringInfo(sql);

    return true;
}

/* Add incremental data to sync status table */
static bool bigtxn_integratemanager_checksyncstat(bigtxn_integratemanager* bigtxnmgr, int16* stat)
{
    char*      conninfo = NULL;
    char*      catalog_schema = NULL;
    PGconn*    conn = NULL;
    PGresult*  res = NULL;
    StringInfo sql = NULL;

    *stat = 0;

    conninfo = guc_getConfigOption(CFG_KEY_URL);

    conn = conn_get(conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        return false;
    }

    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql,
                     "select stat from \"%s\".\"%s\" where name = \'%s-%lu\';",
                     catalog_schema,
                     SYNC_STATUSTABLE_NAME,
                     STORAGE_BIG_TRANSACTION_DIR,
                     bigtxnmgr->xid);

    res = conn_exec(conn, sql->data);
    if (NULL == res)
    {
        elog(RLOG_WARNING, "Execute commit failed");
        deleteStringInfo(sql);
        return false;
    }

    if (PQntuples(res) == 1)
    {
        /*LSN information*/
        sscanf(PQgetvalue(res, 0, 0), "%hd", stat);
        elog(RLOG_WARNING, " stat %hd", *stat);
    }

    PQclear(res);
    PQfinish(conn);
    deleteStringInfo(sql);

    return true;
}

/* Start incremental worker thread */
static bool bigtxn_integratemanager_startincrementjob(bigtxn_integratemanager* bigtxnmgr)
{
    bigtxn_integratesplittrail*  bigtxnloadrecord = NULL;
    bigtxn_integrateparsertrail* bigtxnparser = NULL;
    bigtxn_integrateincsync*     bigtxnsync = NULL;
    bigtxn_integraterebuild*     bigtxnrebuild = NULL;

    /*---------------------Apply thread begin------------------*/
    bigtxnsync = bigtxn_integrateincsync_init();
    if (NULL == bigtxnsync)
    {
        elog(RLOG_WARNING, "bigtxn integrate sync init error");
        return false;
    }
    bigtxnsync->rebuild2sync = bigtxnmgr->rebuild2sync;
    bigtxnsync->base.conninfo = guc_getConfigOption(CFG_KEY_URL);
    bigtxnsync->base.name = (char*)rmalloc0(NAMEDATALEN);
    if (NULL == bigtxnsync->base.name)
    {
        elog(RLOG_WARNING, "malloc bigtxnsync out of memory");
        bigtxn_integrateincsync_free(bigtxnsync);
        return false;
    }
    rmemset0(bigtxnsync->base.name, 0, '\0', NAMEDATALEN);
    sprintf(bigtxnsync->base.name, "%s-%lu", STORAGE_BIG_TRANSACTION_DIR, bigtxnmgr->xid);

    /*---------------------Apply thread   end------------------*/

    /*---------------------Transaction rebuild thread begin----------------*/
    bigtxnrebuild = bigtxn_integraterebuild_init();
    if (NULL == bigtxnrebuild)
    {
        elog(RLOG_WARNING, "bigtxn integrate rebuild init error");
        return false;
    }
    bigtxnrebuild->parser2rebuild = bigtxnmgr->parser2rebuild;
    bigtxnrebuild->rebuild2sync = bigtxnmgr->rebuild2sync;
    bigtxnrebuild->honlinerefreshfilterdataset = bigtxnmgr->honlinerefreshfilterdataset;
    bigtxnrebuild->onlinerefreshdataset = bigtxnmgr->onlinerefreshdataset;
    /*---------------------Transaction rebuild thread   end----------------*/

    /*---------------------Parser thread begin----------------*/
    /*
     * Parser initialization
     * parserwal callback settings
     */
    bigtxnparser = bigtxn_integrateparsertrail_init();
    if (NULL == bigtxnparser)
    {
        elog(RLOG_WARNING, "bigtxn integrate parsertrail init error");
        return false;
    }
    bigtxnparser->decodingctx->parsertrail.parser2txn = bigtxnmgr->parser2rebuild;
    bigtxnparser->decodingctx->recordscache = bigtxnmgr->recordscache;

    /*---------------------Trail split thread begin--------------*/
    bigtxnloadrecord = bigtxn_integratesplittrail_init();
    if (NULL == bigtxnloadrecord)
    {
        elog(RLOG_WARNING, "bigtxn integrate init splittrail error");
        return false;
    }
    rmemset1(bigtxnloadrecord->splittrailctx->capturedata, 0, '\0', MAXPGPATH);
    snprintf(bigtxnloadrecord->splittrailctx->capturedata,
             MAXPGPATH,
             "%s/%s/%lu",
             guc_getConfigOption(CFG_KEY_TRAIL_DIR),
             STORAGE_BIG_TRANSACTION_DIR,
             bigtxnmgr->xid);

    if (false == loadtrailrecords_setloadsource(bigtxnloadrecord->splittrailctx->loadrecords,
                                                bigtxnloadrecord->splittrailctx->capturedata))
    {
        elog(RLOG_WARNING, "integrate bigtxn set capture data error");
        return false;
    }
    bigtxnloadrecord->splittrailctx->recordscache = bigtxnmgr->recordscache;
    /*---------------------Trail split thread   end--------------*/

    /*
     * Start all threads
     */
    /* Register apply thread */
    if (false == threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                      THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNSYNC,
                                      bigtxnmgr->thrsmgr->submgrref.no,
                                      (void*)bigtxnsync,
                                      bigtxn_integrateincsync_free,
                                      NULL,
                                      bigtxn_integrateincsync_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment sync job error");
        return false;
    }

    /* Register transaction rebuild thread */
    if (false == threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                      THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNREBUILD,
                                      bigtxnmgr->thrsmgr->submgrref.no,
                                      (void*)bigtxnrebuild,
                                      bigtxn_integraterebuild_free,
                                      NULL,
                                      bigtxn_integraterebuild_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment rebuild job error");
        return false;
    }

    /* Register parser thread */
    if (false == threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                      THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNPARSER,
                                      bigtxnmgr->thrsmgr->submgrref.no,
                                      (void*)bigtxnparser,
                                      bigtxn_integrateparsertrail_free,
                                      NULL,
                                      bigtxn_integrateparsertrail_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment parsertrail job error");
        return false;
    }

    /* Register loadrecords thread */
    if (false == threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                      THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNLOADRECORDS,
                                      bigtxnmgr->thrsmgr->submgrref.no,
                                      (void*)bigtxnloadrecord,
                                      bigtxn_integratesplittrail_free,
                                      NULL,
                                      bigtxn_integratesplittrail_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment splittrail job error");
        return false;
    }
    return true;
}

bigtxn_integratemanager* bigtxn_integratemanager_init(void)
{
    bigtxn_integratemanager* bigtxnmgr = NULL;

    bigtxnmgr = (bigtxn_integratemanager*)rmalloc0(sizeof(bigtxn_integratemanager));
    if (NULL == bigtxnmgr)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }

    rmemset0(bigtxnmgr, 0, '\0', sizeof(bigtxn_integratemanager));
    bigtxn_integratemanager_stat_set(bigtxnmgr, BIGTXN_INTEGRATEMANAGER_STAT_NOP);
    bigtxnmgr->thrsmgr = NULL;
    bigtxnmgr->honlinerefreshfilterdataset = NULL;
    bigtxnmgr->onlinerefreshdataset = NULL;
    bigtxnmgr->recordscache = queue_init();
    bigtxnmgr->parser2rebuild = cache_txn_init();
    bigtxnmgr->rebuild2sync = cache_txn_init();
    return bigtxnmgr;
}

void* bigtxn_integratemanager_main(void* args)
{
    int16                    syncstat = 0;
    int                      jobcnt = 0;
    bigtxn_integrate_stat    jobstat = BIGTXN_INTEGRATE_STAT_JOBNOP;
    ListCell*                lc = NULL;
    thrref*                  thr_ref = NULL;
    thrnode*                 thr_node = NULL;
    thrnode*                 bigtxnsyncthrnode = NULL;
    thrnode*                 bigtxnrebuildthrnode = NULL;
    thrnode*                 bigtxnparsertrailthrnode = NULL;
    thrnode*                 bigtxnloadrecthrnode = NULL;
    bigtxn_integratemanager* integratemgr = NULL;

    thr_node = (thrnode*)args;

    integratemgr = (bigtxn_integratemanager*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "integrate bigtxn mgr stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    bigtxn_integratemanager_checksyncstat(integratemgr, &syncstat);
    if (1 == syncstat)
    {
        elog(RLOG_DEBUG, "integrate bigtxn %lu already completed", integratemgr->xid);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    if (false == bigtxn_integratemanager_setsynctable(integratemgr, thr_node))
    {
        elog(RLOG_WARNING, "bigtxn integrate set synctable error");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    if (false == bigtxn_integratemanager_startincrementjob(integratemgr))
    {
        elog(RLOG_WARNING, "bigtxn integrate start job thread error");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Pre-obtain increment->parser thread for subsequent logical judgment */
    /* Get loadrecord thread */
    lc = integratemgr->thrsmgr->childthrrefs->head;
    thr_ref = (thrref*)lfirst(lc);
    bigtxnloadrecthrnode = threads_getthrnodebyno(integratemgr->thrsmgr->parents, thr_ref->no);
    if (NULL == bigtxnloadrecthrnode)
    {
        elog(RLOG_WARNING,
             "bigtxn integratemanager can not get load record thread by no:%lu",
             thr_ref->no);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Get parser thread */
    lc = lc->next;
    thr_ref = (thrref*)lfirst(lc);
    bigtxnparsertrailthrnode = threads_getthrnodebyno(integratemgr->thrsmgr->parents, thr_ref->no);
    if (NULL == bigtxnparsertrailthrnode)
    {
        elog(RLOG_WARNING,
             "bigtxn integratemanager can not get parser thread by no:%lu",
             thr_ref->no);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Get rebuild thread */
    lc = lc->next;
    thr_ref = (thrref*)lfirst(lc);
    bigtxnrebuildthrnode = threads_getthrnodebyno(integratemgr->thrsmgr->parents, thr_ref->no);
    if (NULL == bigtxnrebuildthrnode)
    {
        elog(RLOG_WARNING,
             "bigtxn integratemanager can not get rebuild thread by no:%lu",
             thr_ref->no);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Get sync thread */
    lc = lc->next;
    thr_ref = (thrref*)lfirst(lc);
    bigtxnsyncthrnode = threads_getthrnodebyno(integratemgr->thrsmgr->parents, thr_ref->no);
    if (NULL == bigtxnsyncthrnode)
    {
        elog(
            RLOG_WARNING, "bigtxn integratemanager can not get sync thread by no:%lu", thr_ref->no);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    jobstat = BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING;
    /* Enter work */
    while (1)
    {
        usleep(50000);
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialization/flush to disk */
            thr_node->stat = THRNODE_STAT_EXIT;
            integratemgr->stat = BIGTXN_INTEGRATEMANAGER_STAT_SIGTERM;
            bigtxn_integratemanager_checksyncstat(integratemgr, &syncstat);
            if (1 == syncstat)
            {
                integratemgr->stat = BIGTXN_INTEGRATEMANAGER_STAT_EXIT;
            }
            elog(RLOG_DEBUG, "bigtxn integratemgr %d", integratemgr->stat);

            break;
        }

        /* Wait for all child threads to start successfully */
        if (BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING == jobstat)
        {
            /* Check if started successfully */
            jobcnt = 0;
            if (false == threads_countsubmgrjobthredsabovework(integratemgr->thrsmgr->parents,
                                                               integratemgr->thrsmgr->childthrrefs,
                                                               &jobcnt))
            {
                elog(RLOG_WARNING, "integrate bigtxn count job thread above work stat error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }

            if (jobcnt != integratemgr->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE;
            continue;
        }
        else if (BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE == jobstat)
        {
            if (THRNODE_STAT_EXITED != bigtxnsyncthrnode->stat ||
                THRNODE_STAT_EXITED != bigtxnparsertrailthrnode->stat)
            {
                /* Parser thread not exited, waiting */
                continue;
            }

            /* Set loadrecords thread to exit */
            if (THRNODE_STAT_TERM > bigtxnloadrecthrnode->stat)
            {
                bigtxnloadrecthrnode->stat = THRNODE_STAT_TERM;
                continue;
            }

            /* Set rebuild thread to exit */
            if (THRNODE_STAT_TERM > bigtxnrebuildthrnode->stat)
            {
                bigtxnrebuildthrnode->stat = THRNODE_STAT_TERM;
                continue;
            }

            if (THRNODE_STAT_EXITED != bigtxnloadrecthrnode->stat ||
                THRNODE_STAT_EXITED != bigtxnrebuildthrnode->stat)
            {
                /* loadrecords, rebuild threads not exited, waiting */
                continue;
            }

            /* All threads have exited, manager thread can exit */
            jobcnt = integratemgr->thrsmgr->childthrrefs->length;
            threads_setsubmgrjobthredsfree(
                integratemgr->thrsmgr->parents, integratemgr->thrsmgr->childthrrefs, 0, jobcnt);

            /* Set this thread to exit */
            thr_node->stat = THRNODE_STAT_EXIT;
            integratemgr->stat = BIGTXN_INTEGRATEMANAGER_STAT_EXIT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

/* Release resources */
void bigtxn_integratemanager_free(void* args)
{
    bigtxn_integratemanager* bigtxnmgr = NULL;

    bigtxnmgr = (bigtxn_integratemanager*)args;

    if (NULL == bigtxnmgr)
    {
        return;
    }

    queue_destroy(bigtxnmgr->recordscache, dlist_freevoid);

    cache_txn_destroy(bigtxnmgr->parser2rebuild);

    cache_txn_destroy(bigtxnmgr->rebuild2sync);

    if (bigtxnmgr->honlinerefreshfilterdataset)
    {
        hash_destroy(bigtxnmgr->honlinerefreshfilterdataset);
    }

    if (bigtxnmgr->onlinerefreshdataset)
    {
        dlist_free(bigtxnmgr->onlinerefreshdataset->onlinerefresh,
                   onlinerefresh_integratedataset_free);
        rfree(bigtxnmgr->onlinerefreshdataset);
    }

    rfree(bigtxnmgr);
    return;
}
