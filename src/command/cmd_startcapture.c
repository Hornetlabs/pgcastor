#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/uuid/uuid.h"
#include "utils/conn/conn.h"
#include "utils/init/datainit.h"
#include "utils/daemon/process.h"
#include "utils/regex/regex.h"
#include "utils/init/databaserecv.h"
#include "misc/misc_stat.h"
#include "misc/misc_lockfiles.h"
#include "misc/misc_control.h"
#include "signal/app_signal.h"
#include "storage/file_buffer.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "queue/queue.h"
#include "snapshot/snapshot.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "serial/serial.h"
#include "threads/threads.h"
#include "xmanager/xmanager_msg.h"
#include "net/netpacket/netpacket.h"
#include "refresh/refresh_tables.h"
#include "refresh/capture/refresh_capture.h"
#include "metric/capture/metric_capture.h"
#include "increment/capture/flush/increment_captureflush.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/bigtxn.h"
#include "bigtransaction/capture/flush/bigtxn_captureflush.h"
#include "bigtransaction/capture/serial/bigtxn_captureserial.h"
#include "onlinerefresh/capture/onlinerefresh_capture.h"
#include "increment/capture/serial/increment_captureserial.h"
#include "strategy/filter_dataset.h"
#include "works/splitwork/wal/wal_define.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/parserwork_wal.h"
#include "increment/capture/increment_capture.h"
#include "command/cmd_startcapture.h"

/* Start resident threads */
static bool cmd_startcapturethreads(increment_capture* inccapture)
{
    thrnode* thr_node = NULL;

    /*-------------------------------Start resident worker threads
     * begin---------------------------------*/
    /* Threads are started in reverse order of exit, i.e., first started exits last */
    /* Start flush thread */
    if (false == threads_addpersistthread(inccapture->threads,
                                          &thr_node,
                                          THRNODE_IDENTITY_INC_CAPTURE_BIGTXNFLUSH,
                                          inccapture->persistno,
                                          (void*)inccapture->bigtxnwritestate,
                                          NULL,
                                          NULL,
                                          bigtxn_captureflush_main))
    {
        elog(RLOG_WARNING, "add capture increment bigtxn flush persist to threads error");
        return false;
    }

    /* Start big transaction serialization thread */
    if (false == threads_addpersistthread(inccapture->threads,
                                          &thr_node,
                                          THRNODE_IDENTITY_INC_CAPTURE_BIGTXNSERIAL,
                                          inccapture->persistno,
                                          (void*)inccapture->bigtxnserialstate,
                                          NULL,
                                          NULL,
                                          bigtxn_captureserial_main))
    {
        elog(RLOG_WARNING, "add capture increment bigtxn serial persist to threads error");
        return false;
    }

    /* Start increment flush thread */
    if (false == threads_addpersistthread(inccapture->threads,
                                          &thr_node,
                                          THRNODE_IDENTITY_INC_CAPTURE_FLUSH,
                                          inccapture->persistno,
                                          (void*)inccapture->writestate,
                                          NULL,
                                          NULL,
                                          increment_captureflush_main))
    {
        elog(RLOG_WARNING, "add capture increment flush persist to threads error");
        return false;
    }

    /* Start increment serialization thread */
    if (false == threads_addpersistthread(inccapture->threads,
                                          &thr_node,
                                          THRNODE_IDENTITY_INC_CAPTURE_SERIAL,
                                          inccapture->persistno,
                                          (void*)inccapture->serialstate,
                                          NULL,
                                          NULL,
                                          increment_captureserial_main))
    {
        elog(RLOG_WARNING, "add capture increment serial persist to threads error");
        return false;
    }

    /* Start parser thread */
    if (false == threads_addpersistthread(inccapture->threads,
                                          &thr_node,
                                          THRNODE_IDENTITY_INC_CAPTURE_PARSER,
                                          inccapture->persistno,
                                          (void*)inccapture->decodingctx,
                                          NULL,
                                          NULL,
                                          parserwork_wal_main))
    {
        elog(RLOG_WARNING, "add capture increment parser persist to threads error");
        return false;
    }

    /* Start walwork thread */
    if (false == threads_addpersistthread(inccapture->threads,
                                          &thr_node,
                                          THRNODE_IDENTITY_INC_CAPTURE_LOADRECORD,
                                          inccapture->persistno,
                                          (void*)inccapture->splitwalctx,
                                          NULL,
                                          NULL,
                                          splitwork_wal_main))
    {
        elog(RLOG_WARNING, "add capture increment splitwal persist to threads error");
        return false;
    }

    /* Start metric thread */
    if (false == threads_addpersistthread(inccapture->threads,
                                          &thr_node,
                                          THRNODE_IDENTITY_CAPTURE_METRIC,
                                          inccapture->persistno,
                                          (void*)inccapture->metric,
                                          NULL,
                                          NULL,
                                          metric_capture_main))
    {
        elog(RLOG_WARNING, "add capture increment metric persist to threads error");
        return false;
    }

    /*-------------------------------Start resident worker threads
     * end---------------------------------*/
    return true;
}

/* Remove specified onlinrefresh node from olrefreshing list */
static void cmd_startcapture_removeonlinerefresh(void* pinccapture, void* polrefresh)
{
    increment_capture* inccapture = NULL;

    inccapture = (increment_capture*)pinccapture;

    osal_thread_lock(&inccapture->olrefreshlock);
    dlist_deletebyvalue(inccapture->olrefreshing, polrefresh, onlinerefresh_capture_cmp, NULL);
    osal_thread_unlock(&inccapture->olrefreshlock);
}

/* Build onlinerefresh response packet to xmanager */
static void cmd_startcaputre_assembleolrefreshpacket(increment_capture* inccapture,
                                                     refresh_tables*    rtables,
                                                     bool               result,
                                                     int                errcode,
                                                     char*              msg)
{
    /*
     * 1. Build network packet
     * 2. Attach packet to metric
     */
    uint8          u8value = 0;
    int            ivalue = 0;
    int            msglen = 0;
    int            resultlen = 0;
    uint8*         uptr = NULL;
    netpacket*     npacket = NULL;
    refresh_table* rtable = NULL;

    /* totallen + crc32 + type + flag + errorlen + errorcode */
    msglen = (4 + 4 + 4 + 1 + 4 + 4);

    resultlen = strlen("start refresh: ");
    for (rtable = rtables->tables; NULL != rtable; rtable = rtable->next)
    {
        resultlen += strlen(rtable->schema);

        /* '.' */
        resultlen += 1;
        resultlen += strlen(rtable->table);
    }

    /* ' ' */
    resultlen += 1;
    /* Total length */
    if (false == result)
    {
        resultlen += strlen("failed, ");
        resultlen += strlen(msg);
        resultlen += 1;
    }
    else
    {
        resultlen += strlen(msg);
    }

    msglen += resultlen;

    npacket = netpacket_init();
    if (NULL == npacket)
    {
        return;
    }

    npacket->used = msglen;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        goto cmd_startcaputre_assembleolrefreshpacket_error;
    }

    uptr = npacket->data;

    /* Add length */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* Type */
    ivalue = XMANAGER_MSG_CAPTUREREFRESH;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* Flag */
    if (false == result)
    {
        u8value = 1;
    }
    else
    {
        u8value = 0;
    }
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;

    /* Total length */
    ivalue = (resultlen + 8);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* Error code */
    ivalue = errcode;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    ivalue = strlen("start refresh: ");
    rmemcpy1(uptr, 0, "start refresh: ", ivalue);
    uptr += ivalue;

    /* Fill table information */
    for (rtable = rtables->tables; NULL != rtable; rtable = rtable->next)
    {
        /* Schema name */
        ivalue = strlen(rtable->schema);
        rmemcpy1(uptr, 0, rtable->schema, ivalue);
        uptr += ivalue;

        /* Add separator */
        *uptr = '.';
        uptr++;

        /* Table name */
        ivalue = strlen(rtable->table);
        rmemcpy1(uptr, 0, rtable->table, ivalue);
        uptr += ivalue;
    }

    *uptr = ' ';
    uptr++;
    if (false == result)
    {
        ivalue = strlen("failed, ");
        rmemcpy1(uptr, 0, "failed, ", ivalue);
        uptr += ivalue;

        ivalue = strlen(msg);
        rmemcpy1(uptr, 0, msg, ivalue);
    }
    else
    {
        ivalue = strlen(msg);
        rmemcpy1(uptr, 0, msg, ivalue);
    }
    uptr += ivalue;

    /* Add to pending send queue */
    metric_capture_addpackets(inccapture->metric, npacket);
    return;
cmd_startcaputre_assembleolrefreshpacket_error:

    netpacket_destroy(npacket);
    return;
}

/* Start onlinerefresh node */
static bool cmd_startcapture_startonlinerefresh(increment_capture* inccapture)
{
    bool                   bmatch = true;
    bool                   increment = true;
    TransactionId          olxid = InvalidTransactionId;
    PGconn*                snapconn = NULL;
    List*                  ntables = NULL;
    HTAB*                  hnamespace = NULL;
    HTAB*                  hclass = NULL;
    HTAB*                  hsyncdataset = NULL;
    dlistnode*             dnode = NULL;
    uuid_t*                uuid = NULL;
    txn*                   olbegin_txn = NULL;
    snapshot*              olsnapshot = NULL;
    refresh_table*         rtable = NULL;
    refresh_tables*        rtables = NULL;
    onlinerefresh*         olinerefresh = NULL;
    txnstmt_onlinerefresh* olrtxnstmt = NULL;
    onlinerefresh_capture* olcapture = NULL;
    capturebase            temp_base = {'\0'};
    char                   errmsg[1024] = {0};

    /* Check for duplicate onlinerefresh tables */
    osal_thread_lock(&inccapture->olrefreshlock);
    /* Check if onlinerefresh needs to be initiated */

    if (true == dlist_isnull(inccapture->olrefreshtables))
    {
        osal_thread_unlock(&inccapture->olrefreshlock);
        return true;
    }

    /* Pause parser */
    parserwork_stat_setpause(inccapture->decodingctx);

    hnamespace = inccapture->decodingctx->trans_cache->sysdicts->by_namespace;
    hclass = inccapture->decodingctx->trans_cache->sysdicts->by_class;
    hsyncdataset = inccapture->decodingctx->trans_cache->hsyncdataset;
    for (dnode = inccapture->olrefreshtables->head; NULL != dnode;)
    {
        inccapture->olrefreshtables->head = dnode->next;
        inccapture->olrefreshtables->length--;
        rtables = (refresh_tables*)dnode->value;

        /* Fill oids in refreshtables */
        if (false == onlinerefresh_rebuildrefreshtables(rtables, hnamespace, hclass, &bmatch))
        {
            parserwork_stat_setresume(inccapture->decodingctx);

            /* Build onlinerefresh failure message */
            osal_thread_unlock(&inccapture->olrefreshlock);
            snprintf(errmsg, 1024, "ERROR: can not rebuild refresh tables.");
            cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_NOENT, errmsg);
            refresh_freetables(rtables);
            return true;
        }

        inccapture->olrefreshtables = dlist_delete(inccapture->olrefreshtables, dnode, NULL);
        break;
    }

    /* No tables to sync */
    if (!rtables || false == bmatch)
    {
        /* TODO Build onlinerefresh success message */
        snprintf(errmsg, 1024, "No table match refresh!!!");
        elog(RLOG_WARNING, errmsg);
        parserwork_stat_setresume(inccapture->decodingctx);
        osal_thread_unlock(&inccapture->olrefreshlock);
        cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, true, ERROR_SUCCESS, errmsg);
        refresh_freetables(rtables);
        return true;
    }

    /* Check for duplicate onlinerefresh tables */
    if (false == dlist_isnull(inccapture->olrefreshing))
    {
        dnode = inccapture->olrefreshing->head;
        while (dnode)
        {
            onlinerefresh_capture* olcapture = (onlinerefresh_capture*)dnode->value;
            if (false == refresh_tables_hasrepeat(olcapture->tables, rtables, &rtable))
            {
                dnode = dnode->next;
                continue;
            }
            snprintf(errmsg, 1024, "ERROR: %s.%s refreshing.", rtable->schema, rtable->table);
            elog(RLOG_WARNING, "%s, repeat table when do online refresh", errmsg);
            osal_thread_unlock(&inccapture->olrefreshlock);
            parserwork_stat_setresume(inccapture->decodingctx);
            cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_MSGEXIST, errmsg);
            refresh_freetables(rtables);
            return true;
        }
    }
    osal_thread_unlock(&inccapture->olrefreshlock);

    /* Generate new table list */
    ntables = onlinerefresh_get_newtable(hsyncdataset, rtables);
    if (ntables)
    {
        /* Add to sync dataset */
        filter_dataset_updatedatasets_onlinerefresh(hsyncdataset, ntables);
    }

    /*
     * Connect to database with repeatable read and get snapshot
     */
    /* Connect to database */
    snapconn = conn_get(guc_getConfigOption("url"));
    if (NULL == snapconn)
    {
        snprintf(errmsg, 1024, "ERROR: connect database error.");
        elog(RLOG_WARNING, errmsg);
        parserwork_stat_setresume(inccapture->decodingctx);
        cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_DISCONN, errmsg);
        refresh_freetables(rtables);
        return false;
    }

    /* Set connection to repeatable read */
    conn_settxnisolationlevel(snapconn, TXNISOLVL_REPEATABLE_READ);

    /* Get snapshot */
    olsnapshot = snapshot_buildfromdb(snapconn);

    /* Get current transaction ID, used for filtering transactions */
    olxid = databaserecv_transactionid_get(snapconn);
    /* Determine txid and xmax */
    if (increment)
    {
        /*
         * Determine if there are active transactions by comparing the latest transaction ID
         *  1. Meaning of xmin in snapshot: the minimum active transaction, when there are no
         * active transactions, xmin = xmax = next transaction to be used
         *  2. When xmin == olxid, it means no transaction was running at the moment of getting
         * the snapshot, so incremental sync is not needed
         */
        increment = (olxid == olsnapshot->xmin) ? false : true;
    }

    elog(RLOG_DEBUG, "online refresh: %s increment data", increment ? "need do" : "needn't do");

    /* Generate transaction */
    olrtxnstmt = txnstmt_onlinerefresh_init();

    /* Set increment flag */
    txnstmt_onlinerefresh_set_increment(olrtxnstmt, increment);

    /* Set txid */
    txnstmt_onlinerefresh_set_txid(olrtxnstmt, olxid);

    /* Set uuid */
    uuid = random_uuid();
    txnstmt_onlinerefresh_set_no(olrtxnstmt, uuid);

    /* Set tables */
    txnstmt_onlinerefresh_set_refreshtables(olrtxnstmt, rtables);

    /* Generate onlinerefresh node and set values */
    olinerefresh = onlinerefresh_init();
    onlinerefresh_state_setsearchmax(olinerefresh);
    onlinerefresh_no_set(olinerefresh, uuid_copy(uuid));
    onlinerefresh_txid_set(olinerefresh, olxid);
    onlinerefresh_snapshot_set(olinerefresh, olsnapshot);
    onlinerefresh_increment_set(olinerefresh, increment);
    onlinerefresh_newtables_set(olinerefresh, ntables);

    /* Only need to do incremental, xmin is not added when only doing stock data */
    if (increment)
    {
        /* Add xmin to xids */
        onlinerefresh_xids_append(olinerefresh, olsnapshot->xmin);

        /* Add xiplist from snapshot to xids */
        onlinerefresh_add_xids_from_snapshot(olinerefresh, olsnapshot);
    }

    transcache_make_xids_from_txn(inccapture->decodingctx, olinerefresh);
    /* Build begin txn */
    olbegin_txn = parserwork_build_onlinerefresh_begin_txn(olrtxnstmt, inccapture->decodingctx->parselsn);

    /* Add onlinerefresh transaction and node to increment parser */
    parserwork_decodingctx_addonlinerefresh(inccapture->decodingctx, olinerefresh, olbegin_txn);

    /*-------------------onlinerefresh capture management thread begin--------------------------*/
    /* Initialize onlinerefresh capture management thread */
    olcapture = onlinerefresh_capture_init(increment);
    if (NULL == olcapture)
    {
        snprintf(errmsg, 1024, "ERROR: add onlinerefresh error, capture out of memory.");
        elog(RLOG_WARNING, errmsg);
        cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_OOM, errmsg);
        parserwork_stat_setresume(inccapture->decodingctx);
        return false;
    }

    /* Set onlinerefresh capture management thread */
    onlinerefresh_capture_increment_set(olcapture, increment);
    misc_stat_loaddecode((void*)&temp_base);
    onlinerefresh_capture_redo_set(olcapture, temp_base.redolsn);
    onlinerefresh_capture_conninfo_set(olcapture, guc_getConfigOption("url"));
    onlinerefresh_capture_snapshot_set(olcapture, snapshot_copy(olsnapshot));
    onlinerefresh_capture_snap_conn_set(olcapture, snapconn);
    onlinerefresh_capture_no_set(olcapture, uuid_copy(uuid));
    onlinerefresh_capture_tables_set(olcapture, refresh_tables_copy(rtables));
    onlinerefresh_capture_txid_set(olcapture, (FullTransactionId)olxid);

    /* Only need to do incremental, xmin is not added when only doing stock data */
    if (increment)
    {
        /* Add minimum transaction */
        onlinerefresh_capture_xids_append(olcapture, olsnapshot->xmin);

        /* Add transactions from xlist in snapshot */
        onlinerefresh_capture_add_xids_from_snapshot(olcapture, olsnapshot);
    }

    olcapture->privdata = inccapture;
    olcapture->removeolrefresh = cmd_startcapture_removeonlinerefresh;
    osal_thread_lock(&inccapture->olrefreshlock);
    inccapture->olrefreshing = dlist_put(inccapture->olrefreshing, olcapture);
    osal_thread_unlock(&inccapture->olrefreshlock);

    /* Register and start onlinerefresh capture management thread */
    if (false == threads_addsubmanger(inccapture->threads,
                                      THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_MGR,
                                      inccapture->persistno,
                                      &olcapture->thrsmgr,
                                      (void*)olcapture,
                                      onlinerefresh_capture_destroy,
                                      NULL,
                                      onlinerefresh_capture_main))
    {
        snprintf(errmsg, 1024, "ERROR: start onlinerefresh work threads error.");
        elog(RLOG_WARNING, errmsg);
        cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_STARTTHREAD, errmsg);

        /*
         * 1. Remove onlinerefresh from increment capture
         * 2. Remove onlinerefresh transaction from increment->parser thread
         */
        dlist_deletebyvalue(inccapture->olrefreshing,
                            olcapture,
                            onlinerefresh_capture_cmp,
                            onlinerefresh_capture_destroy);

        /* Delete onlinerefresh */
        parserwork_decodingctx_removeonlinerefresh(inccapture->decodingctx, olinerefresh);
        parserwork_stat_setresume(inccapture->decodingctx);
        return false;
    }

    /*-------------------onlinerefresh capture management thread   end--------------------------*/
    /* Resume parser */
    snprintf(errmsg, 1024, "success.");
    cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, true, ERROR_SUCCESS, errmsg);
    parserwork_stat_setresume(inccapture->decodingctx);
    return true;
}

/* Capture startup */
bool cmd_startcapture(void)
{
    /*
     * 1. Change working directory
     * 2. Create lock file
     * 3. Initialize log
     * 4. Load Control information
     * 5. Delete temporary files
     * 6. Control file lock initialization
     * 7. Sync strategy initialization
     */
    bool               bret = true;
    int                gctime = 0;
    int                forcefree = 0;
    int                refreshstragety = 0;
    XLogRecPtr         endlsn = InvalidXLogRecPtr;
    XLogRecPtr         startlsn = InvalidXLogRecPtr;
    char*              wdata = NULL;
    char*              parserddl = NULL;
    snapshot*          snapshot = NULL;
    decodingcontext*   decodingctx = NULL;
    refresh_capture*   rcapture = NULL;
    refresh_tables*    refreshtables = NULL;
    refresh_tables*    mgr_tables = NULL;
    increment_capture* inccapture = NULL;

    /* Get working directory */
    wdata = guc_getdata();

    /* Check if data directory exists */
    if (false == osal_dir_exist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        bret = false;
        goto cmd_startcapture_done;
    }

    /* Change working directory */
    chdir(wdata);

    /* Set to daemon mode */
    makedaemon();

    /* Get main thread ID */
    g_mainthrid = pthread_self();

    /* Check if lock file exists in wdata, create if not, check if process is running if exists */
    misc_lockfiles_create(LOCK_FILE);

    /* Initialize log */
    log_init();

    /* Get memory reclaim time */
    gctime = guc_getConfigOptionInt(CFG_KEY_GCTIME);

    /* inccapture initialization*/
    inccapture = increment_capture_init();

    /* Load ControlData */
    misc_controldata_load();

    g_pgcastorstat = misc_controldata_stat_get();

    /* Delete temporary files */
    datainit_clear(CATALOG_DIR);

    /*
     * Start worker threads
     */
    /* Set signal handler */
    signal_init();

    refreshstragety = guc_getConfigOptionInt(CFG_KEY_REFRESHSTRAGETY);

    /* parser thread initialization */
    decodingctx = inccapture->decodingctx;

    if (PGCASTORSTAT_REWIND == g_pgcastorstat)
    {
        /* Set stat */
        parserwork_stat_setrewind(decodingctx);

        /* Load database information */
        if (!parserwork_wal_initfromdb(decodingctx))
        {
            bret = false;
            goto cmd_startcapture_done;
        }

        /* After getting basic data, persist first */
        misc_stat_decodewrite(&(inccapture->decodingctx->base), &inccapture->writestate->basefd);

        /* Temporarily set split thread timeline*/
        inccapture->splitwalctx->loadrecords->timeline = decodingctx->base.curtlid;

        /*
         * Open new connection
         *  1. Set connection transaction level to repeatable read
         *  2. Get data dictionary
         *      Open new connection, enable FULL mode for data dictionary
         *  3. Get snapshot
         */
        decodingctx->rewind_ptr->conn = conn_get(guc_getConfigOption("url"));
        if (NULL == decodingctx->rewind_ptr->conn)
        {
            elog(RLOG_WARNING, "capture can't conn database:%s", guc_getConfigOption("url"));
            bret = false;
            goto cmd_startcapture_done;
        }

        /* Start transaction and set transaction level to repeatable read */
        conn_settxnisolationlevel(decodingctx->rewind_ptr->conn, TXNISOLVL_REPEATABLE_READ);

        /* Load dictionary tables and initialize sync dataset*/
        catalog_sysdict_getfromdb(decodingctx->rewind_ptr->conn, decodingctx->trans_cache->sysdicts);

        /*Open new connection and set full, close after use*/
        if (false == catalog_sysdict_setfullmode(decodingctx->trans_cache->sysdicts->by_class))
        {
            elog(RLOG_WARNING, "capture set table replica identity full error");
            bret = false;
            goto cmd_startcapture_done;
        }

        /* Persist system dictionary for next startup */
        sysdictscache_write(decodingctx->trans_cache->sysdicts, decodingctx->base.redolsn);

        /* Generate sync dataset and persist to disk */
        filter_dataset_init(decodingctx->trans_cache->tableincludes,
                            decodingctx->trans_cache->tableexcludes,
                            decodingctx->trans_cache->sysdicts->by_namespace,
                            decodingctx->trans_cache->sysdicts->by_class);

        decodingctx->trans_cache->hsyncdataset = filter_dataset_load(decodingctx->trans_cache->sysdicts->by_namespace,
                                                                     decodingctx->trans_cache->sysdicts->by_class);

        decodingctx->trans_cache->htxnfilterdataset =
            filter_dataset_txnfilterload(decodingctx->trans_cache->sysdicts->by_namespace,
                                         decodingctx->trans_cache->sysdicts->by_class);

        decodingctx->trans_cache->sysdicts->by_relfilenode =
            cache_sysdicts_buildrelfilenode2oid(decodingctx->database, (void*)decodingctx->trans_cache->sysdicts);
        snapshot = snapshot_buildfromdb(decodingctx->rewind_ptr->conn);

        decodingctx->rewind_ptr->currentlsn = databaserecv_currentlsn_get(decodingctx->rewind_ptr->conn);
        decodingctx->rewind_ptr->currentxid = databaserecv_transactionid_get(decodingctx->rewind_ptr->conn);

        if (refreshstragety)
        {
            refreshtables = filter_dataset_buildrefreshtables(decodingctx->trans_cache->hsyncdataset);
            mgr_tables = refresh_tables_copy(refreshtables);
            parserwork_buildrefreshtransaction(decodingctx, refreshtables);

            /* Initialize refresh mgr thread related structures */
            rcapture = refresh_capture_init();
            if (NULL == rcapture)
            {
                bret = false;
                elog(RLOG_WARNING, "init refresh error");
                goto cmd_startcapture_done;
            }
            refresh_capture_setsnapshotname(rcapture, snapshot->name);

            /* TODO, tables generated after integration */
            refresh_capture_setrefreshtables(mgr_tables, rcapture);
            refresh_capture_setconn(decodingctx->rewind_ptr->conn, rcapture);
            decodingctx->rewind_ptr->conn = NULL;
        }
        else
        {
            conn_close(decodingctx->rewind_ptr->conn);
            decodingctx->rewind_ptr->conn = NULL;
        }

        /*Set snapshot to rewind*/
        rewind_strategy_setfastrewind(snapshot, decodingctx);

        startlsn = GetXlogSegmentBegin(decodingctx->rewind_ptr->redolsn, (g_walsegsize * 1048576));

        endlsn = decodingctx->rewind_ptr->currentlsn;

        /* Clean snapshot without cleaning snapshot->xids */
        snapshot_free(snapshot);
    }
    else
    {
        /* Set stat */
        parserwork_stat_setrunning(decodingctx);

        /* Load decodingctx info */
        parserwork_walinitphase2(decodingctx);

        /* Set split thread timeline */
        inccapture->splitwalctx->loadrecords->timeline = decodingctx->base.curtlid;

        startlsn = decodingctx->base.redolsn;
        endlsn = InvalidXLogRecPtr;
    }

    /*Set splitwork split start and end points*/
    inccapture->decodingctx->callback.setloadlsn((void*)inccapture, startlsn, endlsn);

    if (NULL != parserddl)
    {
        parserddl = guc_getConfigOption(CFG_KEY_DDL);
        if (strlen("on") == strlen(parserddl) && 0 == strcmp("on", parserddl))
        {
            g_parserddl = 1;
        }
        else
        {
            g_parserddl = 0;
        }
    }

    /*
     * Add main resident thread
     */
    if (false == threads_addpersist(inccapture->threads, &inccapture->persistno, "CAPTURE INCREMENT"))
    {
        bret = false;
        elog(RLOG_WARNING, "add capture increment persist to threads error");
        goto cmd_startcapture_done;
    }

    /* Start resident worker threads */
    if (false == cmd_startcapturethreads(inccapture))
    {
        bret = false;
        elog(RLOG_WARNING, "start capture increment persist job threads error");
        goto cmd_startcapture_done;
    }

    /* Start refresh mgr */
    if (NULL != rcapture)
    {
        /* Register refresh management thread */
        if (false == threads_addsubmanger(inccapture->threads,
                                          THRNODE_IDENTITY_CAPTURE_REFRESH_MGR,
                                          inccapture->persistno,
                                          &rcapture->thrsmgr,
                                          (void*)rcapture,
                                          refresh_capture_free,
                                          NULL,
                                          refresh_capture_main))
        {
            bret = false;
            elog(RLOG_WARNING, "start refresh mgr failed");
            goto cmd_startcapture_done;
        }
    }

    /* Unblock signals */
    singal_setmask();

    elog(RLOG_INFO, "capture start, pid:%d", getpid());

    log_destroyerrorstack();
    /* Close stdin/stdout/stderr */
    closestd();

    while (1)
    {
        /* Print log information */
        if (true == g_gotsigterm)
        {
            /* Caught sigterm signal, set thread to exit */
            threads_exit(inccapture->threads);
            break;
        }

        /* Start onlinerefresh */
        if (false == cmd_startcapture_startonlinerefresh(inccapture))
        {
            elog(RLOG_WARNING, "capture add onlinerefresh error");
            continue;
        }

        /* Start threads */
        threads_startthread(inccapture->threads);

        /* Try to capture abnormal threads */
        threads_tryjoin(inccapture->threads);

        /* Recycle FREE nodes */
        threads_thrnoderecycle(inccapture->threads);

        if (false == threads_hasthrnode(inccapture->threads))
        {
            /* All threads exit, main thread exits */
            /* Record */
            misc_stat_decodewrite(&(inccapture->writestate->base), &inccapture->writestate->basefd);
            break;
        }

        if (0 == gctime)
        {
            ;
        }
        else if (gctime > forcefree)
        {
            forcefree++;
        }
        else
        {
            /* Reclaim memory */
            malloc_trim(0);
            forcefree = 0;
        }
        usleep(100000);
        continue;
    }

    /* All threads have exited, main thread also exits */
    /* Normal exit, no need to send info to xmanager */
    /* Persist new address to disk */
    elog(RLOG_INFO,
         "capture writestate persistent trail, redolsn: %X/%X, restartlsn %X/%X, last commit "
         "lsn:%X/%X, fileid:%lu, offset:%u, timeline:%u",
         (uint32)(inccapture->writestate->base.redolsn >> 32),
         (uint32)inccapture->writestate->base.redolsn,
         (uint32)(inccapture->writestate->base.restartlsn >> 32),
         (uint32)inccapture->writestate->base.restartlsn,
         (uint32)(inccapture->writestate->base.confirmedlsn >> 32),
         (uint32)inccapture->writestate->base.confirmedlsn,
         inccapture->writestate->base.fileid,
         inccapture->writestate->base.fileoffset,
         inccapture->writestate->base.curtlid);

cmd_startcapture_done:

    /* inccapture resource cleanup*/
    increment_capture_destroy(inccapture);

    /* control file memory release */
    misc_controldata_destroy();

    /* Lock file release */
    misc_lockfiles_unlink(0, NULL);

    guc_destroy();

    /* Print leaked memory */
    mem_print(MEMPRINT_ALL);
    if (true == bret)
    {
        /* Already entered logic processing, can exit directly */
        exit(0);
    }

    /* Report failure to xmanager */
    return bret;
}
