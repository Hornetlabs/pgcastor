#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "port/net/net.h"
#include "net/netiomp/netiomp.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "misc/misc_stat.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "snapshot/snapshot.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "refresh/capture/refresh_capture.h"
#include "refresh/sharding2file/refresh_sharding2file.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "onlinerefresh/capture/onlinerefresh_capture.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "works/splitwork/wal/wal_define.h"
#include "serial/serial.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_parser.h"
#include "onlinerefresh/capture/serial/onlinerefresh_captureserial.h"
#include "onlinerefresh/capture/loadrecord/onlinerefresh_captureloadrecord.h"
#include "onlinerefresh/capture/flush/onlinerefresh_captureflush.h"
#include "utils/conn/conn.h"
#include "works/parserwork/wal/parserwork_wal.h"

typedef enum ONLINEREFRESH_CAPTURE_STAT
{
    ONLINEREFRESH_CAPTURE_STAT_JOBNOP = 0x00,
    ONLINEREFRESH_CAPTURE_STAT_JOBSTARTING,         /* Job thread starting */
    ONLINEREFRESH_CAPTURE_STAT_JOBWORKING,          /* Job thread working */
    ONLINEREFRESH_CAPTURE_STAT_JOBWAITREFRESHDONE,  /* Waiting for bulk work thread to complete */
    ONLINEREFRESH_CAPTURE_STAT_JOBWAITINCREMENTDONE /* Waiting for bulk work thread to complete */
} onlinerefresh_capture_stat;

/*---------------------Basic initialization and settings begin--------------------------------*/

/* Print onlinerefresh information */
static void onlinerefresh_capture_print(onlinerefresh_capture* olcapture)
{
    HASH_SEQ_STATUS    snap_status;
    char*              uuid = NULL;
    dlistnode*         dlnode = NULL;
    FullTransactionId* xid_p = NULL;
    snapshot_xid*      entry = NULL;
    refresh_table*     table = NULL;

    uuid = uuid2string(olcapture->no);
    elog(RLOG_INFO, "onlinerefresh capture :%s, increment: %d, txid:%lu, redo:%X/%X", uuid,
         olcapture->increment, olcapture->txid, (uint32)(olcapture->redo.wal.lsn >> 32),
         (uint32)(olcapture->redo.wal.lsn));

    elog(RLOG_INFO, "onlinerefresh capture:%s, snapshot: %s, xmin:%lu, xmax:%lu", uuid,
         olcapture->snapshot->name, olcapture->snapshot->xmin, olcapture->snapshot->xmax);
    hash_seq_init(&snap_status, olcapture->snapshot->xids);
    while (NULL != (entry = hash_seq_search(&snap_status)))
    {
        /* Copy hash */
        elog(RLOG_INFO, "onlinerefresh snapshotxids:%s - %lu", uuid, entry->xid);
    }

    table = olcapture->tables->tables;
    while (NULL != table)
    {
        elog(RLOG_INFO, "onlinerefresh refresh table:%s - %s.%s", uuid, table->schema,
             table->table);
        table = table->next;
    }

    if (dlist_isnull(olcapture->xids))
    {
        rfree(uuid);
        return;
    }

    dlnode = olcapture->xids->head;
    while (dlnode)
    {
        xid_p = (FullTransactionId*)dlnode->value;
        elog(RLOG_INFO, "onlinerefresh xids:%s - %lu", uuid, *xid_p);
        dlnode = dlnode->next;
    }

    rfree(uuid);
    return;
}

/* Initialize onlinerefresh node */
onlinerefresh_capture* onlinerefresh_capture_init(bool increment)
{
    onlinerefresh_capture* olrefresh = NULL;

    olrefresh = rmalloc0(sizeof(onlinerefresh_capture));
    if (!olrefresh)
    {
        elog(RLOG_WARNING, "oom");
        return NULL;
    }
    rmemset0(olrefresh, 0, 0, sizeof(onlinerefresh_capture));
    olrefresh->parallelcnt = guc_getConfigOptionInt(CFG_KEY_MAX_WORK_PER_REFRESH);
    if (0 == olrefresh->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }
    olrefresh->thrsmgr = NULL;

    /* Initialize stock data task queue */
    olrefresh->refreshtqueue = queue_init();

    if (increment)
    {
        /* Cache initialization */
        olrefresh->parser2serialtxns = cache_txn_init();
        olrefresh->txn2filebuffer = file_buffer_init();
        olrefresh->recordqueue = queue_init();
    }

    return olrefresh;
}

/* Compare function */
int onlinerefresh_capture_cmp(void* s1, void* s2)
{
    onlinerefresh_capture* olrefresh1 = NULL;
    onlinerefresh_capture* olrefresh2 = NULL;

    olrefresh1 = (onlinerefresh_capture*)s1;
    olrefresh2 = (onlinerefresh_capture*)s2;

    if (0 == memcmp(olrefresh1->no->data, olrefresh2->no->data, UUID_LEN))
    {
        return 0;
    }
    return 1;
}

/* Get timeline from parser */
static bool onlinerefresh_capture_gettlidfromparser(void* args, TimeLineID* tlid)
{
    ListCell*                    lc = NULL;
    thrref*                      thr_ref_obj = NULL;
    thrnode*                     thr_node = NULL;
    decodingcontext*             decodingctx;
    onlinerefresh_capture*       olcapture = NULL;
    onlinerefresh_captureparser* cparser = NULL;

    if (NULL == args)
    {
        /* never come here */
        elog(RLOG_WARNING, "capture parserwal rewinding state exception, privdata point is NULL");
        return false;
    }
    olcapture = (onlinerefresh_capture*)args;

    /* Get loadrecord thread */
    lc = olcapture->thrsmgr->childthrrefs->head;

    /* Get parser thread */
    lc = lc->next;
    thr_ref_obj = (thrref*)lfirst(lc);
    thr_node = threads_getthrnodebyno(olcapture->thrsmgr->parents, thr_ref_obj->no);
    if (NULL == thr_node)
    {
        elog(RLOG_WARNING, "capture onlinerefresh can not get parser thread by no:%lu",
             thr_ref_obj->no);
        return false;
    }

    /* Get onlinerefresh parser node */
    cparser = (onlinerefresh_captureparser*)thr_node->data;
    if (NULL == cparser)
    {
        elog(RLOG_WARNING, "olcapture_serial point is NULL");
        return false;
    }
    decodingctx = cparser->decodingctx;

    *tlid = decodingctx->base.curtlid;
    return true;
}

/* Set working directory */
static void onlinerefresh_capture_setdata(onlinerefresh_capture* olcapture, char* data)
{
    olcapture->data = data;
}

void onlinerefresh_capture_increment_set(onlinerefresh_capture* onlinerefresh_capture,
                                         bool                   increment)
{
    onlinerefresh_capture->increment = increment;
}

void onlinerefresh_capture_redo_set(onlinerefresh_capture* onlinerefresh_capture, XLogRecPtr redo)
{
    onlinerefresh_capture->redo.wal.lsn = redo;
}

void onlinerefresh_capture_conninfo_set(onlinerefresh_capture* onlinerefresh_capture,
                                        char*                  conninfo)
{
    onlinerefresh_capture->conninfo = conninfo;
}

void onlinerefresh_capture_snapshot_set(onlinerefresh_capture* onlinerefresh_capture,
                                        snapshot*              snapshot)
{
    onlinerefresh_capture->snapshot = snapshot;
}

void onlinerefresh_capture_conn_set(onlinerefresh_capture* onlinerefresh_capture, PGconn* conn)
{
    onlinerefresh_capture->conn = conn;
}

void onlinerefresh_capture_snap_conn_set(onlinerefresh_capture* onlinerefresh_capture,
                                         PGconn*                snap_conn)
{
    onlinerefresh_capture->snap_conn = snap_conn;
}

void onlinerefresh_capture_no_set(onlinerefresh_capture* onlinerefresh_capture, uuid_t* no)
{
    onlinerefresh_capture->no = no;
}

void onlinerefresh_capture_txid_set(onlinerefresh_capture* onlinerefresh_capture,
                                    FullTransactionId      txid)
{
    onlinerefresh_capture->txid = txid;
}

void onlinerefresh_capture_xids_append(onlinerefresh_capture* onlinerefresh_capture,
                                       TransactionId          xid)
{
    FullTransactionId* xid_p = NULL;
    dlistnode*         xid_dlnode = NULL;

    /* Since xid is not repeatable, check for duplicate xid here first */
    if (onlinerefresh_capture->xids)
    {
        xid_dlnode = onlinerefresh_capture->xids->head;
        while (xid_dlnode)
        {
            FullTransactionId* xid_temp = (FullTransactionId*)xid_dlnode->value;
            if (*xid_temp == (FullTransactionId)xid)
            {
                return;
            }
            xid_dlnode = xid_dlnode->next;
        }
    }

    xid_p = rmalloc0(sizeof(FullTransactionId));
    if (!xid_p)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(xid_p, 0, 0, sizeof(FullTransactionId));
    *xid_p = (FullTransactionId)xid;
    onlinerefresh_capture->xids = dlist_put(onlinerefresh_capture->xids, xid_p);
}

void onlinerefresh_capture_add_xids_from_snapshot(onlinerefresh_capture* onlinerefresh_capture,
                                                  snapshot*              snap)
{
    HASH_SEQ_STATUS status;
    snapshot_xid*   entry = NULL;
    TransactionId   xid = InvalidTransactionId;

    hash_seq_init(&status, snap->xids);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        xid = entry->xid;
        onlinerefresh_capture_xids_append(onlinerefresh_capture, xid);
    }
}

bool onlinerefresh_capture_isxidinsnapshot(onlinerefresh_capture* onlinerefresh_capture,
                                           FullTransactionId      xid)
{
    TransactionId txid = (TransactionId)xid;
    bool          find = false;
    if (!onlinerefresh_capture || !onlinerefresh_capture->snapshot->xids)
    {
        return false;
    }

    /* Note: The key in snapshot's xids hash is actually TransactionId */
    hash_search(onlinerefresh_capture->snapshot->xids, &txid, HASH_FIND, &find);

    return find;
}

bool onlinerefresh_capture_isxidinxids(onlinerefresh_capture* onlinerefresh_capture,
                                       FullTransactionId      xid)
{
    dlistnode*         dlnode = NULL;
    FullTransactionId* xid_p = NULL;

    dlnode = onlinerefresh_capture->xids->head;
    while (dlnode)
    {
        xid_p = (FullTransactionId*)dlnode->value;
        if (*xid_p == xid)
        {
            return true;
        }
        dlnode = dlnode->next;
    }
    return false;
}

static void onlinerefresh_capture_free_xid(void* xid_p)
{
    if (xid_p)
    {
        rfree(xid_p);
    }
}

void onlinerefresh_capture_xids_delete(onlinerefresh_capture* olcapture, dlistnode* dlnode)
{
    olcapture->xids = dlist_delete(olcapture->xids, dlnode, onlinerefresh_capture_free_xid);
}

bool onlinerefresh_capture_xids_isnull(onlinerefresh_capture* olcapture)
{
    return dlist_isnull(olcapture->xids);
}

void onlinerefresh_capture_tables_set(onlinerefresh_capture* onlinerefresh_capture,
                                      refresh_tables*        tables)
{
    onlinerefresh_capture->tables = tables;
}

/*---------------------Basic initialization and settings   end--------------------------------*/

/* Create working directory */
static bool onlinerefresh_capture_trymkdatadir(onlinerefresh_capture* olcapture)
{
    char  path[MAXPATH] = {'\0'};
    char* uuid_str = NULL;
    if (NULL == olcapture->data)
    {
        /* Generate main directory */
        uuid_str = uuid2string(olcapture->no);
        sprintf(path, "%s/%s/%s", guc_getConfigOption(CFG_KEY_DATA), REFRESH_ONLINEREFRESH,
                uuid_str);
        onlinerefresh_capture_setdata(olcapture, rstrdup(path));
        rfree(uuid_str);
    }

    /* Create onlinerefresh uuid main directory */
    if (!osal_dir_exist(olcapture->data))
    {
        if (0 != osal_make_dir(olcapture->data))
        {
            elog(RLOG_WARNING, "could not create directory:%s, %s", olcapture->data,
                 strerror(errno));
            return false;
        }
    }

    return true;
}

/* Create onlinerefresh->refresh storage directory */
static bool onlinerefresh_capture_trymkrefreshdir(onlinerefresh_capture* olcapture)
{
    StringInfo      path = NULL;
    StringInfo      path_partial = NULL;
    StringInfo      path_complete = NULL;
    refresh_table*  table = NULL;
    refresh_tables* tables = olcapture->tables;

    path = makeStringInfo();
    path_partial = makeStringInfo();
    path_complete = makeStringInfo();

    /* Create stock table directory */
    for (table = tables->tables; table != NULL; table = table->next)
    {
        resetStringInfo(path);
        appendStringInfo(path, "%s/%s/%s_%s", olcapture->data, REFRESH_REFRESH, table->schema,
                         table->table);
        if (!osal_dir_exist(path->data))
        {
            resetStringInfo(path_partial);
            resetStringInfo(path_complete);

            if (0 != osal_make_dir(path->data))
            {
                elog(RLOG_WARNING, "could not create directory:%s, %s", path, strerror(errno));
                return false;
            }
            appendStringInfo(path_partial, "%s/%s", path->data, REFRESH_PARTIAL);
            appendStringInfo(path_complete, "%s/%s", path->data, REFRESH_COMPLETE);

            if (0 != osal_make_dir(path_partial->data))
            {
                elog(RLOG_WARNING, "could not create directory:%s, %s", path_partial->data,
                     strerror(errno));
                return false;
            }

            if (0 != osal_make_dir(path_complete->data))
            {
                elog(RLOG_WARNING, "could not create directory:%s, %s", path_complete->data,
                     strerror(errno));
                return false;
            }
        }
    }

    deleteStringInfo(path);
    deleteStringInfo(path_complete);
    deleteStringInfo(path_partial);
    return true;
}

/* Create increment storage directory */
static bool onlinerefresh_capture_trymkincrementdir(onlinerefresh_capture* olcapture)
{
    StringInfo path = NULL;
    if (false == olcapture->increment)
    {
        /* This onlinerefresh does not involve increment sync */
        return true;
    }

    /* Create increment table directory */
    path = makeStringInfo();
    appendStringInfo(path, "%s/%s", olcapture->data, STORAGE_TRAIL_DIR);
    if (!osal_dir_exist(path->data))
    {
        if (0 != osal_make_dir(path->data))
        {
            elog(RLOG_WARNING, "could not create directory:%s, %s", path, strerror(errno));
            deleteStringInfo(path);
            return false;
        }
    }
    deleteStringInfo(path);
    return true;
}

/* Start refresh job thread */
static bool onlinerefresh_capture_startrefreshjob(onlinerefresh_capture* olcapture)
{
    int                         index = 0;
    task_refresh_sharding2file* sharding2file = NULL;
    char                        refreshpath[ABSPATH] = {0};
    for (index = 0; index < olcapture->parallelcnt; index++)
    {
        /* Allocate space and initialize */
        sharding2file = refresh_sharding2file_init();
        sharding2file->conn = NULL;
        sharding2file->conn_info = olcapture->conninfo;

        /* Generate storage directory */
        rmemset1(refreshpath, 0, '\0', ABSPATH);
        snprintf(refreshpath, ABSPATH, "%s/%s", olcapture->data, REFRESH_REFRESH);
        sharding2file->refresh_path = rstrdup(refreshpath);

        /* Set snapshot name for exporting data and task queue */
        sharding2file->snap_shot_name = olcapture->snapshot->name;
        sharding2file->tqueue = olcapture->refreshtqueue;

        /* Register job thread */
        if (false == threads_addjobthread(
                         olcapture->thrsmgr->parents, THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_JOB,
                         olcapture->thrsmgr->submgrref.no, (void*)sharding2file,
                         refresh_sharding2file_free, NULL, refresh_sharding2file_work))
        {
            elog(RLOG_WARNING, "onlinerefresh capture start job error");
            return false;
        }
    }
    return true;
}

/* Start increment job thread */
static bool onlinerefresh_capture_startincrementjob(onlinerefresh_capture* olcapture)
{
    onlinerefresh_captureflush*      cflush = NULL;
    onlinerefresh_captureserial*     cserial = NULL;
    onlinerefresh_captureparser*     cparser = NULL;
    onlinerefresh_captureloadrecord* cloadrecord = NULL;
    char                             path[MAXPATH] = {'\0'};
    if (false == olcapture->increment)
    {
        /* No need to start increment */
        return true;
    }

    /*---------------------Flush thread begin------------------*/
    cflush = onlinerefresh_captureflush_init();
    if (NULL == cflush)
    {
        elog(RLOG_WARNING, "onlinerefresh capture flush init error");
        return false;
    }
    cflush->txn2filebuffer = olcapture->txn2filebuffer;
    rmemset1(path, 0, 0, MAXPATH);
    sprintf(path, "%s/%s", olcapture->data, STORAGE_TRAIL_DIR);
    cflush->trail = rstrdup(path);

    /*---------------------Flush thread   end------------------*/

    /*---------------------Serialization thread begin----------------*/
    cserial = onlinerefresh_captureserial_init();
    if (NULL == cserial)
    {
        elog(RLOG_WARNING, "onlinerefresh capture serial init error");
        return false;
    }
    cserial->privdata = (void*)olcapture;
    cserial->parser2serialtxns = olcapture->parser2serialtxns;
    cserial->serialstate->txn2filebuffer = olcapture->txn2filebuffer;
    cserial->callback.parserstat_curtlid_get = onlinerefresh_capture_gettlidfromparser;
    /*---------------------Serialization thread   end----------------*/

    /*---------------------Parser thread begin----------------*/
    /*
     * Parser initialization
     * parserwal callback settings
     */
    cparser = onlinerefresh_captureparser_init();
    cparser->decodingctx->privdata = (void*)olcapture;
    cparser->decodingctx->recordqueue = olcapture->recordqueue;
    cparser->decodingctx->parser2txns = olcapture->parser2serialtxns;

    /* Load parsing metadata info, timezone charset data dictionary etc. */
    onlinerefresh_captureparser_loadmetadata(cparser);

    /* Generate sync dataset */
    if (false == onlinerefresh_captureparser_datasetinit(cparser->decodingctx, olcapture))
    {
        elog(RLOG_WARNING, "onlinerefresh capture init sync dataset error");
        return false;
    }

    /*
     * Set split lsn callback function to NULL
     *  onlinerefresh's parsing starting point: redolsn/restartlsn/confirmlsn from increment
     * constants
     */
    cparser->decodingctx->callback.setloadlsn = NULL;

    /*---------------------Parser thread   end----------------*/

    /*---------------------Log split thread begin--------------*/
    cloadrecord = onlinerefresh_captureloadrecord_init();
    if (NULL == cloadrecord)
    {
        elog(RLOG_WARNING, "onlinerefresh capture init splitwanl error");
        return false;
    }
    cloadrecord->splitwalctx->privdata = (void*)olcapture;
    cloadrecord->splitwalctx->recordqueue = olcapture->recordqueue;

    /* Set parsing timeline */
    cloadrecord->splitwalctx->loadrecords->timeline = cparser->decodingctx->base.curtlid;
    cloadrecord->splitwalctx->loadrecords->startptr = olcapture->redo.wal.lsn;
    cloadrecord->splitwalctx->callback.parserwal_rewindstat_setemiting = NULL;
    /*---------------------Log split thread   end--------------*/

    /*
     * Start all threads
     */
    /* Register flush thread */
    if (false == threads_addjobthread(
                     olcapture->thrsmgr->parents, THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_FLUSH,
                     olcapture->thrsmgr->submgrref.no, (void*)cflush,
                     onlinerefresh_captureflush_free, NULL, onlinerefresh_captureflush_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment flush job error");
        return false;
    }

    /* Register serialization thread */
    if (false == threads_addjobthread(
                     olcapture->thrsmgr->parents, THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_SERIAL,
                     olcapture->thrsmgr->submgrref.no, (void*)cserial,
                     onlinerefresh_captureserial_free, NULL, onlinerefresh_captureserial_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment serial job error");
        return false;
    }

    /* Register parser thread */
    if (false == threads_addjobthread(
                     olcapture->thrsmgr->parents, THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_PARSER,
                     olcapture->thrsmgr->submgrref.no, (void*)cparser,
                     onlinerefresh_captureparser_free, NULL, onlinerefresh_captureparser_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment parser job error");
        return false;
    }

    /* Register loadrecords thread */
    if (false == threads_addjobthread(olcapture->thrsmgr->parents,
                                      THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_LOADRECORDS,
                                      olcapture->thrsmgr->submgrref.no, (void*)cloadrecord,
                                      onlinerefresh_captureloadrecord_free, NULL,
                                      onlinerefresh_captureloadrecord_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment parser job error");
        return false;
    }
    return true;
}

/* Traverse tables to be refreshed, generate queue */
static bool onlinerefresh_capture_tables2shardings(onlinerefresh_capture* olcapture)
{
    StringInfo     str = NULL;
    PGresult*      res = NULL;
    PGconn*        conn = olcapture->conn;
    uint32         ctid_blkid_max = 0;
    refresh_table* table = NULL;
    int            max_shard_num = guc_getConfigOptionInt(CFG_KEY_MAX_PAGE_PER_REFRESHSHARDING);

    str = makeStringInfo();

    elog(RLOG_DEBUG, "capture refresh mgr, gen queue begin");

    /* First check if connection exists, if not, open connection */
    while (!conn)
    {
        conn = conn_get(olcapture->conninfo);

        if (NULL != conn)
        {
            olcapture->conn = conn;
            break;
        }
    }

    /* Traverse sync table list, query max ctid estimate, generate tasks after sharding */
    for (table = olcapture->tables->tables; table != NULL; table = table->next)
    {
        uint32 left = 0;
        uint32 right = 0;
        uint32 remain = 0;
        int    shard_no = 1;

        appendStringInfo(str, "select pg_relation_size('\"%s\".\"%s\"')/%d;", table->schema,
                         table->table, g_blocksize);
        res = PQexec(conn, str->data);
        if (PGRES_TUPLES_OK != PQresultStatus(res))
        {
            elog(RLOG_WARNING, "Failed execute in compute max ctid: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            olcapture->conn = NULL;
            deleteStringInfo(str);
            return false;
        }

        if (PQntuples(res) != 0)
        {
            ctid_blkid_max = (uint32)atoi(PQgetvalue(res, 0, 0));
        }
        else
        {
            elog(RLOG_WARNING, "Failed execute in compute max ctid: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            olcapture->conn = NULL;
            deleteStringInfo(str);
            return false;
        }

        /* Reset */
        resetStringInfo(str);
        PQclear(res);

        /* If no data, directly set sharding to 0 */
        if (ctid_blkid_max == 0)
        {
            refresh_table_sharding* table_shard = NULL;

            table_shard = refresh_table_sharding_init();

            /* Set sharding info */
            refresh_table_sharding_set_schema(table_shard, table->schema);
            refresh_table_sharding_set_table(table_shard, table->table);
            refresh_table_sharding_set_shardings(table_shard, 0);
            refresh_table_sharding_set_shardno(table_shard, 0);
            refresh_table_sharding_set_condition(table_shard, NULL);

            elog(RLOG_DEBUG, "capture refresh mgr, queue: %s.%s %4d %4d", table_shard->schema,
                 table_shard->table, table_shard->shardings, table_shard->sharding_no);
            /* Add to cache */
            queue_put(olcapture->refreshtqueue, (void*)table_shard);
            continue;
        }

        /* First time calculating sharding value */
        right = ctid_blkid_max < max_shard_num ? ctid_blkid_max : max_shard_num;
        remain = ctid_blkid_max;

        /* Generate queue */
        do
        {
            refresh_table_sharding*  table_shard = NULL;
            refresh_table_condition* cond = NULL;

            table_shard = refresh_table_sharding_init();
            cond = refresh_table_sharding_condition_init();

            /* Shard */
            refresh_table_sharding_set_schema(table_shard, table->schema);
            refresh_table_sharding_set_table(table_shard, table->table);
            refresh_table_sharding_set_shardings(table_shard,
                                                 ((ctid_blkid_max - 1) / max_shard_num) + 1);
            refresh_table_sharding_set_shardno(table_shard, shard_no++);
            refresh_table_sharding_set_condition(table_shard, cond);

            cond->left_condition = left;
            cond->right_condition = right;

            elog(RLOG_DEBUG, "capture refresh mgr, queue: %s.%s %4d %4d", table_shard->schema,
                 table_shard->table, table_shard->shardings, table_shard->sharding_no);

            /* Add to cache */
            queue_put(olcapture->refreshtqueue, (void*)table_shard);

            remain = ctid_blkid_max - right;
            left = right;
            right += remain > max_shard_num ? max_shard_num : remain;
        } while (remain);
    }

    /* Cleanup work */
    deleteStringInfo(str);

    return true;
}

static void recordqueue_dlist_free(dlist* record_dlist)
{
    dlist_free(record_dlist, (dlistvaluefree)record_free);
}

static bool onlinerefresh_capture_keep_alive(PGconn* conn)
{
    PGresult* res = NULL;
    res = PQexec(conn, "SELECT 1;");
    if (PGRES_TUPLES_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "Failed in snapshot keep alive: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

/* Main processing flow */
void* onlinerefresh_capture_main(void* args)
{
    int                        skipcnt = 0;
    int                        jobcnt = 0;
    uint32                     delay = 200;
    onlinerefresh_capture_stat jobstat = ONLINEREFRESH_CAPTURE_STAT_JOBNOP;
    ListCell*                  lc = NULL;
    thrref*                    thr_ref_obj = NULL;
    thrnode*                   incflush_thr_node = NULL;
    thrnode*                   incserial_thr_node = NULL;
    thrnode*                   incparser_thr_node = NULL;
    thrnode*                   incloadrec_thr_node = NULL;
    onlinerefresh_capture*     olcapture = NULL;

    thrnode* thr_node = (thrnode*)args;
    olcapture = (onlinerefresh_capture*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "onlinerefresh capture stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    thr_node->stat = THRNODE_STAT_WORK;

    /* Can exit if no tables to sync */
    if (NULL == olcapture)
    {
        elog(RLOG_WARNING, "no tables need do onlinerefresh");
        thr_node->stat = THRNODE_STAT_EXIT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Set to startup state */
    elog(RLOG_DEBUG, "onlinerefresh capture start");
    onlinerefresh_capture_print(olcapture);

    /* Create main directory */
    if (false == onlinerefresh_capture_trymkdatadir(olcapture))
    {
        /* Failed to create working directory */
        elog(RLOG_WARNING, "capture onlinerefresh mk data dir error");
        thr_node->stat = THRNODE_STAT_EXIT;
        pthread_exit(NULL);
        return NULL;
    }

    /*
     * Create stock data directory, and register job threads
     */
    onlinerefresh_capture_trymkrefreshdir(olcapture);
    if (false == onlinerefresh_capture_startrefreshjob(olcapture))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start refresh job thread error");
        thr_node->stat = THRNODE_STAT_EXIT;
        pthread_exit(NULL);
        return NULL;
    }

    /*
     * Create increment directory, and start increment job threads
     */
    if (false == onlinerefresh_capture_trymkincrementdir(olcapture))
    {
        elog(RLOG_WARNING, "onlinerefresh capture make increment job thread data error");
        thr_node->stat = THRNODE_STAT_EXIT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Start increment job thread */
    if (false == onlinerefresh_capture_startincrementjob(olcapture))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment thread error");
        thr_node->stat = THRNODE_STAT_EXIT;
        pthread_exit(NULL);
        return NULL;
    }

    if (true == olcapture->increment)
    {
        skipcnt = 4;
        /* Get increment->parser thread in advance for subsequent logic judgment */
        /* Get loadrecord thread */
        lc = olcapture->thrsmgr->childthrrefs->head;
        thr_ref_obj = (thrref*)lfirst(lc);
        incloadrec_thr_node = threads_getthrnodebyno(olcapture->thrsmgr->parents, thr_ref_obj->no);
        if (NULL == incloadrec_thr_node)
        {
            elog(RLOG_WARNING, "capture onlinerefresh can not get load record thread by no:%lu",
                 thr_ref_obj->no);
            thr_node->stat = THRNODE_STAT_ABORT;
            pthread_exit(NULL);
        }

        /* Get parser thread */
        lc = lc->next;
        thr_ref_obj = (thrref*)lfirst(lc);
        incparser_thr_node = threads_getthrnodebyno(olcapture->thrsmgr->parents, thr_ref_obj->no);
        if (NULL == incparser_thr_node)
        {
            elog(RLOG_WARNING, "capture onlinerefresh can not get parser thread by no:%lu",
                 thr_ref_obj->no);
            thr_node->stat = THRNODE_STAT_ABORT;
            pthread_exit(NULL);
        }

        /* Get serial thread */
        lc = lc->next;
        thr_ref_obj = (thrref*)lfirst(lc);
        incserial_thr_node = threads_getthrnodebyno(olcapture->thrsmgr->parents, thr_ref_obj->no);
        if (NULL == incserial_thr_node)
        {
            elog(RLOG_WARNING, "capture onlinerefresh can not get serial thread by no:%lu",
                 thr_ref_obj->no);
            thr_node->stat = THRNODE_STAT_ABORT;
            pthread_exit(NULL);
        }

        /* Get flush thread */
        lc = lc->next;
        thr_ref_obj = (thrref*)lfirst(lc);
        incflush_thr_node = threads_getthrnodebyno(olcapture->thrsmgr->parents, thr_ref_obj->no);
        if (NULL == incflush_thr_node)
        {
            elog(RLOG_WARNING, "capture onlinerefresh can not get inc flush thread by no:%lu",
                 thr_ref_obj->no);
            thr_node->stat = THRNODE_STAT_ABORT;
            pthread_exit(NULL);
        }
    }

    jobstat = ONLINEREFRESH_CAPTURE_STAT_JOBSTARTING;
    while (true)
    {
        /*
         * First check if exit signal is received
         *  For child management threads, receiving TERM signal has two scenarios:
         *  1、Parent resident thread of child management thread exits
         *  2、Exit flag is received
         *
         * In both cases above, child management thread does not need to set job thread to FREE
         * state
         */
        usleep(50000);

        /* Snapshot keepalive */
        if (delay >= 200)
        {
            delay = 0;
            onlinerefresh_capture_keep_alive(olcapture->snap_conn);
        }

        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Wait for all child threads to start successfully */
        if (ONLINEREFRESH_CAPTURE_STAT_JOBSTARTING == jobstat)
        {
            /* Check if already started successfully */
            jobcnt = 0;
            delay++;
            if (false == threads_countsubmgrjobthredsabovework(olcapture->thrsmgr->parents,
                                                               olcapture->thrsmgr->childthrrefs,
                                                               &jobcnt))
            {
                elog(RLOG_WARNING, "capture onlinerefresh count job thread above work stat error");
                thr_node->stat = THRNODE_STAT_ABORT;
                goto onlinerefresh_capture_main_done;
            }

            if (jobcnt != olcapture->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = ONLINEREFRESH_CAPTURE_STAT_JOBWORKING;
            continue;
        }
        else if (ONLINEREFRESH_CAPTURE_STAT_JOBWORKING == jobstat)
        {
            /* All started, now add stock data tasks to queue */
            if (false == onlinerefresh_capture_tables2shardings(olcapture))
            {
                /* Failed to add task to queue, management thread exits, child thread recovery
                 * handled by main thread */
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            jobstat = ONLINEREFRESH_CAPTURE_STAT_JOBWAITREFRESHDONE;
            delay++;
            continue;
        }
        else if (ONLINEREFRESH_CAPTURE_STAT_JOBWAITREFRESHDONE == jobstat)
        {
            /*
             * Wait for stock data thread to exit
             *  1、Queue is empty
             *  2、Stock data thread completely exited
             */
            if (false == queue_isnull(olcapture->refreshtqueue))
            {
                /* Queue is not empty, indicating there are still tasks to process */
                delay++;
                continue;
            }

            /* Set idle threads to exit and count the number of exited threads */
            jobcnt = olcapture->parallelcnt;
            if (false == threads_setsubmgrjobthredstermandcountexit(
                             olcapture->thrsmgr->parents, olcapture->thrsmgr->childthrrefs, skipcnt,
                             &jobcnt))
            {
                elog(RLOG_WARNING, "capture refresh set job threads term in idle error");
                thr_node->stat = THRNODE_STAT_ABORT;
                goto onlinerefresh_capture_main_done;
            }

            if (jobcnt != olcapture->parallelcnt)
            {
                continue;
            }

            /* Set stock data thread to exit, skip the first 4 increment threads */
            threads_setsubmgrjobthredsfree(olcapture->thrsmgr->parents,
                                           olcapture->thrsmgr->childthrrefs, skipcnt, jobcnt);

            jobstat = ONLINEREFRESH_CAPTURE_STAT_JOBWAITINCREMENTDONE;

            /* Cleanup connections */
            if (olcapture->conn)
            {
                PQfinish(olcapture->conn);
                olcapture->conn = NULL;
            }

            if (olcapture->snap_conn)
            {
                PQfinish(olcapture->snap_conn);
                olcapture->snap_conn = NULL;
            }
            continue;
        }
        else if (ONLINEREFRESH_CAPTURE_STAT_JOBWAITINCREMENTDONE == jobstat)
        {
            /*
             * In onlinerefresh, need to wait for increment->parser thread to exit first
             *  parser will exit after capturing all transactions in snapshot during sync
             *  Parser thread sets onlinerefreshend when exiting, then serialization thread and
             * flush thread will exit in sequence After waiting for parser, serialization, flush
             * threads to exit, set loadrecord thread to TERM
             */
            if (false == olcapture->increment)
            {
                /* No increment thread, set this thread to exit */
                thr_node->stat = THRNODE_STAT_EXIT;
                break;
            }

            if (THRNODE_STAT_EXITED != incparser_thr_node->stat ||
                THRNODE_STAT_EXITED != incserial_thr_node->stat ||
                THRNODE_STAT_EXITED != incflush_thr_node->stat)
            {
                /* Parser thread not exited, waiting */
                continue;
            }

            /* Set loadrecords thread to exit */
            if (THRNODE_STAT_TERM > incloadrec_thr_node->stat)
            {
                incloadrec_thr_node->stat = THRNODE_STAT_TERM;
                queue_destroy(olcapture->recordqueue, (dlistvaluefree)recordqueue_dlist_free);
                olcapture->recordqueue = NULL;
                continue;
            }

            if (THRNODE_STAT_EXITED != incloadrec_thr_node->stat)
            {
                continue;
            }

            /* All threads have exited, management thread can exit */
            jobcnt = olcapture->thrsmgr->childthrrefs->length - olcapture->parallelcnt;
            threads_setsubmgrjobthredsfree(olcapture->thrsmgr->parents,
                                           olcapture->thrsmgr->childthrrefs, 0, jobcnt);

            /* Set this thread to exit */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
    }

onlinerefresh_capture_main_done:
    pthread_exit(NULL);
    return NULL;
}

/* Release */
void onlinerefresh_capture_destroy(void* privdata)
{
    onlinerefresh_capture* olcapture = NULL;

    olcapture = (onlinerefresh_capture*)privdata;
    if (NULL == olcapture)
    {
        return;
    }

    /* Remove this node from parent's olrefreshing list */
    olcapture->removeolrefresh(olcapture->privdata, privdata);

    if (olcapture->conn)
    {
        PQfinish(olcapture->conn);
    }

    if (olcapture->snap_conn)
    {
        PQfinish(olcapture->snap_conn);
    }

    if (olcapture->snapshot)
    {
        if (olcapture->snapshot->name)
        {
            rfree(olcapture->snapshot->name);
        }
        if (olcapture->snapshot->xids)
        {
            hash_destroy(olcapture->snapshot->xids);
        }
        rfree(olcapture->snapshot);
    }

    if (olcapture->data)
    {
        rfree(olcapture->data);
    }

    if (olcapture->no)
    {
        uuid_free(olcapture->no);
    }

    if (olcapture->refreshtqueue)
    {
        queue_destroy(olcapture->refreshtqueue, NULL);
    }

    /* Cleanup refresh tables */
    if (olcapture->tables)
    {
        refresh_freetables(olcapture->tables);
    }

    if (olcapture->recordqueue)
    {
        queue_destroy(olcapture->recordqueue, (dlistvaluefree)recordqueue_dlist_free);
    }

    if (olcapture->parser2serialtxns)
    {
        cache_txn_destroy(olcapture->parser2serialtxns);
    }

    if (olcapture->txn2filebuffer)
    {
        file_buffer_destroy(olcapture->txn2filebuffer);
    }

    rfree(olcapture);
}
