#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/conn/conn.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/guc/guc.h"
#include "loadrecords/record.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "misc/misc_stat.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "net/netpacket/netpacket.h"
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
#include "cache/fpwcache.h"
#include "catalog/catalog.h"
#include "catalog/control.h"
#include "catalog/class.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "works/parserwork/wal/parserwork_wal.h"
#include "works/parserwork/wal/decode_checkpoint.h"
#include "metric/capture/metric_capture.h"
#include "utils/init/databaserecv.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "utils/regex/regex.h"
#include "strategy/filter_dataset.h"

static void parserwork_wal_reload(decodingcontext* decodingctx);

void parserwork_stat_setpause(decodingcontext* decodingctx)
{
    decodingctx->stat = DECODINGCONTEXT_SET_PAUSE;

    while (decodingctx->stat == DECODINGCONTEXT_SET_PAUSE)
    {
        usleep(10000);
    }
    return;
}

void parserwork_stat_setresume(decodingcontext* decodingctx)
{
    decodingctx->stat = DECODINGCONTEXT_RESUME;
    return;
}

HTAB* decodingcontext_stat_getsyncdataset(decodingcontext* decodingctx)
{
    return decodingctx->trans_cache->hsyncdataset;
}

/* Remove onlinerefresh */
void parserwork_decodingctx_removeonlinerefresh(decodingcontext* ctx, onlinerefresh* onlinerefresh)
{
    ctx->onlinerefresh =
        dlist_deletebyvalue(ctx->onlinerefresh, onlinerefresh, onlinerefresh_cmp, onlinerefresh_destroyvoid);

    if (NULL != ctx->refreshtxn)
    {
        txn_free(ctx->refreshtxn);
        rfree(ctx->refreshtxn);
        ctx->refreshtxn = NULL;
    }
}

void parserwork_decodingctx_addonlinerefresh(decodingcontext* ctx, onlinerefresh* onlinerefresh, txn* txn)
{
    ctx->onlinerefresh = dlist_put(ctx->onlinerefresh, onlinerefresh);
    ctx->refreshtxn = txn;
}

static void parserwork_wal_check_reloadstate(decodingcontext* decodingctx, int state)
{
    if (CAPTURERELOAD_STATUS_RELOADING_PARSERWAL == state)
    {
        parserwork_wal_reload(decodingctx);
        g_gotsigreload = CAPTURERELOAD_STATUS_RELOADING_WRITE;
    }
    return;
}

void parserwork_stat_setrewind(decodingcontext* decodingctx)
{
    decodingctx->stat = DECODINGCONTEXT_REWIND;
    return;
}

void parserwork_stat_setrunning(decodingcontext* decodingctx)
{
    decodingctx->stat = DECODINGCONTEXT_RUNNING;
    return;
}

decodingcontext* parserwork_walinitphase1(void)
{
    decodingcontext* decodingctx = NULL;
    HASHCTL          hash_ctl;
    if (NULL != decodingctx)
    {
        return decodingctx;
    }

    decodingctx = (decodingcontext*)rmalloc1(sizeof(decodingcontext));
    if (NULL == decodingctx)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx, 0, '\0', sizeof(decodingcontext));
    decodingctx->parselsn = FRISTVALID_LSN;
    decodingctx->decode_record = NULL;
    decodingctx->stat = DECODINGCONTEXT_INIT;

    /* Load BASE info from disk */
    misc_stat_loaddecode((void*)&decodingctx->base);
    decodingctx->decode_record = NULL;

    decodingctx->trans_cache = (transcache*)rmalloc0(sizeof(transcache));
    if (NULL == decodingctx->trans_cache)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->trans_cache, 0, '\0', sizeof(transcache));

    /*
     * Load rules:
     *  1. Sync rules
     *  2. Filter rules
     *  3. New table filter rules
     */
    decodingctx->trans_cache->tableincludes = filter_dataset_inittableinclude(decodingctx->trans_cache->tableincludes);
    decodingctx->trans_cache->tableexcludes = filter_dataset_inittableexclude(decodingctx->trans_cache->tableexcludes);
    decodingctx->trans_cache->addtablepattern =
        filter_dataset_initaddtablepattern(decodingctx->trans_cache->addtablepattern);

    /* Add information required by the parser library */
    decodingctx->walpre.m_dbtype = g_idbtype;
    decodingctx->walpre.m_dbversion = guc_getConfigOption(CFG_KEY_DBVERION);
    decodingctx->walpre.m_debugLevel = 0;
    decodingctx->walpre.m_pagesize = g_blocksize;
    decodingctx->walpre.m_walLevel = PG_PARSER_WALLEVEL_LOGICAL;
    decodingctx->walpre.m_record = NULL;

    /* Initialize linked list structure */
    decodingctx->trans_cache->transdlist = (txn_dlist*)rmalloc0(sizeof(txn_dlist));
    if (NULL == decodingctx->trans_cache->transdlist)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->trans_cache->transdlist, 0, '\0', sizeof(txn_dlist));
    decodingctx->trans_cache->transdlist->head = NULL;
    decodingctx->trans_cache->transdlist->tail = NULL;

    /* Initialize capture_buffer and convert to bytes */
    decodingctx->trans_cache->capture_buffer = MB2BYTE(guc_getConfigOptionInt(CFG_KEY_CAPTURE_BUFFER));

    /* Initialize transaction HASH table */
    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(FullTransactionId);
    hash_ctl.entrysize = sizeof(txn);
    decodingctx->trans_cache->by_txns = hash_create("transaction hash", 8192, &hash_ctl, HASH_ELEM | HASH_BLOBS);
    /* Initialize sysdicts */
    decodingctx->trans_cache->sysdicts = (cache_sysdicts*)rmalloc0(sizeof(cache_sysdicts));
    if (NULL == decodingctx->trans_cache->sysdicts)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->trans_cache->sysdicts, 0, '\0', sizeof(cache_sysdicts));

    /* Initialize rewind_ptr */
    decodingctx->rewind_ptr = (rewind_info*)rmalloc0(sizeof(rewind_info));
    if (NULL == decodingctx->rewind_ptr)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->rewind_ptr, 0, '\0', sizeof(rewind_info));

    /* Initialize fpw */
    decodingctx->trans_cache->by_fpwtuples = fpwcache_init(decodingctx->trans_cache);

    /* Set large transaction filter flag */
    decodingctx->filterbigtrans = true;

    return decodingctx;
}

void parserwork_walinitphase2(decodingcontext* decodingctx)
{
    Oid dbid = INVALIDOID;

    /* Load BASE info from disk */
    misc_stat_loaddecode((void*)&decodingctx->base);

    decodingctx->database = misc_controldata_database_get(NULL);
    decodingctx->monetary = rstrdup(misc_controldata_monetary_get());
    decodingctx->numeric = rstrdup(misc_controldata_numeric_get());
    decodingctx->tzname = rstrdup(misc_controldata_timezone_get());
    decodingctx->orgdbcharset = rstrdup(misc_controldata_orgencoding_get());
    decodingctx->tgtdbcharset = misc_controldata_dstencoding_get();

    /* Load dictionary tables */
    dbid = misc_controldata_database_get(NULL);
    cache_sysdictsload((void**)&decodingctx->trans_cache->sysdicts);

    decodingctx->trans_cache->hsyncdataset = filter_dataset_load(decodingctx->trans_cache->sysdicts->by_namespace,
                                                                 decodingctx->trans_cache->sysdicts->by_class);

    decodingctx->trans_cache->htxnfilterdataset =
        filter_dataset_txnfilterload(decodingctx->trans_cache->sysdicts->by_namespace,
                                     decodingctx->trans_cache->sysdicts->by_class);

    decodingctx->trans_cache->sysdicts->by_relfilenode =
        cache_sysdicts_buildrelfilenode2oid(dbid, (void*)decodingctx->trans_cache->sysdicts);
    /* Initialize checkpoint node */
    decode_chkpt_init(decodingctx, decodingctx->base.redolsn);

    /* Initialize capture_buffer and convert to bytes */
    decodingctx->trans_cache->capture_buffer = MB2BYTE(guc_getConfigOptionInt(CFG_KEY_CAPTURE_BUFFER));

    elog(RLOG_INFO,
         "ripple parser from, redolsn %X/%X, restartlsn %X/%X, last commit lsn:%X/%X, fileid:%lu, "
         "offset:%u, timeline:%u",
         (uint32)(decodingctx->base.redolsn >> 32),
         (uint32)decodingctx->base.redolsn,
         (uint32)(decodingctx->base.restartlsn >> 32),
         (uint32)decodingctx->base.restartlsn,
         (uint32)(decodingctx->base.confirmedlsn >> 32),
         (uint32)decodingctx->base.confirmedlsn,
         decodingctx->base.fileid,
         decodingctx->base.fileoffset,
         decodingctx->base.curtlid);
}

bool parserwork_wal_initfromdb(decodingcontext* decodingctx)
{
    const char* url = NULL;

    PGconn*     conn = NULL;
    checkpoint* checkpoint = NULL;

    /* Get connection info */
    url = guc_getConfigOption("url");

    /* Connect to database */
    conn = conn_get(url);

    /* Exit on connection error */
    if (NULL == conn)
    {
        return false;
    }

    checkpoint = databaserecv_checkpoint_get(conn);

    decodingctx->database = databaserecv_database_get(conn);
    decodingctx->monetary = databaserecv_monetary_get(conn);
    decodingctx->numeric = databaserecv_numeric_get(conn);
    decodingctx->tzname = databaserecv_timezone_get(conn);
    decodingctx->orgdbcharset = databaserecv_orgencoding_get(conn);
    decodingctx->tgtdbcharset = DSTENCODING;

    /* Set controlfile info */
    misc_controldata_database_set(decodingctx->database);
    misc_controldata_dbname_set(PQdb(conn));
    misc_controldata_monetary_set(decodingctx->monetary);
    misc_controldata_numeric_set(decodingctx->numeric);
    misc_controldata_timezone_set(decodingctx->tzname);
    misc_controldata_orgencoding_set(decodingctx->orgdbcharset);
    misc_controldata_dstencoding_set(decodingctx->tgtdbcharset);

    /* Fill base file info */
    decodingctx->base.curtlid = checkpoint->tlid;
    decodingctx->base.redolsn = checkpoint->redolsn;

    decodingctx->rewind_ptr->redolsn = checkpoint->redolsn;

    elog(RLOG_INFO,
         "ripple redolsn fromdb, redolsn %X/%X, timeline:%u",
         (uint32)(checkpoint->redolsn >> 32),
         (uint32)checkpoint->redolsn,
         decodingctx->base.curtlid);

    /* Initialize checkpoint node */
    decode_chkpt_init(decodingctx, decodingctx->base.redolsn);

    /* Create trigger */
    if (!databaserecv_trigger_set(conn))
    {
        return false;
    }

    databaserecv_checkpoint(conn);

    /* Close conn */
    PQfinish(conn);
    conn = NULL;

    rfree(checkpoint);

    return true;
}

/* Iterate and parse records */
static void parserwork_wal_work(decodingcontext* decodingctx, dlist* record_dlist)
{
    dlistnode* dlnode = NULL;
    record*    record_obj = NULL;

    dlnode = record_dlist->head;

    while (dlnode)
    {
        record_obj = (record*)dlnode->value;

        /* End LSN */
        decodingctx->callback.setmetricparselsn(decodingctx->privdata, record_obj->end.wal.lsn);

        decodingctx->parselsn = record_obj->end.wal.lsn;

        /* Parse record */
        decodingctx->decode_record = record_obj;

        /* Call parsing function */
        g_parserecno++;
        parserwork_waldecode(decodingctx);
        decodingctx->decode_record = NULL;

        dlnode = dlnode->next;
    }
}

/* Iterate records to execute rewind_ptr, finding checkpoint is useless */
static void parserwork_wal_rewind_ptr(decodingcontext* decodingctx, dlist* record_dlist)
{
    dlistnode* dlnode = NULL;
    record*    record_obj = NULL;

    dlnode = record_dlist->head;

    /* Iterate linked list */
    while (dlnode)
    {
        record_obj = (record*)dlnode->value;

        /* End LSN */
        decodingctx->callback.setmetricparselsn(decodingctx->privdata, record_obj->end.wal.lsn);

        decodingctx->parselsn = record_obj->end.wal.lsn;

        /* Parse record */
        decodingctx->decode_record = record_obj;

        if (decodingctx->rewind_ptr->stat == REWIND_EMITING)
        {
            if (decodingctx->decode_record->start.wal.lsn >= decodingctx->rewind_ptr->currentlsn)
            {
                /* Found the commit of transaction greater than xmax */
                decodingctx->base.confirmedlsn = decodingctx->decode_record->start.wal.lsn - 1;
                decodingctx->base.restartlsn = decodingctx->rewind_ptr->redolsn;
                decodingctx->stat = DECODINGCONTEXT_RUNNING;
                if (decodingctx->callback.setparserlsn)
                {
                    decodingctx->callback.setparserlsn(decodingctx->privdata,
                                                       decodingctx->base.confirmedlsn,
                                                       decodingctx->base.restartlsn,
                                                       decodingctx->base.restartlsn);
                }
                else
                {
                    elog(RLOG_WARNING, "be carefull! setparserlsn is null");
                }

                elog(RLOG_INFO,
                     "parserwork wal rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, "
                     "confirmedlsn: %X/%X",
                     (uint32)(decodingctx->base.redolsn >> 32),
                     (uint32)decodingctx->base.redolsn,
                     (uint32)(decodingctx->base.restartlsn >> 32),
                     (uint32)decodingctx->base.restartlsn,
                     (uint32)(decodingctx->base.confirmedlsn >> 32),
                     (uint32)decodingctx->base.confirmedlsn);
                rewind_stat_setemited(decodingctx->rewind_ptr);
            }
        }

        /* Call rewind_ptr function */
        if (decodingctx->rewind_ptr->stat == REWIND_SEARCHCHECKPOINT)
        {
            rewind_fastrewind(decodingctx);
            /* If in rewinding state, reset the split start point */
            if (decodingctx->rewind_ptr->stat == REWIND_REWINDING)
            {
                decodingctx->callback.setloadlsn(decodingctx->privdata,
                                                 decodingctx->rewind_ptr->redolsn,
                                                 InvalidXLogRecPtr);
                break;
            }
        }
        else if (decodingctx->rewind_ptr->stat == REWIND_EMITING)
        {
            rewind_fastrewind_emit(decodingctx);
        }
        else if (decodingctx->rewind_ptr->stat == REWIND_EMITED)
        {
            /* This branch may be reached, proceed with normal logic */
            parserwork_waldecode(decodingctx);
        }
        decodingctx->decode_record = NULL;

        dlnode = dlnode->next;
    }
}

void parserwork_wal_getpos(decodingcontext* decodingctx, uint64* fileid, uint64* fileoffset)
{
    if (NULL == decodingctx)
    {
        return;
    }

    *fileid = decodingctx->base.fileid;
    *fileoffset = decodingctx->base.fileoffset;
    *fileid = 0;
    *fileoffset = 0;
    return;
}

void* parserwork_wal_main(void* args)
{
    int              timeout = 0;
    thrnode*         thrnode_ptr = NULL;
    decodingcontext* decodingctx = NULL;
    dlist*           record_dlist = NULL;

    thrnode_ptr = (thrnode*)args;
    decodingctx = (decodingcontext*)thrnode_ptr->data;

    /* Check state */
    if (THRNODE_STAT_STARTING != thrnode_ptr->stat)
    {
        elog(RLOG_WARNING, "increment capture parser stat exception, expected state is THRNODE_STAT_STARTING");
        thrnode_ptr->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thrnode_ptr->stat = THRNODE_STAT_WORK;
    while (1)
    {
        record_dlist = NULL;
        if (THRNODE_STAT_TERM == thrnode_ptr->stat)
        {
            /* Parser exiting */
            thrnode_ptr->stat = THRNODE_STAT_EXIT;
            break;
        }

        if (decodingctx->stat == DECODINGCONTEXT_REWIND && !rewind_check_stat_allow_get_entry(decodingctx->rewind_ptr))
        {
            /* Sleep 10ms */
            usleep(10000);
            continue;
        }

        if (decodingctx->stat == DECODINGCONTEXT_SET_PAUSE)
        {
            decodingctx->stat = DECODINGCONTEXT_PAUSE;
        }

        /* Paused state, sleep */
        if (decodingctx->stat == DECODINGCONTEXT_PAUSE)
        {
            /* Sleep 50ms */
            usleep(50000);
            continue;
        }

        /* Resume state, put online refresh begin transaction into serialization cache */
        if (decodingctx->stat == DECODINGCONTEXT_RESUME)
        {
            /* Put refreshtxn into cache first */
            if (decodingctx->refreshtxn)
            {
                cache_txn_add(decodingctx->parser2txns, decodingctx->refreshtxn);
                decodingctx->refreshtxn = NULL;
            }

            decodingctx->stat = DECODINGCONTEXT_RUNNING;
        }

        /* Reload parameters */
        parserwork_wal_check_reloadstate(decodingctx, g_gotsigreload);

        /* Get data */
        record_dlist = queue_get(decodingctx->recordqueue, &timeout);
        if (NULL == record_dlist)
        {
            /* Need to exit, wait for worknode->status to become WORK_STATUS_TERM before exiting */
            if (ERROR_TIMEOUT == timeout)
            {
                /* Sleep 10 milliseconds */
                continue;
            }

            elog(RLOG_WARNING, "capture parser get records from queue error");
            thrnode_ptr->stat = THRNODE_STAT_ABORT;
            break;
        }

        if (decodingctx->stat == DECODINGCONTEXT_REWIND)
        {
            if (decodingctx->refreshtxn)
            {
                cache_txn_add(decodingctx->parser2txns, decodingctx->refreshtxn);
                decodingctx->refreshtxn = NULL;
            }

            if (decodingctx->rewind_ptr->stat == REWIND_INIT)
            {
                rewind_stat_setsearchcheckpoint(decodingctx->rewind_ptr);
            }
            /* Execute rewind_ptr logic to find checkpoint and emit point */
            parserwork_wal_rewind_ptr(decodingctx, record_dlist);
        }
        else if (decodingctx->stat >= DECODINGCONTEXT_RUNNING)
        {
            /* Get data based on entry content */
            parserwork_wal_work(decodingctx, record_dlist);
        }

        /* Free record doubly-linked list memory */
        dlist_free(record_dlist, (dlistvaluefree)record_free);
    }

    pthread_exit(NULL);
    return NULL;
}

void parserwork_wal_getparserinfo(decodingcontext* decodingctx, XLogRecPtr* prestartlsn, XLogRecPtr* pconfirmlsn)
{
    if (NULL == decodingctx)
    {
        return;
    }

    *prestartlsn = decodingctx->base.restartlsn;
    *pconfirmlsn = decodingctx->base.confirmedlsn;
}

static void parserwork_wal_reload(decodingcontext* decodingctx)
{
    /* Load guc parameters */
    guc_loadcfg(g_profilepath, true);

    /* Load rules */
    decodingctx->trans_cache->tableincludes = filter_dataset_inittableinclude(decodingctx->trans_cache->tableincludes);
    decodingctx->trans_cache->tableexcludes = filter_dataset_inittableexclude(decodingctx->trans_cache->tableexcludes);
    decodingctx->trans_cache->addtablepattern =
        filter_dataset_initaddtablepattern(decodingctx->trans_cache->addtablepattern);

    decodingctx->trans_cache->hsyncdataset = filter_dataset_reload(decodingctx->trans_cache->sysdicts->by_namespace,
                                                                   decodingctx->trans_cache->sysdicts->by_class,
                                                                   decodingctx->trans_cache->hsyncdataset);
    return;
}

bool parserwork_buildrefreshtransaction(decodingcontext* decodingctx, refresh_tables* tables)
{
    txn*     refreshtxn = NULL;
    txnstmt* stmt = NULL;

    if (!tables || 0 == tables->cnt)
    {
        refresh_freetables(tables);
        decodingctx->refreshtxn = NULL;
        return true;
    }

    refreshtxn = txn_init(REFRESH_TXNID, 1, InvalidXLogRecPtr);
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt*)rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_REFRESH;
    stmt->stmt = (void*)tables;
    stmt->extra0.wal.lsn = REFRESH_LSN;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    txn_addcommit(refreshtxn);

    decodingctx->refreshtxn = refreshtxn;
    return true;
}

txn* parserwork_build_onlinerefresh_end_txn(unsigned char* uuid, XLogRecPtr parserlsn)
{
    txn*     refreshtxn = NULL;
    txnstmt* stmt = NULL;

    refreshtxn = txn_init(REFRESH_TXNID, InvalidXLogRecPtr, (parserlsn + 1));
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt*)rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_ONLINEREFRESH_END;

    stmt->stmt = rmalloc0(UUID_LEN);
    if (!stmt->stmt)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(stmt->stmt, 0, 0, UUID_LEN);
    rmemcpy0(stmt->stmt, 0, uuid, UUID_LEN);

    /* Set lsn for onlinerefreshend->stmt */
    stmt->extra0.wal.lsn = parserlsn + 1;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    txn_addcommit(refreshtxn);

    return refreshtxn;
}

txn* parserwork_build_onlinerefresh_increment_end_txn(unsigned char* uuid)
{
    txn*     refreshtxn = NULL;
    txnstmt* stmt = NULL;

    refreshtxn = txn_init(REFRESH_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt*)rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END;

    stmt->stmt = rmalloc0(UUID_LEN);
    if (!stmt->stmt)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(stmt->stmt, 0, 0, UUID_LEN);
    rmemcpy0(stmt->stmt, 0, uuid, UUID_LEN);
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    txn_addcommit(refreshtxn);

    return refreshtxn;
}

txn* parserwork_build_onlinerefresh_begin_txn(txnstmt_onlinerefresh* olstmt, XLogRecPtr parserlsn)
{
    txn*     refreshtxn = NULL;
    txnstmt* stmt = NULL;

    refreshtxn = txn_init(REFRESH_TXNID, InvalidXLogRecPtr, (parserlsn + 1));
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt*)rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_ONLINEREFRESH_BEGIN;
    /* Set lsn for onlinerefreshbegin->stmt */
    stmt->extra0.wal.lsn = parserlsn + 1;

    stmt->stmt = (void*)olstmt;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    txn_addcommit(refreshtxn);

    return refreshtxn;
}

/* Cache cleanup */
void parserwork_wal_destroy(decodingcontext* decodingctx)
{
    if (NULL == decodingctx)
    {
        return;
    }

    if (NULL != decodingctx->rewind_ptr)
    {
        if (NULL != decodingctx->rewind_ptr->conn)
        {
            PQfinish(decodingctx->rewind_ptr->conn);
            decodingctx->rewind_ptr->conn = NULL;
        }

        if (NULL != decodingctx->rewind_ptr->strategy.xips)
        {
            hash_destroy(decodingctx->rewind_ptr->strategy.xips);
        }
        rfree(decodingctx->rewind_ptr);
    }

    if (NULL != decodingctx->monetary)
    {
        rfree(decodingctx->monetary);
    }

    if (NULL != decodingctx->numeric)
    {
        rfree(decodingctx->numeric);
    }

    if (NULL != decodingctx->tzname)
    {
        rfree(decodingctx->tzname);
    }

    if (NULL != decodingctx->orgdbcharset)
    {
        rfree(decodingctx->orgdbcharset);
    }

    /* Cleanup trans_cache */
    transcache_free(decodingctx->trans_cache);
    rfree(decodingctx->trans_cache);
    decodingctx->trans_cache = NULL;

    decodingctx->privdata = NULL;
    decodingctx->recordqueue = NULL;
    decodingctx->parser2txns = NULL;

    rfree(decodingctx);
}
