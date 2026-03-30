#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/regex/regex.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "loadrecords/record.h"
#include "queue/queue.h"
#include "threads/threads.h"
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
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "misc/misc_stat.h"
#include "misc/misc_control.h"
#include "queue/queue.h"
#include "snapshot/snapshot.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "onlinerefresh/capture/onlinerefresh_capture.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "works/splitwork/wal/wal_define.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_checkpoint.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_parser.h"
#include "metric/capture/metric_capture.h"
#include "strategy/filter_dataset.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode.h"
#include "works/parserwork/wal/parserwork_wal.h"

static decodingcontext* onlinerefresh_captureparser_decodingctxinit(void)
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
    decodingctx->decode_record = NULL;
    decodingctx->stat = DECODINGCONTEXT_INIT;
    decodingctx->filterbigtrans = false;

    /* Load BASE information from disk */
    misc_stat_loaddecode((void*)&decodingctx->base);
    decodingctx->decode_record = NULL;

    decodingctx->trans_cache = (transcache*)rmalloc0(sizeof(transcache));
    if (NULL == decodingctx->trans_cache)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->trans_cache, 0, '\0', sizeof(transcache));
    decodingctx->trans_cache->tableincludes = filter_dataset_inittableinclude(decodingctx->trans_cache->tableincludes);
    decodingctx->trans_cache->tableexcludes = filter_dataset_inittableexclude(decodingctx->trans_cache->tableexcludes);
    decodingctx->trans_cache->addtablepattern =
        filter_dataset_initaddtablepattern(decodingctx->trans_cache->addtablepattern);

    /* Add information required by parsing library */
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

    /* Initialize capture_buffer and convert to byte size */
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

    /* rewind initialization */
    decodingctx->rewind_ptr = (rewind_info*)rmalloc0(sizeof(rewind_info));
    if (NULL == decodingctx->rewind_ptr)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->rewind_ptr, 0, '\0', sizeof(rewind_info));

    /* fpw initialization */
    decodingctx->trans_cache->by_fpwtuples = fpwcache_init(decodingctx->trans_cache);

    return decodingctx;
}

/* Generate filter set, only includes tables waiting for incremental work */
bool onlinerefresh_captureparser_datasetinit(decodingcontext* ctx, onlinerefresh_capture* onlinerefresh)
{
    refresh_tables*         tables = onlinerefresh->tables;
    refresh_table*          table_node = NULL;
    HASHCTL                 hashCtl_o2d = {'\0'};
    filter_oid2datasetnode* temp_o2d_entry = NULL;

    /* Create oid2dataset hash */
    hashCtl_o2d.keysize = sizeof(Oid);
    hashCtl_o2d.entrysize = sizeof(filter_oid2datasetnode);

    ctx->trans_cache->hsyncdataset =
        hash_create("online refresh filter_o2d_htab", 256, &hashCtl_o2d, HASH_ELEM | HASH_BLOBS);

    table_node = tables->tables;

    while (table_node)
    {
        temp_o2d_entry = hash_search(ctx->trans_cache->hsyncdataset, &table_node->oid, HASH_ENTER, NULL);
        if (!temp_o2d_entry)
        {
            elog(RLOG_WARNING, "can't insert hsyncdataset hash");
            return false;
        }
        temp_o2d_entry->oid = table_node->oid;
        strcpy(temp_o2d_entry->dataset.schema, table_node->schema);
        strcpy(temp_o2d_entry->dataset.table, table_node->table);

        table_node = table_node->next;
    }

    return true;
}

onlinerefresh_captureparser* onlinerefresh_captureparser_init(void)
{
    onlinerefresh_captureparser* result = NULL;

    result = rmalloc0(sizeof(onlinerefresh_captureparser));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    result = rmemset0(result, 0, 0, sizeof(onlinerefresh_captureparser));
    result->decodingctx = onlinerefresh_captureparser_decodingctxinit();

    return result;
}

/*
 * Set basic information required by the parser
 *  1、Charset/timezone/source charset/target charset
 *  2、Load system dictionary
 *  3、Build checkpoint information
 */
void onlinerefresh_captureparser_loadmetadata(onlinerefresh_captureparser* olcparser)
{
    Oid              dbid = INVALIDOID;
    decodingcontext* decodingctx = NULL;

    decodingctx = olcparser->decodingctx;

    /* Load BASE information from disk */
    misc_stat_loaddecode((void*)&decodingctx->base);

    decodingctx->database = misc_controldata_database_get(NULL);
    decodingctx->monetary = rstrdup(misc_controldata_monetary_get());
    decodingctx->numeric = rstrdup(misc_controldata_numeric_get());
    decodingctx->tzname = rstrdup(misc_controldata_timezone_get());
    decodingctx->orgdbcharset = rstrdup(misc_controldata_orgencoding_get());
    decodingctx->tgtdbcharset = misc_controldata_dstencoding_get();

    /*Load dictionary table*/
    dbid = misc_controldata_database_get(NULL);
    cache_sysdictsload((void**)&decodingctx->trans_cache->sysdicts);

    decodingctx->trans_cache->htxnfilterdataset =
        filter_dataset_txnfilterload(decodingctx->trans_cache->sysdicts->by_namespace,
                                     decodingctx->trans_cache->sysdicts->by_class);

    decodingctx->trans_cache->sysdicts->by_relfilenode =
        cache_sysdicts_buildrelfilenode2oid(dbid, (void*)decodingctx->trans_cache->sysdicts);
    /* checkpoint node initialization */
    decode_chkpt_init(decodingctx, decodingctx->base.redolsn);

    elog(RLOG_INFO,
         "capture onlinerefresh parser from, restartlsn %X/%X, last commit lsn:%X/%X, fileid:%lu, "
         "offset:%u, curtlid:%u",
         (uint32)(decodingctx->base.restartlsn >> 32),
         (uint32)decodingctx->base.restartlsn,
         (uint32)(decodingctx->base.confirmedlsn >> 32),
         (uint32)decodingctx->base.confirmedlsn,
         decodingctx->base.fileid,
         decodingctx->base.fileoffset,
         decodingctx->base.curtlid);
}

/* Traverse and parse record */
static void onlinerefresh_captureparser_parser(decodingcontext* decodingctx, dlist* record_dlist)
{
    record*                rec = NULL;
    onlinerefresh_capture* olcapture = (onlinerefresh_capture*)decodingctx->privdata;
    dlistnode*             dlnode = NULL;

    dlnode = record_dlist->head;

    while (dlnode)
    {
        rec = (record*)dlnode->value;

        /* Parse record */
        decodingctx->decode_record = rec;

        /* Call parsing function */
        g_parserecno++;
        parserwork_waldecode_onlinerefresh(decodingctx);
        decodingctx->decode_record = NULL;

        if (onlinerefresh_capture_xids_isnull(olcapture))
        {
            break;
        }
        dlnode = dlnode->next;
    }
}

void* onlinerefresh_captureparser_main(void* args)
{
    int                          timeout = 0;
    thrnode*                     thr_node = NULL;
    decodingcontext*             decodingctx = NULL;
    onlinerefresh_capture*       olcapture = NULL;
    onlinerefresh_captureparser* parser_task = NULL;
    dlist*                       record_dlist = NULL;

    thr_node = (thrnode*)args;

    parser_task = (onlinerefresh_captureparser*)thr_node->data;
    decodingctx = parser_task->decodingctx;
    olcapture = (onlinerefresh_capture*)decodingctx->privdata;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh capture parser stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }
    thr_node->stat = THRNODE_STAT_WORK;

    while (1)
    {
        record_dlist = NULL;

        /* First check if exit signal is received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Check if parsing is complete */
        if (true == onlinerefresh_capture_xids_isnull(olcapture))
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data */
        record_dlist = queue_get(decodingctx->recordqueue, &timeout);
        if (NULL == record_dlist)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                continue;
            }
            elog(RLOG_WARNING, "onlinerefresh capture get records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Get data according to entry content */
        onlinerefresh_captureparser_parser(decodingctx, record_dlist);

        /* record doubly linked list memory release */
        dlist_free(record_dlist, (dlistvaluefree)record_free);
    }
    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_captureparser_free(void* args)
{
    onlinerefresh_captureparser* olparserwal = NULL;

    olparserwal = (onlinerefresh_captureparser*)args;
    if (olparserwal)
    {
        if (olparserwal->decodingctx)
        {
            parserwork_wal_destroy(olparserwal->decodingctx);
        }
        rfree(olparserwal);
    }
}
