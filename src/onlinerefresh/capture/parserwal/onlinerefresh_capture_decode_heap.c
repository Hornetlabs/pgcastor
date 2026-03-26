#include "app_incl.h"
#include "port/thread/thread.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/regex/regex.h"
#include "threads/threads.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_errnodef.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "cache/fpwcache.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "catalog/catalog.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "works/parserwork/wal/decode_ddl.h"
#include "works/parserwork/wal/decode_colvalue.h"
#include "task/task_slot.h"
#include "queue/queue.h"
#include "storage/file_buffer.h"
#include "refresh/refresh_tables.h"
#include "strategy/filter_dataset.h"
#include "stmts/txnstmt_refresh.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode_heap.h"
#include "onlinerefresh/capture/onlinerefresh_capture.h"

#define PGTEMP_NAME     "pg_temp"
#define PGTEMP_NAME_LEN 7

#define CHECK_NEED_DDL_TRANS(iscatalog, txn, name) \
    (!(iscatalog) && txn->sysdict && strncmp(name, PGTEMP_NAME, PGTEMP_NAME_LEN))

#define CHECK_CATALOG_BY_OID(oid) ((oid < 16384) ? (true) : (false))

#define HEAP_STORAGE_CATALOG(txn, trans_return) \
    txn->sysdict = lappend(txn->sysdict, (void*)trans_return)

static bool heap_check_catalog(txn* txn, Oid oid)
{
    if (txn->oidmap)
    {
        Oid real = get_real_oid_from_oidmap(txn->oidmap, oid);
        if (real)
        {
            return CHECK_CATALOG_BY_OID(real);
        }
    }
    return CHECK_CATALOG_BY_OID(oid);
}

static bool heap_check_special_table(Oid oid, decodingcontext* decodingctx, txn* txn)
{
    HTAB*                    class_htab = decodingctx->trans_cache->sysdicts->by_class;
    pg_sysdict_Form_pg_class class = NULL;
    class = (pg_sysdict_Form_pg_class)catalog_get_class_sysdict(
        class_htab, txn->sysdict, txn->sysdictHis, oid);
    if (!strncmp(class->relname.data, PGTEMP_NAME, PGTEMP_NAME_LEN) || CHECK_EXTERNAL(class))
    {
        return true;
    }
    return false;
}

static pg_parser_translog_tuplecache* get_tuple_from_cache(HTAB*                        tuple_cache,
                                                           pg_parser_translog_pre_heap* heap_pre)
{
    pg_parser_translog_tuplecache* result = NULL;
    ReorderBufferFPWKey            key = {'\0'};
    ReorderBufferFPWEntry*         entry = NULL;
    bool                           find = false;

    key.blcknum = heap_pre->m_pagenos;
    key.relfilenode = heap_pre->m_relfilenode;
    key.itemoffset = heap_pre->m_tupitemoff;

    entry = hash_search(tuple_cache, &key, HASH_FIND, &find);
    if (!find)
    {
        elog(RLOG_ERROR,
             "can't find tuple cache by relfilenode: %u,"
             " blcknum: %u, itemoffset: %hu",
             key.relfilenode,
             key.blcknum,
             key.itemoffset);
    }
    elog(RLOG_DEBUG,
         "get tuple, rel: %u, blk: %u, off:%hu",
         key.relfilenode,
         key.blcknum,
         key.itemoffset);
    result = rmalloc0(sizeof(pg_parser_translog_tuplecache));
    result->m_itemoffnum = key.itemoffset;
    result->m_pageno = key.blcknum;
    result->m_tuplelen = entry->len;
    result->m_tupledata = rmalloc0(entry->len);
    rmemset0(result->m_tupledata, 0, 0, entry->len);
    rmemcpy0(result->m_tupledata, 0, entry->data, entry->len);

    return result;
}

static void storage_tuple(transcache*                   storage,
                          XLogRecPtr                    lsn,
                          pg_parser_translog_tbcolbase* trans_return)
{
    ReorderBufferFPWKey   key = {'\0'};
    ReorderBufferFPWEntry entry = {'\0'};

    if (!storage->by_fpwtuples)
    {
        storage->by_fpwtuples = fpwcache_init(storage);
    }

    entry.lsn = lsn;

    /* multi insert return structure is different, needs to be handled separately from other dml */
    if (trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
    {
        int                               index_tuple_cnt = 0;
        pg_parser_translog_tbcol_nvalues* nvalues = (pg_parser_translog_tbcol_nvalues*)trans_return;

        key.relfilenode = nvalues->m_relfilenode;
        for (index_tuple_cnt = 0; index_tuple_cnt < nvalues->m_tupleCnt; index_tuple_cnt++)
        {
            key.blcknum = nvalues->m_tuple[index_tuple_cnt].m_pageno;
            key.itemoffset = nvalues->m_tuple[index_tuple_cnt].m_itemoffnum;

            entry.blcknum = key.blcknum;
            entry.itemoffset = key.itemoffset;
            entry.relfilenode = key.relfilenode;

            entry.data = nvalues->m_tuple[index_tuple_cnt].m_tupledata;
            entry.len = nvalues->m_tuple[index_tuple_cnt].m_tuplelen;

            fpwcache_add(storage, &key, &entry);
        }
    }
    else
    {
        int                              index_tuple_cnt = 0;
        pg_parser_translog_tbcol_values* values = (pg_parser_translog_tbcol_values*)trans_return;

        key.relfilenode = values->m_relfilenode;
        for (index_tuple_cnt = 0; index_tuple_cnt < values->m_tupleCnt; index_tuple_cnt++)
        {
            key.blcknum = values->m_tuple[index_tuple_cnt].m_pageno;
            key.itemoffset = values->m_tuple[index_tuple_cnt].m_itemoffnum;

            entry.blcknum = key.blcknum;
            entry.itemoffset = key.itemoffset;
            entry.relfilenode = key.relfilenode;

            entry.data = values->m_tuple[index_tuple_cnt].m_tupledata;
            entry.len = values->m_tuple[index_tuple_cnt].m_tuplelen;
            elog(RLOG_DEBUG,
                 "storage tuple, rel: %u, blk: %u, off:%hu",
                 key.relfilenode,
                 key.blcknum,
                 key.itemoffset);

            fpwcache_add(storage, &key, &entry);
        }
    }
}

static void init_heap_trans_data(pg_parser_translog_translog2col* trans_data,
                                 decodingcontext*                 decodingctx,
                                 txn*                             txn,
                                 pg_parser_translog_pre_heap*     heap_pre,
                                 Oid                              oid)
{
    bool search_his = true;

    if (heap_pre->m_needtuple && trans_data->m_iscatalog)
    {
        trans_data->m_tuplecnt = 1;
        trans_data->m_tuples =
            get_tuple_from_cache(decodingctx->trans_cache->by_fpwtuples, heap_pre);
    }
    else
    {
        trans_data->m_tuplecnt = 0;
        trans_data->m_tuples = NULL;
    }

    /*trans_data->m_iscatalog has been assigned before calling init*/

    /* Reuse parts of pre-parser input parameters, no need to release after secondary parsing */
    trans_data->m_pagesize = decodingctx->walpre.m_pagesize;
    trans_data->m_record = decodingctx->walpre.m_record;
    trans_data->m_dbtype = decodingctx->walpre.m_dbtype;
    trans_data->m_dbversion = decodingctx->walpre.m_dbversion;
    trans_data->m_debugLevel = decodingctx->walpre.m_debugLevel;
    trans_data->m_walLevel = decodingctx->walpre.m_walLevel;

    // todo free
    /* Build convert structure */
    trans_data->m_convert = rmalloc0(sizeof(pg_parser_translog_convertinfo));
    trans_data->m_convert->m_dbcharset = decodingctx->orgdbcharset;
    trans_data->m_convert->m_tartgetcharset = decodingctx->tgtdbcharset;
    trans_data->m_convert->m_tzname = decodingctx->tzname;
    trans_data->m_convert->m_monetary = decodingctx->monetary;
    trans_data->m_convert->m_numeric = decodingctx->numeric;

    search_his = true;
    trans_data->m_sysdicts = heap_get_sysdict_by_oid((void*)decodingctx, txn, oid, search_his);
}

/* Determine if capture is needed based on dboid */
static bool heap_check_dboid(uint32_t dboid, uint32_t capture_dboid)
{
    if (dboid && dboid != capture_dboid)
    {
        return false;
    }
    return true;
}

static void trans_cache_dlist_append(decodingcontext* ctx, txn* txn)
{
    if (NULL == txn)
    {
        return;
    }

    if (NULL == ctx->trans_cache->transdlist->head)
    {
        ctx->trans_cache->transdlist->head = txn;
        ctx->trans_cache->transdlist->tail = txn;
    }
    else
    {
        ctx->trans_cache->transdlist->tail->next = txn;
        txn->prev = ctx->trans_cache->transdlist->tail;
        ctx->trans_cache->transdlist->tail = txn;
    }
}

static txn* trans_cache_onlinerefresh_getTXNByXid(void* in_ctx, uint64_t xid)
{
    bool             find = false;
    HTAB*            tx_htab = NULL;
    decodingcontext* ctx = NULL;
    txn*             txn_entry = NULL;

    /* Invalid transaction, no need to maintain in hash */
    if (InvalidFullTransactionId == xid)
    {
        return NULL;
    }

    ctx = (decodingcontext*)in_ctx;
    tx_htab = ctx->trans_cache->by_txns;

    txn_entry = (txn*)hash_search(tx_htab, &xid, HASH_ENTER, &find);
    if (!find)
    {
        /* First time capturing this transaction */
        onlinerefresh_capture* olcapture = ctx->privdata;

        /* Initialize */
        txn_initset(txn_entry, xid, ctx->decode_record->start.wal.lsn);
        TXN_SET_TRANS_INHASH(txn_entry->flag);

        /* Add transaction to doubly linked list */
        trans_cache_dlist_append(ctx, txn_entry);

        /* Transactions less than xmin do not need to be captured */
        if (xid < olcapture->snapshot->xmin)
        {
            txn_entry->filter = true;
            return txn_entry;
        }

        /* Transactions greater than txid exceed the capture range */
        if (xid >= olcapture->txid)
        {
            txn_entry->filter = true;
            return txn_entry;
        }

        /*
         * 1、Capture transactions between xmax--->txid
         * 2、Capture active transactions between xmin--->xmax
         */
        if ((xid != olcapture->snapshot->xmin && xid >= olcapture->snapshot->xmax) ||
            onlinerefresh_capture_isxidinsnapshot(olcapture, (FullTransactionId)xid))
        {
            elog(RLOG_INFO, "xid: %lu, xmax: %lu", xid, olcapture->snapshot->xmax);
            onlinerefresh_capture_xids_append(olcapture, xid);
        }
        else
        {
            if (!onlinerefresh_capture_isxidinxids(olcapture, xid))
            {
                txn_entry->filter = true;
                return txn_entry;
            }
        }
    }

    return txn_entry;
}

void onlinerefresh_decode_heap(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    pg_parser_translog_translog2col* trans_data = NULL;
    pg_parser_translog_tbcolbase*    trans_return = NULL;
    pg_parser_translog_pre_heap*     heap_pre = (pg_parser_translog_pre_heap*)pbase;
    pg_parser_sysdict_pgclass*       temp_class = NULL;
    txn*                             txn = NULL;
    bool                             find = false;
    bool                             is_catalog = false;
    bool                             trans_restart = false;
    char*                            table_name = NULL;

    Oid                              oid = 0;
    int32_t                          err_num = 0;

    bool                             isexternal = false;

    if (!heap_check_dboid(heap_pre->m_dboid, decodingctx->database))
    {
        return;
    }

    if (!heap_pre->m_dboid)
    {
        heap_pre->m_dboid = decodingctx->database;
    }

    if (decodingctx->decode_record->start.wal.lsn < decodingctx->base.restartlsn)
    {
        trans_restart = true;
    }

    // Get current transaction information
    txn = trans_cache_onlinerefresh_getTXNByXid((void*)decodingctx, pbase->m_xid);

    /* Get oid through relfilenode */
    oid = catalog_get_oid_by_relfilenode(decodingctx->trans_cache->sysdicts->by_relfilenode,
                                         txn->sysdictHis,
                                         txn->sysdict,
                                         heap_pre->m_dboid,
                                         heap_pre->m_tbspcoid,
                                         heap_pre->m_relfilenode,
                                         true);

    is_catalog = heap_check_catalog(txn, oid);

    temp_class = (pg_parser_sysdict_pgclass*)catalog_get_class_sysdict(
        decodingctx->trans_cache->sysdicts->by_class, txn->sysdict, txn->sysdictHis, oid);
    table_name = temp_class->relname.data;
    /* Before parsing data, first determine whether to parse DDL */
    if (CHECK_NEED_DDL_TRANS(is_catalog, txn, table_name))
    {
        /* Do not parse DDL, but maintain DDL-related cache */
        transcache_sysdict2his(txn);
        transcache_sysdict_free(txn);
    }

    /* Filter transactions containing state tables bidirectionally */
    if (true == txn->filter)
    {
        if (!is_catalog)
        {
            return;
        }
    }
    else
    {
        hash_search(decodingctx->trans_cache->htxnfilterdataset, &oid, HASH_FIND, &find);
        if (true == find)
        {
            txn->filter = true;
            return;
        }
    }

    /* Check if capture is needed */
    if (!is_catalog && !heap_check_special_table(oid, decodingctx, txn))
    {
        /* Normal table statements not in filter set or not needing capture will be filtered */
        if ((false == filter_dataset_dml(decodingctx->trans_cache->hsyncdataset, oid) &&
             false == filter_dataset_dml(txn->hsyncdataset, oid)) ||
            true == txn->filter)
        {
            return;
        }
    }

    if (trans_restart)
    {
        /* Not parsing system tables, toast tables of system tables are also system tables, so no
         * need to worry about toast being filtered */
        if (!is_catalog)
        {
            return;
        }
    }

    trans_data = rmalloc0(sizeof(pg_parser_translog_translog2col));
    rmemset0(trans_data, 0, 0, sizeof(pg_parser_translog_translog2col));
    trans_data->m_iscatalog = is_catalog;
    elog(RLOG_DEBUG,
         "oid: %u, relfilenode:%u, iscatalog: %s",
         oid,
         heap_pre->m_relfilenode,
         trans_data->m_iscatalog ? "true" : "false");

    /* Initialize input parameters */
    init_heap_trans_data(trans_data, decodingctx, txn, heap_pre, oid);

    /* Check if it is pg_toast data, pgclass record related to oid must be the first one */
    isexternal = CHECK_EXTERNAL(&trans_data->m_sysdicts->m_pg_class.m_pg_class[0]);

    /* Call parsing interface */
    if (!pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
    {
        elog(RLOG_ERROR,
             "error in trans heap errcode: %x, msg: %s",
             err_num,
             pg_parser_errno_getErrInfo(err_num));
    }

    if (trans_data->m_iscatalog)
    {
        storage_tuple(
            decodingctx->trans_cache, decodingctx->decode_record->start.wal.lsn, trans_return);
    }

    /* If operating on pg_temp tables in pg_class, first use the mapping we saved */
    if (!strcmp(trans_return->m_schemaname, "pg_catalog") &&
        !strcmp(trans_return->m_tbname, "pg_class") &&
        trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_INSERT)
    {
        pg_parser_translog_tbcol_values* col = (pg_parser_translog_tbcol_values*)trans_return;

        char*                            temp_relname = get_class_value_from_colvalue(
            col->m_new_values, CLASS_MAPNUM_RELNAME, g_idbtype, g_idbversion);

        if (temp_relname && !strncmp(temp_relname, "pg_temp_", 8))
        {
            uint32_t real_oid = 0;
            char*    temp_str = temp_relname;
            char*    temp_nspname = get_class_value_from_colvalue(
                col->m_new_values, CLASS_MAPNUM_RELNSPOID, g_idbtype, g_idbversion);
            temp_str = temp_str + 8;
            real_oid = (uint32_t)atoi(temp_str);

            if (CHECK_CATALOG_BY_OID(real_oid) && temp_nspname && !strcmp(temp_nspname, "11"))
            {
                Oid   temp_oid = INVALIDOID;
                char* temp_oid_char = NULL;

                rfree(col->m_new_values[7].m_value);
                col->m_new_values[7].m_value = rstrdup((char*)col->m_new_values[0].m_value);

                temp_oid_char = get_class_value_from_colvalue(
                    col->m_new_values, CLASS_MAPNUM_OID, g_idbtype, g_idbversion);
                temp_oid = (Oid)atoi(temp_oid_char);

                free_class_value_from_colvalue(
                    col->m_new_values, CLASS_MAPNUM_RELFILENODE, g_idbtype, g_idbversion);
                set_class_value_from_colvalue(col->m_new_values,
                                              temp_oid_char,
                                              CLASS_MAPNUM_RELFILENODE,
                                              g_idbtype,
                                              g_idbversion);

                if (!txn->oidmap)
                {
                    txn->oidmap = init_oidmap_hash();
                }

                elog(RLOG_DEBUG,
                     "capture catalog temp table, oid:%u, relfilenode :%u, real oid: %u",
                     temp_oid,
                     temp_oid,
                     real_oid);

                add_oidmap(txn->oidmap, temp_oid, real_oid);
            }
        }
    }

    /* Determine if it is an external storage table, save external storage data */
    if (isexternal)
    {
        heap_storage_external_data(txn, trans_return);
        heap_free_trans_result(trans_return);
        TXN_SET_TRANS_TOAST(txn->flag);
    }
    else if (trans_data->m_iscatalog)
    {
        /*
         * Save system table secondary parsing results
         * Add system table data to trans_cache_txn->sysdict
         * System table data does not need to be converted to sql statements
         */
        txn_sysdict* dict = rmalloc0(sizeof(txn_sysdict));
        dict->colvalues = (pg_parser_translog_tbcol_values*)trans_return;
        dict->convert_colvalues = NULL;
        HEAP_STORAGE_CATALOG(txn, dict);
        TXN_SET_TRANS_DDL(txn->flag);
    }
    else if (strncmp(trans_return->m_tbname, PGTEMP_NAME, PGTEMP_NAME_LEN))
    {
        /* Normal statement, iterate through parsed values, convert external storage, save parsing
         * results */
        heap_parser_count_size((void*)decodingctx, txn, trans_return, oid);
    }
    else
    {
        /* pg_temp temporary table, no need to parse statement */
        elog(RLOG_DEBUG, "get temp table by decode heap, ignore");

        heap_free_trans_result(trans_return);
    }
    heap_free_trans_pre(trans_data);
}
