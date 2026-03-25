#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_errnodef.h"
#include "common/pg_parser_translog.h"
#include "stmts/txnstmt.h"
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
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_heap.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "works/parserwork/wal/decode_ddl.h"
#include "works/parserwork/wal/decode_colvalue.h"
#include "utils/regex/regex.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "strategy/filter_dataset.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/bigtxn.h"

#define PGTEMP_NAME "pg_temp"
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
    class = (pg_sysdict_Form_pg_class)catalog_get_class_sysdict(class_htab, txn->sysdict,
                                                                txn->sysdictHis, oid);
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
             key.relfilenode, key.blcknum, key.itemoffset);
    }
    elog(RLOG_DEBUG, "get tuple, rel: %u, blk: %u, off:%hu", key.relfilenode, key.blcknum,
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

static void storage_tuple(transcache* storage, XLogRecPtr lsn,
                          pg_parser_translog_tbcolbase* trans_return)
{
    ReorderBufferFPWKey   key = {'\0'};
    ReorderBufferFPWEntry entry = {'\0'};

    if (!storage->by_fpwtuples)
    {
        storage->by_fpwtuples = fpwcache_init(storage);
    }

    entry.lsn = lsn;

    /* multi insert returns different structure, need to process separately from other dml */
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
            elog(RLOG_DEBUG, "storage tuple, rel: %u, blk: %u, off:%hu", key.relfilenode,
                 key.blcknum, key.itemoffset);

            fpwcache_add(storage, &key, &entry);
        }
    }
}

static bool large_txn_filter(decodingcontext* ctx)
{
    uint64     max_stmtsize = 0;
    txn*       max_txn = NULL;
    txn*       current = NULL;
    txnstmt*   txnstmt = NULL;
    txn_dlist* transdlist = NULL;

    UNUSED(max_txn);

    /* Check if need to filter large transaction */
    if (!ctx->filterbigtrans)
    {
        return true;
    }

    transdlist = ctx->trans_cache->transdlist;

    if (transdlist == NULL)
    {
        elog(RLOG_WARNING, " transdlist is null");
        return true;
    }

    /* Iterate all transactions, select the largest one not in DDL and TOAST */
    for (current = transdlist->head; NULL != current; current = current->next)
    {
        if (TXN_CHECK_COULD_SAVE(current->flag))
        {
            continue;
        }

        if (max_stmtsize < current->stmtsize)
        {
            max_stmtsize = current->stmtsize;
            max_txn = current;
        }
    }

    if (NULL == max_txn)
    {
        elog(RLOG_WARNING, " Not found valid transactions ");
        return true;
    }

    /* Already got selected transaction, copy necessary info and put into cache */
    if (!TXN_ISBIGTXN(max_txn->flag))
    {
        txn*               begintxn = NULL;
        txn*               copytxn = NULL;
        FullTransactionId* xid = NULL;

        /* First time put into large transaction serialization cache */
        TXN_SET_BIGTXN(max_txn->flag);
        max_txn->type = TXN_TYPE_BIGTXN_BEGIN;

        /* Build large transaction begin txnstmt */
        begintxn = txn_initbigtxn(max_txn->xid);
        if (NULL == begintxn)
        {
            elog(RLOG_WARNING, "init big txn error");
            return false;
        }
        begintxn->end.wal.lsn = ctx->parselsn + 1;

        /* Large transaction begin stmt */
        txnstmt = txnstmt_init();
        if (NULL == txnstmt)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return false;
        }
        xid = rmalloc0(sizeof(FullTransactionId));
        if (NULL == xid)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return false;
        }
        rmemset0(xid, 0, 0, sizeof(FullTransactionId));
        *xid = max_txn->xid;
        txnstmt->type = TXNSTMT_TYPE_BIGTXN_BEGIN;
        txnstmt->stmt = (void*)xid;
        txnstmt->extra0.wal.lsn = ctx->parselsn + 1;
        begintxn->stmts = lappend(begintxn->stmts, txnstmt);

        /* Add to cache */
        txn_addcommit(begintxn);
        cache_txn_add(ctx->parser2txns, begintxn);

        /* Perform txn copy */
        copytxn = txn_copy(max_txn);

        if (max_txn->sysdict)
        {
            elog(RLOG_WARNING, "big txn copy, txn in ddl");
            return false;
        }

        /* Reset some pointers of original txn */
        max_txn->sysdictHis = NULL;
        max_txn->stmts = NULL;
        /* Initialize stmt size */
        max_txn->stmtsize = 4;

        /* Set some pointers to NULL after copy */
        copytxn->toast_hash = NULL;
        copytxn->hsyncdataset = NULL;
        copytxn->oidmap = NULL;
        copytxn->prev = NULL;
        copytxn->next = NULL;
        copytxn->cachenext = NULL;

        /* Handle system catalog, perform copy */
        max_txn->sysdictHis = decode_heap_sysdicthis_copy(copytxn->sysdictHis);

        /* Add to large transaction cache */
        ctx->trans_cache->totalsize -= (copytxn->stmtsize - 4);
        cache_txn_add(ctx->parser2bigtxns, copytxn);
    }
    else
    {
        /* Second time and onwards */
        txn* copytxn = NULL;

        /* Cancel setting as large transaction begin */
        max_txn->type = TXN_TYPE_NORMAL;

        /* Perform txn copy */
        copytxn = txn_copy(max_txn);

        if (max_txn->sysdict)
        {
            elog(RLOG_WARNING, "big txn copy, txn in ddl");
            return false;
        }

        /* Reset some pointers of original txn */
        max_txn->sysdictHis = NULL;
        max_txn->stmts = NULL;
        /* Initialize stmt size */
        max_txn->stmtsize = 4;

        /* Set some pointers to NULL after copy */
        copytxn->toast_hash = NULL;
        copytxn->hsyncdataset = NULL;
        copytxn->oidmap = NULL;
        copytxn->prev = NULL;
        copytxn->next = NULL;
        copytxn->cachenext = NULL;

        /* Handle system catalog, perform copy */
        max_txn->sysdictHis = decode_heap_sysdicthis_copy(copytxn->sysdictHis);

        /* Add to large transaction cache */
        ctx->trans_cache->totalsize -= (copytxn->stmtsize - 4);
        cache_txn_add(ctx->parser2bigtxns, copytxn);
    }
    return true;
}

static void init_heap_trans_data(pg_parser_translog_translog2col* trans_data,
                                 decodingcontext* decodingctx, txn* txn,
                                 pg_parser_translog_pre_heap* heap_pre, Oid oid)
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

    /* trans_data->m_iscatalog is assigned before calling init */

    /* Reuse some pre-decode input parameters, no need to free after second decode */
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

/*
 * Check if need to capture by dboid
 *  1, Tables in global database need to capture
 *  2, Database being captured
 */
static bool heap_check_dboid(uint32_t dboid, uint32_t capture_dboid)
{
    /* In transaction log, global database oid is 0, and tables in global database also need to be
     * parsed */
    if (dboid && dboid != capture_dboid)
    {
        return false;
    }
    return true;
}

void decode_heap(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    bool                             find = false;
    bool                             isexternal = false;
    bool                             is_catalog = false;
    bool                             recovery = false;
    Oid                              oid = 0;
    int32_t                          err_num = 0;
    txn*                             txn = NULL;
    char*                            table_name = NULL;
    pg_parser_translog_translog2col* trans_data = NULL;
    pg_parser_translog_tbcolbase*    trans_return = NULL;
    pg_parser_translog_pre_heap*     heap_pre = NULL;
    pg_parser_sysdict_pgclass*       temp_class = NULL;

    heap_pre = (pg_parser_translog_pre_heap*)pbase;

    /*----------------Pre-process data before calling parser interface
     * begin------------------------*/
    /* Check if it's the database need to parse */
    if (!heap_check_dboid(heap_pre->m_dboid, decodingctx->database))
    {
        return;
    }

    /* If it's global database, reset to current database */
    if (INVALIDOID == heap_pre->m_dboid)
    {
        heap_pre->m_dboid = decodingctx->database;
    }

    /* Data between redolsn--->restartlsn only needs system catalog */
    if (decodingctx->decode_record->start.wal.lsn < decodingctx->base.restartlsn)
    {
        recovery = true;
    }

    /* Get current transaction info */
    txn = transcache_getTXNByXid((void*)decodingctx, pbase->m_xid);

    /* Get oid by relfilenode */
    oid = catalog_get_oid_by_relfilenode(decodingctx->trans_cache->sysdicts->by_relfilenode,
                                         txn->sysdictHis, txn->sysdict, heap_pre->m_dboid,
                                         heap_pre->m_tbspcoid, heap_pre->m_relfilenode, true);

    is_catalog = heap_check_catalog(txn, oid);
    if (recovery)
    {
        /*
         * In recovery mode, only capture system catalog data
         *  System catalog's toast table oid is also less than 16384
         *      Currently known case that changes toast table oid is only: alter table ... column
         * type
         */
        if (!is_catalog)
        {
            return;
        }
    }

    temp_class = (pg_parser_sysdict_pgclass*)catalog_get_class_sysdict(
        decodingctx->trans_cache->sysdicts->by_class, txn->sysdict, txn->sysdictHis, oid);
    table_name = temp_class->relname.data;

    /*
     * Before parsing data, check if need to parse ddl
     * Conditions for redeeming DDL
     *  1, Current record is non-system catalog, check condition
     *      1.1 Oid > 16384
     *      1.2 Table name does not start with pg_temp
     *  2, Contains pending redeem system dictionary
     */
    if (CHECK_NEED_DDL_TRANS(is_catalog, txn, table_name))
    {
        /* Try to handle possible update intermediate statement */
        dml2ddl(decodingctx, txn);
        transcache_sysdict2his(txn);
        transcache_sysdict_free(txn);
        TXN_UNSET_TRANS_DDL(txn->flag);
    }

    /* Bidirectional filtering of transactions containing state table */
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

    if (!is_catalog && !heap_check_special_table(oid, decodingctx, txn))
    {
        if ((false == filter_dataset_dml(decodingctx->trans_cache->hsyncdataset, oid) &&
             false == filter_dataset_dml(txn->hsyncdataset, oid)))
        {
            return;
        }
    }

    /*
     * Check if it's the table need to parse, tables need to sync are the following four types
     *  1, System catalog
     *  2, Tables starting with pg_temp need to sync
     *  3, Tables under PG_TOAST_NAMESPACE mode (TOAST)
     *  4, Tables in sync dataset
     */
    if (!is_catalog && !heap_check_special_table(oid, decodingctx, txn))
    {
        if ((false == filter_dataset_dml(decodingctx->trans_cache->hsyncdataset, oid) &&
             false == filter_dataset_dml(txn->hsyncdataset, oid)))
        {
            return;
        }
    }

    /* Large transaction filter */
    if (decodingctx->trans_cache->totalsize >= decodingctx->trans_cache->capture_buffer)
    {
        large_txn_filter(decodingctx);
    }

    /*----------------Pre-process data before calling parser interface end------------------------*/

    /*----------------Prepare data for calling parser interface begin--------------------------*/
    trans_data = rmalloc0(sizeof(pg_parser_translog_translog2col));
    if (NULL == trans_data)
    {
        elog(RLOG_WARNING, "heap out of memory");
        return;
    }
    rmemset0(trans_data, 0, 0, sizeof(pg_parser_translog_translog2col));
    trans_data->m_iscatalog = is_catalog;
    elog(RLOG_DEBUG, "oid: %u, relfilenode:%u, iscatalog: %s", oid, heap_pre->m_relfilenode,
         trans_data->m_iscatalog ? "true" : "false");

    /* Initialize input parameters */
    init_heap_trans_data(trans_data, decodingctx, txn, heap_pre, oid);

    /* Check if it's pg_toast data, oid related pgclass record is always the first one */
    isexternal = CHECK_EXTERNAL(&trans_data->m_sysdicts->m_pg_class.m_pg_class[0]);

    /*----------------Prepare data for calling parser interface end--------------------------*/

    /* Call parser interface */
    if (!pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
    {
        elog(RLOG_ERROR, "error in trans heap errcode: %x, msg: %s", err_num,
             pg_parser_errno_getErrInfo(err_num));
    }

    if (trans_data->m_iscatalog)
    {
        storage_tuple(decodingctx->trans_cache, decodingctx->decode_record->start.wal.lsn,
                      trans_return);
    }

    /*
     * Operations on records within pg_class table
     *  For example: vacuum full table
     *        truncate table
     */
    if (0 == strcmp(trans_return->m_schemaname, "pg_catalog") &&
        0 == strcmp(trans_return->m_tbname, "pg_class") &&
        PG_PARSER_TRANSLOG_DMLTYPE_INSERT == trans_return->m_dmltype)
    {
        char*                            temp_relname = NULL;
        pg_parser_translog_tbcol_values* col = NULL;

        col = (pg_parser_translog_tbcol_values*)trans_return;
        temp_relname = get_class_value_from_colvalue(col->m_new_values, CLASS_MAPNUM_RELNAME,
                                                     g_idbtype, g_idbversion);

        /*
         * Steps for vacuum full table in pg series database are as follows:
         *  1, Create new table, new table name is: pg_temp_OLD_TABLE_OID
         *      The new table's structure is consistent with old table
         *  2, Import new table's data into old table
         *  3, Exchange new table's relfilenode with old table's relfilenode
         *  4, If the table vacuum full is a system catalog, transaction commit will record:
         * RM_RELMAP_ID->XLOG_RELMAP_UPDATE new mapping relationship
         *
         * The processing logic below is to record the mapping between pg_temp_ prefix table's oid
         * and current table's oid
         */
        if (temp_relname && 0 == strncmp(temp_relname, "pg_temp_", 8))
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

                temp_oid_char = get_class_value_from_colvalue(col->m_new_values, CLASS_MAPNUM_OID,
                                                              g_idbtype, g_idbversion);
                temp_oid = (Oid)atoi(temp_oid_char);

                free_class_value_from_colvalue(col->m_new_values, CLASS_MAPNUM_RELFILENODE,
                                               g_idbtype, g_idbversion);
                set_class_value_from_colvalue(col->m_new_values, temp_oid_char,
                                              CLASS_MAPNUM_RELFILENODE, g_idbtype, g_idbversion);

                if (!txn->oidmap)
                {
                    txn->oidmap = init_oidmap_hash();
                }

                elog(RLOG_DEBUG,
                     "capture catalog temp table, oid:%u, relfilenode :%u, real oid: %u", temp_oid,
                     temp_oid, real_oid);

                add_oidmap(txn->oidmap, temp_oid, real_oid);
            }
        }
    }

    /* Check if TOAST table, save TOAST data */
    if (isexternal)
    {
        heap_storage_external_data(txn, trans_return);
        heap_free_trans_result(trans_return);
        TXN_SET_TRANS_TOAST(txn->flag);
    }
    else if (trans_data->m_iscatalog)
    {
        /*
         * Save system catalog secondary parsing result
         * Add system catalog data to trans_cache_txn->sysdict
         * System catalog data does not need to be converted to SQL
         */
        if (trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
        {
            /* For pg14 and above, DDL statements for system tables have been optimized to multi
             * insert */
            txn->sysdict =
                decode_heap_multi_insert_save_sysdict_as_insert(txn->sysdict, trans_return);

            TXN_SET_TRANS_DDL(txn->flag);
        }
        else
        {
            txn_sysdict* dict = rmalloc0(sizeof(txn_sysdict));

            dict->colvalues = (pg_parser_translog_tbcol_values*)trans_return;
            dict->convert_colvalues = NULL;
            HEAP_STORAGE_CATALOG(txn, dict);
            TXN_SET_TRANS_DDL(txn->flag);
        }
    }
    else if (strncmp(trans_return->m_tbname, PGTEMP_NAME, PGTEMP_NAME_LEN))
    {
        /* Normal statement, iterate parsed values, expand TOAST, save parsing result */
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

void decode_heap_emit(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    pg_parser_translog_translog2col* trans_data = NULL;
    pg_parser_translog_tbcolbase*    trans_return = NULL;
    pg_parser_translog_pre_heap*     heap_pre = (pg_parser_translog_pre_heap*)pbase;
    pg_parser_sysdict_pgclass*       temp_class = NULL;
    txn*                             txn = NULL;
    bool                             is_catalog = false;
    bool                             find = false;
    char*                            table_name = NULL;

    Oid     oid = 0;
    int32_t err_num = 0;

    bool isexternal = false;

    if (!heap_check_dboid(heap_pre->m_dboid, decodingctx->database))
    {
        return;
    }

    if (!heap_pre->m_dboid)
    {
        heap_pre->m_dboid = decodingctx->database;
    }

    /* All data parsed normally */

    // Get current transaction info
    txn = transcache_getTXNByXid((void*)decodingctx, pbase->m_xid);

    /* Get oid by relfilenode */
    oid = catalog_get_oid_by_relfilenode(decodingctx->trans_cache->sysdicts->by_relfilenode,
                                         txn->sysdictHis, txn->sysdict, heap_pre->m_dboid,
                                         heap_pre->m_tbspcoid, heap_pre->m_relfilenode, false);

    if (oid == INVALIDOID)
    {
        return;
    }

    is_catalog = heap_check_catalog(txn, oid);

    temp_class = (pg_parser_sysdict_pgclass*)catalog_get_class_sysdict(
        decodingctx->trans_cache->sysdicts->by_class, txn->sysdict, txn->sysdictHis, oid);
    table_name = temp_class->relname.data;
    /* Before parsing data, check if need to parse ddl */
    if (CHECK_NEED_DDL_TRANS(is_catalog, txn, table_name))
    {
        /* Try to handle possible update intermediate statement */
        dml2ddl(decodingctx, txn);
        transcache_sysdict2his(txn);
        transcache_sysdict_free(txn);
        TXN_UNSET_TRANS_DDL(txn->flag);
    }

    /* Bidirectional filtering of transactions containing state table */
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

    if (!is_catalog && !heap_check_special_table(oid, decodingctx, txn))
    {
        if ((false == filter_dataset_dml(decodingctx->trans_cache->hsyncdataset, oid) &&
             false == filter_dataset_dml(txn->hsyncdataset, oid)))
        {
            return;
        }
    }

    trans_data = rmalloc0(sizeof(pg_parser_translog_translog2col));
    rmemset0(trans_data, 0, 0, sizeof(pg_parser_translog_translog2col));
    trans_data->m_iscatalog = is_catalog;
    elog(RLOG_DEBUG, "oid: %u, relfilenode:%u, iscatalog: %s", oid, heap_pre->m_relfilenode,
         trans_data->m_iscatalog ? "true" : "false");

    /* Initialize input parameters */
    init_heap_trans_data(trans_data, decodingctx, txn, heap_pre, oid);

    /* Check if it's pg_toast data, oid related pgclass record is always the first one */
    isexternal = CHECK_EXTERNAL(&trans_data->m_sysdicts->m_pg_class.m_pg_class[0]);

    /* Call parser interface */
    if (!pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
    {
        elog(RLOG_ERROR, "error in trans heap errcode: %x, msg: %s", err_num,
             pg_parser_errno_getErrInfo(err_num));
    }

    if (trans_data->m_iscatalog)
    {
        storage_tuple(decodingctx->trans_cache, decodingctx->decode_record->start.wal.lsn,
                      trans_return);
    }

    /* If operating on pg_temp table of pg_class, first use our saved mapping */
    if (!strcmp(trans_return->m_schemaname, "pg_catalog") &&
        !strcmp(trans_return->m_tbname, "pg_class") &&
        trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_INSERT)
    {
        pg_parser_translog_tbcol_values* col = (pg_parser_translog_tbcol_values*)trans_return;

        char* temp_relname = get_class_value_from_colvalue(col->m_new_values, CLASS_MAPNUM_RELNAME,
                                                           g_idbtype, g_idbversion);

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

                temp_oid_char = get_class_value_from_colvalue(col->m_new_values, CLASS_MAPNUM_OID,
                                                              g_idbtype, g_idbversion);
                temp_oid = (Oid)atoi(temp_oid_char);

                free_class_value_from_colvalue(col->m_new_values, CLASS_MAPNUM_RELFILENODE,
                                               g_idbtype, g_idbversion);
                set_class_value_from_colvalue(col->m_new_values, temp_oid_char,
                                              CLASS_MAPNUM_RELFILENODE, g_idbtype, g_idbversion);

                if (!txn->oidmap)
                {
                    txn->oidmap = init_oidmap_hash();
                }

                elog(RLOG_DEBUG,
                     "capture catalog temp table, oid:%u, relfilenode :%u, real oid: %u", temp_oid,
                     temp_oid, real_oid);

                add_oidmap(txn->oidmap, temp_oid, real_oid);
            }
        }
    }

    /* Check if it's TOAST table, save TOAST data */
    if (isexternal)
    {
        heap_storage_external_data(txn, trans_return);
        heap_free_trans_result(trans_return);
        TXN_SET_TRANS_TOAST(txn->flag);
    }
    else if (trans_data->m_iscatalog)
    {
        /*
         * Save system catalog second parse result
         * Add system catalog data to trans_cache_txn->sysdict
         * System catalog data does not need to be converted to SQL statements
         */
        if (trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
        {
            /* pg14 and above, ddl statement for system catalog insert optimized to multi insert */
            txn->sysdict =
                decode_heap_multi_insert_save_sysdict_as_insert(txn->sysdict, trans_return);

            TXN_SET_TRANS_DDL(txn->flag);
        }
        else
        {
            txn_sysdict* dict = rmalloc0(sizeof(txn_sysdict));

            dict->colvalues = (pg_parser_translog_tbcol_values*)trans_return;
            dict->convert_colvalues = NULL;
            HEAP_STORAGE_CATALOG(txn, dict);
            TXN_SET_TRANS_DDL(txn->flag);
        }
    }
    else if (strncmp(trans_return->m_tbname, PGTEMP_NAME, PGTEMP_NAME_LEN))
    {
        /* Normal statement, iterate parsed values, redeem TOAST, save parse result */
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

void heap_truncate(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    pg_parser_translog_pre_heap_truncate* truncate = (pg_parser_translog_pre_heap_truncate*)pbase;
    txn*                                  txn = NULL;
    int                                   index_relnum = 0;

    if (truncate->dbid != decodingctx->database)
    {
        return;
    }

    txn = transcache_getTXNByXid((void*)decodingctx, pbase->m_xid);

    for (index_relnum = 0; index_relnum < truncate->nrelids; index_relnum++)
    {
        heap_ddl_assemble_truncate(decodingctx, txn, truncate->relids[index_relnum]);
    }
    /* Memory free is completed in pre-decode release */
}

void heap_fpw_tuples(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    Oid                                 oid = 0;
    pg_parser_translog_pre_image_tuple* tup = (pg_parser_translog_pre_image_tuple*)pbase;
    transcache*                         storage = decodingctx->trans_cache;
    txn*                                txn = NULL;
    List*                               sysdict = NULL;
    List*                               sysdicthis = NULL;

    ReorderBufferFPWKey   key = {'\0'};
    ReorderBufferFPWEntry entry = {'\0'};
    int                   index_tuple_cnt = 0;

    if (pbase->m_xid)
    {
        txn = transcache_getTXNByXid((void*)decodingctx, pbase->m_xid);
        sysdict = txn->sysdict;
        sysdicthis = txn->sysdictHis;
    }

    if (tup->m_dboid == 0)
    {
        tup->m_dboid = decodingctx->database;
    }

    oid = catalog_get_oid_by_relfilenode(decodingctx->trans_cache->sysdicts->by_relfilenode,
                                         sysdicthis, sysdict, tup->m_dboid, tup->m_tbspcoid,
                                         tup->m_relfilenode, false);
    if (!oid)
    {
        return;
    }

    /* Check if fpw cache exists */
    if (!storage->by_fpwtuples)
    {
        storage->by_fpwtuples = fpwcache_init(decodingctx->trans_cache);
    }

    /* Ignore non-system catalog cache, because it's logical mode */
    if (!txn)
    {
        if (!CHECK_CATALOG_BY_OID(oid))
        {
            return;
        }
    }
    else
    {
        if (!heap_check_catalog(txn, oid))
        {
            return;
        }
    }

    key.relfilenode = tup->m_relfilenode;
    for (index_tuple_cnt = 0; index_tuple_cnt < tup->m_tuplecnt; index_tuple_cnt++)
    {
        key.blcknum = tup->m_tuples[index_tuple_cnt].m_pageno;
        key.itemoffset = tup->m_tuples[index_tuple_cnt].m_itemoffnum;

        entry.blcknum = key.blcknum;
        entry.itemoffset = key.itemoffset;
        entry.relfilenode = key.relfilenode;

        entry.data = tup->m_tuples[index_tuple_cnt].m_tupledata;
        entry.len = tup->m_tuples[index_tuple_cnt].m_tuplelen;
        entry.lsn = decodingctx->decode_record->start.wal.lsn;
        elog(RLOG_DEBUG, "get tuple, rel: %u, blk: %u, off:%hu", key.relfilenode, key.blcknum,
             key.itemoffset);
        fpwcache_add(decodingctx->trans_cache, &key, &entry);
    }
}
