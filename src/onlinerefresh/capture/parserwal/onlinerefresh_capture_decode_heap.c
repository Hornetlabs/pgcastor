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

#define PGTEMP_NAME "pg_temp"
#define PGTEMP_NAME_LEN 7

#define CHECK_NEED_DDL_TRANS(iscatalog, txn, name) \
    (!(iscatalog) && txn->sysdict && strncmp(name, PGTEMP_NAME, PGTEMP_NAME_LEN))

#define CHECK_CATALOG_BY_OID(oid) ((oid < 16384) ? (true) : (false))

#define HEAP_STORAGE_CATALOG(txn, trans_return) txn->sysdict = lappend(txn->sysdict, (void*)trans_return)

static bool heap_check_catalog(txn *txn, Oid oid)
{
    if (txn->oidmap)
    {
        Oid real = get_real_oid_from_oidmap(txn->oidmap, oid);
        if (real)
            return CHECK_CATALOG_BY_OID(real);
    }
    return CHECK_CATALOG_BY_OID(oid);
}

static bool heap_check_special_table(Oid oid,
                                decodingcontext* decodingctx,
                                txn *txn)
{
    HTAB *class_htab = decodingctx->trans_cache->sysdicts->by_class;
    pg_sysdict_Form_pg_class class = NULL;
    class = (pg_sysdict_Form_pg_class) catalog_get_class_sysdict(class_htab,
                                                                           txn->sysdict,
                                                                           txn->sysdictHis,
                                                                           oid);
    if (!strncmp(class->relname.data, PGTEMP_NAME, PGTEMP_NAME_LEN) || CHECK_EXTERNAL(class))
    {
        return true;
    }
    return false;
}

static pg_parser_translog_tuplecache *get_tuple_from_cache(HTAB *tuple_cache,
                                                              pg_parser_translog_pre_heap *heap_pre)
{
    pg_parser_translog_tuplecache *result = NULL;
    ReorderBufferFPWKey key = {'\0'};
    ReorderBufferFPWEntry *entry = NULL;
    bool find = false;

    key.blcknum = heap_pre->m_pagenos;
    key.relfilenode = heap_pre->m_relfilenode;
    key.itemoffset = heap_pre->m_tupitemoff;

    entry = hash_search(tuple_cache, &key, HASH_FIND, &find);
    if (!find)
        elog(RLOG_ERROR, "can't find tuple cache by relfilenode: %u,"
                         " blcknum: %u, itemoffset: %hu",
                         key.relfilenode,
                         key.blcknum,
                         key.itemoffset);
    elog(RLOG_DEBUG, "get tuple, rel: %u, blk: %u, off:%hu", key.relfilenode, key.blcknum, key.itemoffset);
    result = rmalloc0(sizeof(pg_parser_translog_tuplecache));
    result->m_itemoffnum = key.itemoffset;
    result->m_pageno = key.blcknum;
    result->m_tuplelen = entry->len;
    result->m_tupledata = rmalloc0(entry->len);
    rmemset0(result->m_tupledata, 0, 0, entry->len);
    rmemcpy0(result->m_tupledata, 0, entry->data, entry->len);

    return result;
}

static void storage_tuple(transcache *storage,
                           XLogRecPtr lsn,
                           pg_parser_translog_tbcolbase *trans_return)
{
    ReorderBufferFPWKey key = {'\0'};
    ReorderBufferFPWEntry entry = {'\0'};

    if (!storage->by_fpwtuples)
        storage->by_fpwtuples = fpwcache_init(storage);

    entry.lsn = lsn;

    /* multi insert返回结构不同, 需要与其他dml分别处理 */
    if (trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
    {
        int index_tuple_cnt = 0;
        pg_parser_translog_tbcol_nvalues *nvalues =
            (pg_parser_translog_tbcol_nvalues *)trans_return;

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
        int index_tuple_cnt = 0;
        pg_parser_translog_tbcol_values *values =
            (pg_parser_translog_tbcol_values *)trans_return;

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
            elog(RLOG_DEBUG, "storage tuple, rel: %u, blk: %u, off:%hu", key.relfilenode, key.blcknum, key.itemoffset);

            fpwcache_add(storage, &key, &entry);
        }
    }
}

static void init_heap_trans_data(pg_parser_translog_translog2col *trans_data,
                                 decodingcontext* decodingctx,
                                 txn *txn,
                                 pg_parser_translog_pre_heap *heap_pre,
                                 Oid oid)
{
    bool search_his = true;

    if (heap_pre->m_needtuple && trans_data->m_iscatalog)
    {
        trans_data->m_tuplecnt = 1;
        trans_data->m_tuples = get_tuple_from_cache(decodingctx->trans_cache->by_fpwtuples, heap_pre);
    }
    else
    {
        trans_data->m_tuplecnt = 0;
        trans_data->m_tuples = NULL;
    }

    /*trans_data->m_iscatalog在调用init之前就已经赋值*/

    /* 复用预解析入参的部分, 无需在二次解析后释放 */
    trans_data->m_pagesize = decodingctx->walpre.m_pagesize;
    trans_data->m_record = decodingctx->walpre.m_record;
    trans_data->m_dbtype = decodingctx->walpre.m_dbtype;
    trans_data->m_dbversion = decodingctx->walpre.m_dbversion;
    trans_data->m_debugLevel = decodingctx->walpre.m_debugLevel;
    trans_data->m_walLevel = decodingctx->walpre.m_walLevel;

    //todo free
    /* 构建convert结构 */
    trans_data->m_convert = rmalloc0(sizeof(pg_parser_translog_convertinfo));
    trans_data->m_convert->m_dbcharset = decodingctx->orgdbcharset;
    trans_data->m_convert->m_tartgetcharset = decodingctx->tgtdbcharset;
    trans_data->m_convert->m_tzname = decodingctx->tzname;
    trans_data->m_convert->m_monetary = decodingctx->monetary;
    trans_data->m_convert->m_numeric = decodingctx->numeric;

    search_his = true;
    trans_data->m_sysdicts = heap_get_sysdict_by_oid((void *)decodingctx, txn, oid, search_his);
}

/* 通过dboid判断是否需要捕获 */
static bool heap_check_dboid(uint32_t dboid, uint32_t capture_dboid)
{
    if (dboid && dboid != capture_dboid)
        return false;
    return true;
}

static void trans_cache_dlist_append(decodingcontext* ctx, txn* txn)
{
    if(NULL == txn)
    {
        return;
    }

    if(NULL == ctx->trans_cache->transdlist->head)
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


static txn *trans_cache_onlinerefresh_getTXNByXid(void* in_ctx, uint64_t xid)
{
    bool find = false;
    HTAB *tx_htab =  NULL;
    decodingcontext* ctx = NULL;
    txn *txn_entry = NULL;

    /* 无效事务，那么不需要放入到 hash 中维护 */
    if(InvalidFullTransactionId == xid)
    {
        return NULL;
    }

    ctx = (decodingcontext*)in_ctx;
    tx_htab = ctx->trans_cache->by_txns;

    txn_entry = (txn *) hash_search(tx_htab, &xid, HASH_ENTER, &find);
    if (!find)
    {
        /* 第一次捕获该事务 */
        onlinerefresh_capture *olcapture = ctx->privdata;

        /* 初始化 */
        txn_initset(txn_entry, xid, ctx->decode_record->start.wal.lsn);
        TXN_SET_TRANS_INHASH(txn_entry->flag);

        /* 将事务加入到双向链表中 */
        trans_cache_dlist_append(ctx, txn_entry);

        /* 小于 xmin 的事务不需要捕获 */
        if (xid < olcapture->snapshot->xmin)
        {
            txn_entry->filter = true;
            return txn_entry;
        }

        /* 大于 txid 的事务，超出了事务的捕获范围 */
        if (xid >= olcapture->txid)
        {
            txn_entry->filter = true;
            return txn_entry;
        }

        /* 
         * 1、捕获 xmax--->txid 之间的事务
         * 2、捕获 xmin--->xmax 活跃的事务
         */
        if ((xid != olcapture->snapshot->xmin && xid >= olcapture->snapshot->xmax)
            || onlinerefresh_capture_isxidinsnapshot(olcapture, (FullTransactionId)xid))
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
    pg_parser_translog_translog2col *trans_data = NULL;
    pg_parser_translog_tbcolbase *trans_return = NULL;
    pg_parser_translog_pre_heap *heap_pre = (pg_parser_translog_pre_heap*)pbase;
    pg_parser_sysdict_pgclass *temp_class = NULL;
    txn *txn = NULL;
    bool        find = false;
    bool        is_catalog = false;
    bool        trans_restart = false;
    char        *table_name = NULL;

    Oid oid = 0;
    int32_t err_num = 0;

    bool        isexternal = false;

    if (!heap_check_dboid(heap_pre->m_dboid, decodingctx->database))
        return;

    if (!heap_pre->m_dboid)
        heap_pre->m_dboid = decodingctx->database;

    if (decodingctx->decode_record->start.wal.lsn < decodingctx->base.restartlsn)
    {
        trans_restart = true;
    }

    //获取当前事务的信息
    txn = trans_cache_onlinerefresh_getTXNByXid((void*)decodingctx, pbase->m_xid);

    /* 通过relfilenode获取oid */
    oid = catalog_get_oid_by_relfilenode(decodingctx->trans_cache->sysdicts->by_relfilenode,
                                                txn->sysdictHis,
                                                txn->sysdict,
                                                heap_pre->m_dboid,
                                                heap_pre->m_tbspcoid,
                                                heap_pre->m_relfilenode,
                                                true);

    is_catalog = heap_check_catalog(txn, oid);

    temp_class = (pg_parser_sysdict_pgclass *)catalog_get_class_sysdict(decodingctx->trans_cache->sysdicts->by_class,
                                                                                  txn->sysdict,
                                                                                  txn->sysdictHis,
                                                                                  oid);
    table_name = temp_class->relname.data;
    /* 在解析数据之前, 先进行是否解析ddl判断 */
    if (CHECK_NEED_DDL_TRANS(is_catalog, txn, table_name))
    {
        /* 不进行DDL解析, 但维护DDL相关缓存 */
        transcache_sysdict2his(txn);
        transcache_sysdict_free(txn);
    }

    /* 双向过滤含有状态表的事务过滤 */
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

    /* 检查是否需要捕获 */
    if (!is_catalog && !heap_check_special_table(oid, decodingctx, txn))
    {
        /* 不再过滤集中或者不需要捕获的事物的普通表语句会被过滤 */
        if ((false == filter_dataset_dml(decodingctx->trans_cache->hsyncdataset, oid)
            && false == filter_dataset_dml(txn->hsyncdataset, oid)) || true == txn->filter)
        {
            return;
        }
    }

    if (trans_restart)
    {
        /* 不是解析系统表, 系统表的toast表也是系统表, 因此这里无须担心toast被过滤 */
        if (!is_catalog)
        {
            return;
        }
    }

    trans_data = rmalloc0(sizeof(pg_parser_translog_translog2col));
    rmemset0(trans_data, 0, 0, sizeof(pg_parser_translog_translog2col));
    trans_data->m_iscatalog = is_catalog;
    elog(RLOG_DEBUG, "oid: %u, relfilenode:%u, iscatalog: %s", oid, heap_pre->m_relfilenode, trans_data->m_iscatalog ? "true" : "false");

    /* 初始化入参 */
    init_heap_trans_data(trans_data, decodingctx, txn, heap_pre, oid);

    /* 检查是否为pg_toast的数据, oid相关的pgclass记录一定是第一条 */
    isexternal = CHECK_EXTERNAL(&trans_data->m_sysdicts->m_pg_class.m_pg_class[0]);

    /* 调用解析接口 */
    if (!pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
    {
        elog(RLOG_ERROR, "error in trans heap errcode: %x, msg: %s", err_num, pg_parser_errno_getErrInfo(err_num));
    }

    if (trans_data->m_iscatalog)
        storage_tuple(decodingctx->trans_cache, decodingctx->decode_record->start.wal.lsn, trans_return);

    /* 如果是对pg_class的pg_temp表进行操作, 首先使用我们保存的映射 */
    if (!strcmp(trans_return->m_schemaname, "pg_catalog")
     && !strcmp(trans_return->m_tbname, "pg_class")
     && trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_INSERT)
    {
        pg_parser_translog_tbcol_values *col = 
            (pg_parser_translog_tbcol_values *) trans_return;

        char *temp_relname = get_class_value_from_colvalue(col->m_new_values,
                                                                  CLASS_MAPNUM_RELNAME,
                                                                  g_idbtype,
                                                                  g_idbversion);

        if (temp_relname && !strncmp(temp_relname, "pg_temp_", 8))
        {
            uint32_t real_oid = 0;
            char *temp_str = temp_relname;
            char *temp_nspname = get_class_value_from_colvalue(col->m_new_values,
                                                                      CLASS_MAPNUM_RELNSPOID,
                                                                      g_idbtype,
                                                                      g_idbversion);
            temp_str = temp_str + 8;
            real_oid = (uint32_t) atoi(temp_str);

            if (CHECK_CATALOG_BY_OID(real_oid) && temp_nspname && !strcmp(temp_nspname, "11"))
            {
                Oid temp_oid = InvalidOid;
                char *temp_oid_char = NULL;

                rfree(col->m_new_values[7].m_value);
                col->m_new_values[7].m_value = rstrdup((char *) col->m_new_values[0].m_value);


                temp_oid_char = get_class_value_from_colvalue(col->m_new_values,
                                                                     CLASS_MAPNUM_OID,
                                                                     g_idbtype,
                                                                     g_idbversion);
                temp_oid = (Oid) atoi(temp_oid_char);

                free_class_value_from_colvalue(col->m_new_values,
                                                      CLASS_MAPNUM_RELFILENODE,
                                                      g_idbtype,
                                                      g_idbversion);
                set_class_value_from_colvalue(col->m_new_values,
                                                     temp_oid_char,
                                                     CLASS_MAPNUM_RELFILENODE,
                                                     g_idbtype,
                                                     g_idbversion);

                if (!txn->oidmap)
                    txn->oidmap = init_oidmap_hash();
                
                elog(RLOG_DEBUG, "capture catalog temp table, oid:%u, relfilenode :%u, real oid: %u",
                                    temp_oid,
                                    temp_oid,
                                    real_oid);

                add_oidmap(txn->oidmap, temp_oid, real_oid);
            }
        }
    }

    /* 判断是否为行外存储表, 保存行外存储数据 */
    if (isexternal)
    {
        heap_storage_external_data(txn, trans_return);
        heap_free_trans_result(trans_return);
        TXN_SET_TRANS_TOAST(txn->flag);
    }
    else if (trans_data->m_iscatalog)
    {
        /*
         * 保存系统表二次解析结果
         * 添加系统表数据到trans_cache_txn->sysdict
         * 系统表数据无需转为sql语句
         */
        txn_sysdict *dict = rmalloc0(sizeof(txn_sysdict));
        dict->colvalues = (pg_parser_translog_tbcol_values *)trans_return;
        dict->convert_colvalues = NULL;
        HEAP_STORAGE_CATALOG(txn, dict);
        TXN_SET_TRANS_DDL(txn->flag);
    }
    else if (strncmp(trans_return->m_tbname, PGTEMP_NAME, PGTEMP_NAME_LEN))
    {
        /* 正常语句, 遍历解析后的值, 兑换行外存储, 保存解析结果 */
        heap_parser_count_size((void *)decodingctx, txn, trans_return, oid);
    }
    else
    {
        /* pg_temp临时表, 无需解析语句 */
        elog(RLOG_DEBUG, "get temp table by decode heap, ignore");

        heap_free_trans_result(trans_return);
    }
    heap_free_trans_pre(trans_data);
}
