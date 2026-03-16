#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/regex/ripple_regex.h"
#include "threads/ripple_threads.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "cache/ripple_fpwcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "catalog/ripple_catalog.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"
#include "works/parserwork/wal/ripple_decode_ddl.h"
#include "works/parserwork/wal/ripple_decode_colvalue.h"
#include "task/ripple_task_slot.h"
#include "queue/ripple_queue.h"
#include "storage/ripple_file_buffer.h"
#include "refresh/ripple_refresh_tables.h"
#include "strategy/ripple_filter_dataset.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "onlinerefresh/capture/parserwal/ripple_onlinerefresh_capture_decode_heap.h"
#include "onlinerefresh/capture/ripple_onlinerefresh_capture.h"

#define PGTEMP_NAME "pg_temp"
#define PGTEMP_NAME_LEN 7

#define CHECK_NEED_DDL_TRANS(iscatalog, txn, name) \
    (!(iscatalog) && txn->sysdict && strncmp(name, PGTEMP_NAME, PGTEMP_NAME_LEN))

#define CHECK_CATALOG_BY_OID(oid) ((oid < 16384) ? (true) : (false))

#define HEAP_STORAGE_CATALOG(txn, trans_return) txn->sysdict = lappend(txn->sysdict, (void*)trans_return)

static bool heap_check_catalog(ripple_txn *txn, Oid oid)
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
                                ripple_decodingcontext* decodingctx,
                                ripple_txn *txn)
{
    HTAB *class_htab = decodingctx->transcache->sysdicts->by_class;
    xk_pg_sysdict_Form_pg_class class = NULL;
    class = (xk_pg_sysdict_Form_pg_class) ripple_catalog_get_class_sysdict(class_htab,
                                                                           txn->sysdict,
                                                                           txn->sysdictHis,
                                                                           oid);
    if (!strncmp(class->relname.data, PGTEMP_NAME, PGTEMP_NAME_LEN) || CHECK_EXTERNAL(class))
    {
        return true;
    }
    return false;
}

static xk_pg_parser_translog_tuplecache *get_tuple_from_cache(HTAB *tuple_cache,
                                                              xk_pg_parser_translog_pre_heap *heap_pre)
{
    xk_pg_parser_translog_tuplecache *result = NULL;
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
    result = rmalloc0(sizeof(xk_pg_parser_translog_tuplecache));
    result->m_itemoffnum = key.itemoffset;
    result->m_pageno = key.blcknum;
    result->m_tuplelen = entry->len;
    result->m_tupledata = rmalloc0(entry->len);
    rmemset0(result->m_tupledata, 0, 0, entry->len);
    rmemcpy0(result->m_tupledata, 0, entry->data, entry->len);

    return result;
}

static void storage_tuple(ripple_transcache *storage,
                          XLogRecPtr lsn,
                          xk_pg_parser_translog_tbcolbase *trans_return)
{
    ReorderBufferFPWKey key = {'\0'};
    ReorderBufferFPWEntry entry = {'\0'};

    if (!storage->by_fpwtuples)
        storage->by_fpwtuples = ripple_fpwcache_init(storage);

    entry.lsn = lsn;

    /* multi insert返回结构不同, 需要与其他dml分别处理 */
    if (trans_return->m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
    {
        int index_tuple_cnt = 0;
        xk_pg_parser_translog_tbcol_nvalues *nvalues =
            (xk_pg_parser_translog_tbcol_nvalues *)trans_return;

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

            ripple_fpwcache_add(storage, &key, &entry);
        }
    }
    else
    {
        int index_tuple_cnt = 0;
        xk_pg_parser_translog_tbcol_values *values =
            (xk_pg_parser_translog_tbcol_values *)trans_return;

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

            ripple_fpwcache_add(storage, &key, &entry);
        }
    }
}

static void init_heap_trans_data(xk_pg_parser_translog_translog2col *trans_data,
                                 ripple_decodingcontext* decodingctx,
                                 ripple_txn *txn,
                                 xk_pg_parser_translog_pre_heap *heap_pre,
                                 Oid oid)
{
    bool search_his = true;

    if (heap_pre->m_needtuple && trans_data->m_iscatalog)
    {
        trans_data->m_tuplecnt = 1;
        trans_data->m_tuples = get_tuple_from_cache(decodingctx->transcache->by_fpwtuples, heap_pre);
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
    trans_data->m_convert = rmalloc0(sizeof(xk_pg_parser_translog_convertinfo));
    trans_data->m_convert->m_dbcharset = decodingctx->orgdbcharset;
    trans_data->m_convert->m_tartgetcharset = decodingctx->tgtdbcharset;
    trans_data->m_convert->m_tzname = decodingctx->tzname;
    trans_data->m_convert->m_monetary = decodingctx->monetary;
    trans_data->m_convert->m_numeric = decodingctx->numeric;

    search_his = true;
    trans_data->m_sysdicts = heap_get_sysdict_by_oid((void *)decodingctx, txn, oid, search_his);
}

/* 通过dboid判断是否需要捕获 */
static bool ripple_heap_check_dboid(uint32_t dboid, uint32_t capture_dboid)
{
    if (dboid && dboid != capture_dboid)
        return false;
    return true;
}

static void ripple_transcache_dlist_append(ripple_decodingcontext* ctx, ripple_txn* txn)
{
    if(NULL == txn)
    {
        return;
    }

    if(NULL == ctx->transcache->transdlist->head)
    {
        ctx->transcache->transdlist->head = txn;
        ctx->transcache->transdlist->tail = txn;
    }
    else
    {
        ctx->transcache->transdlist->tail->next = txn;
        txn->prev = ctx->transcache->transdlist->tail;
        ctx->transcache->transdlist->tail = txn;
    }
}


static ripple_txn *ripple_transcache_onlinerefresh_getTXNByXid(void* in_ctx, uint64_t xid)
{
    bool find = false;
    HTAB *tx_htab =  NULL;
    ripple_decodingcontext* ctx = NULL;
    ripple_txn *txn_entry = NULL;

    /* 无效事务，那么不需要放入到 hash 中维护 */
    if(InvalidFullTransactionId == xid)
    {
        return NULL;
    }

    ctx = (ripple_decodingcontext*)in_ctx;
    tx_htab = ctx->transcache->by_txns;

    txn_entry = (ripple_txn *) hash_search(tx_htab, &xid, HASH_ENTER, &find);
    if (!find)
    {
        /* 第一次捕获该事务 */
        ripple_onlinerefresh_capture *olcapture = ctx->privdata;

        /* 初始化 */
        ripple_txn_initset(txn_entry, xid, ctx->decode_record->start.wal.lsn);
        RIPPLE_TXN_SET_TRANS_INHASH(txn_entry->flag);

        /* 将事务加入到双向链表中 */
        ripple_transcache_dlist_append(ctx, txn_entry);

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
            || ripple_onlinerefresh_capture_isxidinsnapshot(olcapture, (FullTransactionId)xid))
        {
            elog(RLOG_INFO, "xid: %lu, xmax: %lu", xid, olcapture->snapshot->xmax);
            ripple_onlinerefresh_capture_xids_append(olcapture, xid);
        }
        else
        {
            if (!ripple_onlinerefresh_capture_isxidinxids(olcapture, xid))
            {
                txn_entry->filter = true;
                return txn_entry;
            }
        }
    }

    return txn_entry;
}

void ripple_onlinerefresh_decode_heap(ripple_decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase)
{
    xk_pg_parser_translog_translog2col *trans_data = NULL;
    xk_pg_parser_translog_tbcolbase *trans_return = NULL;
    xk_pg_parser_translog_pre_heap *heap_pre = (xk_pg_parser_translog_pre_heap*)pbase;
    xk_pg_parser_sysdict_pgclass *temp_class = NULL;
    ripple_txn *txn = NULL;
    bool        find = false;
    bool        is_catalog = false;
    bool        trans_restart = false;
    char        *table_name = NULL;

    Oid oid = 0;
    int32_t err_num = 0;

    bool        isexternal = false;

    if (!ripple_heap_check_dboid(heap_pre->m_dboid, decodingctx->database))
        return;

    if (!heap_pre->m_dboid)
        heap_pre->m_dboid = decodingctx->database;

    if (decodingctx->decode_record->start.wal.lsn < decodingctx->base.restartlsn)
    {
        trans_restart = true;
    }

    //获取当前事务的信息
    txn = ripple_transcache_onlinerefresh_getTXNByXid((void*)decodingctx, pbase->m_xid);

    /* 通过relfilenode获取oid */
    oid = ripple_catalog_get_oid_by_relfilenode(decodingctx->transcache->sysdicts->by_relfilenode,
                                                txn->sysdictHis,
                                                txn->sysdict,
                                                heap_pre->m_dboid,
                                                heap_pre->m_tbspcoid,
                                                heap_pre->m_relfilenode,
                                                true);

    is_catalog = heap_check_catalog(txn, oid);

    temp_class = (xk_pg_parser_sysdict_pgclass *)ripple_catalog_get_class_sysdict(decodingctx->transcache->sysdicts->by_class,
                                                                                  txn->sysdict,
                                                                                  txn->sysdictHis,
                                                                                  oid);
    table_name = temp_class->relname.data;
    /* 在解析数据之前, 先进行是否解析ddl判断 */
    if (CHECK_NEED_DDL_TRANS(is_catalog, txn, table_name))
    {
        /* 不进行DDL解析, 但维护DDL相关缓存 */
        ripple_transcache_sysdict2his(txn);
        ripple_transcache_sysdict_free(txn);
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
        hash_search(decodingctx->transcache->htxnfilterdataset, &oid, HASH_FIND, &find);
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
        if ((false == ripple_filter_dataset_dml(decodingctx->transcache->hsyncdataset, oid)
            && false == ripple_filter_dataset_dml(txn->hsyncdataset, oid)) || true == txn->filter)
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

    trans_data = rmalloc0(sizeof(xk_pg_parser_translog_translog2col));
    rmemset0(trans_data, 0, 0, sizeof(xk_pg_parser_translog_translog2col));
    trans_data->m_iscatalog = is_catalog;
    elog(RLOG_DEBUG, "oid: %u, relfilenode:%u, iscatalog: %s", oid, heap_pre->m_relfilenode, trans_data->m_iscatalog ? "true" : "false");

    /* 初始化入参 */
    init_heap_trans_data(trans_data, decodingctx, txn, heap_pre, oid);

    /* 检查是否为pg_toast的数据, oid相关的pgclass记录一定是第一条 */
    isexternal = CHECK_EXTERNAL(&trans_data->m_sysdicts->m_pg_class.m_pg_class[0]);

    /* 调用解析接口 */
    if (!xk_pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
    {
        elog(RLOG_ERROR, "error in trans heap errcode: %x, msg: %s", err_num, xk_pg_parser_errno_getErrInfo(err_num));
    }

    if (trans_data->m_iscatalog)
        storage_tuple(decodingctx->transcache, decodingctx->decode_record->start.wal.lsn, trans_return);

    /* 如果是对pg_class的pg_temp表进行操作, 首先使用我们保存的映射 */
    if (!strcmp(trans_return->m_schemaname, "pg_catalog")
     && !strcmp(trans_return->m_tbname, "pg_class")
     && trans_return->m_dmltype == XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT)
    {
        xk_pg_parser_translog_tbcol_values *col = 
            (xk_pg_parser_translog_tbcol_values *) trans_return;

        char *temp_relname = ripple_get_class_value_from_colvalue(col->m_new_values,
                                                                  RIPPLE_CLASS_MAPNUM_RELNAME,
                                                                  g_idbtype,
                                                                  g_idbversion);

        if (temp_relname && !strncmp(temp_relname, "pg_temp_", 8))
        {
            uint32_t real_oid = 0;
            char *temp_str = temp_relname;
            char *temp_nspname = ripple_get_class_value_from_colvalue(col->m_new_values,
                                                                      RIPPLE_CLASS_MAPNUM_RELNSPOID,
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


                temp_oid_char = ripple_get_class_value_from_colvalue(col->m_new_values,
                                                                     RIPPLE_CLASS_MAPNUM_OID,
                                                                     g_idbtype,
                                                                     g_idbversion);
                temp_oid = (Oid) atoi(temp_oid_char);

                ripple_free_class_value_from_colvalue(col->m_new_values,
                                                      RIPPLE_CLASS_MAPNUM_RELFILENODE,
                                                      g_idbtype,
                                                      g_idbversion);
                ripple_set_class_value_from_colvalue(col->m_new_values,
                                                     temp_oid_char,
                                                     RIPPLE_CLASS_MAPNUM_RELFILENODE,
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
        RIPPLE_TXN_SET_TRANS_TOAST(txn->flag);
    }
    else if (trans_data->m_iscatalog)
    {
        /*
         * 保存系统表二次解析结果
         * 添加系统表数据到ripple_transcache_txn->sysdict
         * 系统表数据无需转为sql语句
         */
        ripple_txn_sysdict *dict = rmalloc0(sizeof(ripple_txn_sysdict));
        dict->colvalues = (xk_pg_parser_translog_tbcol_values *)trans_return;
        dict->convert_colvalues = NULL;
        HEAP_STORAGE_CATALOG(txn, dict);
        RIPPLE_TXN_SET_TRANS_DDL(txn->flag);
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
