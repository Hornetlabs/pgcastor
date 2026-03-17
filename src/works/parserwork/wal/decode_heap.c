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

static bool large_txn_filter(decodingcontext* ctx)
{
    uint64 max_stmtsize = 0;
    txn *max_txn = NULL;
    txn *current = NULL;
    txnstmt* txnstmt = NULL;
    txn_dlist* transdlist = NULL;

    UNUSED(max_txn);

    /* 判断是否需要过滤大事务 */
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

    /* 遍历所有事物, 挑选出最大的不处于DDL和TOAST的事物 */
    for(current = transdlist->head; NULL != current; current = current->next)
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

    /* 已经拿到了筛选出的事物, 拷贝必要信息后放到缓存中 */
    if (!TXN_ISBIGTXN(max_txn->flag))
    {
        txn *begintxn = NULL;
        txn *copytxn = NULL;
        FullTransactionId* xid = NULL;

        /* 第一次放入大事务序列化缓存 */
        TXN_SET_BIGTXN(max_txn->flag);
        max_txn->type = TXN_TYPE_BIGTXN_BEGIN;

        /* 构建大事务开始txnstmt */
        begintxn = txn_initbigtxn(max_txn->xid);
        if(NULL == begintxn)
        {
            elog(RLOG_WARNING, "init big txn error");
            return false;
        }
        begintxn->end.wal.lsn = ctx->parselsn + 1;

        /* 大事务开始 stmt */
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

        /* 添加到缓存 */
        txn_addcommit(begintxn);
        cache_txn_add(ctx->parser2txns, begintxn);

        /* 进行txn拷贝 */
        copytxn = txn_copy(max_txn);

        if (max_txn->sysdict)
        {
            elog(RLOG_WARNING, "big txn copy, txn in ddl");
            return false;
        }

        /* 重置原txn的部分指针 */
        max_txn->sysdictHis = NULL;
        max_txn->stmts = NULL;
        /* 初始化stmt大小 */
        max_txn->stmtsize = 4;

        /* 置空拷贝后txn部分指针 */
        copytxn->toast_hash = NULL;
        copytxn->hsyncdataset = NULL;
        copytxn->oidmap = NULL;
        copytxn->prev = NULL;
        copytxn->next = NULL;
        copytxn->cachenext = NULL;

        /* 处理系统表, 进行拷贝 */
        max_txn->sysdictHis = decode_heap_sysdicthis_copy(copytxn->sysdictHis);

        /* 添加到大事务缓存 */
        ctx->trans_cache->totalsize -= (copytxn->stmtsize - 4);
        cache_txn_add(ctx->parser2bigtxns, copytxn);
    }
    else
    {
        /* 第二次及以后 */
        txn *copytxn = NULL;

        /* 取消设置为大事务开始 */
        max_txn->type = TXN_TYPE_NORMAL;

        /* 进行txn拷贝 */
        copytxn = txn_copy(max_txn);

        if (max_txn->sysdict)
        {
            elog(RLOG_WARNING, "big txn copy, txn in ddl");
            return false;
        }

        /* 重置原txn的部分指针 */
        max_txn->sysdictHis = NULL;
        max_txn->stmts = NULL;
        /* 初始化stmt大小 */
        max_txn->stmtsize = 4;

        /* 置空拷贝后txn部分指针 */
        copytxn->toast_hash = NULL;
        copytxn->hsyncdataset = NULL;
        copytxn->oidmap = NULL;
        copytxn->prev = NULL;
        copytxn->next = NULL;
        copytxn->cachenext = NULL;

        /* 处理系统表, 进行拷贝 */
        max_txn->sysdictHis = decode_heap_sysdicthis_copy(copytxn->sysdictHis);

        /* 添加到大事务缓存 */
        ctx->trans_cache->totalsize -= (copytxn->stmtsize - 4);
        cache_txn_add(ctx->parser2bigtxns, copytxn);
    }
    return true;
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
    trans_data->m_sysdicts = heap_get_sysdict_by_oid((void*)decodingctx, txn, oid, search_his);
}


/*
 * 通过dboid判断是否需要捕获
 *  1、global 数据库的表需要捕获
 *  2、正在捕获的库
 */
static bool heap_check_dboid(uint32_t dboid, uint32_t capture_dboid)
{
    /* 在事务日志中, global 数据库的 oid 为 0, 且 global 数据库中的表也是需要解析的 */
    if (dboid && dboid != capture_dboid)
        return false;
    return true;
}

void decode_heap(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    bool find                                       = false;
    bool isexternal                                 = false;
    bool is_catalog                                 = false;
    bool recovery                                   = false;
    Oid oid                                         = 0;
    int32_t err_num                                 = 0;
    txn *txn                                 = NULL;
    char* table_name                                = NULL;
    pg_parser_translog_translog2col *trans_data  = NULL;
    pg_parser_translog_tbcolbase *trans_return   = NULL;
    pg_parser_translog_pre_heap *heap_pre        = NULL;
    pg_parser_sysdict_pgclass *temp_class        = NULL;

    heap_pre = (pg_parser_translog_pre_heap*)pbase;

    /*----------------调用解析接口数据前处理 begin------------------------*/
    /* 查看是否为需要解析的库 */
    if (!heap_check_dboid(heap_pre->m_dboid, decodingctx->database))
    {
        return;
    }

    /* 若为 global 数据库, 重置为当前数据库 */
    if (InvalidOid == heap_pre->m_dboid)
    {
        heap_pre->m_dboid = decodingctx->database;
    }

    /* redolsn--->restartlsn 之间的数据只需要系统表的 */
    if (decodingctx->decode_record->start.wal.lsn < decodingctx->base.restartlsn)
    {
        recovery = true;
    }

    /* 获取当前事务的信息 */
    txn = transcache_getTXNByXid((void*)decodingctx, pbase->m_xid);

    /* 通过relfilenode获取oid */
    oid = catalog_get_oid_by_relfilenode(decodingctx->trans_cache->sysdicts->by_relfilenode,
                                                txn->sysdictHis,
                                                txn->sysdict,
                                                heap_pre->m_dboid,
                                                heap_pre->m_tbspcoid,
                                                heap_pre->m_relfilenode,
                                                true);

    is_catalog = heap_check_catalog(txn, oid);
    if (recovery)
    {
        /* 
         * 在恢复模式下, 只捕获 系统表 数据
         *  系统表的 toast 表的 oid 也小于 16384
         *      目前已知的只有在 alter table ... column type 才会更改 toast 表的 oid
         */
        if (!is_catalog)
        {
            return;
        }
    }

    temp_class = (pg_parser_sysdict_pgclass *)catalog_get_class_sysdict(decodingctx->trans_cache->sysdicts->by_class,
                                                                                  txn->sysdict,
                                                                                  txn->sysdictHis,
                                                                                  oid);
    table_name = temp_class->relname.data;

    /* 
     * 在解析数据之前, 先进行是否解析ddl判断
     * 兑换 DDL 的条件
     *  1、当前的 record 为 非 系统表, 判断条件
     *      1.1 Oid > 16384
     *      1.2 表名不为 pg_temp 开头
     *  2、含有待兑换的系统字典
     */
    if (CHECK_NEED_DDL_TRANS(is_catalog, txn, table_name))
    {
        /* 尝试处理可能存在的update中间语句 */
        dml2ddl(decodingctx, txn);
        transcache_sysdict2his(txn);
        transcache_sysdict_free(txn);
        TXN_UNSET_TRANS_DDL(txn->flag);
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

    /*
     * 查看是否为需要解析的表,需要同步的表为下面四类
     *  1、系统表
     *  2、pg_temp 开头的表需要同步
     *  3、PG_TOAST_NAMESPACE 模式下的表(行外存储)
     *  4、同步集内的表
     */
    if (!is_catalog && !heap_check_special_table(oid, decodingctx, txn))
    {
        if ((false == filter_dataset_dml(decodingctx->trans_cache->hsyncdataset, oid)
        && false == filter_dataset_dml(txn->hsyncdataset, oid)))
        {
            return;
        }
    }

    /* 大事务筛选 */
    if (decodingctx->trans_cache->totalsize >= decodingctx->trans_cache->capture_buffer)
    {
        large_txn_filter(decodingctx);
    }

    /*----------------调用解析接口数据前处理   end------------------------*/

    /*----------------调用解析接口数据准备 begin--------------------------*/
    trans_data = rmalloc0(sizeof(pg_parser_translog_translog2col));
    if(NULL == trans_data)
    {
        elog(RLOG_WARNING, "heap out of memory");
        return;
    }
    rmemset0(trans_data, 0, 0, sizeof(pg_parser_translog_translog2col));
    trans_data->m_iscatalog = is_catalog;
    elog(RLOG_DEBUG, "oid: %u, relfilenode:%u, iscatalog: %s", oid, heap_pre->m_relfilenode, trans_data->m_iscatalog ? "true" : "false");

    /* 初始化入参 */
    init_heap_trans_data(trans_data, decodingctx, txn, heap_pre, oid);

    /* 检查是否为pg_toast的数据, oid相关的pgclass记录一定是第一条 */
    isexternal = CHECK_EXTERNAL(&trans_data->m_sysdicts->m_pg_class.m_pg_class[0]);

    /*----------------调用解析接口数据准备   end--------------------------*/

    /* 调用解析接口 */
    if (!pg_parser_trans_TransRecord(trans_data, &trans_return, &err_num))
    {
        elog(RLOG_ERROR, "error in trans heap errcode: %x, msg: %s", err_num, pg_parser_errno_getErrInfo(err_num));
    }

    if (trans_data->m_iscatalog)
        storage_tuple(decodingctx->trans_cache, decodingctx->decode_record->start.wal.lsn, trans_return);

    /*
     * 对pg_class表内记录的操作
     *  例如: vacuum full 表
     *        truncate 表
     */
    if (0 == strcmp(trans_return->m_schemaname, "pg_catalog")
        && 0 == strcmp(trans_return->m_tbname, "pg_class")
        && PG_PARSER_TRANSLOG_DMLTYPE_INSERT == trans_return->m_dmltype)
    {
        char* temp_relname = NULL;
        pg_parser_translog_tbcol_values *col = NULL;

        col = (pg_parser_translog_tbcol_values *)trans_return;
        temp_relname = get_class_value_from_colvalue(col->m_new_values,
                                                                  CLASS_MAPNUM_RELNAME,
                                                                  g_idbtype,
                                                                  g_idbversion);

        /* 
         * 在 pg 系列的数据库中, 做 vacuum full 表的步骤如下:
         *  1、创建新表, 新表的名称为: pg_temp_旧表的OID
         *      该新表的结构与旧表的结构一致
         *  2、将新表的数据导入到旧表中
         *  3、将新表的 relfilenode 与 旧表 的 relfilenode 交换
         *  4、若被 vacuum full 的表为系统表, 则会在事务提交时记录: RM_RELMAP_ID->XLOG_RELMAP_UPDATE 新的映射关系
         * 
         * 下面的处理逻辑是为了记录下来pg_temp_ 开头的表的 oid 当前表的 oid 的对应关系
         */
        if (temp_relname && 0 == strncmp(temp_relname, "pg_temp_", 8))
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
        if (trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
        {
            /* pg14版本以上, ddl语句针对系统表的insert优化为了multi insert */
            txn->sysdict = decode_heap_multi_insert_save_sysdict_as_insert(txn->sysdict, trans_return);

            TXN_SET_TRANS_DDL(txn->flag);
        }
        else
        {
            txn_sysdict *dict = rmalloc0(sizeof(txn_sysdict));

            dict->colvalues = (pg_parser_translog_tbcol_values *)trans_return;
            dict->convert_colvalues = NULL;
            HEAP_STORAGE_CATALOG(txn, dict);
            TXN_SET_TRANS_DDL(txn->flag);
        }
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

void decode_heap_emit(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    pg_parser_translog_translog2col *trans_data = NULL;
    pg_parser_translog_tbcolbase *trans_return = NULL;
    pg_parser_translog_pre_heap *heap_pre = (pg_parser_translog_pre_heap*)pbase;
     pg_parser_sysdict_pgclass *temp_class = NULL;
    txn *txn = NULL;
    bool        is_catalog = false;
    bool        find = false;
    char        *table_name = NULL;

    Oid oid = 0;
    int32_t err_num = 0;

    bool        isexternal = false;

    if (!heap_check_dboid(heap_pre->m_dboid, decodingctx->database))
        return;

    if (!heap_pre->m_dboid)
        heap_pre->m_dboid = decodingctx->database;

    /* 所有数据正常解析 */

    //获取当前事务的信息
    txn = transcache_getTXNByXid((void*)decodingctx, pbase->m_xid);

    /* 通过relfilenode获取oid */
    oid = catalog_get_oid_by_relfilenode(decodingctx->trans_cache->sysdicts->by_relfilenode,
                                                txn->sysdictHis,
                                                txn->sysdict,
                                                heap_pre->m_dboid,
                                                heap_pre->m_tbspcoid,
                                                heap_pre->m_relfilenode,
                                                false);

    if (oid == InvalidOid)
    {
        return;
    }

    is_catalog = heap_check_catalog(txn, oid);

    temp_class = (pg_parser_sysdict_pgclass *)catalog_get_class_sysdict(decodingctx->trans_cache->sysdicts->by_class,
                                                                                  txn->sysdict,
                                                                                  txn->sysdictHis,
                                                                                  oid);
    table_name = temp_class->relname.data;
    /* 在解析数据之前, 先进行是否解析ddl判断 */
    if (CHECK_NEED_DDL_TRANS(is_catalog, txn, table_name))
    {
        /* 尝试处理可能存在的update中间语句 */
        dml2ddl(decodingctx, txn);
        transcache_sysdict2his(txn);
        transcache_sysdict_free(txn);
        TXN_UNSET_TRANS_DDL(txn->flag);
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

    if (!is_catalog && !heap_check_special_table(oid, decodingctx, txn))
    {
        if ((false == filter_dataset_dml(decodingctx->trans_cache->hsyncdataset, oid)
        && false == filter_dataset_dml(txn->hsyncdataset, oid)))
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
        if (trans_return->m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT)
        {
            /* pg14版本以上, ddl语句针对系统表的insert优化为了multi insert */
            txn->sysdict = decode_heap_multi_insert_save_sysdict_as_insert(txn->sysdict, trans_return);

            TXN_SET_TRANS_DDL(txn->flag);
        }
        else
        {
            txn_sysdict *dict = rmalloc0(sizeof(txn_sysdict));

            dict->colvalues = (pg_parser_translog_tbcol_values *)trans_return;
            dict->convert_colvalues = NULL;
            HEAP_STORAGE_CATALOG(txn, dict);
            TXN_SET_TRANS_DDL(txn->flag);
        }
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

void heap_truncate(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    pg_parser_translog_pre_heap_truncate *truncate =
        (pg_parser_translog_pre_heap_truncate*)pbase;
    txn *txn = NULL;
    int index_relnum = 0;

    if (truncate->dbid != decodingctx->database)
        return;

    txn = transcache_getTXNByXid((void*)decodingctx, pbase->m_xid);

    for (index_relnum = 0; index_relnum < truncate->nrelids; index_relnum++)
    {
        heap_ddl_assemble_truncate(decodingctx, txn, truncate->relids[index_relnum]);
    }
    /* 内存释放在预解析释放中完成 */
}

void heap_fpw_tuples(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    Oid oid = 0;
    pg_parser_translog_pre_image_tuple *tup = (pg_parser_translog_pre_image_tuple *)pbase;
    transcache *storage = decodingctx->trans_cache;
    txn *txn = NULL;
    List *sysdict = NULL;
    List *sysdicthis = NULL;

    ReorderBufferFPWKey key = {'\0'};
    ReorderBufferFPWEntry entry = {'\0'};
    int index_tuple_cnt = 0;

    if (pbase->m_xid)
    {
        txn = transcache_getTXNByXid((void*)decodingctx, pbase->m_xid);
        sysdict = txn->sysdict;
        sysdicthis = txn->sysdictHis;
    }

    if (tup->m_dboid == 0)
        tup->m_dboid = decodingctx->database;

    oid = catalog_get_oid_by_relfilenode(decodingctx->trans_cache->sysdicts->by_relfilenode,
                                                sysdicthis,
                                                sysdict,
                                                tup->m_dboid,
                                                tup->m_tbspcoid,
                                                tup->m_relfilenode,
                                                false);
    if (!oid)
        return;

    /* 判断是否存在fpw缓存*/
    if (!storage->by_fpwtuples)
        storage->by_fpwtuples = fpwcache_init(decodingctx->trans_cache);

    /* 忽略非系统表缓存, 因为是logical模式 */
    if (!txn)
    {
        if (!CHECK_CATALOG_BY_OID(oid))
            return;
    }
    else
    {
        if (!heap_check_catalog(txn, oid))
            return;
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
        elog(RLOG_DEBUG, "get tuple, rel: %u, blk: %u, off:%hu", key.relfilenode, key.blcknum, key.itemoffset);
        fpwcache_add(decodingctx->trans_cache, &key, &entry);
    }
}
