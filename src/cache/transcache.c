#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/hash/hash_utils.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/regex/ripple_regex.h"
#include "port/thread/ripple_thread.h"
#include "misc/ripple_misc_control.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "cache/ripple_cache_sysidcts.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_attribute.h"
#include "catalog/ripple_class.h"
#include "catalog/ripple_database.h"
#include "catalog/ripple_type.h"
#include "catalog/ripple_namespace.h"
#include "catalog/ripple_constraint.h"
#include "catalog/ripple_proc.h"
#include "catalog/ripple_index.h"
#include "stmts/ripple_txnstmt.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "cache/ripple_fpwcache.h"
#include "cache/ripple_toastcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"
#include "utils/guc/guc.h"
#include "refresh/ripple_refresh_tables.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "strategy/ripple_filter_dataset.h"
/*
 * 事务是正在处理中的缓存。
 * 处理中的缓存设置为一个 hash 表，方便快速查找
 *  处理中的缓存在 ENTER/REMOVE/FIND 时都不需要加锁
*/

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

/*
 * 将事务在双向链表中删除
 * 参数:
 *  in_ctx          解析上下文
 *  txn             需要删除的事务
 *  brestart        出参,用于标识是否更新了 restartlsn
 *                      true        更新
 *                      false       未更新
 *  bconfirm        出参,用于标识是否更新了 confirmlsn
 *                      true        更新
 *                      false       未更新
 *  bset            入参，用于标识调用此函数时，是否需要更新 restartlsn 和 confirmlsn
 *                      true        需要更新
 *                      false       不需要更新
*/
void ripple_transcache_dlist_remove(void* in_ctx,
                                    ripple_txn* txn,
                                    bool* brestart,
                                    XLogRecPtr* restartlsn,
                                    bool* bconfirm,
                                    XLogRecPtr* confirmlsn,
                                    bool bset)
{
    ripple_decodingcontext* ctx = NULL;
    if(NULL == txn)
    {
        return;
    }

    *brestart = false;
    *bconfirm = false;
    ctx = (ripple_decodingcontext*)in_ctx;

    if(NULL == txn->prev)
    {
        /* 设置新的 restartlsn */
        if(true == bset && txn->start.wal.lsn > ctx->base.restartlsn)
        {
            ctx->base.restartlsn = txn->start.wal.lsn;
            *brestart = true;
            if(NULL != restartlsn)
            {
                *restartlsn = txn->start.wal.lsn;
            }
            elog(RLOG_DEBUG, "restartlsn update by:xid:%lu, %X/%X",
                            txn->xid,
                            (uint32)(txn->start.wal.lsn >> 32), (uint32)(txn->start.wal.lsn));
        }

        if(NULL == txn->next)
        {
            /* 空链表 */
            if(true == *brestart)
            {
                ctx->base.restartlsn = txn->end.wal.lsn;
                if(NULL != restartlsn)
                {
                    *restartlsn = txn->end.wal.lsn;
                }
            }

            ctx->transcache->transdlist->head = NULL;
            ctx->transcache->transdlist->tail = NULL;
        }
        else
        {
            ctx->transcache->transdlist->head = txn->next;
            txn->next->prev = NULL;
        }
    }
    else
    {
        if(NULL == txn->next)
        {
            ctx->transcache->transdlist->tail = txn->prev;
            txn->prev->next = NULL;
        }
        else
        {
            txn->prev->next = txn->next;
            txn->next->prev = txn->prev;
        }
    }

    if(true == bset && ctx->base.confirmedlsn < txn->end.wal.lsn)
    {
        ctx->base.confirmedlsn = txn->end.wal.lsn;
        *bconfirm = true;
        if(NULL != confirmlsn)
        {
            *confirmlsn = txn->end.wal.lsn;
        }
    }

    txn->prev = NULL;
    txn->next = NULL;
}

ripple_txn *ripple_transcache_getTXNByXid(void* in_ctx, uint64_t xid)
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
        if (ctx->onlinerefresh)
        {
            /* onlinerefresh节点不为空 */
            dlistnode *dlnode = ctx->onlinerefresh->head;
            ripple_onlinerefresh *olnode = NULL;
            for (; dlnode; dlnode = dlnode->next)
            {
                olnode = (ripple_onlinerefresh *)dlnode->value;
                if (!olnode->increment)
                {
                    /* 不需要增量时跳过 */
                    continue;
                }

                if (xid < olnode->snapshot->xmin)
                {
                    /* 无需加入xids */
                    continue;
                }
                if (xid > olnode->txid)
                {
                    if (olnode->state != RIPPLE_ONLINEREFRESH_STATE_FULLSNAPSHOT)
                    {
                        ripple_onlinerefresh_state_setfullsnapshot(olnode);
                    }
                    continue;
                }
                else
                {
                    /* 存在xmin = xmax的情况, 因此首先排除xid = xmin */
                    if (xid != olnode->snapshot->xmin && xid >= olnode->snapshot->xmax)
                    {
                        ripple_onlinerefresh_xids_append(olnode, xid);
                    }
                }
            }
        }
        /* 初始化 */
        ripple_txn_initset(txn_entry, xid, ctx->decode_record->start.wal.lsn);
        RIPPLE_TXN_SET_TRANS_INHASH(txn_entry->flag);

        /* 将事务加入到双向链表中 */
        ripple_transcache_dlist_append(ctx, txn_entry);
    }

    return txn_entry;
}

ripple_txn *ripple_transcache_getTXNByXidFind(ripple_transcache* transcache, uint64_t xid)
{
    ripple_txn *txn_entry = NULL;
    txn_entry = (ripple_txn *) hash_search(transcache->by_txns, &xid, HASH_FIND, NULL);
    return txn_entry;
}

/* 删除 */
/* 改入参 */
void ripple_transcache_removeTXNByXid(ripple_transcache * in_transcache, uint64_t xid)
{
    bool find = false;
    ripple_txn *txn_entry = NULL;

    txn_entry = hash_search(in_transcache->by_txns, &xid, HASH_REMOVE, &find);
    if(false == find)
    {
        elog(RLOG_WARNING, "ripple logical error");
    }
    else
    {
        elog(RLOG_DEBUG, "txn lsn info :restart:%X/%X, confirm:%X/%X, redo:%X/%X,",
                                (uint32)(txn_entry->restart.wal.lsn >> 32), (uint32)(txn_entry->restart.wal.lsn),
                                (uint32)(txn_entry->confirm.wal.lsn >> 32), (uint32)(txn_entry->confirm.wal.lsn),
                                (uint32)(txn_entry->redo.wal.lsn >> 32), (uint32)(txn_entry->redo.wal.lsn));
        rmemset1(txn_entry, 0, '\0', sizeof(ripple_txn));
    }
    //elog(RLOG_INFO, "remove txn, found:%d, xid:%lu", find, xid);
    return;
}

/* 将事务中的 sysdict 转换后的结果放入到 sysdicthis 中 */
void ripple_transcache_sysdict2his(ripple_txn* txn)
{
    ListCell* lc = NULL;
    ripple_txn_sysdict* sysdict = NULL;
    ripple_catalogdata* catalogdata = NULL;
    ripple_txnstmt *stmt = NULL;
    bool first_foreach = true;
    ripple_txnstmt_metadata *metadata = NULL;

    if(NULL == txn
        || NULL == txn->sysdict)
    {
        return;
    }

    /* 添加stmt, 作为系统表段标识 */
    stmt = rmalloc1(sizeof(ripple_txnstmt));
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));
    metadata = rmalloc1(sizeof(ripple_txnstmt_metadata));
    rmemset0(metadata, 0, 0, sizeof(ripple_txnstmt_metadata));

    stmt->type = RIPPLE_TXNSTMT_TYPE_METADATA;

    foreach(lc, txn->sysdict)
    {
        sysdict = (ripple_txn_sysdict*)lfirst(lc);

        catalogdata = ripple_catalog_colvalued2catalog(g_idbtype, g_idbversion, sysdict->colvalues);
        if(NULL == catalogdata)
        {
            continue;
        }

        /* 设置事务startlsn，落盘字典表时过滤使用 */
        catalogdata->lsn.wal.lsn = txn->start.wal.lsn;

        txn->sysdictHis = lappend(txn->sysdictHis, catalogdata);

        /* 第一次时执行, 此时链表的尾部就是开始 */
        if (first_foreach)
        {
            first_foreach = false;
            metadata->begin = list_tail(txn->sysdictHis);
        }
    }
    if(false == first_foreach)
    {
        /* 链表拼装完成后, 此时链表尾部就是结束 */
        metadata->end = list_tail(txn->sysdictHis);
        stmt->stmt = (void *)metadata;
        txn->stmts = lappend(txn->stmts, stmt);
    }
    else
    {
        rfree(metadata);
        rfree(stmt);
    }
}

/* 更新解析节点lsn信息* */
bool ripple_transcache_refreshlsn(void* in_ctx, ripple_txn* txn)
{
    ripple_decodingcontext* ctx = NULL;

    if (NULL == txn || NULL == in_ctx)
    {
        elog(RLOG_WARNING, "ctx or txn is null");
        return false;
    }

    ctx = (ripple_decodingcontext*)in_ctx;
    
    if (RIPPLE_TXN_CHECK_TRANS_INHASH(txn->flag))
    {
        if(NULL == txn->prev)
        {
            /* 设置新的 restartlsn */

            if(txn->start.wal.lsn > ctx->base.restartlsn)
            {
                ctx->base.restartlsn = txn->start.wal.lsn;
                txn->restart.wal.lsn = txn->start.wal.lsn;
                elog(RLOG_DEBUG, "restartlsn update by:xid:%lu, %X/%X",
                                txn->xid,
                                (uint32)(txn->start.wal.lsn >> 32), (uint32)(txn->start.wal.lsn));
            }

            if (NULL == txn->next)
            {
                if(txn->end.wal.lsn > ctx->base.restartlsn)
                {
                    ctx->base.restartlsn = txn->end.wal.lsn;
                }
            }
        }
    }
    else
    {
        if (NULL == ctx->transcache->transdlist->head)
        {
            ctx->base.restartlsn = txn->end.wal.lsn;
        }

        ctx->base.confirmedlsn = txn->end.wal.lsn;
    }

    if(txn->end.wal.lsn > ctx->base.confirmedlsn)
    {
        ctx->base.confirmedlsn = txn->end.wal.lsn;
    }
    
    ripple_fpwcache_calcredolsnbyrestartlsn(ctx->transcache, ctx->base.restartlsn, &(ctx->base.redolsn));

    txn->restart.wal.lsn = ctx->base.restartlsn;
    txn->confirm.wal.lsn = ctx->base.confirmedlsn;
    txn->redo.wal.lsn = ctx->base.redolsn;
    if (NULL != ctx->callback.setmetricsynclsn)
    {
        ctx->callback.setmetricsynclsn(ctx->privdata, ctx->base.redolsn, ctx->base.restartlsn, ctx->base.confirmedlsn);
    }

    return true;
}

/* 在decodingcontext删除txn */
bool ripple_transcache_deletetxn(void* in_ctx, ripple_txn* txn)
{
    ripple_decodingcontext* ctx = NULL;

    if (NULL == txn || NULL == in_ctx)
    {
        elog(RLOG_WARNING, "ctx or txn is null");
        return false;
    }

    ctx = (ripple_decodingcontext*)in_ctx;

    /* 将事务在ctx->transcache->transdlist删除 */
    if(NULL == txn->prev)
    {
        if(NULL == txn->next)
        {
            ctx->transcache->transdlist->head = NULL;
            ctx->transcache->transdlist->tail = NULL;
        }
        else
        {
            ctx->transcache->transdlist->head = txn->next;
            txn->next->prev = NULL;
        }
    }
    else
    {
        if(NULL == txn->next)
        {
            ctx->transcache->transdlist->tail = txn->prev;
            txn->prev->next = NULL;
        }
        else
        {
            txn->prev->next = txn->next;
            txn->next->prev = txn->prev;
        }
    }
    txn->prev = NULL;
    txn->next = NULL;

    /* 将事务在哈希中删除 */
    ripple_transcache_removeTXNByXid(ctx->transcache, txn->xid);

    return true;
}

void ripple_transcache_sysdict_free(ripple_txn* txn)
{
    ListCell *cell = NULL;
    List *sysdict_List = txn->sysdict;

    if (!sysdict_List)
        return;

    foreach(cell, sysdict_List)
    {
        ripple_txn_sysdict *dict = (ripple_txn_sysdict *) lfirst(cell);
        if (dict->convert_colvalues)
        {
            ripple_cache_sysdicts_catalogdatafreevoid(dict->convert_colvalues);
        }
        heap_free_trans_result((xk_pg_parser_translog_tbcolbase *)dict->colvalues);
        rfree(dict);
    }
    list_free(sysdict_List);
    txn->sysdict = NULL;
}

/* transcache 删除 */
void ripple_transcache_free(ripple_transcache* transcache)
{
    HASH_SEQ_STATUS status;
    ListCell* lc = NULL;
    ripple_txn* txn = NULL;
    ripple_checkpointnode* chkptnode = NULL;
    if(NULL == transcache)
    {
        return;
    }

    /* txn hash表释放 */
    if(NULL != transcache->transdlist)
    {
        for(txn = transcache->transdlist->head; NULL != txn; txn = transcache->transdlist->head)
        {
            transcache->transdlist->head = txn->next;

            ripple_txn_free(txn);
        }
        rfree(transcache->transdlist);
    }

    if(NULL != transcache->by_txns)
    {
        hash_destroy(transcache->by_txns);
    }

    /* hash表删除 */
    /* class 表删除 */
    if(NULL != transcache->sysdicts)
    {
        /* relfilenode 缓存清理 */
        if(NULL != transcache->sysdicts->by_relfilenode)
        {
            hash_destroy(transcache->sysdicts->by_relfilenode);
        }

        if(NULL != transcache->sysdicts->by_class)
        {
            ripple_catalog_class_value *catalogclassentry;
            hash_seq_init(&status,transcache->sysdicts->by_class);
            while (NULL != (catalogclassentry = hash_seq_search(&status)))
            {
                if(NULL != catalogclassentry->ripple_class)
                {
                    rfree(catalogclassentry->ripple_class);
                }
            }

            hash_destroy(transcache->sysdicts->by_class);
        }

        /* attributes 表删除 */
        if(NULL != transcache->sysdicts->by_attribute)
        {
            ripple_catalog_attribute_value* catalogattrentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_attribute);
            while(NULL != (catalogattrentry = hash_seq_search(&status)))
            {
                if(NULL != catalogattrentry->attrs)
                {
                    foreach(lc, catalogattrentry->attrs)
                    {
                        rfree(lfirst(lc));
                    }
                    list_free(catalogattrentry->attrs);
                }
            }

            hash_destroy(transcache->sysdicts->by_attribute);
        }

        /* type 表删除 */
        if(NULL != transcache->sysdicts->by_type)
        {
            ripple_catalog_type_value* catalogtypeentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_type);
            while(NULL != (catalogtypeentry = hash_seq_search(&status)))
            {
                if(NULL != catalogtypeentry->ripple_type)
                {
                    rfree(catalogtypeentry->ripple_type);
                }
            }

            hash_destroy(transcache->sysdicts->by_type);
        }

        /* proc 表删除 */
        if(NULL != transcache->sysdicts->by_proc)
        {
            ripple_catalog_proc_value* catalogprocentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_proc);
            while(NULL != (catalogprocentry = hash_seq_search(&status)))
            {
                if(NULL != catalogprocentry->ripple_proc)
                {
                    rfree(catalogprocentry->ripple_proc);
                }
            }

            hash_destroy(transcache->sysdicts->by_proc);
        }

        /* tablespace 表删除 */
        if(NULL != transcache->sysdicts->by_tablespace)
        {
            /* tablespace 表在当前程序中没有用到 */
            hash_destroy(transcache->sysdicts->by_tablespace);
        }

        /* namespace 表删除 */
        if(NULL != transcache->sysdicts->by_namespace)
        {
            ripple_catalog_namespace_value* catalognamespaceentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_namespace);
            while(NULL != (catalognamespaceentry = hash_seq_search(&status)))
            {
                if(NULL != catalognamespaceentry->ripple_namespace)
                {
                    rfree(catalognamespaceentry->ripple_namespace);
                }
            }
            hash_destroy(transcache->sysdicts->by_namespace);
        }

        /* range 表删除 */
        if(NULL != transcache->sysdicts->by_range)
        {
            ripple_catalog_range_value* catalograngeentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_range);
            while(NULL != (catalograngeentry = hash_seq_search(&status)))
            {
                if(NULL != catalograngeentry->ripple_range)
                {
                    rfree(catalograngeentry->ripple_range);
                }
            }
            hash_destroy(transcache->sysdicts->by_range);
        }

        /* enum 表删除 */
        if(NULL != transcache->sysdicts->by_enum)
        {
            ripple_catalog_enum_value* catalogenumentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_enum);
            while(NULL != (catalogenumentry = hash_seq_search(&status)))
            {
                if(NULL != catalogenumentry->enums)
                {
                    foreach(lc, catalogenumentry->enums)
                    {
                        rfree(lfirst(lc));
                    }
                    list_free(catalogenumentry->enums);
                }
            }

            hash_destroy(transcache->sysdicts->by_enum);
        }

        /* operator 表删除 */
        if(NULL != transcache->sysdicts->by_operator)
        {
            ripple_catalog_operator_value* catalogoperatorentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_operator);
            while(NULL != (catalogoperatorentry = hash_seq_search(&status)))
            {
                if(NULL != catalogoperatorentry->ripple_operator)
                {
                    rfree(catalogoperatorentry->ripple_operator);
                }
            }

            hash_destroy(transcache->sysdicts->by_operator);
        }

        /* by_authid */
        if(NULL != transcache->sysdicts->by_authid)
        {
            ripple_catalog_authid_value* catalogauthidentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_authid);
            while(NULL != (catalogauthidentry = hash_seq_search(&status)))
            {
                if(NULL != catalogauthidentry->ripple_authid)
                {
                    rfree(catalogauthidentry->ripple_authid);
                }
            }

            hash_destroy(transcache->sysdicts->by_authid);
        }

        if(NULL != transcache->sysdicts->by_constraint)
        {
            ripple_catalog_constraint_value *catalogconentry;
            hash_seq_init(&status,transcache->sysdicts->by_constraint);
            while (NULL != (catalogconentry = hash_seq_search(&status)))
            {
                if(NULL != catalogconentry->constraint)
                {
                    if (0 != catalogconentry->constraint->conkeycnt)
                    {
                        rfree(catalogconentry->constraint->conkey);
                    }
                    rfree(catalogconentry->constraint);
                }
            }

            hash_destroy(transcache->sysdicts->by_constraint);
        }

        /*by_database*/
        if(NULL != transcache->sysdicts->by_database)
        {
            ripple_catalog_database_value* catalogdatabaseentry = NULL;
            hash_seq_init(&status,transcache->sysdicts->by_database);
            while(NULL != (catalogdatabaseentry = hash_seq_search(&status)))
            {
                if(NULL != catalogdatabaseentry->ripple_database)
                {
                    rfree(catalogdatabaseentry->ripple_database);
                }
            }

            hash_destroy(transcache->sysdicts->by_database);
        }

        /* by_datname2oid */
        if(NULL != transcache->sysdicts->by_datname2oid)
        {
            hash_destroy(transcache->sysdicts->by_datname2oid);
            transcache->sysdicts->by_datname2oid = NULL;
        }

        /* by_index */
        if(NULL != transcache->sysdicts->by_index)
        {
            ripple_catalog_index_value* index = NULL;
            ripple_catalog_index_hash_entry* catalogindexentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_index);
            while(NULL != (catalogindexentry = hash_seq_search(&status)))
            {
                if(NULL != catalogindexentry->ripple_index_list)
                {
                    foreach(lc, catalogindexentry->ripple_index_list)
                    {
                        index = (ripple_catalog_index_value*)lfirst(lc);
                        if (index->ripple_index)
                        {
                            if (index->ripple_index->indkey)
                            {
                                rfree(index->ripple_index->indkey);
                            }
                            rfree(index->ripple_index);
                        }
                        rfree(index);
                    }
                    list_free(catalogindexentry->ripple_index_list);
                }
            }
            hash_destroy(transcache->sysdicts->by_index);
        }

        rfree(transcache->sysdicts);
        transcache->sysdicts = NULL;
    }

    if (NULL != transcache->addtablepattern)
    {
        ripple_filter_dataset_listpairsfree(transcache->addtablepattern);
    }

    if (NULL != transcache->tableexcludes)
    {
        ripple_filter_dataset_listpairsfree(transcache->tableexcludes);
    }

    if (NULL != transcache->tableincludes)
    {
        ripple_filter_dataset_listpairsfree(transcache->tableincludes);
    }

    /* fpw 缓存清理 */
    if(NULL != transcache->by_fpwtuples)
    {
        ReorderBufferFPWEntry *fpwentry = NULL;
        hash_seq_init(&status,transcache->by_fpwtuples);
        while(NULL != (fpwentry = hash_seq_search(&status)))
        {
            if(NULL != fpwentry->data)
            {
                rfree(fpwentry->data);
            }
        }

        hash_destroy(transcache->by_fpwtuples);
    }

    if(NULL != transcache->fpwtupleslist)
    {
        list_free_deep(transcache->fpwtupleslist);
    }

    if (NULL != transcache->hsyncdataset)
    {
        hash_destroy(transcache->hsyncdataset);
        transcache->hsyncdataset = NULL;
    }

    if (NULL != transcache->htxnfilterdataset)
    {
        hash_destroy(transcache->htxnfilterdataset);
        transcache->htxnfilterdataset = NULL;
    }

    /* chkpt 释放 */
    if(NULL != transcache->chkpts)
    {
        for(chkptnode = transcache->chkpts->head; NULL != chkptnode; chkptnode = transcache->chkpts->head)
        {
            transcache->chkpts->head = chkptnode->next;
            rfree(chkptnode);
            chkptnode = NULL;
        }

        rfree(transcache->chkpts);
        transcache->chkpts = NULL;
    }
}

/* 获取数据库的标识 */
Oid ripple_transcache_getdboid(void* in_transcache)
{
    ripple_transcache* transcache = NULL;

    transcache = (ripple_transcache*)in_transcache;
    return ripple_database_getdbid(transcache->sysdicts->by_database);
}

/* 获取数据库的名称 */
char* ripple_transcache_getdbname(Oid dbid, void* in_transcache)
{
    ripple_transcache* transcache = NULL;
    transcache = (ripple_transcache*)in_transcache;
    return ripple_database_getdbname(dbid, transcache->sysdicts->by_database);
}

/* 获取namespace数据 */
void* ripple_transcache_getnamespace(Oid oid, void* in_transcache)
{
    ripple_transcache* transcache = NULL;
    transcache = (ripple_transcache*)in_transcache;
    return ripple_namespace_getbyoid(oid, transcache->sysdicts->by_namespace);
}

/* 获取class数据 */
void* ripple_transcache_getclass(Oid oid, void* in_transcache)
{
    ripple_transcache* transcache = NULL;
    transcache = (ripple_transcache*)in_transcache;
    return ripple_class_getbyoid(oid, transcache->sysdicts->by_class);
}

/* 获取attribute数据 */
void* ripple_transcache_getattributes(Oid oid, void* in_transcache)
{
    ripple_transcache* transcache = NULL;
    transcache = (ripple_transcache*)in_transcache;
    return ripple_attribute_getbyoid(oid, transcache->sysdicts->by_attribute);
}

/* 获取index链表 */
void* ripple_transcache_getindex(Oid oid, void* in_transcache)
{
    ripple_transcache* transcache = NULL;
    transcache = (ripple_transcache*)in_transcache;
    return ripple_index_getbyoid(oid, transcache->sysdicts->by_index);
}

/* 获取type数据 */
void* ripple_transcache_gettype(Oid oid, void* in_transcache)
{
    ripple_transcache* transcache = NULL;
    transcache = (ripple_transcache*)in_transcache;
    return ripple_type_getbyoid(oid, transcache->sysdicts->by_type);
}

