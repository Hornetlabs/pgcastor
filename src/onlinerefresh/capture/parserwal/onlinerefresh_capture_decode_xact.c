#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/sinval/sinval.h"
#include "utils/regex/regex.h"
#include "threads/threads.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_errnodef.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "catalog/control.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "stmts/txnstmt.h"
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
#include "snapshot/snapshot.h"
#include "refresh/refresh_tables.h"
#include "works/parserwork/wal/rewind.h"
#include "task/task_slot.h"
#include "queue/queue.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_ddl.h"
#include "works/parserwork/wal/decode_relmap.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "strategy/filter_dataset.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "works/parserwork/wal/parserwork_wal.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode_xact.h"
#include "onlinerefresh/capture/onlinerefresh_capture.h"

#define GIDSIZE 200

typedef struct xl_xact_parsed_commit
{
	TimestampTz xact_time;
	uint32		xinfo;

	Oid			dbId;			/* MyDatabaseId */
	Oid			tsId;			/* MyDatabaseTableSpace */

	int			nsubxacts;
	TransactionId *subxacts;

	int			nrels;
	RelFileNode *xnodes;

	int			nmsgs;
	SharedInvalidationMessage *msgs;

	TransactionId twophase_xid; /* only for 2PC */
	char		twophase_gid[GIDSIZE];	/* only for 2PC */
	int			nabortrels;		/* only for 2PC */
	RelFileNode *abortnodes;	/* only for 2PC */

	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_parsed_commit;

typedef xl_xact_parsed_commit xl_xact_parsed_prepare;


typedef struct xl_xact_parsed_abort
{
	TimestampTz xact_time;
	uint32		xinfo;

	Oid			dbId;			/* MyDatabaseId */
	Oid			tsId;			/* MyDatabaseTableSpace */

	int			nsubxacts;
	TransactionId *subxacts;

	int			nrels;
	RelFileNode *xnodes;

	TransactionId twophase_xid; /* only for 2PC */
	char		twophase_gid[GIDSIZE];	/* only for 2PC */

	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_parsed_abort;


static void decode_xact_appendsubtxn_obj(List** ptxnstmts, List** psysdicthis, txn* subtxn_obj)
{
    ListCell* lc = NULL;
    txnstmt* stmt = NULL;
    catalogdata* catalog = NULL;

    foreach(lc, subtxn_obj->stmts)
    {
        stmt = (txnstmt*)lfirst(lc);
        *ptxnstmts = lappend(*ptxnstmts, stmt);
    }
    list_free(subtxn_obj->stmts);
    subtxn_obj->stmts = NULL;

    foreach(lc, subtxn_obj->sysdictHis)
    {
        catalog = (catalogdata*)lfirst(lc);
        *psysdicthis = lappend(*psysdicthis, catalog);
    }
    list_free(subtxn_obj->sysdictHis);
    subtxn_obj->sysdictHis = NULL;
}

/*
 * 在 Commit 时, 将子事务中的内容 append 到主事务中
*/
static void decode_xact_buildcommittxn(decodingcontext* ctx,
                                                pg_parser_translog_pre_trans* pretrans,
                                                txn* in_txn)
{
    int index = 0;
    List* txnstmts = NULL;
    List* sysdicthis = NULL;
    xl_xact_parsed_commit* parsedcommit = NULL;

    parsedcommit = (xl_xact_parsed_commit*)pretrans->m_transdata;
    if(0 == parsedcommit->nsubxacts)
    {
        return;
    }

    txnstmts = in_txn->stmts;
    in_txn->stmts = NULL;

    sysdicthis = in_txn->sysdictHis;
    in_txn->sysdictHis = NULL;

    /* 遍历子事务放到合适的位置 */
    for(index = 0; index < parsedcommit->nsubxacts; index++)
    {
        bool brestart = false;
        bool bconfirm = false;
        txn* subtxn_obj = NULL;
        TransactionId subxid = parsedcommit->subxacts[index];

        /* 获取事务号，并获取存储的事务信息 */
        subtxn_obj = transcache_getTXNByXidFind(ctx->trans_cache, subxid);

        /* DDL 兑换 */
        if(NULL != subtxn_obj->sysdict)
        {
            /* 将 sysdict 转移到 sysdicthis 中 */
            transcache_sysdict2his(subtxn_obj);

            /* 释放 */
            transcache_sysdict_free(subtxn_obj);
        }

        /* 将子事务的数据 append 到 主数据库中 */
        if(NULL != subtxn_obj->stmts)
        {
            in_txn->stmtsize += subtxn_obj->stmtsize;
        }
        decode_xact_appendsubtxn_obj(&txnstmts, &sysdicthis, subtxn_obj);

        /* 将事务在事务链表中删除 */
        transcache_dlist_remove((void*)ctx, subtxn_obj, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn_obj 内存释放 */
        txn_free(subtxn_obj);

        /* 将子事务在hash中删除 */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_obj->xid);
    }

    in_txn->sysdictHis = sysdicthis;
    in_txn->stmts = txnstmts;
}

static void check_online_refresh_decode_xids(decodingcontext* ctx, TransactionId xid)
{
    dlistnode *dlnode_xid = NULL;
    onlinerefresh_capture *olcapture = (onlinerefresh_capture *)ctx->privdata;

    if (!olcapture->xids)
    {
        return;
    }

    dlnode_xid = olcapture->xids->head;

    while (dlnode_xid)
    {
        dlistnode *dlnode_next = NULL;
        FullTransactionId *xid_p = (FullTransactionId *)dlnode_xid->value;

        dlnode_next = dlnode_xid->next;

        /* 判断是否在链表中 */
        if (*xid_p == (FullTransactionId) xid)
        {
            /* 在链表中, 删除 */
            onlinerefresh_capture_xids_delete(olcapture, dlnode_xid);
        }
        dlnode_xid = dlnode_next;
    }

    if (onlinerefresh_capture_xids_isnull(olcapture))
    {
        txn *end_txn = NULL;

        /* 构建onlinerefresh end事务 */
        end_txn = parserwork_build_onlinerefresh_increment_end_txn(olcapture->no->data);

        /* 事务添加到缓存 */
        cache_txn_add(ctx->parser2txns, end_txn);

        dlist_free(olcapture->xids, NULL);
        olcapture->xids = NULL;
    }
}

/* 
 * commit 提交，处理逻辑如下
 *  1、首先 commit 时，要先根据提交的lsn与confirmlsn进行比较，当小于 confirmlsn 时说明此事务不需要处理，那么清理数据
 *      1.1 应用系统表数据
 * 
 *  2、将当前事务包含的子事务合并到主事务中
 * 
 * 
 * 在 PG 中的事务逻辑如下:
 *  主事务 xid
 *      savepoint   子事务 xid1
 *      savepoint   子事务 xid2，父事务  在逻辑上为 xid1,但是在 PG 中没有嵌套事务的逻辑，所以父事务为 xid
 * 
*/
void onlinerefresh_decode_xact_commit(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    bool redo = false;
    ListCell* lc = NULL;
    txnstmt* stmt = NULL;
    txn* txn_ptr = NULL;
    txn* txn_copy_obj = NULL;
    pg_parser_translog_pre_trans* pretrans = NULL;

    pretrans = (pg_parser_translog_pre_trans*)pbase;

    /*
     * 根据事务号获取事务链表
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* 如果为空，那么说明不需要处理 */
    if(NULL == txn_ptr)
    {
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    /* 无需捕获的事物, 且没有sysdict或者sysdictHis则不需要处理, 删除事务返回即可 */
    if (true == txn_ptr->filter && !txn_ptr->sysdict && !txn_ptr->sysdictHis)
    {
        /* 将事务在transdlist、by_txns中删除 */
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        transcache_deletetxn((void*)ctx, txn_ptr);
        return;
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    elog(RLOG_DEBUG, "xid:%lu, startlsn:%X/%X, endlsn:%X/%X, confirmed_lsn:%X/%X",
                    txn_ptr->xid,
                    (uint32)(txn_ptr->start.wal.lsn >> 32), (uint32)(txn_ptr->start.wal.lsn),
                    (uint32)(txn_ptr->end.wal.lsn >> 32), (uint32)(txn_ptr->end.wal.lsn),
                    (uint32)(ctx->base.confirmedlsn >> 32), (uint32)(ctx->base.confirmedlsn));

    /* 查看是否处于 redo 中 */
    if(txn_ptr->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        redo = true;
    }

    if(NULL != txn_ptr->sysdict)
    {
        /* 将 sysdict 转移到 sysdicthis 中 */
        transcache_sysdict2his(txn_ptr);

        transcache_sysdict_free(txn_ptr);
    }

    /* 子事务处理 */
    decode_xact_buildcommittxn(ctx, pretrans, txn_ptr);

    /* sysdicthis 应用 */
    cache_sysdicts_txnsysdicthis2cache(ctx->trans_cache->sysdicts, txn_ptr->sysdictHis);

    /* 更新同步数据集 */
    filter_dataset_updatedatasets(ctx->trans_cache->addtablepattern,
                                        ctx->trans_cache->sysdicts->by_namespace,
                                        txn_ptr->sysdictHis,
                                        ctx->trans_cache->hsyncdataset);

    /* 符合过滤条件,那么将语句清理 */
    if(decodingcontext_isstmtsfilter(txn_ptr->filter, redo))
    {
        List* metalist = NULL;
        foreach(lc, txn_ptr->stmts)
        {
            stmt = (txnstmt*)lfirst(lc);
            if(NULL != stmt->stmt && TXNSTMT_TYPE_METADATA != stmt->type)
            {
                /* 根据不同的类型,调用不同的释放函数 */
                txnstmt_free(stmt);
            }
            else
            {
                metalist = lappend(metalist, stmt);
            }
        }
        list_free(txn_ptr->stmts);
        txn_ptr->stmts = metalist;
    }

    /* 将事务写入到缓存中 */
    elog(RLOG_DEBUG, "stmtlen:%lu, startlsn:%X/%X, %lu, xid:%lu, walrec:%lu, parserrec:%lu",
                        txn_ptr->stmtsize,
                        (uint32)(txn_ptr->start.wal.lsn >> 32), (uint32)(txn_ptr->start.wal.lsn),
                        txn_ptr->xid,
                        txn_ptr->debugno,
                        g_walrecno,
                        g_parserecno);

    /* 检查最后一条最后一条语句的extra0并赋值 */
    if (NULL != (lc = list_tail(txn_ptr->stmts)))
    {
        stmt = (txnstmt*)lfirst(lc);
        if (InvalidXLogRecPtr == stmt->extra0.wal.lsn)
        {
            stmt->extra0.wal.lsn = ctx->decode_record->end.wal.lsn;
        }
    }

    /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    txn_ptr->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    txn_copy_obj = txn_copy(txn_ptr);

    /* 将事务在transdlist、by_txns中删除 */
    transcache_deletetxn((void*)ctx, txn_ptr);

    txn_addcommit(txn_copy_obj);
    cache_txn_add(ctx->parser2txns, txn_copy_obj);

    check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
}

/* 
 * 事务回滚
 *  当解析的 lsn < confirmlsn 时，此时释放资源即可
 *  当解析的 lsn > confirmlsn 时，那么需要将事务传递到 格式化 线程，用于向前推进 restartlsn
 */
void onlinerefresh_decode_xact_abort(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    bool brestart = false;
    bool bconfirm = false;
    int index = 0;
    ListCell* lc = NULL;
    txn* txn_ptr = NULL;
    txn* txn_copy_obj = NULL;
    xl_xact_parsed_abort* parsedabort = NULL;
    pg_parser_translog_pre_trans* pretrans = NULL;
    txn* subtxn_obj = NULL;
    TransactionId subxid = 0;

    pretrans = (pg_parser_translog_pre_trans*)pbase;

    /*
     * 根据事务号获取事务链表
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* 如果为空，那么说明不需要处理 */
    if(NULL == txn_ptr)
    {
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    parsedabort = (xl_xact_parsed_abort*)pretrans->m_transdata;
    for(index = 0; index < parsedabort->nsubxacts; index++)
    {
        subxid = parsedabort->subxacts[index];

        /* 获取事务号，并获取存储的事务信息 */
        subtxn_obj = transcache_getTXNByXidFind(ctx->trans_cache, subxid);
        if(NULL == subtxn_obj)
        {
            continue;
        }

        /* 系统表释放 */
        transcache_sysdict_free(subtxn_obj);

        /* 将事务在事务链表中删除 */
        transcache_dlist_remove((void*)ctx, subtxn_obj, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn_obj 内存释放 */
        txn_free(subtxn_obj);

        /* 将子事务在hash中删除 */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_obj->xid);
    }

    /* 系统表释放 */
    transcache_sysdict_free(txn_ptr);

    if(txn_ptr->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        /* txn 释放 */
        /* 将事务在事务链表中删除 */
        transcache_dlist_remove((void*)ctx, txn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* txn 内存释放 */
        txn_free(txn_ptr);

        /* 将事务在hash中删除 */
        transcache_removeTXNByXid(ctx->trans_cache, txn_ptr->xid);

        return;
    }

    /* 将 txn_ptr->stmts 的内容释放掉 */
    foreach(lc, txn_ptr->stmts)
    {
        txnstmt* stmt = (txnstmt*)lfirst(lc);
        txnstmt_free(stmt);
    }
    list_free(txn_ptr->stmts);
    txn_ptr->stmts = NULL;

    /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    txn_ptr->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    txn_copy_obj = txn_copy(txn_ptr);

    /* 将事务在transdlist、by_txns中删除 */
    transcache_deletetxn((void*)ctx, txn_ptr);

    /* 放入到事务缓存中,让 write 线程处理 */
    cache_txn_add(ctx->parser2txns, txn_copy_obj);

    check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
    return;
}
