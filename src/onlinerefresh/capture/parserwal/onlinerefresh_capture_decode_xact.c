#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/sinval/sinval.h"
#include "utils/regex/ripple_regex.h"
#include "threads/ripple_threads.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "catalog/ripple_control.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "stmts/ripple_txnstmt.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "cache/ripple_fpwcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "snapshot/ripple_snapshot.h"
#include "refresh/ripple_refresh_tables.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "task/ripple_task_slot.h"
#include "queue/ripple_queue.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_ddl.h"
#include "works/parserwork/wal/ripple_decode_relmap.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "strategy/ripple_filter_dataset.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "works/parserwork/wal/ripple_parserwork_wal.h"
#include "onlinerefresh/capture/parserwal/ripple_onlinerefresh_capture_decode_xact.h"
#include "onlinerefresh/capture/ripple_onlinerefresh_capture.h"

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


static void ripple_decode_xact_appendsubtxn(List** ptxnstmts, List** psysdicthis, ripple_txn* subtxn)
{
    ListCell* lc = NULL;
    ripple_txnstmt* stmt = NULL;
    ripple_catalogdata* catalog = NULL;

    foreach(lc, subtxn->stmts)
    {
        stmt = (ripple_txnstmt*)lfirst(lc);
        *ptxnstmts = lappend(*ptxnstmts, stmt);
    }
    list_free(subtxn->stmts);
    subtxn->stmts = NULL;

    foreach(lc, subtxn->sysdictHis)
    {
        catalog = (ripple_catalogdata*)lfirst(lc);
        *psysdicthis = lappend(*psysdicthis, catalog);
    }
    list_free(subtxn->sysdictHis);
    subtxn->sysdictHis = NULL;
}

/*
 * 在 Commit 时, 将子事务中的内容 append 到主事务中
*/
static void ripple_decode_xact_buildcommittxn(ripple_decodingcontext* ctx,
                                                xk_pg_parser_translog_pre_trans* pretrans,
                                                ripple_txn* in_txn)
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
        ripple_txn* subtxn = NULL;
        TransactionId subxid = parsedcommit->subxacts[index];

        /* 获取事务号，并获取存储的事务信息 */
        subtxn = ripple_transcache_getTXNByXidFind(ctx->transcache, subxid);

        /* DDL 兑换 */
        if(NULL != subtxn->sysdict)
        {
            /* 将 sysdict 转移到 sysdicthis 中 */
            ripple_transcache_sysdict2his(subtxn);

            /* 释放 */
            ripple_transcache_sysdict_free(subtxn);
        }

        /* 将子事务的数据 append 到 主数据库中 */
        if(NULL != subtxn->stmts)
        {
            in_txn->stmtsize += subtxn->stmtsize;
        }
        ripple_decode_xact_appendsubtxn(&txnstmts, &sysdicthis, subtxn);

        /* 将事务在事务链表中删除 */
        ripple_transcache_dlist_remove((void*)ctx, subtxn, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn 内存释放 */
        ripple_txn_free(subtxn);

        /* 将子事务在hash中删除 */
        ripple_transcache_removeTXNByXid(ctx->transcache, subtxn->xid);
    }

    in_txn->sysdictHis = sysdicthis;
    in_txn->stmts = txnstmts;
}

static void check_online_refresh_decode_xids(ripple_decodingcontext* ctx, TransactionId xid)
{
    dlistnode *dlnode_xid = NULL;
    ripple_onlinerefresh_capture *olcapture = (ripple_onlinerefresh_capture *)ctx->privdata;

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
            ripple_onlinerefresh_capture_xids_delete(olcapture, dlnode_xid);
        }
        dlnode_xid = dlnode_next;
    }

    if (ripple_onlinerefresh_capture_xids_isnull(olcapture))
    {
        ripple_txn *end_txn = NULL;

        /* 构建onlinerefresh end事务 */
        end_txn = ripple_parserwork_build_onlinerefresh_increment_end_txn(olcapture->no->data);

        /* 事务添加到缓存 */
        ripple_cache_txn_add(ctx->parser2txns, end_txn);

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
void ripple_onlinerefresh_decode_xact_commit(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    bool redo = false;
    ListCell* lc = NULL;
    ripple_txnstmt* stmt = NULL;
    ripple_txn* txn = NULL;
    ripple_txn* ctxn = NULL;
    xk_pg_parser_translog_pre_trans* pretrans = NULL;

    pretrans = (xk_pg_parser_translog_pre_trans*)pbase;

    /*
     * 根据事务号获取事务链表
     */
    txn = ripple_transcache_getTXNByXidFind(ctx->transcache, pretrans->m_base.m_xid);
    /* 如果为空，那么说明不需要处理 */
    if(NULL == txn)
    {
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    /* 无需捕获的事物, 且没有sysdict或者sysdictHis则不需要处理, 删除事务返回即可 */
    if (true == txn->filter && !txn->sysdict && !txn->sysdictHis)
    {
        /* 将事务在transdlist、by_txns中删除 */
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        ripple_transcache_deletetxn((void*)ctx, txn);
        return;
    }

    txn->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    elog(RLOG_DEBUG, "xid:%lu, startlsn:%X/%X, endlsn:%X/%X, confirmed_lsn:%X/%X",
                    txn->xid,
                    (uint32)(txn->start.wal.lsn >> 32), (uint32)(txn->start.wal.lsn),
                    (uint32)(txn->end.wal.lsn >> 32), (uint32)(txn->end.wal.lsn),
                    (uint32)(ctx->base.confirmedlsn >> 32), (uint32)(ctx->base.confirmedlsn));

    /* 查看是否处于 redo 中 */
    if(txn->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        redo = true;
    }

    if(NULL != txn->sysdict)
    {
        /* 将 sysdict 转移到 sysdicthis 中 */
        ripple_transcache_sysdict2his(txn);

        ripple_transcache_sysdict_free(txn);
    }

    /* 子事务处理 */
    ripple_decode_xact_buildcommittxn(ctx, pretrans, txn);

    /* sysdicthis 应用 */
    ripple_cache_sysdicts_txnsysdicthis2cache(ctx->transcache->sysdicts, txn->sysdictHis);

    /* 更新同步数据集 */
    ripple_filter_dataset_updatedatasets(ctx->transcache->addtablepattern,
                                        ctx->transcache->sysdicts->by_namespace,
                                        txn->sysdictHis,
                                        ctx->transcache->hsyncdataset);

    /* 符合过滤条件,那么将语句清理 */
    if(ripple_decodingcontext_isstmtsfilter(txn->filter, redo))
    {
        List* metalist = NULL;
        foreach(lc, txn->stmts)
        {
            stmt = (ripple_txnstmt*)lfirst(lc);
            if(NULL != stmt->stmt && RIPPLE_TXNSTMT_TYPE_METADATA != stmt->type)
            {
                /* 根据不同的类型,调用不同的释放函数 */
                ripple_txnstmt_free(stmt);
            }
            else
            {
                metalist = lappend(metalist, stmt);
            }
        }
        list_free(txn->stmts);
        txn->stmts = metalist;
    }

    /* 将事务写入到缓存中 */
    elog(RLOG_DEBUG, "stmtlen:%lu, startlsn:%X/%X, %lu, xid:%lu, walrec:%lu, parserrec:%lu",
                        txn->stmtsize,
                        (uint32)(txn->start.wal.lsn >> 32), (uint32)(txn->start.wal.lsn),
                        txn->xid,
                        txn->debugno,
                        g_walrecno,
                        g_parserecno);

    /* 检查最后一条最后一条语句的extra0并赋值 */
    if (NULL != (lc = list_tail(txn->stmts)))
    {
        stmt = (ripple_txnstmt*)lfirst(lc);
        if (InvalidXLogRecPtr == stmt->extra0.wal.lsn)
        {
            stmt->extra0.wal.lsn = ctx->decode_record->end.wal.lsn;
        }
    }

    /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
    ripple_transcache_refreshlsn((void*)ctx, txn);

    txn->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    ctxn = ripple_txn_copy(txn);

    /* 将事务在transdlist、by_txns中删除 */
    ripple_transcache_deletetxn((void*)ctx, txn);

    ripple_txn_addcommit(ctxn);
    ripple_cache_txn_add(ctx->parser2txns, ctxn);

    check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
}

/* 
 * 事务回滚
 *  当解析的 lsn < confirmlsn 时，此时释放资源即可
 *  当解析的 lsn > confirmlsn 时，那么需要将事务传递到 格式化 线程，用于向前推进 restartlsn
 */
void ripple_onlinerefresh_decode_xact_abort(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    bool brestart = false;
    bool bconfirm = false;
    int index = 0;
    ListCell* lc = NULL;
    ripple_txn* txn = NULL;
    ripple_txn* ctxn = NULL;
    ripple_txnstmt* txnstmt = NULL;
    xl_xact_parsed_abort* parsedabort = NULL;
    xk_pg_parser_translog_pre_trans* pretrans = NULL;

    pretrans = (xk_pg_parser_translog_pre_trans*)pbase;

    /*
     * 根据事务号获取事务链表
     */
    txn = ripple_transcache_getTXNByXidFind(ctx->transcache, pretrans->m_base.m_xid);
    /* 如果为空，那么说明不需要处理 */
    if(NULL == txn)
    {
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    txn->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    parsedabort = (xl_xact_parsed_abort*)pretrans->m_transdata;
    for(index = 0; index < parsedabort->nsubxacts; index++)
    {
        ripple_txn* subtxn = NULL;
        TransactionId subxid = parsedabort->subxacts[index];

        /* 获取事务号，并获取存储的事务信息 */
        subtxn = ripple_transcache_getTXNByXidFind(ctx->transcache, subxid);
        if(NULL == subtxn)
        {
            continue;
        }

        /* 系统表释放 */
        ripple_transcache_sysdict_free(subtxn);

        /* 将事务在事务链表中删除 */
        ripple_transcache_dlist_remove((void*)ctx, subtxn, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn 内存释放 */
        ripple_txn_free(subtxn);

        /* 将子事务在hash中删除 */
        ripple_transcache_removeTXNByXid(ctx->transcache, subtxn->xid);
    }

    /* 系统表释放 */
    ripple_transcache_sysdict_free(txn);

    if(txn->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        /* txn 释放 */
        /* 将事务在事务链表中删除 */
        ripple_transcache_dlist_remove((void*)ctx, txn, &brestart, NULL, &bconfirm, NULL, false);

        /* txn 内存释放 */
        ripple_txn_free(txn);

        /* 将事务在hash中删除 */
        ripple_transcache_removeTXNByXid(ctx->transcache, txn->xid);

        return;
    }

    /* 将 txn->stmts 的内容释放掉 */
    foreach(lc, txn->stmts)
    {
        txnstmt = (ripple_txnstmt*)lfirst(lc);
        ripple_txnstmt_free(txnstmt);
    }
    list_free(txn->stmts);
    txn->stmts = NULL;

    /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
    ripple_transcache_refreshlsn((void*)ctx, txn);

    txn->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    ctxn = ripple_txn_copy(txn);

    /* 将事务在transdlist、by_txns中删除 */
    ripple_transcache_deletetxn((void*)ctx, txn);

    /* 放入到事务缓存中,让 write 线程处理 */
    ripple_cache_txn_add(ctx->parser2txns, ctxn);

    check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
    return;
}
