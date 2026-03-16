#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/sinval/sinval.h"
#include "utils/regex/ripple_regex.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "catalog/ripple_control.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_bigtxnend.h"
#include "cache/ripple_fpwcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_xact.h"
#include "works/parserwork/wal/ripple_decode_ddl.h"
#include "works/parserwork/wal/ripple_decode_relmap.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"
#include "refresh/ripple_refresh_tables.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "strategy/ripple_filter_dataset.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "works/parserwork/wal/ripple_parserwork_wal.h"
#include "stmts/ripple_txnstmt_onlinerefresh_dataset.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/ripple_bigtxn.h"

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

    foreach(lc, subtxn->stmts)
    {
        stmt = (ripple_txnstmt*)lfirst(lc);
        *ptxnstmts = lappend(*ptxnstmts, stmt);
    }
    list_free(subtxn->stmts);
    subtxn->stmts = NULL;

    foreach(lc, subtxn->sysdictHis)
    {
        *psysdicthis = lappend(*psysdicthis, lfirst(lc));
    }
    list_free(subtxn->sysdictHis);
    subtxn->sysdictHis = NULL;
}

static void check_online_refresh_node_need_clean(ripple_onlinerefresh *olnode, dlistnode *dlnode, ripple_decodingcontext* ctx)
{
    if (olnode->state == RIPPLE_ONLINEREFRESH_STATE_FULLSNAPSHOT && ripple_onlinerefresh_xids_isnull(olnode))
    {
        /* xids为空, 所有事务均已完成 */
        ripple_txn *end_txn = NULL;
        ripple_txn *dataset_txn = NULL;

        /* 构建onlinerefresh end事务 */
        end_txn = ripple_parserwork_build_onlinerefresh_end_txn(olnode->no->data, ctx->parselsn);

        /* 事务添加到缓存 */
        ripple_cache_txn_add(ctx->parser2txns, end_txn);

        if (olnode->newtables)
        {
            /* 构建onlinerefresh dataset事务 */
            dataset_txn = (ripple_txn *)ripple_parserwork_build_onlinerefresh_dataset_txn(olnode->newtables);

            /* 事务添加到缓存 */
            ripple_cache_txn_add(ctx->parser2txns, dataset_txn);
        }

        /* 清理 */
        ctx->onlinerefresh = ripple_onlinerefresh_refreshdlist_delete(ctx->onlinerefresh, dlnode);

        /* 判断链表是否为空*/
        if (dlist_isnull(ctx->onlinerefresh))
        {
            dlist_free(ctx->onlinerefresh, NULL);
            ctx->onlinerefresh = NULL;
            return;
        }
    }
}

static void check_online_refresh_xids(ripple_decodingcontext* ctx, TransactionId xid)
{
    dlistnode *dlnode = NULL;
    dlistnode *dlnode_xid = NULL;
    ripple_onlinerefresh *olnode = NULL;

    /* 不存在olrefresh节点时直接返回 */
    if (!ctx->onlinerefresh)
    {
        return;
    }
    dlnode = ctx->onlinerefresh->head;

    while (dlnode)
    {
        dlistnode *dlnode_next = NULL;
        olnode = (ripple_onlinerefresh *)dlnode->value;

        /* 先推进 */
        dlnode_next = dlnode->next;

        if (!olnode->increment)
        {
            /* 不需要增量, 判断commit/abort xid是否大于refresh xid */
            if (xid > olnode->txid)
            {
                ripple_txn *end_txn = NULL;
                ripple_txn *dataset_txn = NULL;

                /* 构建onlinerefresh end事务 */
                end_txn = ripple_parserwork_build_onlinerefresh_end_txn(olnode->no->data, ctx->parselsn);

                /* 事务添加到缓存 */
                ripple_cache_txn_add(ctx->parser2txns, end_txn);

                if (olnode->newtables)
                {
                    /* 构建onlinerefresh dataset事务 */
                    dataset_txn = (ripple_txn *)ripple_parserwork_build_onlinerefresh_dataset_txn(olnode->newtables);

                    /* 事务添加到缓存 */
                    ripple_cache_txn_add(ctx->parser2txns, dataset_txn);
                }

                /* 清理 */
                ctx->onlinerefresh = ripple_onlinerefresh_refreshdlist_delete(ctx->onlinerefresh, dlnode);

                /* 判断链表是否为空*/
                if (dlist_isnull(ctx->onlinerefresh))
                {
                    dlist_free(ctx->onlinerefresh, NULL);
                    ctx->onlinerefresh = NULL;
                    return;
                }
            }
            dlnode = dlnode_next;
            continue;
        }

        if (!olnode->xids)
        {
            check_online_refresh_node_need_clean(olnode, dlnode, ctx);
            dlnode = dlnode_next;
            continue;
        }

        dlnode_xid = olnode->xids->head;

        while (dlnode_xid)
        {
            dlistnode *dlnode_xid_next = NULL;
            FullTransactionId *xid_p = (FullTransactionId *)dlnode_xid->value;

            dlnode_xid_next = dlnode_xid->next;

            /* 判断是否在链表中 */
            if (*xid_p == (FullTransactionId) xid)
            {
                /* 在链表中, 删除 */
                ripple_onlinerefresh_xids_delete(olnode, olnode->xids, dlnode_xid);
            }
            dlnode_xid = dlnode_xid_next;
        }

        check_online_refresh_node_need_clean(olnode, dlnode, ctx);
        dlnode = dlnode_next;
    }
}

/*
 * 在 Commit 时, 将子事务中的内容 append 到主事务中
*/
static void ripple_decode_xact_buildcommittxn(ripple_decodingcontext* ctx,
                                                xk_pg_parser_translog_pre_trans* pretrans,
                                                ripple_txn* in_txn,
                                                bool redo)
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
            /* DDL兑换 */
            if(ripple_decodingcontext_isddlfilter(subtxn->filter, redo))
            {
                ripple_dml2ddl(ctx, subtxn);
            }

            /* 将 sysdict 转移到 sysdicthis 中 */
            ripple_transcache_sysdict2his(subtxn);

            /* 释放 */
            ripple_transcache_sysdict_free(subtxn);
            RIPPLE_TXN_UNSET_TRANS_DDL(subtxn->flag);
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
        check_online_refresh_xids(ctx, subxid);
    }

    in_txn->sysdictHis = sysdicthis;
    in_txn->stmts = txnstmts;
}

static bool ripple_large_txn_end_filter(ripple_decodingcontext* ctx, ripple_txn *commit_txn, bool commit)
{
    ripple_txnstmt* txnstmt = NULL;
    ripple_bigtxn_end_stmt *endstmt = NULL;
    ripple_txn *copytxn = NULL;
    ripple_txn *endtxn = NULL;

    /* 判断是否需要过滤大事务 */
    if (!ctx->filterbigtrans)
    {
        return true;
    }

    /* 检查是否存在大事务 */
    if (!RIPPLE_TXN_ISBIGTXN(commit_txn->flag))
    {
        /* 只处理正常txn的commit */
        if (commit)
        {
            /* 不存在大事务, 构建一个只有系统表的事物给大事务序列化 */
            copytxn = ripple_txn_copy(commit_txn);

            /* 置空拷贝后txn部分指针 */
            copytxn->toast_hash = NULL;
            copytxn->sysdictHis = ripple_decode_heap_sysdicthis_copy(commit_txn->sysdictHis);
            copytxn->stmts = NULL;
            copytxn->hsyncdataset = NULL;
            copytxn->oidmap = NULL;
            copytxn->prev = NULL;
            copytxn->next = NULL;
            copytxn->cachenext = NULL;

            /* 添加到大事务缓存 */
            
            ripple_cache_txn_add(ctx->parser2bigtxns, copytxn);
        }
        return true;
    }

    /* 设置为大事务结束 */
    if (commit)
    {
        commit_txn->type = RIPPLE_TXN_TYPE_BIGTXN_END_COMMIT;
    }
    else
    {
        commit_txn->type = RIPPLE_TXN_TYPE_BIGTXN_END_ABORT;
    }

    /* 
     * 构建大事务结束txnstmt
     * 添加到缓存中
     */
    endtxn = ripple_txn_initbigtxn(commit_txn->xid);
    if(NULL == endtxn)
    {
        elog(RLOG_WARNING, "init bigtxn error, out of memory");
        return false;
    }
    txnstmt = ripple_txnstmt_init();
    if(NULL == txnstmt)
    {
        elog(RLOG_WARNING, "init txn stmt error, out of memory");
        return false;
    }
    endtxn->stmts = lappend(endtxn->stmts, txnstmt);
    endtxn->end.wal.lsn = ctx->parselsn;
    txnstmt->type = RIPPLE_TXNSTMT_TYPE_BIGTXN_END;
    txnstmt->extra0.wal.lsn = ctx->parselsn;
    endstmt = ripple_txnstmt_bigtxnend_init();
    if(NULL == endstmt)
    {
        elog(RLOG_WARNING, "init bigtxn endstmt error, out of memory");
        return false;
    }
    txnstmt->stmt = (void*)endstmt;
    endstmt->xid = commit_txn->xid;
    endstmt->commit = commit;
    endtxn->endtimestamp = commit_txn->endtimestamp;

    ripple_txn_addcommit(endtxn);
    ripple_cache_txn_add(ctx->parser2txns, endtxn);

    /* 进行txn拷贝 */
    copytxn = ripple_txn_copy(commit_txn);
    if (commit_txn->sysdict)
    {
        elog(RLOG_WARNING, "big txn copy, txn in ddl");
        return false;
    }

    /* 重置原txn的部分指针 */
    commit_txn->sysdictHis = NULL;
    commit_txn->stmts = NULL;
    txnstmt = ripple_txnstmt_init();
    if(NULL == txnstmt)
    {
        elog(RLOG_WARNING, "init txn stmt error, out of memory");
        return false;
    }
    txnstmt->type = RIPPLE_TXNSTMT_TYPE_SYSDICTHIS;
    commit_txn->stmts = lappend(commit_txn->stmts, txnstmt);
    /* 初始化stmt大小 */
    commit_txn->stmtsize = 4;

    /* 置空拷贝后txn部分指针 */
    copytxn->toast_hash = NULL;
    copytxn->hsyncdataset = NULL;
    copytxn->oidmap = NULL;
    copytxn->prev = NULL;
    copytxn->next = NULL;
    copytxn->cachenext = NULL;

    /* 处理系统表, 进行拷贝 */
    commit_txn->sysdictHis = ripple_decode_heap_sysdicthis_copy(copytxn->sysdictHis);

    /* 添加到大事务缓存 */
    ripple_txn_addcommit(copytxn);
    ripple_cache_txn_add(ctx->parser2bigtxns, copytxn);

    return true;
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
void ripple_decode_xact_commit(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
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
        check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
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
        /* 
         * DDL兑换
         *  redo    true        处于 redo 的逻辑中，不需要转换 ddl 语句
        */
        if(ripple_decodingcontext_isddlfilter(txn->filter, redo))
        {
            ripple_dml2ddl(ctx, txn);
        }

        /* 将 sysdict 转移到 sysdicthis 中 */
        ripple_transcache_sysdict2his(txn);

        ripple_transcache_sysdict_free(txn);
        RIPPLE_TXN_UNSET_TRANS_DDL(txn->flag);
    }

    /* 子事务处理 */
    ripple_decode_xact_buildcommittxn(ctx, pretrans, txn, redo);

    /* sysdicthis 应用 */
    ripple_cache_sysdicts_txnsysdicthis2cache(ctx->transcache->sysdicts, txn->sysdictHis);

    /* 更新同步数据集 */
    ripple_filter_dataset_updatedatasets(ctx->transcache->addtablepattern,
                                        ctx->transcache->sysdicts->by_namespace,
                                        txn->sysdictHis,
                                        ctx->transcache->hsyncdataset);

    /* 符合过滤条件,那么将语句清理 */
    /* 
     * 后续流程中系统表的变更是通过-----stmt_type_meta 区分
     * 1、将 redo--->confirmlsn 之间的系统表变化在后续流程中应用
     * 2、在双向过滤中, originid 不能为空, 但是后面的流程中又需要系统表的变更
     */
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
        stmt->extra0.wal.lsn = ctx->decode_record->end.wal.lsn;
    }

    /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
    ripple_transcache_refreshlsn((void*)ctx, txn);

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    ctxn = ripple_txn_copy(txn);

    /* 将事务在transdlist、by_txns中删除 */
    ctx->transcache->totalsize -= (txn->stmtsize - 4);
    ripple_transcache_deletetxn((void*)ctx, txn);

    /* 大事务过滤 */
    ripple_large_txn_end_filter(ctx, ctxn, true);

    ripple_txn_addcommit(ctxn);
    ripple_cache_txn_add(ctx->parser2txns, ctxn);
    check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
}

void ripple_decode_xact_commit_emit(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    bool redo = true;
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
        return;
    }

    if (pretrans->m_base.m_xid >= ctx->rewind->strategy.xmax)
    {
        /* 找到了大于xmax的事务的commit */
        ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
        ctx->base.restartlsn = ctx->rewind->redolsn;
        ctx->stat = RIPPLE_DECODINGCONTEXT_RUNNING;
        if (ctx->callback.setparserlsn)
        {
            ctx->callback.setparserlsn(ctx->privdata, ctx->base.confirmedlsn, ctx->base.restartlsn, ctx->base.restartlsn);
        }
        else
        {
            elog(RLOG_WARNING, "be carefull! setparserlsn is null");
        }


        elog(RLOG_INFO, "commit emit rewind end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                        (uint32)(ctx->base.redolsn>>32), (uint32)ctx->base.redolsn,
                        (uint32)(ctx->base.restartlsn>>32), (uint32)ctx->base.restartlsn,
                        (uint32)(ctx->base.confirmedlsn>>32), (uint32)ctx->base.confirmedlsn);
        ripple_rewind_stat_setemited(ctx->rewind);
        redo = false;
    }
    else if (ctx->rewind->strategy.xips)
    {
        bool find = false;
        hash_search(ctx->rewind->strategy.xips, &pretrans->m_base.m_xid, HASH_FIND, &find);
        if (find)
        {
            /* 找到了xid列表中仍活跃事务的commit */
            ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
            ctx->base.restartlsn = ctx->rewind->redolsn;
            ctx->stat = RIPPLE_DECODINGCONTEXT_RUNNING;
            if (ctx->callback.setparserlsn)
            {
                ctx->callback.setparserlsn(ctx->privdata, ctx->base.confirmedlsn, ctx->base.restartlsn, ctx->base.restartlsn);
            }
            else
            {
                elog(RLOG_WARNING, "be carefull! setparserlsn is null");
            }

            elog(RLOG_INFO, "commit emit rewind end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                        (uint32)(ctx->base.redolsn>>32), (uint32)ctx->base.redolsn,
                        (uint32)(ctx->base.restartlsn>>32), (uint32)ctx->base.restartlsn,
                        (uint32)(ctx->base.confirmedlsn>>32), (uint32)ctx->base.confirmedlsn);
            ripple_rewind_stat_setemited(ctx->rewind);
            redo = false;
        }
    }

    txn->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    elog(RLOG_DEBUG, "xid:%lu, startlsn:%X/%X, endlsn:%X/%X, confirmed_lsn:%X/%X",
                    txn->xid,
                    (uint32)(txn->start.wal.lsn >> 32), (uint32)(txn->start.wal.lsn),
                    (uint32)(txn->end.wal.lsn >> 32), (uint32)(txn->end.wal.lsn),
                    (uint32)(ctx->base.confirmedlsn >> 32), (uint32)(ctx->base.confirmedlsn));

    if(NULL != txn->sysdict)
    {
        /* 
         * DDL兑换
         *  redo    true        处于 redo 的逻辑中，不需要转换 ddl 语句
        */
        if(ripple_decodingcontext_isddlfilter(txn->filter, redo))
        {
            ripple_dml2ddl(ctx, txn);
        }

        /* 将 sysdict 转移到 sysdicthis 中 */
        ripple_transcache_sysdict2his(txn);

        ripple_transcache_sysdict_free(txn);
        RIPPLE_TXN_UNSET_TRANS_DDL(txn->flag);
    }

    /* 子事务处理 */
    ripple_decode_xact_buildcommittxn(ctx, pretrans, txn, redo);

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

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    ctxn = ripple_txn_copy(txn);

    /* 将事务在transdlist、by_txns中删除 */
    ripple_transcache_deletetxn((void*)ctx, txn);

    ctx->transcache->totalsize -= (ctxn->stmtsize - 4);
    ripple_cache_txn_add(ctx->parser2txns, ctxn);
}

/* 
 * 事务回滚
 *  当解析的 lsn < confirmlsn 时，此时释放资源即可
 *  当解析的 lsn > confirmlsn 时，那么需要将事务传递到 格式化 线程，用于向前推进 restartlsn
 */
void ripple_decode_xact_abort(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
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
        check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
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

        check_online_refresh_xids(ctx, subxid);
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

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    ctxn = ripple_txn_copy(txn);

    /* 将事务在transdlist、by_txns中删除 */
    ripple_transcache_deletetxn((void*)ctx, txn);

    /* 大事务过滤 */
    ripple_large_txn_end_filter(ctx, ctxn, false);

    /* 放入到事务缓存中,让 write 线程处理 */
    ctx->transcache->totalsize -= (ctxn->stmtsize - 4);
    ripple_cache_txn_add(ctx->parser2txns, ctxn);

    check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
    return;
}

void ripple_decode_xact_abort_emit(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
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
        return;
    }

    if (pretrans->m_base.m_xid >= ctx->rewind->strategy.xmax)
    {
        /* 找到了xid列表中仍活跃事务的commit */
        ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
        ctx->base.restartlsn = ctx->rewind->redolsn;
        ctx->stat = RIPPLE_DECODINGCONTEXT_RUNNING;
        if (ctx->callback.setparserlsn)
        {
            ctx->callback.setparserlsn(ctx->privdata, ctx->base.confirmedlsn, ctx->base.restartlsn, ctx->base.restartlsn);
        }
        else
        {
            elog(RLOG_WARNING, "be carefull! setparserlsn is null");
        }

        elog(RLOG_INFO, "abort emit rewind end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                        (uint32)(ctx->base.redolsn>>32), (uint32)ctx->base.redolsn,
                        (uint32)(ctx->base.restartlsn>>32), (uint32)ctx->base.restartlsn,
                        (uint32)(ctx->base.confirmedlsn>>32), (uint32)ctx->base.confirmedlsn);
        ripple_rewind_stat_setemited(ctx->rewind);
    }
    /* 检查是否在xips中 */
    else if (ctx->rewind->strategy.xips)
    {
        bool find = false;
        hash_search(ctx->rewind->strategy.xips, &pretrans->m_base.m_xid, HASH_FIND, &find);
        if (find)
        {
            /* 找到了xid列表中仍活跃事务的commit */
            ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
            ctx->base.restartlsn = ctx->rewind->redolsn;
            ctx->stat = RIPPLE_DECODINGCONTEXT_RUNNING;
            if (ctx->callback.setparserlsn)
            {
                ctx->callback.setparserlsn(ctx->privdata, ctx->base.confirmedlsn, ctx->base.restartlsn, ctx->base.restartlsn);
            }
            else
            {
                elog(RLOG_WARNING, "be carefull! setparserlsn is null");
            }

            elog(RLOG_INFO, "abort emit rewind end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                        (uint32)(ctx->base.redolsn>>32), (uint32)ctx->base.redolsn,
                        (uint32)(ctx->base.restartlsn>>32), (uint32)ctx->base.restartlsn,
                        (uint32)(ctx->base.confirmedlsn>>32), (uint32)ctx->base.confirmedlsn);
            ripple_rewind_stat_setemited(ctx->rewind);
        }
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

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    ctxn = ripple_txn_copy(txn);

    /* 将事务在transdlist、by_txns中删除 */
    ripple_transcache_deletetxn((void*)ctx, txn);

    /* 放入到事务缓存中,让 write 线程处理 */
    ctx->transcache->totalsize -= (ctxn->stmtsize - 4);
    ripple_cache_txn_add(ctx->parser2txns, ctxn);
    return;
}
