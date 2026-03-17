#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/sinval/sinval.h"
#include "utils/regex/regex.h"
#include "misc/misc_stat.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "catalog/control.h"
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
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_bigtxnend.h"
#include "cache/fpwcache.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_xact.h"
#include "works/parserwork/wal/decode_ddl.h"
#include "works/parserwork/wal/decode_relmap.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "strategy/filter_dataset.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "works/parserwork/wal/parserwork_wal.h"
#include "stmts/txnstmt_onlinerefresh_dataset.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/bigtxn.h"

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


static void decode_xact_appendsubtxn(List** ptxnstmts, List** psysdicthis, txn* subtxn_ptr)
{
    ListCell* lc = NULL;
    txnstmt* stmt = NULL;

    foreach(lc, subtxn_ptr->stmts)
    {
        stmt = (txnstmt*)lfirst(lc);
        *ptxnstmts = lappend(*ptxnstmts, stmt);
    }
    list_free(subtxn_ptr->stmts);
    subtxn_ptr->stmts = NULL;

    foreach(lc, subtxn_ptr->sysdictHis)
    {
        *psysdicthis = lappend(*psysdicthis, lfirst(lc));
    }
    list_free(subtxn_ptr->sysdictHis);
    subtxn_ptr->sysdictHis = NULL;
}

static void check_online_refresh_node_need_clean(onlinerefresh *olnode, dlistnode *dlnode, decodingcontext* ctx)
{
    if (olnode->state == ONLINEREFRESH_STATE_FULLSNAPSHOT && onlinerefresh_xids_isnull(olnode))
    {
        /* xids为空, 所有事务均已完成 */
        txn *end_txn_ptr = NULL;
        txn *dataset_txn_ptr = NULL;

        /* 构建onlinerefresh end事务 */
        end_txn_ptr = parserwork_build_onlinerefresh_end_txn(olnode->no->data, ctx->parselsn);

        /* 事务添加到缓存 */
        cache_txn_add(ctx->parser2txns, end_txn_ptr);

        if (olnode->newtables)
        {
            /* 构建onlinerefresh dataset事务 */
            dataset_txn_ptr = (txn *)parserwork_build_onlinerefresh_dataset_txn(olnode->newtables);

            /* 事务添加到缓存 */
            cache_txn_add(ctx->parser2txns, dataset_txn_ptr);
        }

        /* 清理 */
        ctx->onlinerefresh = onlinerefresh_refreshdlist_delete(ctx->onlinerefresh, dlnode);

        /* 判断链表是否为空*/
        if (dlist_isnull(ctx->onlinerefresh))
        {
            dlist_free(ctx->onlinerefresh, NULL);
            ctx->onlinerefresh = NULL;
            return;
        }
    }
}

static void check_online_refresh_xids(decodingcontext* ctx, TransactionId xid)
{
    dlistnode *dlnode = NULL;
    dlistnode *dlnode_xid = NULL;
    onlinerefresh *olnode = NULL;

    /* 不存在olrefresh节点时直接返回 */
    if (!ctx->onlinerefresh)
    {
        return;
    }
    dlnode = ctx->onlinerefresh->head;

    while (dlnode)
    {
        dlistnode *dlnode_next = NULL;
        olnode = (onlinerefresh *)dlnode->value;

        /* 先推进 */
        dlnode_next = dlnode->next;

        if (!olnode->increment)
        {
            /* 不需要增量, 判断commit/abort xid是否大于refresh xid */
            if (xid > olnode->txid)
                {
                    txn *end_txn_ptr = NULL;
                    txn *dataset_txn_ptr = NULL;

                    /* 构建onlinerefresh end事务 */
                    end_txn_ptr = parserwork_build_onlinerefresh_end_txn(olnode->no->data, ctx->parselsn);

                    /* 事务添加到缓存 */
                    cache_txn_add(ctx->parser2txns, end_txn_ptr);

                    if (olnode->newtables)
                    {
                        /* 构建onlinerefresh dataset事务 */
                        dataset_txn_ptr = (txn *)parserwork_build_onlinerefresh_dataset_txn(olnode->newtables);

                        /* 事务添加到缓存 */
                        cache_txn_add(ctx->parser2txns, dataset_txn_ptr);
                    }

                    /* 清理 */
                    ctx->onlinerefresh = onlinerefresh_refreshdlist_delete(ctx->onlinerefresh, dlnode);

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
                onlinerefresh_xids_delete(olnode, olnode->xids, dlnode_xid);
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
static void decode_xact_buildcommittxn(decodingcontext* ctx,
                                                xk_pg_parser_translog_pre_trans* pretrans,
                                                txn* in_txn_ptr,
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

    txnstmts = in_txn_ptr->stmts;
    in_txn_ptr->stmts = NULL;

    sysdicthis = in_txn_ptr->sysdictHis;
    in_txn_ptr->sysdictHis = NULL;

    /* 遍历子事务放到合适的位置 */
    for(index = 0; index < parsedcommit->nsubxacts; index++)
    {
        bool brestart = false;
        bool bconfirm = false;
        txn* subtxn_ptr = NULL;
        TransactionId subxid = parsedcommit->subxacts[index];

        /* 获取事务号，并获取存储的事务信息 */
        subtxn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, subxid);

        /* DDL 兑换 */
        if(NULL != subtxn_ptr->sysdict)
        {
            /* DDL兑换 */
            if(decodingcontext_isddlfilter(subtxn_ptr->filter, redo))
            {
                dml2ddl(ctx, subtxn_ptr);
            }

            /* 将 sysdict 转移到 sysdicthis 中 */
            transcache_sysdict2his(subtxn_ptr);

            /* 释放 */
            transcache_sysdict_free(subtxn_ptr);
            TXN_UNSET_TRANS_DDL(subtxn_ptr->flag);
        }

        /* 将子事务的数据 append 到 主数据库中 */
        if(NULL != subtxn_ptr->stmts)
        {
            in_txn_ptr->stmtsize += subtxn_ptr->stmtsize;
        }
        decode_xact_appendsubtxn(&txnstmts, &sysdicthis, subtxn_ptr);

        /* 将事务在事务链表中删除 */
        transcache_dlist_remove((void*)ctx, subtxn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn 内存释放 */
        txn_free(subtxn_ptr);

        /* 将子事务在hash中删除 */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_ptr->xid);
        check_online_refresh_xids(ctx, subxid);
    }

    in_txn_ptr->sysdictHis = sysdicthis;
    in_txn_ptr->stmts = txnstmts;
}

static bool large_txn_end_filter(decodingcontext* ctx, txn *commit_txn_ptr, bool commit)
{
    txnstmt* txnstmt_ptr = NULL;
    bigtxn_end_stmt *endstmt = NULL;
    txn *copytxn_ptr = NULL;
    txn *endtxn_ptr = NULL;

    /* 判断是否需要过滤大事务 */
    if (!ctx->filterbigtrans)
    {
        return true;
    }

    /* 检查是否存在大事务 */
    if (!TXN_ISBIGTXN(commit_txn_ptr->flag))
    {
        /* 只处理正常txn的commit */
        if (commit)
        {
            /* 不存在大事务, 构建一个只有系统表的事物给大事务序列化 */
            copytxn_ptr = txn_copy(commit_txn_ptr);

            /* 置空拷贝后txn部分指针 */
            copytxn_ptr->toast_hash = NULL;
            copytxn_ptr->sysdictHis = decode_heap_sysdicthis_copy(commit_txn_ptr->sysdictHis);
            copytxn_ptr->stmts = NULL;
            copytxn_ptr->hsyncdataset = NULL;
            copytxn_ptr->oidmap = NULL;
            copytxn_ptr->prev = NULL;
            copytxn_ptr->next = NULL;
            copytxn_ptr->cachenext = NULL;

            /* 添加到大事务缓存 */
            
            cache_txn_add(ctx->parser2bigtxns, copytxn_ptr);
        }
        return true;
    }

    /* 设置为大事务结束 */
    if (commit)
    {
        commit_txn_ptr->type = TXN_TYPE_BIGTXN_END_COMMIT;
    }
    else
    {
        commit_txn_ptr->type = TXN_TYPE_BIGTXN_END_ABORT;
    }

    /* 
     * 构建大事务结束txnstmt
     * 添加到缓存中
     */
    endtxn_ptr = txn_initbigtxn(commit_txn_ptr->xid);
    if(NULL == endtxn_ptr)
    {
        elog(RLOG_WARNING, "init bigtxn error, out of memory");
        return false;
    }
    txnstmt_ptr = txnstmt_init();
    if(NULL == txnstmt_ptr)
    {
        elog(RLOG_WARNING, "init txn stmt error, out of memory");
        return false;
    }
    endtxn_ptr->stmts = lappend(endtxn_ptr->stmts, txnstmt_ptr);
    endtxn_ptr->end.wal.lsn = ctx->parselsn;
    txnstmt_ptr->type = TXNSTMT_TYPE_BIGTXN_END;
    txnstmt_ptr->extra0.wal.lsn = ctx->parselsn;
    endstmt = txnstmt_bigtxnend_init();
    if(NULL == endstmt)
    {
        elog(RLOG_WARNING, "init bigtxn endstmt error, out of memory");
        return false;
    }
    txnstmt_ptr->stmt = (void*)endstmt;
    endstmt->xid = commit_txn_ptr->xid;
    endstmt->commit = commit;
    endtxn_ptr->endtimestamp = commit_txn_ptr->endtimestamp;

    txn_addcommit(endtxn_ptr);
    cache_txn_add(ctx->parser2txns, endtxn_ptr);

    /* 进行txn拷贝 */
    copytxn_ptr = txn_copy(commit_txn_ptr);
    if (commit_txn_ptr->sysdict)
    {
        elog(RLOG_WARNING, "big txn copy, txn in ddl");
        return false;
    }

    /* 重置原txn的部分指针 */
    commit_txn_ptr->sysdictHis = NULL;
    commit_txn_ptr->stmts = NULL;
    txnstmt_ptr = txnstmt_init();
    if(NULL == txnstmt_ptr)
    {
        elog(RLOG_WARNING, "init txn stmt error, out of memory");
        return false;
    }
    txnstmt_ptr->type = TXNSTMT_TYPE_SYSDICTHIS;
    commit_txn_ptr->stmts = lappend(commit_txn_ptr->stmts, txnstmt_ptr);
    /* 初始化stmt大小 */
    commit_txn_ptr->stmtsize = 4;

    /* 置空拷贝后txn部分指针 */
    copytxn_ptr->toast_hash = NULL;
    copytxn_ptr->hsyncdataset = NULL;
    copytxn_ptr->oidmap = NULL;
    copytxn_ptr->prev = NULL;
    copytxn_ptr->next = NULL;
    copytxn_ptr->cachenext = NULL;

    /* 处理系统表, 进行拷贝 */
    commit_txn_ptr->sysdictHis = decode_heap_sysdicthis_copy(copytxn_ptr->sysdictHis);

    /* 添加到大事务缓存 */
    txn_addcommit(copytxn_ptr);
    cache_txn_add(ctx->parser2bigtxns, copytxn_ptr);

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
void decode_xact_commit(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    bool redo = false;
    ListCell* lc = NULL;
    txnstmt* stmt = NULL;
    txn* txn_ptr = NULL;
    txn* copied_txn = NULL;
    xk_pg_parser_translog_pre_trans* pretrans = NULL;
    List* metalist = NULL;

    pretrans = (xk_pg_parser_translog_pre_trans*)pbase;

    /*
     * 根据事务号获取事务链表
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* 如果为空，那么说明不需要处理 */
    if(NULL == txn_ptr)
    {
        check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
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
        /* 
         * DDL兑换
         *  redo    true        处于 redo 的逻辑中，不需要转换 ddl 语句
        */
        if(decodingcontext_isddlfilter(txn_ptr->filter, redo))
        {
            dml2ddl(ctx, txn_ptr);
        }

        /* 将 sysdict 转移到 sysdicthis 中 */
        transcache_sysdict2his(txn_ptr);

        transcache_sysdict_free(txn_ptr);
        TXN_UNSET_TRANS_DDL(txn_ptr->flag);
    }

    /* 子事务处理 */
    decode_xact_buildcommittxn(ctx, pretrans, txn_ptr, redo);

    /* sysdicthis 应用 */
    cache_sysdicts_txnsysdicthis2cache(ctx->trans_cache->sysdicts, txn_ptr->sysdictHis);

    /* 更新同步数据集 */
    filter_dataset_updatedatasets(ctx->trans_cache->addtablepattern,
                                        ctx->trans_cache->sysdicts->by_namespace,
                                        txn_ptr->sysdictHis,
                                        ctx->trans_cache->hsyncdataset);

    /* 符合过滤条件,那么将语句清理 */
    /* 
     * 后续流程中系统表的变更是通过-----stmt_type_meta 区分
     * 1、将 redo--->confirmlsn 之间的系统表变化在后续流程中应用
     * 2、在双向过滤中, originid 不能为空, 但是后面的流程中又需要系统表的变更
     */
    if(decodingcontext_isstmtsfilter(txn_ptr->filter, redo))
    {
        metalist = NULL;
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
        stmt->extra0.wal.lsn = ctx->decode_record->end.wal.lsn;
    }

    /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn_ptr->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    copied_txn = txn_copy(txn_ptr);

    /* 将事务在transdlist、by_txns中删除 */
    ctx->trans_cache->totalsize -= (txn_ptr->stmtsize - 4);
    transcache_deletetxn((void*)ctx, txn_ptr);

    /* 大事务过滤 */
    large_txn_end_filter(ctx, copied_txn, true);

    txn_addcommit(copied_txn);
    cache_txn_add(ctx->parser2txns, copied_txn);
    check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
}

void decode_xact_commit_emit(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    bool redo = true;
    ListCell* lc = NULL;
    txnstmt* stmt = NULL;
    txn* txn_ptr = NULL;
    txn* copied_txn = NULL;
    xk_pg_parser_translog_pre_trans* pretrans = NULL;
    List* metalist = NULL;
    bool find = false;

    pretrans = (xk_pg_parser_translog_pre_trans*)pbase;

    /*
     * 根据事务号获取事务链表
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* 如果为空，那么说明不需要处理 */
    if(NULL == txn_ptr)
    {
        return;
    }

    if (pretrans->m_base.m_xid >= ctx->rewind_ptr->strategy.xmax)
    {
        /* 找到了大于xmax的事务的commit */
        ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
        ctx->base.restartlsn = ctx->rewind_ptr->redolsn;
        ctx->stat = DECODINGCONTEXT_RUNNING;
        if (ctx->callback.setparserlsn)
        {
            ctx->callback.setparserlsn(ctx->privdata, ctx->base.confirmedlsn, ctx->base.restartlsn, ctx->base.restartlsn);
        }
        else
        {
            elog(RLOG_WARNING, "be carefull! setparserlsn is null");
        }


        elog(RLOG_INFO, "commit emit rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                        (uint32)(ctx->base.redolsn>>32), (uint32)ctx->base.redolsn,
                        (uint32)(ctx->base.restartlsn>>32), (uint32)ctx->base.restartlsn,
                        (uint32)(ctx->base.confirmedlsn>>32), (uint32)ctx->base.confirmedlsn);
        rewind_stat_setemited(ctx->rewind_ptr);
        redo = false;
    }
    else if (ctx->rewind_ptr->strategy.xips)
    {
        find = false;
        hash_search(ctx->rewind_ptr->strategy.xips, &pretrans->m_base.m_xid, HASH_FIND, &find);
        if (find)
        {
            /* 找到了xid列表中仍活跃事务的commit */
            ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
            ctx->base.restartlsn = ctx->rewind_ptr->redolsn;
            ctx->stat = DECODINGCONTEXT_RUNNING;
            if (ctx->callback.setparserlsn)
            {
                ctx->callback.setparserlsn(ctx->privdata, ctx->base.confirmedlsn, ctx->base.restartlsn, ctx->base.restartlsn);
            }
            else
            {
                elog(RLOG_WARNING, "be carefull! setparserlsn is null");
            }

            elog(RLOG_INFO, "commit emit rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                        (uint32)(ctx->base.redolsn>>32), (uint32)ctx->base.redolsn,
                        (uint32)(ctx->base.restartlsn>>32), (uint32)ctx->base.restartlsn,
                        (uint32)(ctx->base.confirmedlsn>>32), (uint32)ctx->base.confirmedlsn);
            rewind_stat_setemited(ctx->rewind_ptr);
            redo = false;
        }
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    elog(RLOG_DEBUG, "xid:%lu, startlsn:%X/%X, endlsn:%X/%X, confirmed_lsn:%X/%X",
                    txn_ptr->xid,
                    (uint32)(txn_ptr->start.wal.lsn >> 32), (uint32)(txn_ptr->start.wal.lsn),
                    (uint32)(txn_ptr->end.wal.lsn >> 32), (uint32)(txn_ptr->end.wal.lsn),
                    (uint32)(ctx->base.confirmedlsn >> 32), (uint32)(ctx->base.confirmedlsn));

    if(NULL != txn_ptr->sysdict)
    {
        /* 
         * DDL兑换
         *  redo    true        处于 redo 的逻辑中，不需要转换 ddl 语句
        */
        if(decodingcontext_isddlfilter(txn_ptr->filter, redo))
        {
            dml2ddl(ctx, txn_ptr);
        }

        /* 将 sysdict 转移到 sysdicthis 中 */
        transcache_sysdict2his(txn_ptr);

        transcache_sysdict_free(txn_ptr);
        TXN_UNSET_TRANS_DDL(txn_ptr->flag);
    }

    /* 子事务处理 */
    decode_xact_buildcommittxn(ctx, pretrans, txn_ptr, redo);

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
        metalist = NULL;
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

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn_ptr->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    copied_txn = txn_copy(txn_ptr);

    /* 将事务在transdlist、by_txns中删除 */
    transcache_deletetxn((void*)ctx, txn_ptr);

    ctx->trans_cache->totalsize -= (txn_ptr->stmtsize - 4);
    cache_txn_add(ctx->parser2txns, copied_txn);
}

/* 
 * 事务回滚
 *  当解析的 lsn < confirmlsn 时，此时释放资源即可
 *  当解析的 lsn > confirmlsn 时，那么需要将事务传递到 格式化 线程，用于向前推进 restartlsn
 */
void decode_xact_abort(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    bool brestart = false;
    bool bconfirm = false;
    int index = 0;
    ListCell* lc = NULL;
    txn* txn_ptr = NULL;
    txn* subtxn_ptr = NULL;
    txn* copied_txn = NULL;
    txnstmt* txnstmt_ptr = NULL;
    xl_xact_parsed_abort* parsedabort = NULL;
    xk_pg_parser_translog_pre_trans* pretrans = NULL;
    TransactionId subxid;

    pretrans = (xk_pg_parser_translog_pre_trans*)pbase;

    /*
     * 根据事务号获取事务链表
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* 如果为空，那么说明不需要处理 */
    if(NULL == txn_ptr)
    {
        check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    parsedabort = (xl_xact_parsed_abort*)pretrans->m_transdata;
    for(index = 0; index < parsedabort->nsubxacts; index++)
    {
        subxid = parsedabort->subxacts[index];

        /* 获取事务号，并获取存储的事务信息 */
        subtxn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, subxid);
        if(NULL == subtxn_ptr)
        {
            continue;
        }

        /* 系统表释放 */
        transcache_sysdict_free(subtxn_ptr);

        /* 将事务在事务链表中删除 */
        transcache_dlist_remove((void*)ctx, subtxn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn 内存释放 */
        txn_free(subtxn_ptr);

        /* 将子事务在hash中删除 */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_ptr->xid);

        check_online_refresh_xids(ctx, subxid);
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
        txnstmt_ptr = (txnstmt*)lfirst(lc);
        txnstmt_free(txnstmt_ptr);
    }
    list_free(txn_ptr->stmts);
    txn_ptr->stmts = NULL;

    /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn_ptr->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    copied_txn = txn_copy(txn_ptr);

    /* 将事务在transdlist、by_txns中删除 */
    transcache_deletetxn((void*)ctx, txn_ptr);

    /* 大事务过滤 */
    large_txn_end_filter(ctx, copied_txn, false);

    /* 放入到事务缓存中,让 write 线程处理 */
    ctx->trans_cache->totalsize -= (txn_ptr->stmtsize - 4);
    cache_txn_add(ctx->parser2txns, copied_txn);

    check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
    return;
}

void decode_xact_abort_emit(decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    bool brestart = false;
    bool bconfirm = false;
    int index = 0;
    ListCell* lc = NULL;
    txn* txn_ptr = NULL;
    txn* subtxn_ptr = NULL;
    txn* copied_txn = NULL;
    txnstmt* txnstmt_ptr = NULL;
    xl_xact_parsed_abort* parsedabort = NULL;
    xk_pg_parser_translog_pre_trans* pretrans = NULL;
    TransactionId subxid;
    bool find = false;

    pretrans = (xk_pg_parser_translog_pre_trans*)pbase;

    /*
     * 根据事务号获取事务链表
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* 如果为空，那么说明不需要处理 */
    if(NULL == txn_ptr)
    {
        return;
    }

    if (pretrans->m_base.m_xid >= ctx->rewind_ptr->strategy.xmax)
    {
        /* 找到了xid列表中仍活跃事务的commit */
        ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
        ctx->base.restartlsn = ctx->rewind_ptr->redolsn;
        ctx->stat = DECODINGCONTEXT_RUNNING;
        if (ctx->callback.setparserlsn)
        {
            ctx->callback.setparserlsn(ctx->privdata, ctx->base.confirmedlsn, ctx->base.restartlsn, ctx->base.restartlsn);
        }
        else
        {
            elog(RLOG_WARNING, "be carefull! setparserlsn is null");
        }

        elog(RLOG_INFO, "abort emit rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                        (uint32)(ctx->base.redolsn>>32), (uint32)ctx->base.redolsn,
                        (uint32)(ctx->base.restartlsn>>32), (uint32)ctx->base.restartlsn,
                        (uint32)(ctx->base.confirmedlsn>>32), (uint32)ctx->base.confirmedlsn);
        rewind_stat_setemited(ctx->rewind_ptr);
    }
    /* 检查是否在xips中 */
    else if (ctx->rewind_ptr->strategy.xips)
    {
        find = false;
        hash_search(ctx->rewind_ptr->strategy.xips, &pretrans->m_base.m_xid, HASH_FIND, &find);
        if (find)
        {
            /* 找到了xid列表中仍活跃事务的commit */
            ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
            ctx->base.restartlsn = ctx->rewind_ptr->redolsn;
            ctx->stat = DECODINGCONTEXT_RUNNING;
            if (ctx->callback.setparserlsn)
            {
                ctx->callback.setparserlsn(ctx->privdata, ctx->base.confirmedlsn, ctx->base.restartlsn, ctx->base.restartlsn);
            }
            else
            {
                elog(RLOG_WARNING, "be carefull! setparserlsn is null");
            }

            elog(RLOG_INFO, "abort emit rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                        (uint32)(ctx->base.redolsn>>32), (uint32)ctx->base.redolsn,
                        (uint32)(ctx->base.restartlsn>>32), (uint32)ctx->base.restartlsn,
                        (uint32)(ctx->base.confirmedlsn>>32), (uint32)ctx->base.confirmedlsn);
            rewind_stat_setemited(ctx->rewind_ptr);
        }
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    parsedabort = (xl_xact_parsed_abort*)pretrans->m_transdata;
    for(index = 0; index < parsedabort->nsubxacts; index++)
    {
        subxid = parsedabort->subxacts[index];

        /* 获取事务号，并获取存储的事务信息 */
        subtxn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, subxid);
        if(NULL == subtxn_ptr)
        {
            continue;
        }

        /* 系统表释放 */
        transcache_sysdict_free(subtxn_ptr);

        /* 将事务在事务链表中删除 */
        transcache_dlist_remove((void*)ctx, subtxn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn 内存释放 */
        txn_free(subtxn_ptr);

        /* 将子事务在hash中删除 */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_ptr->xid);
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
        txnstmt_ptr = (txnstmt*)lfirst(lc);
        txnstmt_free(txnstmt_ptr);
    }
    list_free(txn_ptr->stmts);
    txn_ptr->stmts = NULL;

    /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn_ptr->endtimestamp = pretrans->m_time;

    /* 复制一个事务加到缓存中 */
    copied_txn = txn_copy(txn_ptr);

    /* 将事务在transdlist、by_txns中删除 */
    transcache_deletetxn((void*)ctx, txn_ptr);

    /* 放入到事务缓存中,让 write 线程处理 */
    ctx->trans_cache->totalsize -= (txn_ptr->stmtsize - 4);
    cache_txn_add(ctx->parser2txns, copied_txn);
    return;
}
