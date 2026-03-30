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
#include "common/pg_parser_errnodef.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
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
    TimestampTz                xact_time;
    uint32                     xinfo;

    Oid                        dbId; /* MyDatabaseId */
    Oid                        tsId; /* MyDatabaseTableSpace */

    int                        nsubxacts;
    TransactionId*             subxacts;

    int                        nrels;
    RelFileNode*               xnodes;

    int                        nmsgs;
    SharedInvalidationMessage* msgs;

    TransactionId              twophase_xid;          /* only for 2PC */
    char                       twophase_gid[GIDSIZE]; /* only for 2PC */
    int                        nabortrels;            /* only for 2PC */
    RelFileNode*               abortnodes;            /* only for 2PC */

    XLogRecPtr                 origin_lsn;
    TimestampTz                origin_timestamp;
} xl_xact_parsed_commit;

typedef xl_xact_parsed_commit xl_xact_parsed_prepare;

typedef struct xl_xact_parsed_abort
{
    TimestampTz    xact_time;
    uint32         xinfo;

    Oid            dbId; /* MyDatabaseId */
    Oid            tsId; /* MyDatabaseTableSpace */

    int            nsubxacts;
    TransactionId* subxacts;

    int            nrels;
    RelFileNode*   xnodes;

    TransactionId  twophase_xid;          /* only for 2PC */
    char           twophase_gid[GIDSIZE]; /* only for 2PC */

    XLogRecPtr     origin_lsn;
    TimestampTz    origin_timestamp;
} xl_xact_parsed_abort;

static void decode_xact_appendsubtxn(List** ptxnstmts, List** psysdicthis, txn* subtxn_ptr)
{
    ListCell* lc = NULL;
    txnstmt*  stmt = NULL;

    foreach (lc, subtxn_ptr->stmts)
    {
        stmt = (txnstmt*)lfirst(lc);
        *ptxnstmts = lappend(*ptxnstmts, stmt);
    }
    list_free(subtxn_ptr->stmts);
    subtxn_ptr->stmts = NULL;

    foreach (lc, subtxn_ptr->sysdictHis)
    {
        *psysdicthis = lappend(*psysdicthis, lfirst(lc));
    }
    list_free(subtxn_ptr->sysdictHis);
    subtxn_ptr->sysdictHis = NULL;
}

static void check_online_refresh_node_need_clean(onlinerefresh* olnode, dlistnode* dlnode, decodingcontext* ctx)
{
    if (olnode->state == ONLINEREFRESH_STATE_FULLSNAPSHOT && onlinerefresh_xids_isnull(olnode))
    {
        /* xids is empty, all transactions have completed */
        txn* end_txn_ptr = NULL;
        txn* dataset_txn_ptr = NULL;

        /* Build onlinerefresh end transaction */
        end_txn_ptr = parserwork_build_onlinerefresh_end_txn(olnode->no->data, ctx->parselsn);

        /* Add transaction to cache */
        cache_txn_add(ctx->parser2txns, end_txn_ptr);

        if (olnode->newtables)
        {
            /* Build onlinerefresh dataset transaction */
            dataset_txn_ptr = (txn*)parserwork_build_onlinerefresh_dataset_txn(olnode->newtables);

            /* Add transaction to cache */
            cache_txn_add(ctx->parser2txns, dataset_txn_ptr);
        }

        /* Cleanup */
        ctx->onlinerefresh = onlinerefresh_refreshdlist_delete(ctx->onlinerefresh, dlnode);

        /* Check if linked list is empty */
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
    dlistnode*     dlnode = NULL;
    dlistnode*     dlnode_xid = NULL;
    onlinerefresh* olnode = NULL;

    /* Return directly if olrefresh node does not exist */
    if (!ctx->onlinerefresh)
    {
        return;
    }
    dlnode = ctx->onlinerefresh->head;

    while (dlnode)
    {
        dlistnode* dlnode_next = NULL;
        olnode = (onlinerefresh*)dlnode->value;

        /* Advance first */
        dlnode_next = dlnode->next;

        if (!olnode->increment)
        {
            /* No increment needed, check if commit/abort xid is greater than refresh xid */
            if (xid > olnode->txid)
            {
                txn* end_txn_ptr = NULL;
                txn* dataset_txn_ptr = NULL;

                /* Build onlinerefresh end transaction */
                end_txn_ptr = parserwork_build_onlinerefresh_end_txn(olnode->no->data, ctx->parselsn);

                /* Add transaction to cache */
                cache_txn_add(ctx->parser2txns, end_txn_ptr);

                if (olnode->newtables)
                {
                    /* Build onlinerefresh dataset transaction */
                    dataset_txn_ptr = (txn*)parserwork_build_onlinerefresh_dataset_txn(olnode->newtables);

                    /* Add transaction to cache */
                    cache_txn_add(ctx->parser2txns, dataset_txn_ptr);
                }

                /* Cleanup */
                ctx->onlinerefresh = onlinerefresh_refreshdlist_delete(ctx->onlinerefresh, dlnode);

                /* Check if linked list is empty */
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
            dlistnode*         dlnode_xid_next = NULL;
            FullTransactionId* xid_p = (FullTransactionId*)dlnode_xid->value;

            dlnode_xid_next = dlnode_xid->next;

            /* Check if in linked list */
            if (*xid_p == (FullTransactionId)xid)
            {
                /* In linked list, delete */
                onlinerefresh_xids_delete(olnode, olnode->xids, dlnode_xid);
            }
            dlnode_xid = dlnode_xid_next;
        }

        check_online_refresh_node_need_clean(olnode, dlnode, ctx);
        dlnode = dlnode_next;
    }
}

/*
 * At Commit, append content from subtransactions to the main transaction
 */
static void decode_xact_buildcommittxn(decodingcontext*              ctx,
                                       pg_parser_translog_pre_trans* pretrans,
                                       txn*                          in_txn_ptr,
                                       bool                          redo)
{
    int                    index = 0;
    List*                  txnstmts = NULL;
    List*                  sysdicthis = NULL;
    xl_xact_parsed_commit* parsedcommit = NULL;

    parsedcommit = (xl_xact_parsed_commit*)pretrans->m_transdata;
    if (0 == parsedcommit->nsubxacts)
    {
        return;
    }

    txnstmts = in_txn_ptr->stmts;
    in_txn_ptr->stmts = NULL;

    sysdicthis = in_txn_ptr->sysdictHis;
    in_txn_ptr->sysdictHis = NULL;

    /* Iterate subtransactions to appropriate positions */
    for (index = 0; index < parsedcommit->nsubxacts; index++)
    {
        bool          brestart = false;
        bool          bconfirm = false;
        txn*          subtxn_ptr = NULL;
        TransactionId subxid = parsedcommit->subxacts[index];

        /* Get transaction number and stored transaction information */
        subtxn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, subxid);

        /* DDL conversion */
        if (NULL != subtxn_ptr->sysdict)
        {
            /* DDL conversion */
            if (decodingcontext_isddlfilter(subtxn_ptr->filter, redo))
            {
                dml2ddl(ctx, subtxn_ptr);
            }

            /* Transfer sysdict to sysdicthis */
            transcache_sysdict2his(subtxn_ptr);

            /* Free */
            transcache_sysdict_free(subtxn_ptr);
            TXN_UNSET_TRANS_DDL(subtxn_ptr->flag);
        }

        /* Append subtransaction data to main database */
        if (NULL != subtxn_ptr->stmts)
        {
            in_txn_ptr->stmtsize += subtxn_ptr->stmtsize;
        }
        decode_xact_appendsubtxn(&txnstmts, &sysdicthis, subtxn_ptr);

        /* Delete transaction from transaction linked list */
        transcache_dlist_remove((void*)ctx, subtxn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* Free subtxn memory */
        txn_free(subtxn_ptr);

        /* Delete subtransaction from hash */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_ptr->xid);
        check_online_refresh_xids(ctx, subxid);
    }

    in_txn_ptr->sysdictHis = sysdicthis;
    in_txn_ptr->stmts = txnstmts;
}

static bool large_txn_end_filter(decodingcontext* ctx, txn* commit_txn_ptr, bool commit)
{
    txnstmt*         txnstmt_ptr = NULL;
    bigtxn_end_stmt* endstmt = NULL;
    txn*             copytxn_ptr = NULL;
    txn*             endtxn_ptr = NULL;

    /* Check if large transaction needs to be filtered */
    if (!ctx->filterbigtrans)
    {
        return true;
    }

    /* Check if large transaction exists */
    if (!TXN_ISBIGTXN(commit_txn_ptr->flag))
    {
        /* Only handle normal txn commit */
        if (commit)
        {
            /* No large transaction exists, build a transaction with only system tables for large
             * transaction serialization */
            copytxn_ptr = txn_copy(commit_txn_ptr);

            /* Nullify some txn pointers after copy */
            copytxn_ptr->toast_hash = NULL;
            copytxn_ptr->sysdictHis = decode_heap_sysdicthis_copy(commit_txn_ptr->sysdictHis);
            copytxn_ptr->stmts = NULL;
            copytxn_ptr->hsyncdataset = NULL;
            copytxn_ptr->oidmap = NULL;
            copytxn_ptr->prev = NULL;
            copytxn_ptr->next = NULL;
            copytxn_ptr->cachenext = NULL;

            /* Add to large transaction cache */

            cache_txn_add(ctx->parser2bigtxns, copytxn_ptr);
        }
        return true;
    }

    /* Set as large transaction end */
    if (commit)
    {
        commit_txn_ptr->type = TXN_TYPE_BIGTXN_END_COMMIT;
    }
    else
    {
        commit_txn_ptr->type = TXN_TYPE_BIGTXN_END_ABORT;
    }

    /*
     * Build large transaction end txnstmt
     * Add to cache
     */
    endtxn_ptr = txn_initbigtxn(commit_txn_ptr->xid);
    if (NULL == endtxn_ptr)
    {
        elog(RLOG_WARNING, "init bigtxn error, out of memory");
        return false;
    }
    txnstmt_ptr = txnstmt_init();
    if (NULL == txnstmt_ptr)
    {
        elog(RLOG_WARNING, "init txn stmt error, out of memory");
        return false;
    }
    endtxn_ptr->stmts = lappend(endtxn_ptr->stmts, txnstmt_ptr);
    endtxn_ptr->end.wal.lsn = ctx->parselsn;
    txnstmt_ptr->type = TXNSTMT_TYPE_BIGTXN_END;
    txnstmt_ptr->extra0.wal.lsn = ctx->parselsn;
    endstmt = txnstmt_bigtxnend_init();
    if (NULL == endstmt)
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

    /* Copy txn */
    copytxn_ptr = txn_copy(commit_txn_ptr);
    if (commit_txn_ptr->sysdict)
    {
        elog(RLOG_WARNING, "big txn copy, txn in ddl");
        return false;
    }

    /* Reset some pointers of original txn */
    commit_txn_ptr->sysdictHis = NULL;
    commit_txn_ptr->stmts = NULL;
    txnstmt_ptr = txnstmt_init();
    if (NULL == txnstmt_ptr)
    {
        elog(RLOG_WARNING, "init txn stmt error, out of memory");
        return false;
    }
    txnstmt_ptr->type = TXNSTMT_TYPE_SYSDICTHIS;
    commit_txn_ptr->stmts = lappend(commit_txn_ptr->stmts, txnstmt_ptr);
    /* Initialize stmt size */
    commit_txn_ptr->stmtsize = 4;

    /* Nullify some txn pointers after copy */
    copytxn_ptr->toast_hash = NULL;
    copytxn_ptr->hsyncdataset = NULL;
    copytxn_ptr->oidmap = NULL;
    copytxn_ptr->prev = NULL;
    copytxn_ptr->next = NULL;
    copytxn_ptr->cachenext = NULL;

    /* Process system tables, perform copy */
    commit_txn_ptr->sysdictHis = decode_heap_sysdicthis_copy(copytxn_ptr->sysdictHis);

    /* Add to large transaction cache */
    txn_addcommit(copytxn_ptr);
    cache_txn_add(ctx->parser2bigtxns, copytxn_ptr);

    return true;
}

/*
 * commit processing, logic as follows
 *  1. At commit, first compare submitted lsn with confirmlsn, when less than confirmlsn
 *     it means this transaction does not need processing, then cleanup data 1.1 Apply system table
 * data
 *
 *  2. Merge subtransactions contained in current transaction to main transaction
 *
 *
 * Transaction logic in PG is as follows:
 *  Main transaction xid
 *      savepoint   subtransaction xid1
 *      savepoint   subtransaction xid2, parent transaction logically is xid1, but in PG
 *     there is no nested transaction logic, so parent transaction is xid
 *
 */
void decode_xact_commit(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    bool                          redo = false;
    ListCell*                     lc = NULL;
    txnstmt*                      stmt = NULL;
    txn*                          txn_ptr = NULL;
    txn*                          copied_txn = NULL;
    pg_parser_translog_pre_trans* pretrans = NULL;
    List*                         metalist = NULL;

    pretrans = (pg_parser_translog_pre_trans*)pbase;

    /*
     * Get transaction linked list by transaction number
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* If empty, then it means no processing needed */
    if (NULL == txn_ptr)
    {
        check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    elog(RLOG_DEBUG,
         "xid:%lu, startlsn:%X/%X, endlsn:%X/%X, confirmed_lsn:%X/%X",
         txn_ptr->xid,
         (uint32)(txn_ptr->start.wal.lsn >> 32),
         (uint32)(txn_ptr->start.wal.lsn),
         (uint32)(txn_ptr->end.wal.lsn >> 32),
         (uint32)(txn_ptr->end.wal.lsn),
         (uint32)(ctx->base.confirmedlsn >> 32),
         (uint32)(ctx->base.confirmedlsn));

    /* Check if in redo */
    if (txn_ptr->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        redo = true;
    }

    if (NULL != txn_ptr->sysdict)
    {
        /*
         * DDL conversion
         *  redo    true        In redo logic, no need to convert ddl statements
         */
        if (decodingcontext_isddlfilter(txn_ptr->filter, redo))
        {
            dml2ddl(ctx, txn_ptr);
        }

        /* Transfer sysdict to sysdicthis */
        transcache_sysdict2his(txn_ptr);

        transcache_sysdict_free(txn_ptr);
        TXN_UNSET_TRANS_DDL(txn_ptr->flag);
    }

    /* Subtransaction processing */
    decode_xact_buildcommittxn(ctx, pretrans, txn_ptr, redo);

    /* Apply sysdicthis */
    cache_sysdicts_txnsysdicthis2cache(ctx->trans_cache->sysdicts, txn_ptr->sysdictHis);

    /* Update sync dataset */
    filter_dataset_updatedatasets(ctx->trans_cache->addtablepattern,
                                  ctx->trans_cache->sysdicts->by_namespace,
                                  txn_ptr->sysdictHis,
                                  ctx->trans_cache->hsyncdataset);

    /* If filter conditions are met, cleanup statements */
    /*
     * System table changes in subsequent flow are distinguished by stmt_type_meta
     * 1. Apply system table changes between redo--->confirmlsn in subsequent flow
     * 2. In bidirectional filter, originid cannot be empty, but subsequent flow needs system table
     * changes
     */
    if (decodingcontext_isstmtsfilter(txn_ptr->filter, redo))
    {
        metalist = NULL;
        foreach (lc, txn_ptr->stmts)
        {
            stmt = (txnstmt*)lfirst(lc);
            if (NULL != stmt->stmt && TXNSTMT_TYPE_METADATA != stmt->type)
            {
                /* Call different free functions based on different types */
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

    /* Write transaction to cache */
    elog(RLOG_DEBUG,
         "stmtlen:%lu, startlsn:%X/%X, %lu, xid:%lu, walrec:%lu, parserrec:%lu",
         txn_ptr->stmtsize,
         (uint32)(txn_ptr->start.wal.lsn >> 32),
         (uint32)(txn_ptr->start.wal.lsn),
         txn_ptr->xid,
         txn_ptr->debugno,
         g_walrecno,
         g_parserecno);

    /* Check and assign extra0 for the last statement */
    if (NULL != (lc = list_tail(txn_ptr->stmts)))
    {
        stmt = (txnstmt*)lfirst(lc);
        stmt->extra0.wal.lsn = ctx->decode_record->end.wal.lsn;
    }

    /* Update redo/restart/confirm lsn based on transaction startlsn/endlsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn_ptr->endtimestamp = pretrans->m_time;

    /* Copy a transaction and add to cache */
    copied_txn = txn_copy(txn_ptr);

    /* Delete transaction from transdlist, by_txns */
    ctx->trans_cache->totalsize -= (txn_ptr->stmtsize - 4);
    transcache_deletetxn((void*)ctx, txn_ptr);

    /* Large transaction filter */
    large_txn_end_filter(ctx, copied_txn, true);

    txn_addcommit(copied_txn);
    cache_txn_add(ctx->parser2txns, copied_txn);
    check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
}

void decode_xact_commit_emit(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    bool                          redo = true;
    ListCell*                     lc = NULL;
    txnstmt*                      stmt = NULL;
    txn*                          txn_ptr = NULL;
    txn*                          copied_txn = NULL;
    pg_parser_translog_pre_trans* pretrans = NULL;
    List*                         metalist = NULL;
    bool                          find = false;

    pretrans = (pg_parser_translog_pre_trans*)pbase;

    /*
     * Get transaction linked list by transaction number
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* If empty, then it means no processing needed */
    if (NULL == txn_ptr)
    {
        return;
    }

    if (pretrans->m_base.m_xid >= ctx->rewind_ptr->strategy.xmax)
    {
        /* Found commit of transaction greater than xmax */
        ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
        ctx->base.restartlsn = ctx->rewind_ptr->redolsn;
        ctx->stat = DECODINGCONTEXT_RUNNING;
        if (ctx->callback.setparserlsn)
        {
            ctx->callback.setparserlsn(ctx->privdata,
                                       ctx->base.confirmedlsn,
                                       ctx->base.restartlsn,
                                       ctx->base.restartlsn);
        }
        else
        {
            elog(RLOG_WARNING, "be carefull! setparserlsn is null");
        }

        elog(RLOG_INFO,
             "commit emit rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
             (uint32)(ctx->base.redolsn >> 32),
             (uint32)ctx->base.redolsn,
             (uint32)(ctx->base.restartlsn >> 32),
             (uint32)ctx->base.restartlsn,
             (uint32)(ctx->base.confirmedlsn >> 32),
             (uint32)ctx->base.confirmedlsn);
        rewind_stat_setemited(ctx->rewind_ptr);
        redo = false;
    }
    else if (ctx->rewind_ptr->strategy.xips)
    {
        find = false;
        hash_search(ctx->rewind_ptr->strategy.xips, &pretrans->m_base.m_xid, HASH_FIND, &find);
        if (find)
        {
            /* Found commit of still active transaction in xid list */
            ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
            ctx->base.restartlsn = ctx->rewind_ptr->redolsn;
            ctx->stat = DECODINGCONTEXT_RUNNING;
            if (ctx->callback.setparserlsn)
            {
                ctx->callback.setparserlsn(ctx->privdata,
                                           ctx->base.confirmedlsn,
                                           ctx->base.restartlsn,
                                           ctx->base.restartlsn);
            }
            else
            {
                elog(RLOG_WARNING, "be carefull! setparserlsn is null");
            }

            elog(RLOG_INFO,
                 "commit emit rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: "
                 "%X/%X",
                 (uint32)(ctx->base.redolsn >> 32),
                 (uint32)ctx->base.redolsn,
                 (uint32)(ctx->base.restartlsn >> 32),
                 (uint32)ctx->base.restartlsn,
                 (uint32)(ctx->base.confirmedlsn >> 32),
                 (uint32)ctx->base.confirmedlsn);
            rewind_stat_setemited(ctx->rewind_ptr);
            redo = false;
        }
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    elog(RLOG_DEBUG,
         "xid:%lu, startlsn:%X/%X, endlsn:%X/%X, confirmed_lsn:%X/%X",
         txn_ptr->xid,
         (uint32)(txn_ptr->start.wal.lsn >> 32),
         (uint32)(txn_ptr->start.wal.lsn),
         (uint32)(txn_ptr->end.wal.lsn >> 32),
         (uint32)(txn_ptr->end.wal.lsn),
         (uint32)(ctx->base.confirmedlsn >> 32),
         (uint32)(ctx->base.confirmedlsn));

    if (NULL != txn_ptr->sysdict)
    {
        /*
         * DDL conversion
         *  redo    true        In redo logic, no need to convert ddl statements
         */
        if (decodingcontext_isddlfilter(txn_ptr->filter, redo))
        {
            dml2ddl(ctx, txn_ptr);
        }

        /* Transfer sysdict to sysdicthis */
        transcache_sysdict2his(txn_ptr);

        transcache_sysdict_free(txn_ptr);
        TXN_UNSET_TRANS_DDL(txn_ptr->flag);
    }

    /* Subtransaction processing */
    decode_xact_buildcommittxn(ctx, pretrans, txn_ptr, redo);

    /* Apply sysdicthis */
    cache_sysdicts_txnsysdicthis2cache(ctx->trans_cache->sysdicts, txn_ptr->sysdictHis);

    /* Update sync dataset */
    filter_dataset_updatedatasets(ctx->trans_cache->addtablepattern,
                                  ctx->trans_cache->sysdicts->by_namespace,
                                  txn_ptr->sysdictHis,
                                  ctx->trans_cache->hsyncdataset);

    /* If filter conditions are met, cleanup statements */
    if (decodingcontext_isstmtsfilter(txn_ptr->filter, redo))
    {
        metalist = NULL;
        foreach (lc, txn_ptr->stmts)
        {
            stmt = (txnstmt*)lfirst(lc);
            if (NULL != stmt->stmt && TXNSTMT_TYPE_METADATA != stmt->type)
            {
                /* Call different free functions based on different types */
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

    /* Write transaction to cache */
    elog(RLOG_DEBUG,
         "stmtlen:%lu, startlsn:%X/%X, %lu, xid:%lu, walrec:%lu, parserrec:%lu",
         txn_ptr->stmtsize,
         (uint32)(txn_ptr->start.wal.lsn >> 32),
         (uint32)(txn_ptr->start.wal.lsn),
         txn_ptr->xid,
         txn_ptr->debugno,
         g_walrecno,
         g_parserecno);

    /* Check and assign extra0 for the last statement */
    if (NULL != (lc = list_tail(txn_ptr->stmts)))
    {
        stmt = (txnstmt*)lfirst(lc);
        if (InvalidXLogRecPtr == stmt->extra0.wal.lsn)
        {
            stmt->extra0.wal.lsn = ctx->decode_record->end.wal.lsn;
        }
    }

    /* Update redo/restart/confirm lsn based on transaction startlsn/endlsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn_ptr->endtimestamp = pretrans->m_time;

    /* Copy a transaction and add to cache */
    copied_txn = txn_copy(txn_ptr);

    /* Delete transaction from transdlist, by_txns */
    transcache_deletetxn((void*)ctx, txn_ptr);

    ctx->trans_cache->totalsize -= (txn_ptr->stmtsize - 4);
    cache_txn_add(ctx->parser2txns, copied_txn);
}

/*
 * Transaction rollback
 *  When parsed lsn < confirmlsn, just release resources
 *  When parsed lsn > confirmlsn, need to pass transaction to format thread to advance restartlsn
 */
void decode_xact_abort(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    bool                          brestart = false;
    bool                          bconfirm = false;
    int                           index = 0;
    ListCell*                     lc = NULL;
    txn*                          txn_ptr = NULL;
    txn*                          subtxn_ptr = NULL;
    txn*                          copied_txn = NULL;
    txnstmt*                      txnstmt_ptr = NULL;
    xl_xact_parsed_abort*         parsedabort = NULL;
    pg_parser_translog_pre_trans* pretrans = NULL;
    TransactionId                 subxid;

    pretrans = (pg_parser_translog_pre_trans*)pbase;

    /*
     * Get transaction linked list by transaction number
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* If empty, then it means no processing needed */
    if (NULL == txn_ptr)
    {
        check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    parsedabort = (xl_xact_parsed_abort*)pretrans->m_transdata;
    for (index = 0; index < parsedabort->nsubxacts; index++)
    {
        subxid = parsedabort->subxacts[index];

        /* Get transaction number and stored transaction information */
        subtxn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, subxid);
        if (NULL == subtxn_ptr)
        {
            continue;
        }

        /* Free system table */
        transcache_sysdict_free(subtxn_ptr);

        /* Delete transaction from transaction linked list */
        transcache_dlist_remove((void*)ctx, subtxn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* Free subtxn memory */
        txn_free(subtxn_ptr);

        /* Delete subtransaction from hash */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_ptr->xid);

        check_online_refresh_xids(ctx, subxid);
    }

    /* Free system table */
    transcache_sysdict_free(txn_ptr);

    if (txn_ptr->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        /* Free txn */
        /* Delete transaction from transaction linked list */
        transcache_dlist_remove((void*)ctx, txn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* Free txn memory */
        txn_free(txn_ptr);

        /* Delete transaction from hash */
        transcache_removeTXNByXid(ctx->trans_cache, txn_ptr->xid);

        return;
    }

    /* Free content of txn_ptr->stmts */
    foreach (lc, txn_ptr->stmts)
    {
        txnstmt_ptr = (txnstmt*)lfirst(lc);
        txnstmt_free(txnstmt_ptr);
    }
    list_free(txn_ptr->stmts);
    txn_ptr->stmts = NULL;

    /* Update redo/restart/confirm lsn based on transaction startlsn/endlsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn_ptr->endtimestamp = pretrans->m_time;

    /* Copy a transaction and add to cache */
    copied_txn = txn_copy(txn_ptr);

    /* Delete transaction from transdlist, by_txns */
    transcache_deletetxn((void*)ctx, txn_ptr);

    /* Large transaction filter */
    large_txn_end_filter(ctx, copied_txn, false);

    /* Add to transaction cache for write thread to process */
    ctx->trans_cache->totalsize -= (txn_ptr->stmtsize - 4);
    cache_txn_add(ctx->parser2txns, copied_txn);

    check_online_refresh_xids(ctx, pretrans->m_base.m_xid);
    return;
}

void decode_xact_abort_emit(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    bool                          brestart = false;
    bool                          bconfirm = false;
    int                           index = 0;
    ListCell*                     lc = NULL;
    txn*                          txn_ptr = NULL;
    txn*                          subtxn_ptr = NULL;
    txn*                          copied_txn = NULL;
    txnstmt*                      txnstmt_ptr = NULL;
    xl_xact_parsed_abort*         parsedabort = NULL;
    pg_parser_translog_pre_trans* pretrans = NULL;
    TransactionId                 subxid;
    bool                          find = false;

    pretrans = (pg_parser_translog_pre_trans*)pbase;

    /*
     * Get transaction linked list by transaction number
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* If empty, then it means no processing needed */
    if (NULL == txn_ptr)
    {
        return;
    }

    if (pretrans->m_base.m_xid >= ctx->rewind_ptr->strategy.xmax)
    {
        /* Found commit of still active transaction in xid list */
        ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
        ctx->base.restartlsn = ctx->rewind_ptr->redolsn;
        ctx->stat = DECODINGCONTEXT_RUNNING;
        if (ctx->callback.setparserlsn)
        {
            ctx->callback.setparserlsn(ctx->privdata,
                                       ctx->base.confirmedlsn,
                                       ctx->base.restartlsn,
                                       ctx->base.restartlsn);
        }
        else
        {
            elog(RLOG_WARNING, "be carefull! setparserlsn is null");
        }

        elog(RLOG_INFO,
             "abort emit rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
             (uint32)(ctx->base.redolsn >> 32),
             (uint32)ctx->base.redolsn,
             (uint32)(ctx->base.restartlsn >> 32),
             (uint32)ctx->base.restartlsn,
             (uint32)(ctx->base.confirmedlsn >> 32),
             (uint32)ctx->base.confirmedlsn);
        rewind_stat_setemited(ctx->rewind_ptr);
    }
    /* Check if in xips */
    else if (ctx->rewind_ptr->strategy.xips)
    {
        find = false;
        hash_search(ctx->rewind_ptr->strategy.xips, &pretrans->m_base.m_xid, HASH_FIND, &find);
        if (find)
        {
            /* Found commit of still active transaction in xid list */
            ctx->base.confirmedlsn = ctx->decode_record->start.wal.lsn - 1;
            ctx->base.restartlsn = ctx->rewind_ptr->redolsn;
            ctx->stat = DECODINGCONTEXT_RUNNING;
            if (ctx->callback.setparserlsn)
            {
                ctx->callback.setparserlsn(ctx->privdata,
                                           ctx->base.confirmedlsn,
                                           ctx->base.restartlsn,
                                           ctx->base.restartlsn);
            }
            else
            {
                elog(RLOG_WARNING, "be carefull! setparserlsn is null");
            }

            elog(RLOG_INFO,
                 "abort emit rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                 (uint32)(ctx->base.redolsn >> 32),
                 (uint32)ctx->base.redolsn,
                 (uint32)(ctx->base.restartlsn >> 32),
                 (uint32)ctx->base.restartlsn,
                 (uint32)(ctx->base.confirmedlsn >> 32),
                 (uint32)ctx->base.confirmedlsn);
            rewind_stat_setemited(ctx->rewind_ptr);
        }
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    parsedabort = (xl_xact_parsed_abort*)pretrans->m_transdata;
    for (index = 0; index < parsedabort->nsubxacts; index++)
    {
        subxid = parsedabort->subxacts[index];

        /* Get transaction number and stored transaction information */
        subtxn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, subxid);
        if (NULL == subtxn_ptr)
        {
            continue;
        }

        /* Free system table */
        transcache_sysdict_free(subtxn_ptr);

        /* Delete transaction from transaction linked list */
        transcache_dlist_remove((void*)ctx, subtxn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* Free subtxn memory */
        txn_free(subtxn_ptr);

        /* Delete subtransaction from hash */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_ptr->xid);
    }

    /* Free system table */
    transcache_sysdict_free(txn_ptr);

    if (txn_ptr->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        /* Free txn */
        /* Delete transaction from transaction linked list */
        transcache_dlist_remove((void*)ctx, txn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* Free txn memory */
        txn_free(txn_ptr);

        /* Delete transaction from hash */
        transcache_removeTXNByXid(ctx->trans_cache, txn_ptr->xid);

        return;
    }

    /* Free content of txn_ptr->stmts */
    foreach (lc, txn_ptr->stmts)
    {
        txnstmt_ptr = (txnstmt*)lfirst(lc);
        txnstmt_free(txnstmt_ptr);
    }
    list_free(txn_ptr->stmts);
    txn_ptr->stmts = NULL;

    /* Update redo/restart/confirm lsn based on transaction startlsn/endlsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    ctx->callback.setmetricparsetimestamp(ctx->privdata, (TimestampTz)pretrans->m_time);
    txn_ptr->endtimestamp = pretrans->m_time;

    /* Copy a transaction and add to cache */
    copied_txn = txn_copy(txn_ptr);

    /* Delete transaction from transdlist, by_txns */
    transcache_deletetxn((void*)ctx, txn_ptr);

    /* Add to transaction cache for write thread to process */
    ctx->trans_cache->totalsize -= (txn_ptr->stmtsize - 4);
    cache_txn_add(ctx->parser2txns, copied_txn);
    return;
}
