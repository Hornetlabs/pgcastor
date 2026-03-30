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

static void decode_xact_appendsubtxn_obj(List** ptxnstmts, List** psysdicthis, txn* subtxn_obj)
{
    ListCell*    lc = NULL;
    txnstmt*     stmt = NULL;
    catalogdata* catalog = NULL;

    foreach (lc, subtxn_obj->stmts)
    {
        stmt = (txnstmt*)lfirst(lc);
        *ptxnstmts = lappend(*ptxnstmts, stmt);
    }
    list_free(subtxn_obj->stmts);
    subtxn_obj->stmts = NULL;

    foreach (lc, subtxn_obj->sysdictHis)
    {
        catalog = (catalogdata*)lfirst(lc);
        *psysdicthis = lappend(*psysdicthis, catalog);
    }
    list_free(subtxn_obj->sysdictHis);
    subtxn_obj->sysdictHis = NULL;
}

/*
 * On Commit, append the contents of sub-transactions to the main transaction
 */
static void decode_xact_buildcommittxn(decodingcontext* ctx, pg_parser_translog_pre_trans* pretrans, txn* in_txn)
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

    txnstmts = in_txn->stmts;
    in_txn->stmts = NULL;

    sysdicthis = in_txn->sysdictHis;
    in_txn->sysdictHis = NULL;

    /* Traverse sub-transactions to the appropriate position */
    for (index = 0; index < parsedcommit->nsubxacts; index++)
    {
        bool          brestart = false;
        bool          bconfirm = false;
        txn*          subtxn_obj = NULL;
        TransactionId subxid = parsedcommit->subxacts[index];

        /* Get transaction number, and get stored transaction information */
        subtxn_obj = transcache_getTXNByXidFind(ctx->trans_cache, subxid);

        /* DDL conversion */
        if (NULL != subtxn_obj->sysdict)
        {
            /* Transfer sysdict to sysdicthis */
            transcache_sysdict2his(subtxn_obj);

            /* Release */
            transcache_sysdict_free(subtxn_obj);
        }

        /* Append sub-transaction data to main transaction */
        if (NULL != subtxn_obj->stmts)
        {
            in_txn->stmtsize += subtxn_obj->stmtsize;
        }
        decode_xact_appendsubtxn_obj(&txnstmts, &sysdicthis, subtxn_obj);

        /* Delete transaction from transaction linked list */
        transcache_dlist_remove((void*)ctx, subtxn_obj, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn_obj memory release */
        txn_free(subtxn_obj);

        /* Delete sub-transaction from hash */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_obj->xid);
    }

    in_txn->sysdictHis = sysdicthis;
    in_txn->stmts = txnstmts;
}

static void check_online_refresh_decode_xids(decodingcontext* ctx, TransactionId xid)
{
    dlistnode*             dlnode_xid = NULL;
    onlinerefresh_capture* olcapture = (onlinerefresh_capture*)ctx->privdata;

    if (!olcapture->xids)
    {
        return;
    }

    dlnode_xid = olcapture->xids->head;

    while (dlnode_xid)
    {
        dlistnode*         dlnode_next = NULL;
        FullTransactionId* xid_p = (FullTransactionId*)dlnode_xid->value;

        dlnode_next = dlnode_xid->next;

        /* Check if in linked list */
        if (*xid_p == (FullTransactionId)xid)
        {
            /* In linked list, delete */
            onlinerefresh_capture_xids_delete(olcapture, dlnode_xid);
        }
        dlnode_xid = dlnode_next;
    }

    if (onlinerefresh_capture_xids_isnull(olcapture))
    {
        txn* end_txn = NULL;

        /* Build onlinerefresh end transaction */
        end_txn = parserwork_build_onlinerefresh_increment_end_txn(olcapture->no->data);

        /* Add transaction to cache */
        cache_txn_add(ctx->parser2txns, end_txn);

        dlist_free(olcapture->xids, NULL);
        olcapture->xids = NULL;
    }
}

/*
 * commit submission, processing logic is as follows
 *  1、First on commit, compare submitted lsn with confirmlsn, when less than confirmlsn
 * indicates this transaction does not need processing, then clean up data 1.1 Apply system table
 * data
 *
 *  2、Merge sub-transactions contained in the current transaction into the main transaction
 *
 *
 * Transaction logic in PG is as follows:
 *  Main transaction xid
 *      savepoint   sub-transaction xid1
 *      savepoint   sub-transaction xid2, parent transaction  logically is xid1, but in PG
 * there is no nested transaction logic, so parent transaction is xid
 *
 */
void onlinerefresh_decode_xact_commit(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    bool                          redo = false;
    ListCell*                     lc = NULL;
    txnstmt*                      stmt = NULL;
    txn*                          txn_ptr = NULL;
    txn*                          txn_copy_obj = NULL;
    pg_parser_translog_pre_trans* pretrans = NULL;

    pretrans = (pg_parser_translog_pre_trans*)pbase;

    /*
     * Get transaction linked list by transaction number
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* If NULL, no processing needed */
    if (NULL == txn_ptr)
    {
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    /* Transactions not needing capture, and without sysdict or sysdictHis do not need processing,
     * just delete transaction and return */
    if (true == txn_ptr->filter && !txn_ptr->sysdict && !txn_ptr->sysdictHis)
    {
        /* Delete transaction from transdlist, by_txns */
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        transcache_deletetxn((void*)ctx, txn_ptr);
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

    /* Check if in redo state */
    if (txn_ptr->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        redo = true;
    }

    if (NULL != txn_ptr->sysdict)
    {
        /* Transfer sysdict to sysdicthis */
        transcache_sysdict2his(txn_ptr);

        transcache_sysdict_free(txn_ptr);
    }

    /* Sub-transaction processing */
    decode_xact_buildcommittxn(ctx, pretrans, txn_ptr);

    /* sysdicthis apply */
    cache_sysdicts_txnsysdicthis2cache(ctx->trans_cache->sysdicts, txn_ptr->sysdictHis);

    /* Update sync dataset */
    filter_dataset_updatedatasets(ctx->trans_cache->addtablepattern,
                                  ctx->trans_cache->sysdicts->by_namespace,
                                  txn_ptr->sysdictHis,
                                  ctx->trans_cache->hsyncdataset);

    /* If it meets filter conditions, then clean up the statements */
    if (decodingcontext_isstmtsfilter(txn_ptr->filter, redo))
    {
        List* metalist = NULL;
        foreach (lc, txn_ptr->stmts)
        {
            stmt = (txnstmt*)lfirst(lc);
            if (NULL != stmt->stmt && TXNSTMT_TYPE_METADATA != stmt->type)
            {
                /* Call different release functions based on different types */
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

    /* Check and assign extra0 of the last statement */
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

    txn_ptr->endtimestamp = pretrans->m_time;

    /* Copy a transaction and add to cache */
    txn_copy_obj = txn_copy(txn_ptr);

    /* Delete transaction from transdlist, by_txns */
    transcache_deletetxn((void*)ctx, txn_ptr);

    txn_addcommit(txn_copy_obj);
    cache_txn_add(ctx->parser2txns, txn_copy_obj);

    check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
}

/*
 * Transaction rollback
 *  When parsed lsn < confirmlsn, just release resources
 *  When parsed lsn > confirmlsn, then need to pass transaction to formatting thread to advance
 * restartlsn
 */
void onlinerefresh_decode_xact_abort(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    bool                          brestart = false;
    bool                          bconfirm = false;
    int                           index = 0;
    ListCell*                     lc = NULL;
    txn*                          txn_ptr = NULL;
    txn*                          txn_copy_obj = NULL;
    xl_xact_parsed_abort*         parsedabort = NULL;
    pg_parser_translog_pre_trans* pretrans = NULL;
    txn*                          subtxn_obj = NULL;
    TransactionId                 subxid = 0;

    pretrans = (pg_parser_translog_pre_trans*)pbase;

    /*
     * Get transaction linked list by transaction number
     */
    txn_ptr = transcache_getTXNByXidFind(ctx->trans_cache, pretrans->m_base.m_xid);
    /* If empty, then no processing is needed */
    if (NULL == txn_ptr)
    {
        check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
        return;
    }

    txn_ptr->end.wal.lsn = ctx->decode_record->end.wal.lsn;
    parsedabort = (xl_xact_parsed_abort*)pretrans->m_transdata;
    for (index = 0; index < parsedabort->nsubxacts; index++)
    {
        subxid = parsedabort->subxacts[index];

        /* Get transaction number, and get stored transaction information */
        subtxn_obj = transcache_getTXNByXidFind(ctx->trans_cache, subxid);
        if (NULL == subtxn_obj)
        {
            continue;
        }

        /* System table release */
        transcache_sysdict_free(subtxn_obj);

        /* Delete transaction from transaction linked list */
        transcache_dlist_remove((void*)ctx, subtxn_obj, &brestart, NULL, &bconfirm, NULL, false);

        /* subtxn_obj memory release */
        txn_free(subtxn_obj);

        /* Delete sub-transaction from hash */
        transcache_removeTXNByXid(ctx->trans_cache, subtxn_obj->xid);
    }

    /* System table release */
    transcache_sysdict_free(txn_ptr);

    if (txn_ptr->end.wal.lsn <= ctx->base.confirmedlsn)
    {
        /* txn release */
        /* Delete transaction from transaction linked list */
        transcache_dlist_remove((void*)ctx, txn_ptr, &brestart, NULL, &bconfirm, NULL, false);

        /* txn memory release */
        txn_free(txn_ptr);

        /* Delete transaction from hash */
        transcache_removeTXNByXid(ctx->trans_cache, txn_ptr->xid);

        return;
    }

    /* Release the contents of txn_ptr->stmts */
    foreach (lc, txn_ptr->stmts)
    {
        txnstmt* stmt = (txnstmt*)lfirst(lc);
        txnstmt_free(stmt);
    }
    list_free(txn_ptr->stmts);
    txn_ptr->stmts = NULL;

    /* Update redo/restart/confirm lsn based on transaction startlsn/endlsn */
    transcache_refreshlsn((void*)ctx, txn_ptr);

    txn_ptr->endtimestamp = pretrans->m_time;

    /* Copy a transaction and add to cache */
    txn_copy_obj = txn_copy(txn_ptr);

    /* Delete transaction from transdlist, by_txns */
    transcache_deletetxn((void*)ctx, txn_ptr);

    /* Put into transaction cache, let write thread process */
    cache_txn_add(ctx->parser2txns, txn_copy_obj);

    check_online_refresh_decode_xids(ctx, pretrans->m_base.m_xid);
    return;
}
