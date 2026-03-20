#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_commit.h"
#include "cache/cache_sysidcts.h"
#include "cache/toastcache.h"
#include "cache/txn.h"
#include "cache/transcache.h"

void txn_initset(txn* tx_entry, FullTransactionId xid, XLogRecPtr startlsn)
{
    if (NULL == tx_entry)
    {
        return;
    }
    rmemset1(tx_entry, 0, 0, sizeof(txn));
    tx_entry->xid = xid;
    tx_entry->flag = TXN_FLAG_NORMAL;
    /* When initialized, stmtsize = 4 */
    tx_entry->stmtsize = 4;
    tx_entry->start.wal.lsn = startlsn;
    tx_entry->next = NULL;
    tx_entry->prev = NULL;
    tx_entry->cachenext = NULL;
    tx_entry->filter = false;
}

/* Generate a transaction without xid */
txn* txn_init(FullTransactionId xid, XLogRecPtr startlsn, XLogRecPtr endlsn)
{
    txn* txn_obj = NULL;

    txn_obj = (txn*)rmalloc0(sizeof(txn));
    if (NULL == txn_obj)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(txn_obj, 0, '\0', sizeof(txn));

    txn_initset(txn_obj, xid, startlsn);
    txn_obj->end.wal.lsn = endlsn;

    return txn_obj;
}

txn* txn_copy(txn* txn_src)
{
    txn* new_txn = NULL;

    new_txn = (txn*)rmalloc0(sizeof(txn));
    if (NULL == new_txn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(new_txn, 0, '\0', sizeof(txn));
    rmemcpy0(new_txn, 0, txn_src, sizeof(txn));

    return new_txn;
}

/* Add transaction commit */
bool txn_addcommit(txn* txn)
{
    txnstmt*     txnstmt = NULL;
    commit_stmt* commit = NULL;

    txnstmt = txnstmt_init();
    if (NULL == txnstmt)
    {
        return false;
    }

    commit = txnstmt_commit_init();
    if (NULL == txnstmt)
    {
        return false;
    }

    commit->endtimestamp = txn->endtimestamp;

    txnstmt->type = TXNSTMT_TYPE_COMMIT;
    txnstmt->extra0.wal.lsn = txn->end.wal.lsn;
    txnstmt->stmt = (void*)commit;
    txn->stmts = lappend(txn->stmts, txnstmt);

    return true;
}

/* Big transaction start */
txn* txn_initbigtxn(FullTransactionId xid)
{
    txn* bigtxnbegin = NULL;

    bigtxnbegin = txn_init(xid, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == bigtxnbegin)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    return bigtxnbegin;
}

/* Abandoned transaction */
txn* txn_initabandon(txn* txninhash)
{
    txn*     abandon = NULL;
    txnstmt* txnstmt = NULL;

    abandon = txn_init(txninhash->xid, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == abandon)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }

    abandon->type = TXN_TYPE_ABANDON;
    abandon->xid = txninhash->xid;
    abandon->segno = txninhash->segno;
    abandon->start.wal.lsn = txninhash->start.wal.lsn;
    abandon->confirm.wal.lsn = MAX_LSN;
    abandon->end.trail.offset = txninhash->end.trail.offset;
    txnstmt = txnstmt_init();
    if (NULL == txnstmt)
    {
        elog(RLOG_WARNING, "init abandon txn error");
        return false;
    }
    txnstmt->type = TXNSTMT_TYPE_ABANDON;
    abandon->stmts = lappend(abandon->stmts, txnstmt);
    return abandon;
}

/* Delete transaction cache */
void txn_free(txn* txn)
{
    ListCell*       lc = NULL;
    txnstmt*        stmt = NULL;
    HASH_SEQ_STATUS status;
    if (NULL == txn)
    {
        return;
    }

    if (NULL != txn->toast_hash)
    {
        chunk_data*        chunk = NULL;
        toast_cache_entry* toastentry = NULL;
        hash_seq_init(&status, txn->toast_hash);
        while (NULL != (toastentry = hash_seq_search(&status)))
        {
            foreach (lc, toastentry->chunk_list)
            {
                chunk = (chunk_data*)lfirst(lc);
                if (chunk->chunk_data)
                {
                    rfree(chunk->chunk_data);
                }
                rfree(chunk);
            }
            list_free(toastentry->chunk_list);
            toastentry->chunk_list = NULL;
        }
        hash_destroy(txn->toast_hash);
    }

    if (NULL != txn->oidmap)
    {
        hash_destroy(txn->oidmap);
    }

    if (NULL != txn->sysdict)
    {
        /* Delete sysdict */
        transcache_sysdict_free(txn);
        txn->sysdict = NULL;
    }

    if (NULL != txn->sysdictHis)
    {
        cache_sysdicts_txnsysdicthisfree(txn->sysdictHis);
        list_free(txn->sysdictHis);
        txn->sysdictHis = NULL;
    }

    if (NULL != txn->hsyncdataset)
    {
        hash_destroy(txn->hsyncdataset);
        txn->hsyncdataset = NULL;
    }

    foreach (lc, txn->stmts)
    {
        stmt = (txnstmt*)lfirst(lc);
        txnstmt_free(stmt);
    }
    list_free(txn->stmts);
    txn->stmts = NULL;
}

void txn_freevoid(void* args)
{
    txn* txn_obj = NULL;

    if (NULL == args)
    {
        return;
    }
    txn_obj = (txn*)args;
    txn_free(txn_obj);
    rfree(txn_obj);
}
