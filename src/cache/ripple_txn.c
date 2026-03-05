#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_commit.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_toastcache.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_transcache.h"

void ripple_txn_initset(ripple_txn *tx_entry, FullTransactionId xid, XLogRecPtr startlsn)
{
    if(NULL == tx_entry)
    {
        return;
    }
    rmemset1(tx_entry, 0, 0, sizeof(ripple_txn));
    tx_entry->xid = xid;
    tx_entry->flag = RIPPLE_TXN_FLAG_NORMAL;
    /* 初始化时, stmtsize = 4 */
    tx_entry->stmtsize = 4;
    tx_entry->start.wal.lsn = startlsn;
    tx_entry->next = NULL;
    tx_entry->prev = NULL;
    tx_entry->cachenext = NULL;
    tx_entry->filter = false;
}

/* 生成一个没有 xid 的事务 */
ripple_txn* ripple_txn_init(FullTransactionId xid, XLogRecPtr startlsn, XLogRecPtr endlsn)
{
    ripple_txn *txn = NULL;

    txn = (ripple_txn*)rmalloc0(sizeof(ripple_txn));
    if(NULL == txn)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(txn, 0, '\0', sizeof(ripple_txn));

    ripple_txn_initset(txn, xid, startlsn);
    txn->end.wal.lsn = endlsn;

    return txn;
}


ripple_txn* ripple_txn_copy(ripple_txn* txn)
{
    ripple_txn* new_txn = NULL;

    new_txn = (ripple_txn*)rmalloc0(sizeof(ripple_txn));
    if(NULL == new_txn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(new_txn, 0, '\0', sizeof(ripple_txn));
    rmemcpy0(new_txn, 0, txn, sizeof(ripple_txn));

    return new_txn;
}

/* 添加事务提交*/
bool ripple_txn_addcommit(ripple_txn* txn)
{
    ripple_txnstmt* txnstmt     = NULL;
    ripple_commit_stmt* commit  = NULL;

    txnstmt = ripple_txnstmt_init();
    if (NULL == txnstmt)
    {
        return false;
    }

    commit = ripple_txnstmt_commit_init();
    if (NULL == txnstmt)
    {
        return false;
    }

    commit->endtimestamp = txn->endtimestamp;

    txnstmt->type = RIPPLE_TXNSTMT_TYPE_COMMIT;
    txnstmt->extra0.wal.lsn = txn->end.wal.lsn;
    txnstmt->stmt = (void*)commit;
    txn->stmts = lappend(txn->stmts, txnstmt);

    return true;
}


/* 大事务开始 */
ripple_txn *ripple_txn_initbigtxn(FullTransactionId xid)
{
    ripple_txn *bigtxnbegin = NULL;

    bigtxnbegin = ripple_txn_init(xid, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == bigtxnbegin)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    return bigtxnbegin;
}

/* 放弃掉的事务 */
ripple_txn *ripple_txn_initabandon(ripple_txn *txninhash)
{
    ripple_txn *abandon = NULL;
    ripple_txnstmt* txnstmt = NULL;

    abandon = ripple_txn_init(txninhash->xid, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == abandon)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }

    abandon->type = RIPPLE_TXN_TYPE_ABANDON;
    abandon->xid = txninhash->xid;
    abandon->segno = txninhash->segno;
    abandon->start.wal.lsn = txninhash->start.wal.lsn;
    abandon->confirm.wal.lsn = RIPPLE_MAX_LSN;
    abandon->end.trail.offset = txninhash->end.trail.offset;
    txnstmt = ripple_txnstmt_init();
    if(NULL == txnstmt)
    {
        elog(RLOG_WARNING, "init abandon txn error");
        return false;
    }
    txnstmt->type = RIPPLE_TXNSTMT_TYPE_ABANDON;
    abandon->stmts = lappend(abandon->stmts, txnstmt);
    return abandon;
}

/* 删除事务缓存 */
void ripple_txn_free(ripple_txn* txn)
{
    ListCell* lc = NULL;
    ripple_txnstmt* stmt = NULL;
    HASH_SEQ_STATUS status;
    if(NULL == txn)
    {
        return;
    }

    if(NULL != txn->toast_hash)
    {
        ripple_chunk_data *chunk = NULL;
        ripple_toast_cache_entry* toastentry = NULL;
        hash_seq_init(&status, txn->toast_hash);
        while (NULL != (toastentry = hash_seq_search(&status)))
        {
            foreach(lc, toastentry->chunk_list)
            {
                chunk = (ripple_chunk_data *)lfirst(lc);
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

    if(NULL != txn->sysdict)
    {
        /* 删除 sysdict */
        ripple_transcache_sysdict_free(txn);
        txn->sysdict = NULL;
    }

    if(NULL != txn->sysdictHis)
    {
        ripple_cache_sysdicts_txnsysdicthisfree(txn->sysdictHis);
        list_free(txn->sysdictHis);
        txn->sysdictHis = NULL;
    }

    if (NULL != txn->hsyncdataset)
    {
        hash_destroy(txn->hsyncdataset);
        txn->hsyncdataset = NULL;
    }

    foreach(lc, txn->stmts)
    {
        stmt = (ripple_txnstmt*)lfirst(lc);
        ripple_txnstmt_free(stmt);
    }
    list_free(txn->stmts);
    txn->stmts = NULL;
}

void ripple_txn_freevoid(void* args)
{
    ripple_txn* txn = NULL;

    if(NULL == args)
    {
        return;
    }
    txn = (ripple_txn*)args;
    ripple_txn_free(txn);
    rfree(txn);
}
