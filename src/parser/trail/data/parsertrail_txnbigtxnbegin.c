#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "queue/queue.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "loadrecords/record.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_txnbigtxnbegin.h"

static void parsertrail_txnbigtxnbegin2hash(parsertrail* parsertrail, ff_txndata* txndata)
{
    /*
     * 1. Check if the transaction exists, create if not, append if exists
     * 2. Return
     */
    txnstmt* rstmt = NULL;
    record*  record_obj = NULL;

    rstmt = (txnstmt*)txndata->data;

    elog(RLOG_DEBUG, "txnbigtxn begin:%lu", txndata->header.transid);

    /* Check the type */
    if (FF_DATA_TRANSIND_START == (FF_DATA_TRANSIND_START & txndata->header.transind))
    {
        /* Check if exists in hash, add if not found */
        HTAB*             tx_htab = parsertrail->transcache->by_txns;
        txn*              txn_entry = NULL;
        bool              find = false;
        FullTransactionId xid = txndata->header.transid;

        /* Search hash */
        txn_entry = (txn*)hash_search(tx_htab, &xid, HASH_ENTER, &find);
        if (!find)
        {
            /* Initialize */
            txn_initset(txn_entry, xid, InvalidXLogRecPtr);
        }
        else
        {
            /* Add abandon */
            elog(RLOG_WARNING,
                 "txnbigtxn begin the same transaction in hash:%lu",
                 txndata->header.transid);
        }

        /* Mark as big transaction */
        TXN_SET_BIGTXN(txn_entry->flag);
        txn_entry->type = TXN_TYPE_BIGTXN_BEGIN;
        parsertrail->lasttxn = txn_entry;
        parsertrail->lasttxn->start.wal.lsn = rstmt->extra0.wal.lsn;
        parsertrail->lasttxn->stmts = lappend(parsertrail->lasttxn->stmts, rstmt);
        parsertrail->lasttxn->stmtsize += rstmt->len;
        parsertrail->transcache->totalsize += rstmt->len;

        elog(RLOG_DEBUG, "then txnbigtxn begin of transaction:%lu", txndata->header.transid);
    }
    record_obj = (record*)(parsertrail->ffsmgrstate->fdata->extradata);
    parsertrail->lasttxn->segno = record_obj->end.trail.fileid;
    parsertrail->lasttxn->end.trail.offset = record_obj->end.trail.offset;
    parsertrail->lasttxn->confirm.wal.lsn = rstmt->extra0.wal.lsn;

    return;
}

bool parsertrail_txnbigtxnbeginapply(parsertrail* parsertrail, void* data)
{
    ff_txndata* txndata = NULL;

    if (NULL == data)
    {
        return true;
    }

    /* Put data into cache */
    txndata = (ff_txndata*)data;
    parsertrail_txnbigtxnbegin2hash(parsertrail, txndata);

    /* Check if file switch occurred, if so need to clean up cache */
    if (FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* Swap */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}
