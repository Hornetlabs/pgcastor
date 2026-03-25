#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "loadrecords/record.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_txndelete.h"

static void parsertrail_txndelete2hash(parsertrail* parsertrail, ff_txndata* txndata)
{
    /*
     * 1. Check if the transaction exists, create if not, append if exists
     * 2. Return
     */
    uint16                           index = 0;
    txnstmt*                         rstmt = NULL;
    pg_parser_translog_tbcol_values* colvalues = NULL;

    rstmt = (txnstmt*)txndata->data;

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
            parsertrail->dtxns = dlist_put(parsertrail->dtxns, txn_initabandon(txn_entry));
        }
        parsertrail->lasttxn = txn_entry;
        parsertrail->lasttxn->start.wal.lsn = rstmt->extra0.wal.lsn;

        elog(RLOG_DEBUG, "then begin of transaction:%lu", txndata->header.transid);
    }

    if (FF_DATA_TRANSIND_IN == (FF_DATA_TRANSIND_IN & txndata->header.transind))
    {
        /* Check if exists in hash, add if not found */
        if (!parsertrail->lasttxn)
        {
            elog(RLOG_ERROR, "missing trans start, transid:%lu", txndata->header.transid);
        }
        elog(RLOG_DEBUG, "part of transaction:%lu", txndata->header.transid);
    }
    colvalues = (pg_parser_translog_tbcol_values*)rstmt->stmt;
    parsertrail->lasttxn->stmts = lappend(parsertrail->lasttxn->stmts, rstmt);
    parsertrail->lasttxn->stmtsize += rstmt->len;
    parsertrail->transcache->totalsize += rstmt->len;

    /* Output content */
    if (RLOG_DEBUG == g_loglevel)
    {
        elog(RLOG_DEBUG, "delete %s.%s begin", colvalues->m_base.m_schemaname,
             colvalues->m_base.m_tbname);
        for (index = 0; index < colvalues->m_valueCnt; index++)
        {
            elog(RLOG_DEBUG, "column:%s, value:%s", colvalues->m_old_values[index].m_colName,
                 colvalues->m_old_values[index].m_value == NULL
                     ? "NULL"
                     : (char*)(colvalues->m_old_values[index].m_value));
        }
        elog(RLOG_DEBUG, "delete %s.%s end", colvalues->m_base.m_schemaname,
             colvalues->m_base.m_tbname);
    }

    return;
}

/* Add table data to transaction cache */
bool parsertrail_txndeleteapply(parsertrail* parsertrail, void* data)
{
    ff_txndata* txndata = NULL;

    if (NULL == data)
    {
        return true;
    }

    txndata = (ff_txndata*)data;

    /* Put data into cache */
    parsertrail_txndelete2hash(parsertrail, txndata);

    /* Check if file switch occurred, if so need to clean up cache */
    if (FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* Swap */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}
