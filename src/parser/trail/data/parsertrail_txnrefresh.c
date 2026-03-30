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
#include "queue/queue.h"
#include "threads/threads.h"
#include "stmts/txnstmt.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "loadrecords/record.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_txnrefresh.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "refresh/integrate/refresh_integrate.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"

static bool parsertrail_txnrefresh2cache(parsertrail* parsertrail, ff_txndata* txndata)
{
    /*
     * 1. Check if the transaction exists, create if not, append if exists
     * 2. Return
     */
    txn*              cur_txn = NULL;
    txnstmt*          rstmt = NULL;
    record*           record_obj = NULL;
    refresh_table*    table = NULL;
    refresh_tables*   refreshstmt = NULL;
    FullTransactionId xid = txndata->header.transid;

    rstmt = (txnstmt*)txndata->data;
    cur_txn = txn_init(xid, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == cur_txn)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    parsertrail->lasttxn = cur_txn;
    cur_txn->type = TXN_TYPE_REFRESH;
    cur_txn->start.wal.lsn = rstmt->extra0.wal.lsn;
    cur_txn->confirm.wal.lsn = rstmt->extra0.wal.lsn;

    /* Set segno */
    record_obj = (record*)(parsertrail->ffsmgrstate->fdata->extradata);
    cur_txn->end.trail.offset = record_obj->end.trail.offset;
    cur_txn->segno = record_obj->end.trail.fileid;

    elog(RLOG_DEBUG, "then begin of transaction:%lu", txndata->header.transid);

    refreshstmt = (refresh_tables*)rstmt->stmt;
    table = refreshstmt->tables;

    cur_txn->stmts = lappend(cur_txn->stmts, rstmt);
    cur_txn->stmtsize += rstmt->len;
    parsertrail->transcache->totalsize += rstmt->len;

    /* Output content */
    if (RLOG_DEBUG == g_loglevel)
    {
        while (NULL != table)
        {
            elog(RLOG_DEBUG, "refresh is %s.%s", table->schema, table->table);
            table = table->next;
        }
    }

    elog(RLOG_DEBUG, "then end of transaction:%lu", txndata->header.transid);

    return true;
}

/* Add table data to transaction cache */
bool parsertrail_txnrefreshapply(parsertrail* parsertrail, void* data)
{
    ff_txndata* txndata = NULL;

    if (NULL == data)
    {
        return true;
    }

    txndata = (ff_txndata*)data;

    /* Put data into cache */
    parsertrail_txnrefresh2cache(parsertrail, txndata);

    /* Check if file switch occurred, if so need to cleanup cache */
    if (FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* Swap */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}
