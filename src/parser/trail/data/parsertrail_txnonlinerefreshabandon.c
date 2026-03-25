
#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "task/task_slot.h"
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
#include "storage/trail/fftrail.h"
#include "loadrecords/record.h"
#include "parser/trail/parsertrail.h"
#include "queue/queue.h"
#include "parser/trail/data/parsertrail_txnonlinerefreshabandon.h"

bool parsertrail_txntxnonlinerefreshabandonapply(parsertrail* parsertrail, void* data)
{
    /* Remove big transaction from hash, and add big transaction end to completion list */
    txnstmt*    rstmt = NULL;
    txn*        cur_txn = NULL;
    record*     record_obj = NULL;
    ff_txndata* txndata = NULL;

    if (NULL == data)
    {
        return true;
    }

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;

    cur_txn = txn_init(FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == cur_txn)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    parsertrail->lasttxn = cur_txn;

    cur_txn->type = TXN_TYPE_ONLINEREFRESH_ABANDON;
    cur_txn->start.wal.lsn = rstmt->extra0.wal.lsn;
    cur_txn->confirm.wal.lsn = MAX_LSN;

    /* Set segno */
    record_obj = (record*)(parsertrail->ffsmgrstate->fdata->extradata);
    cur_txn->end.trail.offset = record_obj->end.trail.offset;
    cur_txn->segno = record_obj->end.trail.fileid;

    cur_txn->stmts = lappend(cur_txn->stmts, rstmt);
    cur_txn->stmtsize += rstmt->len;

    elog(RLOG_INFO, "then onlinerefresh abandon");

    /* Check if file switch occurred, if so need to cleanup cache */
    if (FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* Swap */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}
