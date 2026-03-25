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
#include "storage/trail/fftrail.h"
#include "loadrecords/record.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_txncommit.h"

static txn* parsertrail_txncommit_copytxn(parsertrail* parsertrail)
{
    txn* copy_txn = NULL;

    if (NULL == parsertrail->lasttxn)
    {
        return NULL;
    }

    if (parsertrail->lasttxn->type > TXN_TYPE_NORMAL &&
        TXN_TYPE_BIGTXN_BEGIN != parsertrail->lasttxn->type)
    {
        copy_txn = parsertrail->lasttxn;
    }
    else
    {
        copy_txn = txn_copy(parsertrail->lasttxn);
    }
    return copy_txn;
}

/*
 * Transaction end marker
 * End the current transaction, report error if no transaction exists
 */
bool parsertrail_txncommitapply(parsertrail* parsertrail, void* data)
{
    ListCell*         lc = NULL;
    txn*              copy_txn = NULL;
    txnstmt*          stmt = NULL;
    txnstmt*          rstmt = NULL;
    record*           record_obj = NULL;
    commit_stmt*      commit = NULL;
    ff_txndata*       txndata = NULL;
    fftrail_privdata* privdata = NULL;

    UNUSED(privdata);

    if (NULL == data)
    {
        return true;
    }

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;

    commit = (commit_stmt*)rstmt->stmt;

    /* Received transaction end marker */
    elog(RLOG_DEBUG, "txn commit apply, xid:%lu", txndata->header.transid);

    if (!parsertrail->lasttxn)
    {
        return true;
    }

    /* Add to cache, clear currenttxn */
    /* Add transaction end lsn */
    if (parsertrail->lasttxn->stmts != NULL)
    {
        parsertrail->lasttxn->confirm.wal.lsn = txndata->header.orgpos;
        if (NULL != (lc = list_tail(parsertrail->lasttxn->stmts)))
        {
            stmt = (txnstmt*)lfirst(lc);
            if (InvalidXLogRecPtr == stmt->extra0.wal.lsn)
            {
                stmt->extra0.wal.lsn = txndata->header.orgpos;
            }
        }
        elog(RLOG_DEBUG, "decodingctx->currenttxn->end.wal.lsn %lu",
             parsertrail->lasttxn->end.wal.lsn);
    }

    /* Set transaction end position */
    record_obj = (record*)(parsertrail->ffsmgrstate->fdata->extradata);
    parsertrail->lasttxn->end.trail.offset = record_obj->end.trail.offset;
    parsertrail->lasttxn->segno = record_obj->end.trail.fileid;
    parsertrail->lasttxn->stmts = lappend(parsertrail->lasttxn->stmts, rstmt);
    parsertrail->lasttxn->endtimestamp = commit->endtimestamp;
    copy_txn = parsertrail_txncommit_copytxn(parsertrail);

    if (!(TXN_TYPE_NORMAL < copy_txn->type))
    {
        transcache_removeTXNByXid(parsertrail->transcache, parsertrail->lasttxn->xid);
    }

    parsertrail->dtxns = dlist_put(parsertrail->dtxns, copy_txn);
    parsertrail->lasttxn = NULL;
    elog(RLOG_DEBUG, "commit transaction:%lu : %lu/%lu", txndata->header.transid,
         record_obj->end.trail.fileid, record_obj->end.trail.offset);
    return true;
}
