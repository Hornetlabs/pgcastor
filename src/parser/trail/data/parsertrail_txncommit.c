#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "loadrecords/ripple_record.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/data/ripple_parsertrail_txncommit.h"

static ripple_txn* ripple_parsertrail_txncommit_copytxn(ripple_parsertrail* parsertrail)
{
    ripple_txn* copy_txn                = NULL;

    if (NULL == parsertrail->lasttxn)
    {
        return NULL;
    }

    if (parsertrail->lasttxn->type > RIPPLE_TXN_TYPE_NORMAL 
        && RIPPLE_TXN_TYPE_BIGTXN_BEGIN != parsertrail->lasttxn->type)
    {
        copy_txn = parsertrail->lasttxn;
    }
    else
    {
        copy_txn = ripple_txn_copy(parsertrail->lasttxn);
    }
    return copy_txn;
}


/*
 * 事务结束标识
 *  结束当前的事务，若没有事务那么报错
*/
bool ripple_parsertrail_txncommitapply(ripple_parsertrail* parsertrail, void* data)
{
    ListCell* lc                        = NULL;
    ripple_txn* copy_txn                = NULL;
    ripple_txnstmt* stmt                = NULL;
    ripple_txnstmt* rstmt               = NULL;
    ripple_record* record               = NULL;
    ripple_commit_stmt* commit          = NULL;
    ripple_ff_txndata* txndata          = NULL;
    ripple_fftrail_privdata* privdata   = NULL;

    RIPPLE_UNUSED(privdata);

    if(NULL == data)
    {
        return true;
    }

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;

    commit = (ripple_commit_stmt*)rstmt->stmt;

    /* 接收到事务结束标识 */
    elog(RLOG_DEBUG, "txn commit apply, xid:%lu", txndata->header.transid);

    if (!parsertrail->lasttxn)
    {
        return true;
    }

    /* 添加到缓存, currenttxn置空 */
    /* 添加事务结束lsn */
    if (parsertrail->lasttxn->stmts != NULL)
    {
        parsertrail->lasttxn->confirm.wal.lsn = txndata->header.orgpos;
        if (NULL != (lc = list_tail(parsertrail->lasttxn->stmts)))
        {
            stmt = (ripple_txnstmt*)lfirst(lc);
            if (InvalidXLogRecPtr == stmt->extra0.wal.lsn)
            {
                stmt->extra0.wal.lsn = txndata->header.orgpos;
            }
        }
        elog(RLOG_DEBUG,"decodingctx->currenttxn->end.wal.lsn %lu", parsertrail->lasttxn->end.wal.lsn);
    }

    /* 设置事务的结束位置 */
    record = (ripple_record*)(parsertrail->ffsmgrstate->fdata->extradata);
    parsertrail->lasttxn->end.trail.offset = record->end.trail.offset;
    parsertrail->lasttxn->segno = record->end.trail.fileid;
    parsertrail->lasttxn->stmts = lappend(parsertrail->lasttxn->stmts, rstmt);
    parsertrail->lasttxn->endtimestamp = commit->endtimestamp;
    copy_txn = ripple_parsertrail_txncommit_copytxn(parsertrail);

    if (!(RIPPLE_TXN_TYPE_NORMAL < copy_txn->type))
    {
        ripple_transcache_removeTXNByXid(parsertrail->transcache, parsertrail->lasttxn->xid);
    }

    parsertrail->dtxns = dlist_put(parsertrail->dtxns, copy_txn);
    parsertrail->lasttxn = NULL;
    elog(RLOG_DEBUG, "commit transaction:%lu : %lu/%lu", txndata->header.transid, record->end.trail.fileid,record->end.trail.offset);
    return true;
}

