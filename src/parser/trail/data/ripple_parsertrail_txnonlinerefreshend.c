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
#include "parser/trail/data/ripple_parsertrail_txnonlinerefreshend.h"

/* 将表数据加入到事务缓存中 */
bool ripple_parsertrail_txnonlinerefreshendapply(ripple_parsertrail* parsertrail,
                                                            void* data)
{
    FullTransactionId xid                   = InvalidFullTransactionId;
    ripple_txn* cur_txn                     = NULL;
    ripple_txnstmt* stmt                    = NULL;
    ripple_record* record                   = NULL;
    ripple_ff_txndata* txndata              = NULL;
    ripple_fftrail_privdata* privdata       = NULL;

    RIPPLE_UNUSED(privdata);

    if(NULL == data)
    {
        return true;
    }

    txndata = (ripple_ff_txndata*)data;

    xid = txndata->header.transid;

    stmt = (ripple_txnstmt*)txndata->data;

    /* 接收到事务结束标识 */
    elog(RLOG_DEBUG, "txn nonlinerefreshen apply, xid:%lu", txndata->header.transid);

    cur_txn = ripple_txn_init(xid, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "onlinefresh end init txn error");
        return false;
    }
    parsertrail->lasttxn = cur_txn;

    RIPPLE_TXN_SET_ONLINEREFRESHTXN(cur_txn->flag);
    cur_txn->type = RIPPLE_TXN_TYPE_ONLINEREFRESH_END;
    cur_txn->start.wal.lsn = stmt->extra0.wal.lsn;
    cur_txn->confirm.wal.lsn = stmt->extra0.wal.lsn;

    /* 设置segno */
    record = (ripple_record*)(parsertrail->ffsmgrstate->fdata->extradata);
    cur_txn->end.trail.offset = record->end.trail.offset;
    cur_txn->segno = record->end.trail.fileid;

    elog(RLOG_DEBUG, "then begin of transaction:%lu", txndata->header.transid);

    cur_txn->stmts = lappend(cur_txn->stmts, stmt);
    cur_txn->stmtsize += stmt->len;
    parsertrail->transcache->totalsize += stmt->len;

    elog(RLOG_DEBUG, "then end of transaction:%lu", txndata->header.transid);

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        ripple_parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}