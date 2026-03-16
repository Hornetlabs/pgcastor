
#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "task/ripple_task_slot.h"
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
#include "queue/ripple_queue.h"
#include "parser/trail/data/ripple_parsertrail_txnbigtxnend.h"

bool ripple_parsertrail_txnbigtxnendapply(ripple_parsertrail* parsertrail, void* data)
{
    /* 将大事务在hash中移除, 并添加大事务结束加入到完成列表中 */
    bool find = false;
    FullTransactionId xid = InvalidFullTransactionId;
    ripple_txnstmt* rstmt = NULL;
    ripple_txn* bigtxn = NULL;
    ripple_record* record = NULL;
    ripple_ff_txndata* txndata = NULL;
    ripple_bigtxn_end_stmt* bigtxnendstmt = NULL;

    if(NULL == data)
    {
        return true;
    }

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;
    bigtxnendstmt = (ripple_bigtxn_end_stmt*)rstmt->stmt;
    xid = bigtxnendstmt->xid;

    elog(RLOG_DEBUG, "txnbigtxn end:%lu", xid);

    /* 将数据放入到缓存当中 */
    hash_search(parsertrail->transcache->by_txns, &xid, HASH_FIND, &find);
    if (find)
    {
        ripple_transcache_removeTXNByXid(parsertrail->transcache, xid);
    }

    bigtxn = ripple_txn_initbigtxn(xid);
    if(NULL == bigtxn)
    {
        elog(RLOG_WARNING, "init big txn error, xid:%lu", xid);
        return false;
    }
    parsertrail->lasttxn = bigtxn;

    bigtxn->stmts = lappend(bigtxn->stmts, rstmt);
    txndata->data = NULL;
    RIPPLE_TXN_SET_BIGTXN(bigtxn->flag);
    if(bigtxnendstmt->commit)
    {
        bigtxn->type = RIPPLE_TXN_TYPE_BIGTXN_END_COMMIT;
    }
    else
    {
        bigtxn->type = RIPPLE_TXN_TYPE_BIGTXN_END_ABORT;
    }

    bigtxn->start.wal.lsn = rstmt->extra0.wal.lsn;
    bigtxn->stmtsize += rstmt->len;
    parsertrail->transcache->totalsize += rstmt->len;

    record = (ripple_record*)(parsertrail->ffsmgrstate->fdata->extradata);
    bigtxn->segno = record->end.trail.fileid;
    bigtxn->end.trail.offset = record->end.trail.offset;
    bigtxn->confirm.wal.lsn = rstmt->extra0.wal.lsn;

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        ripple_parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;

}
