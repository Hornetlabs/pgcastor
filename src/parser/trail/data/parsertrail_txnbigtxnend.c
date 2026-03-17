
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
#include "parser/trail/data/parsertrail_txnbigtxnend.h"

bool parsertrail_txnbigtxnendapply(parsertrail* parsertrail, void* data)
{
    /* 将大事务在hash中移除, 并添加大事务结束加入到完成列表中 */
    bool find = false;
    FullTransactionId xid = InvalidFullTransactionId;
    txnstmt* rstmt = NULL;
    txn* bigtxn = NULL;
    record* record_obj = NULL;
    ff_txndata* txndata = NULL;
    bigtxn_end_stmt* bigtxnendstmt = NULL;

    if(NULL == data)
    {
        return true;
    }

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;
    bigtxnendstmt = (bigtxn_end_stmt*)rstmt->stmt;
    xid = bigtxnendstmt->xid;

    elog(RLOG_DEBUG, "txnbigtxn end:%lu", xid);

    /* 将数据放入到缓存当中 */
    hash_search(parsertrail->transcache->by_txns, &xid, HASH_FIND, &find);
    if (find)
    {
        transcache_removeTXNByXid(parsertrail->transcache, xid);
    }

    bigtxn = txn_initbigtxn(xid);
    if(NULL == bigtxn)
    {
        elog(RLOG_WARNING, "init big txn error, xid:%lu", xid);
        return false;
    }
    parsertrail->lasttxn = bigtxn;

    bigtxn->stmts = lappend(bigtxn->stmts, rstmt);
    txndata->data = NULL;
    TXN_SET_BIGTXN(bigtxn->flag);
    if(bigtxnendstmt->commit)
    {
        bigtxn->type = TXN_TYPE_BIGTXN_END_COMMIT;
    }
    else
    {
        bigtxn->type = TXN_TYPE_BIGTXN_END_ABORT;
    }

    bigtxn->start.wal.lsn = rstmt->extra0.wal.lsn;
    bigtxn->stmtsize += rstmt->len;
    parsertrail->transcache->totalsize += rstmt->len;

    record_obj = (record*)(parsertrail->ffsmgrstate->fdata->extradata);
    bigtxn->segno = record_obj->end.trail.fileid;
    bigtxn->end.trail.offset = record_obj->end.trail.offset;
    bigtxn->confirm.wal.lsn = rstmt->extra0.wal.lsn;

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;

}
