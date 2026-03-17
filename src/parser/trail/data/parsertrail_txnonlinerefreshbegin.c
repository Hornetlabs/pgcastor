#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "task/task_slot.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "loadrecords/record.h"
#include "onlinerefresh/onlinerefresh_persist.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_txnonlinerefreshbegin.h"

/* 将表数据加入到事务缓存中 */
bool parsertrail_txnonlinerefreshbeginapply(parsertrail* parsertrail,
                                                    void* data)
{
    FullTransactionId xid                               = InvalidFullTransactionId;
    txn* cur_txn                                 = NULL;
    txnstmt* stmt                                = NULL;
    record* record_obj                               = NULL;
    ff_txndata* txndata                          = NULL;

    if(NULL == data)
    {
        return true;
    }

    txndata = (ff_txndata*)data;

    xid = txndata->header.transid;

    stmt = (txnstmt*)txndata->data;

    /* 接收到事务结束标识 */
    elog(RLOG_DEBUG, "txn onlinerefreshbegin apply, xid:%lu", txndata->header.transid);

    cur_txn = txn_init(xid, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    parsertrail->lasttxn = cur_txn;

    TXN_SET_ONLINEREFRESHTXN(cur_txn->flag);
    cur_txn->type = TXN_TYPE_ONLINEREFRESH_BEGIN;
    cur_txn->start.wal.lsn = stmt->extra0.wal.lsn;
    cur_txn->confirm.wal.lsn = stmt->extra0.wal.lsn;

    /* 设置segno */
    record_obj = (record*)(parsertrail->ffsmgrstate->fdata->extradata);
    cur_txn->end.trail.offset = record_obj->end.trail.offset;
    cur_txn->segno = record_obj->end.trail.fileid;

    elog(RLOG_DEBUG, "then begin of transaction:%lu", txndata->header.transid);

    cur_txn->stmts = lappend(cur_txn->stmts, stmt);
    cur_txn->stmtsize += stmt->len;
    parsertrail->transcache->totalsize += stmt->len;

    elog(RLOG_DEBUG, "then begin of transaction:%lu", txndata->header.transid);

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}