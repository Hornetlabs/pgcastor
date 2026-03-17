
#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "task/task_slot.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
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
    /* 将大事务在hash中移除, 并添加大事务结束加入到完成列表中 */
    txnstmt* rstmt = NULL;
    txn* cur_txn = NULL;
    record* record_obj = NULL;
    ff_txndata* txndata = NULL;

    if(NULL == data)
    {
        return true;
    }

    txndata = (ff_txndata*)data;
    rstmt = (txnstmt*)txndata->data;

    cur_txn = txn_init(FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    parsertrail->lasttxn = cur_txn;

    cur_txn->type = TXN_TYPE_ONLINEREFRESH_ABANDON;
    cur_txn->start.wal.lsn = rstmt->extra0.wal.lsn;
    cur_txn->confirm.wal.lsn = MAX_LSN;

    /* 设置segno */
    record_obj = (record*)(parsertrail->ffsmgrstate->fdata->extradata);
    cur_txn->end.trail.offset = record_obj->end.trail.offset;
    cur_txn->segno = record_obj->end.trail.fileid;

    cur_txn->stmts = lappend(cur_txn->stmts, rstmt);
    cur_txn->stmtsize += rstmt->len;

    elog(RLOG_INFO, "then onlinerefresh abandon");

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;

}
