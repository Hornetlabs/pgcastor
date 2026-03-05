
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
#include "parser/trail/data/ripple_parsertrail_txnonlinerefreshabandon.h"

bool ripple_parsertrail_txntxnonlinerefreshabandonapply(ripple_parsertrail* parsertrail, void* data)
{
    /* 将大事务在hash中移除, 并添加大事务结束加入到完成列表中 */
    ripple_txnstmt* rstmt = NULL;
    ripple_txn* cur_txn = NULL;
    ripple_record* record = NULL;
    ripple_ff_txndata* txndata = NULL;

    if(NULL == data)
    {
        return true;
    }

    txndata = (ripple_ff_txndata*)data;
    rstmt = (ripple_txnstmt*)txndata->data;

    cur_txn = ripple_txn_init(RIPPLE_FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    parsertrail->lasttxn = cur_txn;

    cur_txn->type = RIPPLE_TXN_TYPE_ONLINEREFRESH_ABANDON;
    cur_txn->start.wal.lsn = rstmt->extra0.wal.lsn;
    cur_txn->confirm.wal.lsn = RIPPLE_MAX_LSN;

    /* 设置segno */
    record = (ripple_record*)(parsertrail->ffsmgrstate->fdata->extradata);
    cur_txn->end.trail.offset = record->end.trail.offset;
    cur_txn->segno = record->end.trail.fileid;

    cur_txn->stmts = lappend(cur_txn->stmts, rstmt);
    cur_txn->stmtsize += rstmt->len;

    elog(RLOG_INFO, "then onlinerefresh abandon");

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        ripple_parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;

}
