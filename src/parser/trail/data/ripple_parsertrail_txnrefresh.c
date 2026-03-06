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
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "threads/ripple_threads.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "loadrecords/ripple_record.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/data/ripple_parsertrail_txnrefresh.h"
#include "works/dyworks/ripple_dyworks.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/integrate/ripple_refresh_integrate.h"
#include "increment/integrate/parser/ripple_increment_integrateparsertrail.h"

static bool ripple_parsertrail_txnrefresh2cache(ripple_parsertrail* parsertrail,
                                                ripple_ff_txndata* txndata)
{
    /* 
     * 1、查看事务是否存在，不存在则创建，存在则append
     * 2、返回
     */
    ripple_txn* cur_txn                             = NULL;
    ripple_txnstmt* rstmt                           = NULL;
    ripple_record* record                           = NULL;
    ripple_refresh_table* table                     = NULL;
    ripple_refresh_tables* refreshstmt              = NULL;
    FullTransactionId xid                           = txndata->header.transid;

    rstmt = (ripple_txnstmt*)txndata->data;
    cur_txn = ripple_txn_init(xid, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "out of memory");
        return false;
    }
    parsertrail->lasttxn = cur_txn;
    cur_txn->type = RIPPLE_TXN_TYPE_REFRESH;
    cur_txn->start.wal.lsn = rstmt->extra0.wal.lsn;
    cur_txn->confirm.wal.lsn = rstmt->extra0.wal.lsn;

    /* 设置segno */
    record = (ripple_record*)(parsertrail->ffsmgrstate->fdata->extradata);
    cur_txn->end.trail.offset = record->end.trail.offset;
    cur_txn->segno = record->end.trail.fileid;

    elog(RLOG_DEBUG, "then begin of transaction:%lu", txndata->header.transid);

    refreshstmt = (ripple_refresh_tables*)rstmt->stmt;
    table = refreshstmt->tables;
    
    cur_txn->stmts = lappend(cur_txn->stmts, rstmt);
    cur_txn->stmtsize += rstmt->len;
    parsertrail->transcache->totalsize += rstmt->len;

    /* 输出内容 */
    if(RLOG_DEBUG == g_loglevel)
    {
        while(NULL != table)
        {
            elog(RLOG_DEBUG, "refresh is %s.%s", table->schema, table->table);
            table = table->next;
        }
    }

    elog(RLOG_DEBUG, "then end of transaction:%lu", txndata->header.transid);

    return true;
}

/* 将表数据加入到事务缓存中 */
bool ripple_parsertrail_txnrefreshapply(ripple_parsertrail* parsertrail, void* data)
{
    ripple_ff_txndata* txndata = NULL;

    if(NULL == data)
    {
        return true;
    }

    txndata = (ripple_ff_txndata*)data;

    /* 将数据放入到缓存当中 */
    ripple_parsertrail_txnrefresh2cache(parsertrail, txndata);

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        ripple_parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}
