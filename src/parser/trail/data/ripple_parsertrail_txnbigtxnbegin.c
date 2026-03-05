#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "queue/ripple_queue.h"
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
#include "parser/trail/data/ripple_parsertrail_txnbigtxnbegin.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"

static void ripple_parsertrail_txnbigtxnbegin2hash(ripple_parsertrail* parsertrail,
                                                        ripple_ff_txndata* txndata)
{
    /* 
     * 1、查看事务是否存在，不存在则创建，存在则append
     * 2、返回
     */
    ripple_txnstmt* rstmt = NULL;
    ripple_record* record = NULL;

    rstmt = (ripple_txnstmt*)txndata->data;

    elog(RLOG_DEBUG, "txnbigtxn begin:%lu", txndata->header.transid);

    /* 查看类型 */
    if(RIPPLE_FF_DATA_TRANSIND_START == (RIPPLE_FF_DATA_TRANSIND_START & txndata->header.transind))
    {
        /* 在hash中查看是否存在，不存在则添加 */
        HTAB *tx_htab = parsertrail->transcache->by_txns;
        ripple_txn *txn_entry = NULL;
        bool find = false;
        FullTransactionId xid = txndata->header.transid;

        /* 查找哈希 */
        txn_entry = (ripple_txn *) hash_search(tx_htab, &xid, HASH_ENTER, &find);
        if (!find)
        {
            /* 初始化 */
            ripple_txn_initset(txn_entry, xid, InvalidXLogRecPtr);
        }
        else
        {
            /* 添加abandon */
            elog(RLOG_WARNING, "txnbigtxn begin the same transaction in hash:%lu", txndata->header.transid);
        }

        /* 标识为大事务 */
        RIPPLE_TXN_SET_BIGTXN(txn_entry->flag);
        txn_entry->type = RIPPLE_TXN_TYPE_BIGTXN_BEGIN;
        parsertrail->lasttxn = txn_entry;
        parsertrail->lasttxn->start.wal.lsn = rstmt->extra0.wal.lsn;
        parsertrail->lasttxn->stmts = lappend(parsertrail->lasttxn->stmts, rstmt);
        parsertrail->lasttxn->stmtsize += rstmt->len;
        parsertrail->transcache->totalsize += rstmt->len;

        elog(RLOG_DEBUG, "then txnbigtxn begin of transaction:%lu", txndata->header.transid);
    }
    record = (ripple_record*)(parsertrail->ffsmgrstate->fdata->extradata);
    parsertrail->lasttxn->segno = record->end.trail.fileid;
    parsertrail->lasttxn->end.trail.offset = record->end.trail.offset;
    parsertrail->lasttxn->confirm.wal.lsn = rstmt->extra0.wal.lsn;

    return;
}


bool ripple_parsertrail_txnbigtxnbeginapply(ripple_parsertrail* parsertrail, void* data)
{
    ripple_ff_txndata* txndata = NULL;

    if(NULL == data)
    {
        return true;
    }

    /* 将数据放入到缓存当中 */
    txndata = (ripple_ff_txndata*)data;
    ripple_parsertrail_txnbigtxnbegin2hash(parsertrail, txndata);

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        ripple_parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;

}
