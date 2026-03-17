#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "queue/queue.h"
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
#include "parser/trail/data/parsertrail_txnbigtxnbegin.h"

static void parsertrail_txnbigtxnbegin2hash(parsertrail* parsertrail,
                                                        ff_txndata* txndata)
{
    /* 
     * 1、查看事务是否存在，不存在则创建，存在则append
     * 2、返回
     */
    txnstmt* rstmt = NULL;
    record* record_obj = NULL;

    rstmt = (txnstmt*)txndata->data;

    elog(RLOG_DEBUG, "txnbigtxn begin:%lu", txndata->header.transid);

    /* 查看类型 */
    if(FF_DATA_TRANSIND_START == (FF_DATA_TRANSIND_START & txndata->header.transind))
    {
        /* 在hash中查看是否存在，不存在则添加 */
        HTAB *tx_htab = parsertrail->transcache->by_txns;
        txn *txn_entry = NULL;
        bool find = false;
        FullTransactionId xid = txndata->header.transid;

        /* 查找哈希 */
        txn_entry = (txn *) hash_search(tx_htab, &xid, HASH_ENTER, &find);
        if (!find)
        {
            /* 初始化 */
            txn_initset(txn_entry, xid, InvalidXLogRecPtr);
        }
        else
        {
            /* 添加abandon */
            elog(RLOG_WARNING, "txnbigtxn begin the same transaction in hash:%lu", txndata->header.transid);
        }

        /* 标识为大事务 */
        TXN_SET_BIGTXN(txn_entry->flag);
        txn_entry->type = TXN_TYPE_BIGTXN_BEGIN;
        parsertrail->lasttxn = txn_entry;
        parsertrail->lasttxn->start.wal.lsn = rstmt->extra0.wal.lsn;
        parsertrail->lasttxn->stmts = lappend(parsertrail->lasttxn->stmts, rstmt);
        parsertrail->lasttxn->stmtsize += rstmt->len;
        parsertrail->transcache->totalsize += rstmt->len;

        elog(RLOG_DEBUG, "then txnbigtxn begin of transaction:%lu", txndata->header.transid);
    }
    record_obj = (record*)(parsertrail->ffsmgrstate->fdata->extradata);
    parsertrail->lasttxn->segno = record_obj->end.trail.fileid;
    parsertrail->lasttxn->end.trail.offset = record_obj->end.trail.offset;
    parsertrail->lasttxn->confirm.wal.lsn = rstmt->extra0.wal.lsn;

    return;
}


bool parsertrail_txnbigtxnbeginapply(parsertrail* parsertrail, void* data)
{
    ff_txndata* txndata = NULL;

    if(NULL == data)
    {
        return true;
    }

    /* 将数据放入到缓存当中 */
    txndata = (ff_txndata*)data;
    parsertrail_txnbigtxnbegin2hash(parsertrail, txndata);

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;

}
