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
#include "loadrecords/ripple_record.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/data/ripple_parsertrail_txnupdate.h"

static void ripple_parsertrail_txnupdate2hash(ripple_parsertrail* parsertrail,
                                            ripple_ff_txndata* txndata)
{
    /* 
     * 1、查看事务是否存在，不存在则创建，存在则append
     * 2、返回
     */
    uint16 index                                    = 0;
    ripple_txnstmt* rstmt                           = NULL;
    xk_pg_parser_translog_tbcol_values* colvalues   = NULL;

    rstmt = (ripple_txnstmt*)txndata->data;

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
            //初始化
            ripple_txn_initset(txn_entry, xid, InvalidXLogRecPtr);
        }
        else
        {
            parsertrail->dtxns = dlist_put(parsertrail->dtxns, ripple_txn_initabandon(txn_entry));
        }
        parsertrail->lasttxn = txn_entry;
        parsertrail->lasttxn->start.wal.lsn = rstmt->extra0.wal.lsn;

        elog(RLOG_DEBUG, "then begin of transaction:%lu", txndata->header.transid);
    }

    if(RIPPLE_FF_DATA_TRANSIND_IN == (RIPPLE_FF_DATA_TRANSIND_IN & txndata->header.transind))
    {
        /* 在hash中查看是否存在，不存在则添加 */
        if (!parsertrail->lasttxn)
        {
            elog(RLOG_ERROR, "missing trans start, transid:%lu", txndata->header.transid);
        }
        elog(RLOG_DEBUG, "part of transaction:%lu", txndata->header.transid);
    }
    colvalues = (xk_pg_parser_translog_tbcol_values*)rstmt->stmt;
    parsertrail->lasttxn->stmts = lappend(parsertrail->lasttxn->stmts, rstmt);
    parsertrail->lasttxn->stmtsize += rstmt->len;
    parsertrail->transcache->totalsize += rstmt->len;

    /* 输出内容 */
    if(RLOG_DEBUG == g_loglevel)
    {
        elog(RLOG_DEBUG, "update %s.%s begin", colvalues->m_base.m_schemaname, colvalues->m_base.m_tbname);
        for(index = 0; index < colvalues->m_valueCnt; index++)
        {
            elog(RLOG_DEBUG, "column:%s, value:%s", colvalues->m_new_values[index].m_colName,
                                                    colvalues->m_new_values[index].m_value == NULL ? "NULL" : (char*)(colvalues->m_new_values[index].m_value));
            elog(RLOG_DEBUG, "column:%s, value:%s", colvalues->m_old_values[index].m_colName,
                                                    colvalues->m_old_values[index].m_value == NULL ? "NULL" : (char*)(colvalues->m_old_values[index].m_value));
        }
        elog(RLOG_DEBUG, "update %s.%s end", colvalues->m_base.m_schemaname, colvalues->m_base.m_tbname);
    }

    return;
}

/* 将表数据加入到事务缓存中 */
bool ripple_parsertrail_txnupdateapply(ripple_parsertrail* parsertrail, void* data)
{
    ripple_ff_txndata* txndata = NULL;

    if(NULL == data)
    {
        return true;
    }

    txndata = (ripple_ff_txndata*)data;

    /* 将数据放入到缓存当中 */
    ripple_parsertrail_txnupdate2hash(parsertrail, txndata);

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(RIPPLE_FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        ripple_parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}

