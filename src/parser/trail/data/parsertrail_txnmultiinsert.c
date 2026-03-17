#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
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
#include "loadrecords/record.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/data/parsertrail_txnmultiinsert.h"

static void parsertrail_txnmultiinsert2hash(parsertrail*  parsertrail,
                                                        ff_txndata* txndata)
{
    /* 
     * 1、查看事务是否存在，不存在则创建，存在则append
     * 2、返回
     */
    uint16 index                                        = 0;
    int i                                               = 0;
    txnstmt* rstmt                               = NULL;
    xk_pg_parser_translog_tbcol_nvalues* colnvalues     = NULL;

    rstmt = (txnstmt*)txndata->data;

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
            //初始化
            txn_initset(txn_entry, xid, InvalidXLogRecPtr);
        }
        else
        {
            parsertrail->dtxns = dlist_put(parsertrail->dtxns, txn_initabandon(txn_entry));
        }
        parsertrail->lasttxn = txn_entry;
        parsertrail->lasttxn->start.wal.lsn = rstmt->extra0.wal.lsn;

        elog(RLOG_DEBUG, "then begin of transaction:%lu", txndata->header.transid);
    }

    if(FF_DATA_TRANSIND_IN == (FF_DATA_TRANSIND_IN & txndata->header.transind))
    {
        /* 在hash中查看是否存在，不存在则添加 */
        if (!parsertrail->lasttxn)
        {
            elog(RLOG_ERROR, "missing trans start, transid:%lu", txndata->header.transid);
        }
        elog(RLOG_DEBUG, "part of transaction:%lu", txndata->header.transid);
    }
    colnvalues = (xk_pg_parser_translog_tbcol_nvalues*)rstmt->stmt;
    parsertrail->lasttxn->stmts = lappend(parsertrail->lasttxn->stmts, rstmt);
    parsertrail->lasttxn->stmtsize += rstmt->len;
    parsertrail->transcache->totalsize += rstmt->len;

    /* 输出内容 */
    if(RLOG_DEBUG == g_loglevel)
    {
        elog(RLOG_DEBUG, "multiinsert into %s.%s, colnvalues->m_rowCnt:%u colnvalues->m_valueCnt:%u begin", 
                        colnvalues->m_base.m_schemaname, colnvalues->m_base.m_tbname, colnvalues->m_rowCnt, colnvalues->m_valueCnt);
        for (i = 0; i < colnvalues->m_rowCnt; i++)
        {
            for(index = 0; index < colnvalues->m_valueCnt; index++)
            {
                elog(RLOG_DEBUG, "column:%s, value:%s", colnvalues->m_rows[i].m_new_values[index].m_colName,
                                                        colnvalues->m_rows[i].m_new_values[index].m_value == NULL ? "NULL" : (char*)(colnvalues->m_rows[i].m_new_values[index].m_value));
            }
        }
        elog(RLOG_DEBUG, "multiinsert %s.%s end", colnvalues->m_base.m_schemaname, colnvalues->m_base.m_tbname);
    }

    return;
}

/* 将表数据加入到事务缓存中 */
bool parsertrail_txnmultiinsertapply(parsertrail*  parsertrail,
                                                void* data)
{
    ff_txndata* txndata = NULL;

    if(NULL == data)
    {
        return true;
    }

    txndata = (ff_txndata*)data;

    /* 将数据放入到缓存当中 */
    parsertrail_txnmultiinsert2hash(parsertrail, txndata);

    /* 查看是否发生了切换，发生切换那么需要清理缓存 */
    if(FFSMGR_STATUS_SHIFTFILE == parsertrail->ffsmgrstate->status)
    {
        /* 交换 */
        parsertrail_traildata_shiftfile(parsertrail);
    }

    return true;
}



