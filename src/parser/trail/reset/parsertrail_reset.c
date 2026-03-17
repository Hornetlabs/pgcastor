#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "parser/trail/reset/parsertrail_reset.h"

/* 
 * Trail RESET 应用
 *  清理数据
 */
bool parsertrail_trailresetapply(parsertrail* parsertrail, void* data)
{
    /* 大事务相关暂时不需要*/
    txnstmt* rstmt = NULL;
    txn *resettxn = NULL;
    ff_reset* resetdata = NULL;
    txn *cur_txn = NULL;

    if(NULL == data)
    {
        return true;
    }

    /* 数据清理 */
    cur_txn = parsertrail->lasttxn;
    fftrail_invalidprivdata(FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);

    /* 清理所有未完成的事务 */
    if (cur_txn)
    {
        if(InvalidFullTransactionId != cur_txn->xid)
        {
            transcache_removeTXNByXid(parsertrail->transcache, cur_txn->xid);
            parsertrail->lasttxn = NULL;
        }
    }
    resetdata = (ff_reset*)data;

    /* 生成reset事务加入到缓存中 */
    resettxn = txn_init(FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == resettxn)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }

    rstmt = txnstmt_init();
    if(NULL == rstmt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rstmt->extra0.wal.lsn = MAX_LSN;
    resettxn->confirm.wal.lsn = MAX_LSN;
    resettxn->stmts = lappend(resettxn->stmts, rstmt);
    rstmt->type = TXNSTMT_TYPE_RESET;
    resettxn->type = TXN_TYPE_RESET;
    resettxn->segno = resetdata->nexttrailno;

    parsertrail->dtxns = dlist_put(parsertrail->dtxns, resettxn);
    return true;
}

void parsertrail_trailresetclean(parsertrail* parsertrail, void* data)
{
    UNUSED(parsertrail);

    /* 释放data */
    if (data)
    {
        rfree(data);
    }
}
