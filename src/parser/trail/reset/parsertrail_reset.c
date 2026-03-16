#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "parser/trail/ripple_parsertrail.h"
#include "parser/trail/reset/ripple_parsertrail_reset.h"

/* 
 * Trail RESET 应用
 *  清理数据
 */
bool ripple_parsertrail_trailresetapply(ripple_parsertrail* parsertrail, void* data)
{
    /* 大事务相关暂时不需要*/
    ripple_txnstmt* rstmt = NULL;
    ripple_txn *resettxn = NULL;
    ripple_ff_reset* resetdata = NULL;
    ripple_txn *cur_txn = NULL;

    if(NULL == data)
    {
        return true;
    }

    /* 数据清理 */
    cur_txn = parsertrail->lasttxn;
    ripple_fftrail_invalidprivdata(RIPPLE_FFSMGR_IF_OPTYPE_DESERIAL, parsertrail->ffsmgrstate->fdata->ffdata);

    /* 清理所有未完成的事务 */
    if (cur_txn)
    {
        if(InvalidFullTransactionId != cur_txn->xid)
        {
            ripple_transcache_removeTXNByXid(parsertrail->transcache, cur_txn->xid);
            parsertrail->lasttxn = NULL;
        }
    }
    resetdata = (ripple_ff_reset*)data;

    /* 生成reset事务加入到缓存中 */
    resettxn = ripple_txn_init(RIPPLE_FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == resettxn)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }

    rstmt = ripple_txnstmt_init();
    if(NULL == rstmt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rstmt->extra0.wal.lsn = RIPPLE_MAX_LSN;
    resettxn->confirm.wal.lsn = RIPPLE_MAX_LSN;
    resettxn->stmts = lappend(resettxn->stmts, rstmt);
    rstmt->type = RIPPLE_TXNSTMT_TYPE_RESET;
    resettxn->type = RIPPLE_TXN_TYPE_RESET;
    resettxn->segno = resetdata->nexttrailno;

    parsertrail->dtxns = dlist_put(parsertrail->dtxns, resettxn);
    return true;
}

void ripple_parsertrail_trailresetclean(ripple_parsertrail* parsertrail, void* data)
{
    RIPPLE_UNUSED(parsertrail);

    /* 释放data */
    if (data)
    {
        rfree(data);
    }
}
