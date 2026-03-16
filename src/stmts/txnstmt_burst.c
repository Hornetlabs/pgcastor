#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_burst.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"

/* 初始化 */
ripple_txnstmt_burst* ripple_txnstmt_burst_init(void)
{
    ripple_txnstmt_burst* stmtburst = NULL;

    stmtburst = (ripple_txnstmt_burst*)rmalloc0(sizeof(ripple_txnstmt_burst));
    if(NULL == stmtburst)
    {
        elog(RLOG_WARNING,"txnstmt burst oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtburst, 0, '\0', sizeof(ripple_txnstmt_burst));
    stmtburst->optype = RIPPLE_TXNSTMT_TYPE_BURST;
    stmtburst->batchcmd = NULL;
    stmtburst->rows = NULL;
    return stmtburst;
}

/* stmtburst->rows dlist释放函数 */
static void ripple_txnstmt_burstrow_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    heap_free_trans_result((xk_pg_parser_translog_tbcolbase*)data);
    return;
}

/* 释放 */
void ripple_txnstmt_burst_free(void* data)
{
    ripple_txnstmt_burst* stmtburst = NULL;
    if(NULL == data)
    {
        return;
    }

    stmtburst = (ripple_txnstmt_burst*)data;

    if (false == dlist_isnull(stmtburst->rows))
    {
        dlist_free(stmtburst->rows, ripple_txnstmt_burstrow_free);
    }

    if (stmtburst->batchcmd)
    {
        rfree(stmtburst->batchcmd);
    }

    rfree(stmtburst);
}
