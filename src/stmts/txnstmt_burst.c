#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_burst.h"
#include "works/parserwork/wal/decode_heap_util.h"

/* Initialization */
txnstmt_burst* txnstmt_burst_init(void)
{
    txnstmt_burst* stmtburst = NULL;

    stmtburst = (txnstmt_burst*)rmalloc0(sizeof(txnstmt_burst));
    if (NULL == stmtburst)
    {
        elog(RLOG_WARNING, "txnstmt burst oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtburst, 0, '\0', sizeof(txnstmt_burst));
    stmtburst->optype = TXNSTMT_TYPE_BURST;
    stmtburst->batchcmd = NULL;
    stmtburst->rows = NULL;
    return stmtburst;
}

/* Free function for stmtburst->rows dlist */
static void txnstmt_burstrow_free(void* data)
{
    if (NULL == data)
    {
        return;
    }

    heap_free_trans_result((pg_parser_translog_tbcolbase*)data);
    return;
}

/* Release */
void txnstmt_burst_free(void* data)
{
    txnstmt_burst* stmtburst = NULL;
    if (NULL == data)
    {
        return;
    }

    stmtburst = (txnstmt_burst*)data;

    if (false == dlist_isnull(stmtburst->rows))
    {
        dlist_free(stmtburst->rows, txnstmt_burstrow_free);
    }

    if (stmtburst->batchcmd)
    {
        rfree(stmtburst->batchcmd);
    }

    rfree(stmtburst);
}
