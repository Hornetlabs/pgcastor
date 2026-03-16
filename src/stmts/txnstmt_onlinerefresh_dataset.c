#include "ripple_app_incl.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_onlinerefresh_dataset.h"
#include "refresh/ripple_refresh_tables.h"
#include "cache/ripple_transcache.h"

void *ripple_parserwork_build_onlinerefresh_dataset_txn(List *tables_list)
{
    ripple_txn *dataset_txn = NULL;
    ripple_txnstmt *stmt = NULL;

    dataset_txn = ripple_txn_init(RIPPLE_FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == dataset_txn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (ripple_txnstmt *) rmalloc0(sizeof(ripple_txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));

    stmt->type = RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_DATASET;

    stmt->stmt = (void*)tables_list;
    dataset_txn->type = RIPPLE_TXN_TYPE_ONLINEREFRESH_DATASET;
    dataset_txn->stmts = lappend(dataset_txn->stmts, stmt);

    return (void *)dataset_txn;
}

void ripple_txnstmt_onlinerefresh_dataset_free(void* data)
{
    /* 不在此释放 */
}