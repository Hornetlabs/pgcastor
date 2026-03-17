#include "app_incl.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_onlinerefresh_dataset.h"
#include "refresh/refresh_tables.h"
#include "cache/transcache.h"

void *parserwork_build_onlinerefresh_dataset_txn(List *tables_list)
{
    txn *dataset_txn = NULL;
    txnstmt *stmt = NULL;

    dataset_txn = txn_init(FROZEN_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == dataset_txn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt *) rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_ONLINEREFRESH_DATASET;

    stmt->stmt = (void*)tables_list;
    dataset_txn->type = TXN_TYPE_ONLINEREFRESH_DATASET;
    dataset_txn->stmts = lappend(dataset_txn->stmts, stmt);

    return (void *)dataset_txn;
}

void txnstmt_onlinerefresh_dataset_free(void* data)
{
    /* 不在此释放 */
}