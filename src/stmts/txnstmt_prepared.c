#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_prepared.h"
#include "works/parserwork/wal/decode_heap_util.h"

/* Initialization */
txnstmt_prepared* txnstmt_prepared_init(void)
{
    txnstmt_prepared* stmtprepared = NULL;

    stmtprepared = (txnstmt_prepared*)rmalloc0(sizeof(txnstmt_prepared));
    if (NULL == stmtprepared)
    {
        elog(RLOG_WARNING, "txnstmt prepared oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtprepared, 0, '\0', sizeof(txnstmt_prepared));
    stmtprepared->preparedname = NULL;
    stmtprepared->preparedsql = NULL;
    stmtprepared->values = NULL;
    stmtprepared->valuecnt = 0;
    stmtprepared->optype = 0;
    stmtprepared->row = NULL;
    return stmtprepared;
}

/* Release */
void txnstmt_prepared_free(void* data)
{
    uint32            index = 0;
    txnstmt_prepared* stmtprepared = NULL;
    if (NULL == data)
    {
        return;
    }

    stmtprepared = (txnstmt_prepared*)data;

    if (NULL != stmtprepared->preparedname)
    {
        rfree(stmtprepared->preparedname);
    }

    if (NULL != stmtprepared->preparedsql)
    {
        rfree(stmtprepared->preparedsql);
    }

    if (NULL != stmtprepared->values)
    {
        for (index = 0; index < stmtprepared->valuecnt; index++)
        {
            if (NULL != stmtprepared->values[index])
            {
                rfree(stmtprepared->values[index]);
            }
        }
        rfree(stmtprepared->values);
    }

    if (stmtprepared->row)
    {
        heap_free_trans_result((pg_parser_translog_tbcolbase*)stmtprepared->row);
    }
    rfree(stmtprepared);
    stmtprepared = NULL;
}
