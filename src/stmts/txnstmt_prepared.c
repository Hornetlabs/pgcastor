#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_prepared.h"
#include "works/parserwork/wal/ripple_decode_heap_util.h"

/* 初始化 */
ripple_txnstmt_prepared* ripple_txnstmt_prepared_init(void)
{
    ripple_txnstmt_prepared* stmtprepared = NULL;

    stmtprepared = (ripple_txnstmt_prepared*)rmalloc0(sizeof(ripple_txnstmt_prepared));
    if(NULL == stmtprepared)
    {
        elog(RLOG_WARNING,"txnstmt prepared oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtprepared, 0, '\0', sizeof(ripple_txnstmt_prepared));
    stmtprepared->preparedname = NULL;
    stmtprepared->preparedsql = NULL;
    stmtprepared->values = NULL;
    stmtprepared->valuecnt = 0;
    stmtprepared->optype = 0;
    stmtprepared->row = NULL;
    return stmtprepared;
}

/* 释放 */
void ripple_txnstmt_prepared_free(void* data)
{
    uint32 index = 0;
    ripple_txnstmt_prepared* stmtprepared = NULL;
    if(NULL == data)
    {
        return;
    }

    stmtprepared = (ripple_txnstmt_prepared*)data;

    if(NULL != stmtprepared->preparedname)
    {
        rfree(stmtprepared->preparedname);
    }

    if(NULL != stmtprepared->preparedsql)
    {
        rfree(stmtprepared->preparedsql);
    }

    if(NULL != stmtprepared->values)
    {
        for(index = 0; index < stmtprepared->valuecnt; index++)
        {
            if(NULL != stmtprepared->values[index])
            {
                rfree(stmtprepared->values[index]);
            }
        }
        rfree(stmtprepared->values);
    }

    if (stmtprepared->row)
    {
        heap_free_trans_result((xk_pg_parser_translog_tbcolbase*)stmtprepared->row);
        
    }
    rfree(stmtprepared);
    stmtprepared = NULL;
}
