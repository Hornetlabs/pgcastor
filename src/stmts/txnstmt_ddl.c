#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_ddl.h"

void ripple_txnstmt_ddlfree(void* data)
{
    ripple_txnstmt_ddl* txnstmtddl = NULL;

    if(NULL == data)
    {
        return;
    }

    txnstmtddl = (ripple_txnstmt_ddl*)data;

    if(NULL != txnstmtddl->ddlstmt)
    {
        rfree(txnstmtddl->ddlstmt);
    }
    rfree(txnstmtddl);
}
