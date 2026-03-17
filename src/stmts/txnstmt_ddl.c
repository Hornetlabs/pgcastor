#include "app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_ddl.h"

void txnstmt_ddlfree(void* data)
{
    txnstmt_ddl* txnstmtddl = NULL;

    if(NULL == data)
    {
        return;
    }

    txnstmtddl = (txnstmt_ddl*)data;

    if(NULL != txnstmtddl->ddlstmt)
    {
        rfree(txnstmtddl->ddlstmt);
    }
    rfree(txnstmtddl);
}
