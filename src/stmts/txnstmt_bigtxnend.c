#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_bigtxnend.h"

ripple_bigtxn_end_stmt* ripple_txnstmt_bigtxnend_init(void)
{
    ripple_bigtxn_end_stmt* endstmt = NULL;
    endstmt = rmalloc0(sizeof(ripple_bigtxn_end_stmt));
    if (NULL == endstmt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(endstmt, 0, 0, sizeof(ripple_bigtxn_end_stmt));
    return endstmt;
}


void ripple_txnstmt_bigtxnend_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
