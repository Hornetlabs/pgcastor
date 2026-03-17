#include "app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_bigtxnend.h"

bigtxn_end_stmt* txnstmt_bigtxnend_init(void)
{
    bigtxn_end_stmt* endstmt = NULL;
    endstmt = rmalloc0(sizeof(bigtxn_end_stmt));
    if (NULL == endstmt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(endstmt, 0, 0, sizeof(bigtxn_end_stmt));
    return endstmt;
}


void txnstmt_bigtxnend_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
