#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_commit.h"

ripple_commit_stmt* ripple_txnstmt_commit_init(void)
{
    ripple_commit_stmt* endstmt = NULL;
    endstmt = rmalloc0(sizeof(ripple_commit_stmt));
    if (NULL == endstmt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(endstmt, 0, 0, sizeof(ripple_commit_stmt));
    endstmt->endtimestamp = 0;
    return endstmt;
}


void ripple_txnstmt_commit_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
