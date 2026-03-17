#include "app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_commit.h"

commit_stmt* txnstmt_commit_init(void)
{
    commit_stmt* endstmt = NULL;
    endstmt = rmalloc0(sizeof(commit_stmt));
    if (NULL == endstmt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(endstmt, 0, 0, sizeof(commit_stmt));
    endstmt->endtimestamp = 0;
    return endstmt;
}


void txnstmt_commit_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
