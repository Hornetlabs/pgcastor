#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_updaterewind.h"

/* 初始化 */
ripple_txnstmt_updaterewind* ripple_txnstmt_updaterewind_init(void)
{
    ripple_txnstmt_updaterewind* txnstmt = NULL;

    txnstmt = (ripple_txnstmt_updaterewind*)rmalloc1(sizeof(ripple_txnstmt_updaterewind));
    if(NULL == txnstmt)
    {
        elog(RLOG_WARNING,"txnstmt updaterewind init oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(txnstmt, 0, '\0', sizeof(ripple_txnstmt_updaterewind));
    txnstmt->rewind.trail.fileid = 0;
    txnstmt->rewind.trail.offset = 0;
    return txnstmt;
}

void ripple_txnstmt_updaterewind_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
