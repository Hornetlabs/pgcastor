#include "app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_updaterewind.h"

/* 初始化 */
txnstmt_updaterewind* txnstmt_updaterewind_init(void)
{
    txnstmt_updaterewind* txnstmt = NULL;

    txnstmt = (txnstmt_updaterewind*)rmalloc1(sizeof(txnstmt_updaterewind));
    if(NULL == txnstmt)
    {
        elog(RLOG_WARNING,"txnstmt updaterewind init oom %s", strerror(errno));
        return NULL;
    }
    rmemset0(txnstmt, 0, '\0', sizeof(txnstmt_updaterewind));
    txnstmt->rewind.trail.fileid = 0;
    txnstmt->rewind.trail.offset = 0;
    return txnstmt;
}

void txnstmt_updaterewind_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
