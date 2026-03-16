#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_updatesynctable.h"

/* 初始化 */
ripple_txnstmt_updatesynctable* ripple_txnstmt_updatesynctable_init(void)
{
    ripple_txnstmt_updatesynctable* stmtupdatesynctable= NULL;

    stmtupdatesynctable = (ripple_txnstmt_updatesynctable*)rmalloc0(sizeof(ripple_txnstmt_updatesynctable));
    if(NULL == stmtupdatesynctable)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtupdatesynctable, 0, '\0', sizeof(ripple_txnstmt_updatesynctable));
    stmtupdatesynctable->lsn = InvalidXLogRecPtr;
    stmtupdatesynctable->offset = 0;
    stmtupdatesynctable->trailno = 0;
    stmtupdatesynctable->xid = InvalidFullTransactionId;
    return stmtupdatesynctable;
}

void ripple_txnstmt_updatesynctable_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}