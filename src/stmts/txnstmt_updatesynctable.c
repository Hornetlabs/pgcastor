#include "app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_updatesynctable.h"

/* initialize */
txnstmt_updatesynctable* txnstmt_updatesynctable_init(void)
{
    txnstmt_updatesynctable* stmtupdatesynctable = NULL;

    stmtupdatesynctable = (txnstmt_updatesynctable*)rmalloc0(sizeof(txnstmt_updatesynctable));
    if (NULL == stmtupdatesynctable)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtupdatesynctable, 0, '\0', sizeof(txnstmt_updatesynctable));
    stmtupdatesynctable->lsn = InvalidXLogRecPtr;
    stmtupdatesynctable->offset = 0;
    stmtupdatesynctable->trailno = 0;
    stmtupdatesynctable->xid = InvalidFullTransactionId;
    return stmtupdatesynctable;
}

void txnstmt_updatesynctable_free(void* data)
{
    if (NULL == data)
    {
        return;
    }

    rfree(data);
}