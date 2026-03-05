#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_reset.h"

void ripple_txnstmt_reset_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
