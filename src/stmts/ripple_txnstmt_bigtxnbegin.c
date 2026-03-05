#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_bigtxnbegin.h"

void ripple_txnstmt_bigtxnbegin_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
