#include "app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_bigtxnbegin.h"

void txnstmt_bigtxnbegin_free(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
