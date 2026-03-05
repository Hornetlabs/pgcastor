#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_shiftfile.h"

void ripple_txnstmt_shiftfilefree(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}