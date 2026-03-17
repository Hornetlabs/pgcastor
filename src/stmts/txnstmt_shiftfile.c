#include "app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_shiftfile.h"

void txnstmt_shiftfilefree(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}