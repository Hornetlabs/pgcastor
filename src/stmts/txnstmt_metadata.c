#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_metadata.h"

ripple_txnstmt_metadata* ripple_txnstmt_metadata_init(void)
{
    ripple_txnstmt_metadata* metastmt = NULL;

    metastmt = rmalloc0(sizeof(ripple_txnstmt_metadata));
    if(NULL == metastmt)
    {
        elog(RLOG_WARNING, "init metadata stmt error");
        return NULL;
    }
    rmemset0(metastmt, 0, '\0', sizeof(ripple_txnstmt_metadata));
    return metastmt;
}

void ripple_txnstmt_metadatafree(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
