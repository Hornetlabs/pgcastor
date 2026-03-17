#include "app_incl.h"
#include "utils/list/list_func.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_metadata.h"

txnstmt_metadata* txnstmt_metadata_init(void)
{
    txnstmt_metadata* metastmt = NULL;

    metastmt = rmalloc0(sizeof(txnstmt_metadata));
    if(NULL == metastmt)
    {
        elog(RLOG_WARNING, "init metadata stmt error");
        return NULL;
    }
    rmemset0(metastmt, 0, '\0', sizeof(txnstmt_metadata));
    return metastmt;
}

void txnstmt_metadatafree(void* data)
{
    if(NULL == data)
    {
        return;
    }

    rfree(data);
}
