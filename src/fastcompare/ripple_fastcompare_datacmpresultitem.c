#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_datacmpresultitem.h"

ripple_fastcompare_datacmpresultitem *ripple_fastcompare_datacmpresultitem_init(int op)
{
    ripple_fastcompare_datacmpresultitem *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_datacmpresultitem));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result , 0, 0, sizeof(ripple_fastcompare_datacmpresultitem));

    result->op = op;

    return result;
}

void ripple_fastcompare_datacmpresultitem_free(ripple_fastcompare_datacmpresultitem *resultitem)
{
    if (NULL == resultitem)
    {
        return;
    }

    if (resultitem->privalues)
    {
        ripple_fastcompare_columnvalue_list_clean(resultitem->privalues);
    }
    
    rfree(resultitem);
}