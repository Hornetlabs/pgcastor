#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "fastcompare/ripple_fastcompare_columndefine.h"

ripple_fastcompare_columndefine *ripple_fastcompare_columndefine_init(void)
{
    ripple_fastcompare_columndefine *result = NULL;
    result = rmalloc0(sizeof(ripple_fastcompare_columndefine));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_columndefine));

    return result;
}

void ripple_fastcompare_columndefine_list_clean(List *col_list)
{
    if (col_list)
    {
        ListCell *cell = NULL;
        foreach(cell, col_list)
        {
            ripple_fastcompare_columndefine *col = (ripple_fastcompare_columndefine *)lfirst(cell);
            ripple_fastcompare_columndefine_clean(col);
        }
        list_free(col_list);
    }
}

void ripple_fastcompare_columndefine_clean(ripple_fastcompare_columndefine *col)
{
    if (col)
    {
        if (col->colname)
        {
            rfree(col->colname);
        }
        rfree(col);
    }
}
