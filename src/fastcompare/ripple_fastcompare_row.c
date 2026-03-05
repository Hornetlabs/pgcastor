#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"
#include "fastcompare/ripple_fastcompare_row.h"

ripple_fastcompare_row *ripple_fastcompare_row_init(void)
{
    ripple_fastcompare_row *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_row));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result , 0, 0, sizeof(ripple_fastcompare_row));

    return result;
}

void ripple_fastcompare_row_column_init(ripple_fastcompare_row *row, int cnt)
{
    row->cnt = cnt;
    row->column = rmalloc0(sizeof(ripple_fastcompare_columnvalue) * cnt);
    if (!row->column)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(row->column , 0, 0, sizeof(ripple_fastcompare_columnvalue) * cnt);
}

void ripple_fastcompare_row_lsit_clean(List *row_list)
{
    if (row_list)
    {
        ListCell *cell = NULL;
        foreach(cell, row_list)
        {
            ripple_fastcompare_row *row = (ripple_fastcompare_row *)lfirst(cell);
            ripple_fastcompare_row_clean(row);
        }
        list_free(row_list);
    }
}

void ripple_fastcompare_row_clean(ripple_fastcompare_row *row)
{
    if (row)
    {
        if (row->column)
        {
            int index_col = 0;
            ripple_fastcompare_columnvalue *col = NULL;

            for (index_col = 0; index_col < row->cnt; index_col++)
            {
                col = &row->column[index_col];
                rfree(col->value);
            }
            rfree(row->column);
        }
        rfree(row);
    }
}
