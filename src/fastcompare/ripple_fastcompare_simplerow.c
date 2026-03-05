#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/algorithm/crc/crc_check.h"
#include "fastcompare/ripple_fastcompare_simplerow.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"

ripple_fastcompare_simplerow *ripple_fastcompare_simplerow_init(void)
{
    ripple_fastcompare_simplerow *result = NULL;
    result = rmalloc0(sizeof(ripple_fastcompare_simplerow));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_simplerow));

    INIT_CRC32C(result->crc);
    return result;
}

void ripple_fastcompare_simplerow_list_clean(List *row_list)
{
    if (row_list)
    {
        ListCell *cell = NULL;
        foreach(cell, row_list)
        {
            ripple_fastcompare_simplerow *row = (ripple_fastcompare_simplerow *)lfirst(cell);
            ripple_fastcompare_simplerow_clean(row);
        }
        list_free(row_list);
    }
}

void ripple_fastcompare_simplerow_clean(ripple_fastcompare_simplerow *row)
{
    if (row)
    {
        if (row->privalues)
        {
            ripple_fastcompare_columnvalue_list_clean(row->privalues);
        }
        rfree(row);
    }
}

void ripple_fastcompare_simplerow_crcComp(ripple_fastcompare_simplerow *row,
                                          void *data,
                                          uint32 len)
{
    COMP_CRC32C(row->crc, data, len);
}

void ripple_fastcompare_simplerow_crcFin(ripple_fastcompare_simplerow *row)
{
    FIN_CRC32C(row->crc);
}
