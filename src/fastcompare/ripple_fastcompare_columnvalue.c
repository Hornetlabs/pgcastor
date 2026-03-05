#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/algorithm/crc/crc_check.h"
#include "fastcompare/ripple_fastcompare_columnvalue.h"

/*
 * 遍历一个传入的column list, 对其值和列编号进行crc计算
 * 由于crc对于 "abcd"+"efgh" 和 "ab"+"cdefgh"的运算结果是一致的
 * 因此引入一个额外的变量 列编号, 以消除整体相同值带来的影响
 */
uint32 ripple_fastcompare_columnvalue_list_crc(List *column_list)
{
    ListCell *cell = NULL;
    uint32  result = 0;

    if (!column_list)
    {
        elog(RLOG_ERROR, "column list is NULL");
    }

    INIT_CRC32C(result);
    foreach(cell, column_list)
    {
        ripple_fastcompare_columnvalue *col = (ripple_fastcompare_columnvalue *)lfirst(cell);

        /* 先对列编号进行crc运算 */
        COMP_CRC32C(result, &col->colid, sizeof(uint32));
        /* 再对列值进行crc运算 */
        COMP_CRC32C(result, col->value, col->len);
    }
    FIN_CRC32C(result);

    return result;
}

/* 遍历一个传入的column list, 清理 */
void ripple_fastcompare_columnvalue_list_clean(List *list)
{
    ListCell *cell = NULL;

    if (!list)
    {
        return;
    }

    foreach(cell, list)
    {
        ripple_fastcompare_columnvalue *col = (ripple_fastcompare_columnvalue *)lfirst(cell);
        ripple_fastcompare_columnvalue_clean(col);
    }

    list_free(list);
}

ripple_fastcompare_columnvalue *ripple_fastcompare_columnvalue_init(void)
{
    ripple_fastcompare_columnvalue *result = NULL;

    result = rmalloc0(sizeof(ripple_fastcompare_columnvalue));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_fastcompare_columnvalue));

    return result;
}

void ripple_fastcompare_columnvalue_clean(ripple_fastcompare_columnvalue *col)
{
    if (col)
    {
        if (col->value)
        {
            rfree(col->value);
        }
        rfree(col);
    }
}
