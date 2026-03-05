#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "ripple_c.h"
#include "xsynch_fe.h"
#include "xscsci_output.h"

/* 输出结果 */
void xscsci_output(int rownumber, xsynchrow *rows)
{
    int colcnt          = 0;
    int indexcnt        = 0;
    int indexrow        = 0;
    int maxcollen       = 0;
    int *colwidth       = NULL;
    xsynchpair* col     = NULL;
    const char* value   = NULL;

    if (rownumber <= 0)
    {
        return;
    }

    colcnt = rows[0].columncnt;

    /* 计算每一列的最大宽度 */
    colwidth = (int*)malloc((sizeof(int) * colcnt));
    memset(colwidth, 0, (sizeof(int) * colcnt));

    for (indexcnt = 0; indexcnt < colcnt; indexcnt++)
    {
        /* 先用 key 的宽度初始化 */
        maxcollen = rows[0].columns[indexcnt].keylen;

        /* 搜索所有行的最大 value 长度 */
        for (indexrow = 0; indexrow < rownumber; indexrow++)
        {
            col = &rows[indexrow].columns[indexcnt];

            if (col->valuelen > maxcollen)
            {
                maxcollen = col->valuelen;
            }
        }

        if (maxcollen < 4)
        {
            maxcollen = 4;
        }

        colwidth[indexcnt] = maxcollen;
    }

    /* 输出列名 */
    for (indexcnt = 0; indexcnt < colcnt; indexcnt++)
    {
        col = &rows[0].columns[indexcnt];
        printf(" %-*.*s ", colwidth[indexcnt], colwidth[indexcnt], col->key);

        if (indexcnt != colcnt - 1)
        {
            printf("|");
        } 
    }
    printf("\n");

    /* 输出分割线 */
    for (indexcnt = 0; indexcnt < colcnt; indexcnt++)
    {
        int i = 0;
        for (i = 0; i < colwidth[indexcnt] + 2; i++)
        {
            printf("-");
        }
        if (indexcnt != colcnt - 1)
        {
            printf("+");
        }
    }
    printf("\n");

    /* 输出所有行的数据 */
    for (indexrow = 0; indexrow < rownumber; indexrow++)
    {
        for (indexcnt = 0; indexcnt < colcnt; indexcnt++)
        {
            col = &rows[indexrow].columns[indexcnt];

            value = col->value ? col->value : "NULL";
            printf(" %-*.*s ", colwidth[indexcnt], colwidth[indexcnt], value);

            if (indexcnt != colcnt - 1)
            {
                printf("|");
            } 
        }
        printf("\n");
    }

    if (NULL != colwidth)
    {
        free(colwidth);
    }

    XsynchRowFree(rownumber, rows);
}
