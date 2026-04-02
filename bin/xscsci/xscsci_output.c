#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "app_c.h"
#include "pgcastor_fe.h"
#include "xscsci_output.h"

/* output results */
void xscsci_output(int rownumber, pgcastorrow* rows)
{
    int         colcnt = 0;
    int         indexcnt = 0;
    int         indexrow = 0;
    int         maxcollen = 0;
    int*        colwidth = NULL;
    pgcastorpair* col = NULL;
    const char* value = NULL;

    if (rownumber <= 0)
    {
        return;
    }

    colcnt = rows[0].columncnt;

    /* calculate maximum width for each column */
    colwidth = (int*)malloc((sizeof(int) * colcnt));
    memset(colwidth, 0, (sizeof(int) * colcnt));

    for (indexcnt = 0; indexcnt < colcnt; indexcnt++)
    {
        /* initialize with key width first */
        maxcollen = rows[0].columns[indexcnt].keylen;

        /* search for maximum value length across all rows */
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

    /* output column names */
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

    /* output separator line */
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

    /* output all row data */
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

    PGCastorRowFree(rownumber, rows);
}
