/**
 * @file pg_parser_thirdparty_tupleparser_int.c
 * @author bytesync
 * @brief
 * @version 0.1
 * @date 2023-08-03
 *
 * @copyright Copyright (c) 2023
 *
 */
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"
#include "thirdparty/tupleparser/toast/pg_parser_thirdparty_tupleparser_toast.h"
#define MAXINT8LEN 25

typedef struct
{
    int32_t  vl_len_;    /* these fields must match ArrayType! */
    int32_t  ndim;       /* always 1 for pg_parser_int2vector */
    int32_t  dataoffset; /* always 0 for pg_parser_int2vector */
    uint32_t elemtype;
    int32_t  dim1;
    int32_t  lbound1;
    int16_t  values[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_int2vector;

#define PGFUNC_INT_MCXT NULL

/*
 *        int2out            - converts short to "num"
 */
pg_parser_Datum int2out(pg_parser_Datum attr)
{
    int16_t arg1 = (int16_t)attr;
    char*   result = NULL;
    if (!pg_parser_mcxt_malloc(PGFUNC_INT_MCXT, (void**)&result, 7))
    {
        return (pg_parser_Datum)0;
    }

    numutils_itoa(arg1, result);
    return (pg_parser_Datum)result;
}

/*
 *        int2vectorout        - converts internal form to "num num ..."
 */
pg_parser_Datum int2vectorout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    bool                  is_toast = false;
    bool                  need_free = false;
    pg_parser_int2vector* int2Array = (pg_parser_int2vector*)pg_parser_detoast_datum_packed(
        (struct pg_parser_varlena*)attr, &is_toast, &need_free, info->zicinfo->dbtype,
        info->zicinfo->dbversion);
    int32_t num, nnums = int2Array->dim1;
    char*   rp;
    char*   result;

    if (is_toast)
    {
        if (info != NULL)
        {
            info->valueinfo = INFO_COL_IS_TOAST;
        }
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum)int2Array;
    }
    /* assumes sign, 5 digits, ' ' */
    if (!pg_parser_mcxt_malloc(PGFUNC_INT_MCXT, (void**)&result, nnums * 7 + 1))
    {
        return (pg_parser_Datum)0;
    }

    rp = result;
    for (num = 0; num < nnums; num++)
    {
        if (num != 0)
        {
            *rp++ = ' ';
        }
        numutils_itoa(int2Array->values[num], rp);
        while (*++rp != '\0')
            ;
    }
    *rp = '\0';
    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_INT_MCXT, int2Array);
    }
    return (pg_parser_Datum)result;
}

/*
 *        int4out            - converts int4 to "num"
 */
pg_parser_Datum int4out(pg_parser_Datum attr)
{
    int32_t arg1 = (int32_t)attr;
    char*   result = NULL;
    if (!pg_parser_mcxt_malloc(PGFUNC_INT_MCXT, (void**)&result, 12))
    {
        return (pg_parser_Datum)0;
    }

    numutils_ltoa(arg1, result);
    return (pg_parser_Datum)result;
}

/* int8out()
 */
pg_parser_Datum int8out(pg_parser_Datum attr)
{
    int64_t val = (int64_t)(attr);
    char    buf[MAXINT8LEN + 1];
    char*   result;

    numutils_lltoa(val, buf);
    result = pg_parser_mcxt_strdup(buf);
    return (pg_parser_Datum)result;
}
