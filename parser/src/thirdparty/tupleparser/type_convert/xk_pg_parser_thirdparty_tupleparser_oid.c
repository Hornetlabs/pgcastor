/**
 * @file xk_pg_parser_thirdparty_tupleparser_oid.c
 * @author bytesync
 * @brief 
 * @version 0.1
 * @date 2023-08-03
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgfunc.h"

#define PGFUNC_OID_MCXT NULL

typedef struct
{
    int32_t      vl_len_;         /* these fields must match ArrayType! */
    int32_t      ndim;            /* always 1 for xk_pg_parser_oidvector */
    int32_t      dataoffset;      /* always 0 for xk_pg_parser_oidvector */
    uint32_t     elemtype;
    int32_t      dim1;
    int32_t      lbound1;
    uint32_t     values[FLEXIBLE_ARRAY_MEMBER];
} xk_pg_parser_oidvector;

/*****************************************************************************
 *     USER I/O ROUTINES                                                         *
 *****************************************************************************/

xk_pg_parser_Datum oidout(xk_pg_parser_Datum attr)
{
    uint32_t o = (uint32_t) attr;
    char *result = NULL;
    if(!xk_pg_parser_mcxt_malloc(PGFUNC_OID_MCXT, (void **) &result, 12))
        return (xk_pg_parser_Datum) 0;
    snprintf(result, 12, "%u", o);
    return (xk_pg_parser_Datum) result;
}

/*
 *        oidvectorout - converts internal form to "num num ..."
 */
xk_pg_parser_Datum oidvectorout(xk_pg_parser_Datum attr)
{
    xk_pg_parser_oidvector *oidArray = (xk_pg_parser_oidvector *) attr;
    int32_t num,
        nnums = oidArray->dim1;
    char *rp = NULL;
    char *result = NULL;

    /* assumes sign, 10 digits, ' ' */
    if(!xk_pg_parser_mcxt_malloc(PGFUNC_OID_MCXT, (void **) &result, nnums * 12 + 1))
        return (xk_pg_parser_Datum) 0;
    rp = result;
    for (num = 0; num < nnums; num++)
    {
        if (num != 0)
            *rp++ = ' ';
        sprintf(rp, "%u", oidArray->values[num]);
        while (*++rp != '\0')
            ;
    }
    *rp = '\0';
    return (xk_pg_parser_Datum) result;
}
