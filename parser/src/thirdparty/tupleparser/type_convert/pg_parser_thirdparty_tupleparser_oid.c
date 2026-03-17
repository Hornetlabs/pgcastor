/**
 * @file pg_parser_thirdparty_tupleparser_oid.c
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
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"

#define PGFUNC_OID_MCXT NULL

typedef struct
{
    int32_t      vl_len_;         /* these fields must match ArrayType! */
    int32_t      ndim;            /* always 1 for pg_parser_oidvector */
    int32_t      dataoffset;      /* always 0 for pg_parser_oidvector */
    uint32_t     elemtype;
    int32_t      dim1;
    int32_t      lbound1;
    uint32_t     values[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_oidvector;

/*****************************************************************************
 *     USER I/O ROUTINES                                                         *
 *****************************************************************************/

pg_parser_Datum oidout(pg_parser_Datum attr)
{
    uint32_t o = (uint32_t) attr;
    char *result = NULL;
    if(!pg_parser_mcxt_malloc(PGFUNC_OID_MCXT, (void **) &result, 12))
        return (pg_parser_Datum) 0;
    snprintf(result, 12, "%u", o);
    return (pg_parser_Datum) result;
}

/*
 *        oidvectorout - converts internal form to "num num ..."
 */
pg_parser_Datum oidvectorout(pg_parser_Datum attr)
{
    pg_parser_oidvector *oidArray = (pg_parser_oidvector *) attr;
    int32_t num,
        nnums = oidArray->dim1;
    char *rp = NULL;
    char *result = NULL;

    /* assumes sign, 10 digits, ' ' */
    if(!pg_parser_mcxt_malloc(PGFUNC_OID_MCXT, (void **) &result, nnums * 12 + 1))
        return (pg_parser_Datum) 0;
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
    return (pg_parser_Datum) result;
}
