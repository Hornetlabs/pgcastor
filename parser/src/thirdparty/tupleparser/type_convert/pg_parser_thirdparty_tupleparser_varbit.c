/**
 * @file pg_parser_thirdparty_tupleparser_varbit.c
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

#define PGFUNC_VARVBIT_MCXT NULL
#define BITS_PER_BYTE 8

#define VARBITLEN(PTR) (((VarBit *) (PTR))->bit_len)
#define VARBITS(PTR) (((VarBit *) (PTR))->bit_dat)

#define HIGHBIT             (0x80)
#define IS_HIGHBIT_SET(ch)  ((unsigned char)(ch) & HIGHBIT)

/*
 * Modeled on struct pg_parser_varlena from postgres.h, but data type is bits8.
 *
 * Caution: if bit_len is not a multiple of BITS_PER_BYTE, the low-order
 * bits of the last byte of bit_dat[] are unused and MUST be zeroes.
 * (This allows bit_cmp() to not bother masking the last byte.)
 * Also, there should not be any excess bytes counted in the header length.
 */
typedef struct
{
    int32_t        vl_len_;        /* varlena header (do not touch directly!) */
    int32_t        bit_len;        /* number of valid bits */
    uint8_t        bit_dat[FLEXIBLE_ARRAY_MEMBER]; /* bit string, most sig. byte
                                                 * first */
} VarBit;

pg_parser_Datum bit_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo *info)
{
    /* same as varbit output */
    return varbit_out(attr, info);
}

/*
 * varbit_out -
 *      Prints the string as bits to preserve length accurately
 *
 * XXX varbit_recv() and hex input to varbit_in() can load a value that this
 * cannot emit.  Consider using hex output for such values.
 */
pg_parser_Datum varbit_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo *info)
{
    bool        is_toast = false;
    bool        need_free = false;
    VarBit     *s = (VarBit *) pg_parser_detoast_datum((struct pg_parser_varlena *) attr,
                                                          &is_toast,
                                                          &need_free,
                                                           info->zicinfo->dbtype,
                                                           info->zicinfo->dbversion);
    char       *result,
               *r;
    uint8_t    *sp;
    uint8_t     x;
    int32_t         i,
                k,
                len;
    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum) s;
    }
    len = VARBITLEN(s);
    if(!pg_parser_mcxt_malloc(PGFUNC_VARVBIT_MCXT, (void**) &result, len + 1))
        return (pg_parser_Datum) 0;
    sp = VARBITS(s);
    r = result;
    for (i = 0; i <= len - BITS_PER_BYTE; i += BITS_PER_BYTE, sp++)
    {
        /* print full bytes */
        x = *sp;
        for (k = 0; k < BITS_PER_BYTE; k++)
        {
            *r++ = IS_HIGHBIT_SET(x) ? '1' : '0';
            x <<= 1;
        }
    }
    if (i < len)
    {
        /* print the last partial byte */
        x = *sp;
        for (k = i; k < len; k++)
        {
            *r++ = IS_HIGHBIT_SET(x) ? '1' : '0';
            x <<= 1;
        }
    }
    *r = '\0';

    info->valuelen = strlen(result);
    if (need_free)                                                                  
        pg_parser_mcxt_free(PGFUNC_VARVBIT_MCXT, s);
    return (pg_parser_Datum) result;
}