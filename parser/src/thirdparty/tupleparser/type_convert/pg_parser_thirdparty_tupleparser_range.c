/**
 * @file pg_parser_thirdparty_tupleparser_bool.c
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
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/tupleparser/toast/pg_parser_thirdparty_tupleparser_toast.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"
#include "sysdict/pg_parser_sysdict_getinfo.h"
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"
#include "trans/transrec/pg_parser_trans_transrec_heaptuple.h"
#include "thirdparty/stringinfo/pg_parser_thirdparty_stringinfo.h"

#define PGFUNC_RANGE_MCXT   NULL
#define RANGE_EMPTY_LITERAL "empty"

typedef struct
{
    int32_t  vl_len_;    /* varlena header (do not touch directly!) */
    uint32_t rangetypid; /* range type's own OID */
    /* Following the OID are zero to two bound values, then a flags byte */
} RangeType;

/* Internal representation of either bound of a range (not what's on disk) */
typedef struct
{
    pg_parser_Datum val;       /* the bound value, if any */
    bool            infinite;  /* bound is +/- infinity */
    bool            inclusive; /* bound is inclusive (vs exclusive) */
    bool            lower;     /* this is the lower (vs upper) bound */
} RangeBound;

/* A range's flags byte contains these bits: */
#define RANGE_EMPTY   0x01 /* range is empty */
#define RANGE_LB_INC  0x02 /* lower bound is inclusive */
#define RANGE_UB_INC  0x04 /* upper bound is inclusive */
#define RANGE_LB_INF  0x08 /* lower bound is -infinity */
#define RANGE_UB_INF  0x10 /* upper bound is +infinity */
#define RANGE_LB_NULL 0x20 /* lower bound is null (NOT USED) */
#define RANGE_UB_NULL 0x40 /* upper bound is null (NOT USED) */
#define RANGE_CONTAIN_EMPTY                        \
    0x80 /* marks a GiST internal-page entry whose \
          * subtree contains some empty ranges */

#define RANGE_HAS_LBOUND(flags)   (!((flags) & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF)))

#define RANGE_HAS_UBOUND(flags)   (!((flags) & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF)))

#define RangeIsEmpty(r)           ((range_get_flags(r) & RANGE_EMPTY) != 0)
#define RangeIsOrContainsEmpty(r) ((range_get_flags(r) & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY)) != 0)

static char range_get_flags(RangeType* range);

/*
 * range_get_flags: just get the flags from a RangeType value.
 *
 * This is frequently useful in places that only need the flags and not
 * the full results of range_deserialize.
 */
static char range_get_flags(RangeType* range)
{
    /* fetch the flag byte from datum's last byte */
    return *((char*)range + PG_PARSER_VARSIZE(range) - 1);
}

/*
 * range_deserialize: deconstruct a range value
 *
 * NB: the given range object must be fully detoasted; it cannot have a
 * short varlena header.
 *
 * Note that if the element type is pass-by-reference, the datums in the
 * RangeBound structs will be pointers into the given range object.
 */
static void range_deserialize(pg_parser_sysdict_pgtype* npt,
                              RangeType*                range,
                              RangeBound*               lower,
                              RangeBound*               upper,
                              bool*                     empty)
{
    char              flags;
    int16_t           typlen;
    bool              typbyval;
    char              typalign;
    pg_parser_Pointer ptr;
    pg_parser_Datum   lbound;
    pg_parser_Datum   ubound;

    /* fetch the flag byte from datum's last byte */
    flags = *((char*)range + PG_PARSER_VARSIZE(range) - 1);

    /* fetch information about range's element type */
    typlen = npt->typlen;
    typbyval = npt->typbyval;
    typalign = npt->typalign;

    /* initialize data pointer just after the range OID */
    ptr = (pg_parser_Pointer)(range + 1);

    /* fetch lower bound, if any */
    if (RANGE_HAS_LBOUND(flags))
    {
        /* att_align_pointer cannot be necessary here */
        lbound = pg_parser_fetch_att(ptr, typbyval, typlen);
        ptr = (pg_parser_Pointer)pg_parser_att_addlength_pointer(ptr, typlen, ptr);
    }
    else
    {
        lbound = (pg_parser_Datum)0;
    }

    /* fetch upper bound, if any */
    if (RANGE_HAS_UBOUND(flags))
    {
        ptr = (pg_parser_Pointer)pg_parser_att_align_pointer(ptr, typalign, typlen, ptr);
        ubound = pg_parser_fetch_att(ptr, typbyval, typlen);
        /* no need for att_addlength_pointer */
    }
    else
    {
        ubound = (pg_parser_Datum)0;
    }

    /* emit results */

    *empty = (flags & RANGE_EMPTY) != 0;

    lower->val = lbound;
    lower->infinite = (flags & RANGE_LB_INF) != 0;
    lower->inclusive = (flags & RANGE_LB_INC) != 0;
    lower->lower = true;

    upper->val = ubound;
    upper->infinite = (flags & RANGE_UB_INF) != 0;
    upper->inclusive = (flags & RANGE_UB_INC) != 0;
    upper->lower = false;
}

extern const unsigned short int** __ctype_b_loc(void) __THROW __attribute__((__const__));

#define __isctype(c, type) ((*__ctype_b_loc())[(int)(c)] & (unsigned short int)type)

#define isspace(c)         __isctype((c), 8192)

/*
 * Helper for range_deparse: quote a bound value as needed
 *
 * Result is a palloc'd string
 */
static char* range_bound_escape(const char* value)
{
    bool                     nq;
    const char*              ptr;
    char*                    result = NULL;
    pg_parser_StringInfoData buf;

    pg_parser_initStringInfo(&buf);

    /* Detect whether we need double quotes for this value */
    nq = (value[0] == '\0'); /* force quotes for empty string */
    for (ptr = value; *ptr; ptr++)
    {
        char ch = *ptr;

        if (ch == '"' || ch == '\\' || ch == '(' || ch == ')' || ch == '[' || ch == ']' || ch == ',' ||
            isspace((unsigned char)ch))
        {
            nq = true;
            break;
        }
    }

    /* And emit the string */
    if (nq)
    {
        pg_parser_appendStringInfoChar(&buf, '"');
    }
    for (ptr = value; *ptr; ptr++)
    {
        char ch = *ptr;

        if (ch == '"' || ch == '\\')
        {
            pg_parser_appendStringInfoChar(&buf, ch);
        }
        pg_parser_appendStringInfoChar(&buf, ch);
    }
    if (nq)
    {
        pg_parser_appendStringInfoChar(&buf, '"');
    }

    result = pg_parser_mcxt_strdup(buf.data);
    pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, buf.data);
    return result;
}

/*
 * Convert a deserialized range value to text form
 *
 * Inputs are the flags byte, and the two bound values already converted to
 * text (but not yet quoted).  If no bound value, pass NULL.
 *
 * Result is a palloc'd string
 */
static char* range_deparse(char flags, const char* lbound_str, const char* ubound_str)
{
    pg_parser_StringInfoData buf;
    char*                    result = NULL;

    if (flags & RANGE_EMPTY)
    {
        return pg_parser_mcxt_strdup(RANGE_EMPTY_LITERAL);
    }

    pg_parser_initStringInfo(&buf);

    pg_parser_appendStringInfoChar(&buf, (flags & RANGE_LB_INC) ? '[' : '(');

    if (RANGE_HAS_LBOUND(flags))
    {
        char* range_bound_temp = range_bound_escape(lbound_str);
        pg_parser_appendStringInfoString(&buf, range_bound_temp);
        pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, range_bound_temp);
    }

    pg_parser_appendStringInfoChar(&buf, ',');

    if (RANGE_HAS_UBOUND(flags))
    {
        char* range_bound_temp = range_bound_escape(ubound_str);
        pg_parser_appendStringInfoString(&buf, range_bound_temp);
        pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, range_bound_temp);
    }

    pg_parser_appendStringInfoChar(&buf, (flags & RANGE_UB_INC) ? ']' : ')');
    result = pg_parser_mcxt_strdup(buf.data);
    pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, buf.data);
    return result;
}

pg_parser_Datum range_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    bool                      is_toast = false;
    bool                      need_free = false;
    RangeType*                range = (RangeType*)pg_parser_detoast_datum((struct pg_parser_varlena*)attr,
                                                           &is_toast,
                                                           &need_free,
                                                           info->zicinfo->dbtype,
                                                           info->zicinfo->dbversion);
    char*                     output_str;
    char                      flags;
    char*                     lbound_str = NULL;
    char*                     ubound_str = NULL;
    RangeBound                lower;
    RangeBound                upper;
    bool                      empty;
    pg_parser_sysdict_pgtype* type = NULL;

    if (is_toast)
    {
        /* Out-of-line storage parsing for range is not yet supported */
        info->valueinfo = INFO_COL_MAY_NULL;
        info->valuelen = 0;
        return (pg_parser_Datum)range;
    }

    type = pg_parser_sysdict_getSubTypeByRange(range->rangetypid, info->sysdicts);
    if (!type)
    {
        return (pg_parser_Datum)0;
    }
    /* deserialize */
    range_deserialize(type, range, &lower, &upper, &empty);
    flags = range_get_flags(range);

    /* call element type's output function */
    if (RANGE_HAS_LBOUND(flags))
    {
        lbound_str = pg_parser_convert_attr_to_str_char(lower.val, info->sysdicts, type->oid, &is_toast, info->zicinfo);
        if (!lbound_str)
        {
            return (pg_parser_Datum)0;
        }
        if (is_toast)
        {
            pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, lbound_str);
            pg_parser_log_errlog(info->zicinfo->debuglevel, "error: toast value in range out\n");
            return (pg_parser_Datum)0;
        }
    }
    if (RANGE_HAS_UBOUND(flags))
    {
        ubound_str = pg_parser_convert_attr_to_str_char(upper.val, info->sysdicts, type->oid, &is_toast, info->zicinfo);
        if (!ubound_str)
        {
            return (pg_parser_Datum)0;
        }
        if (is_toast)
        {
            pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, ubound_str);
            pg_parser_log_errlog(info->zicinfo->debuglevel, "error: toast value in range out\n");
            return (pg_parser_Datum)0;
        }
    }

    /* construct result string */
    output_str = range_deparse(flags, lbound_str, ubound_str);
    if (lbound_str)
    {
        pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, lbound_str);
    }
    if (ubound_str)
    {
        pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, ubound_str);
    }
    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_RANGE_MCXT, range);
    }

    info->valuelen = strlen(output_str);

    return (pg_parser_Datum)output_str;
}
