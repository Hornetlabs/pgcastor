#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"
#include "thirdparty/tupleparser/toast/pg_parser_thirdparty_tupleparser_toast.h"
#include "thirdparty/parsernode/pg_parser_thirdparty_parsernode.h"

#define PGFUNC_VARLENA_MCXT NULL

pg_parser_Datum byteaout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    bool             is_toast = false;
    bool             need_free = false;
    pg_parser_bytea* vlena = (pg_parser_bytea*)pg_parser_detoast_datum((struct pg_parser_varlena*)attr,
                                                                       &is_toast,
                                                                       &need_free,
                                                                       info->zicinfo->dbtype,
                                                                       info->zicinfo->dbversion);
    char*            result;
    char*            rp;

    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (pg_parser_Datum)vlena;
    }
    /* return bytea */
    if (info->zicinfo->istoast)
    {
        info->valuelen = PG_PARSER_VARSIZE_ANY(vlena);
        if (!pg_parser_mcxt_malloc(PGFUNC_VARLENA_MCXT, (void**)&result, info->valuelen))
        {
            return (pg_parser_Datum)0;
        }
        info->valueinfo = INFO_COL_IS_BYTEA;
        rmemcpy0(result, 0, vlena, info->valuelen);
    }
    /* Print hex format */
    else
    {
        if (!pg_parser_mcxt_malloc(PGFUNC_VARLENA_MCXT,
                                   (void**)&result,
                                   ((int32_t)PG_PARSER_VARSIZE_ANY_EXHDR(vlena)) * 2 + 2 + 1))
        {
            return (pg_parser_Datum)0;
        }
        rp = result;
        *rp++ = '\\';
        *rp++ = 'x';
        rp += hex_encode(PG_PARSER_VARDATA_ANY(vlena), PG_PARSER_VARSIZE_ANY_EXHDR(vlena), rp);
        *rp = '\0';
        info->valuelen = strlen(result);
    }
    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_VARLENA_MCXT, vlena);
    }
    return (pg_parser_Datum)result;
}

static char* text_to_cstring(const pg_parser_text* t, pg_parser_extraTypoutInfo* info)
{
    pg_parser_text* unpacked = NULL;
    /* must cast away the const, unfortunately */
    int32_t         len = 0;
    bool            is_toast = false;
    char*           result;
    bool            need_free = false;

    unpacked = (pg_parser_text*)pg_parser_detoast_datum_packed((struct pg_parser_varlena*)t,
                                                               &is_toast,
                                                               &need_free,
                                                               info->zicinfo->dbtype,
                                                               info->zicinfo->dbversion);
    len = (int32_t)PG_PARSER_VARSIZE_ANY_EXHDR(unpacked);

    /* When out-of-line storage is detected, directly return out-of-line storage value */
    if (is_toast)
    {
        if (info != NULL)
        {
            info->valueinfo = INFO_COL_IS_TOAST;
        }
        info->valuelen = sizeof(struct pg_parser_varatt_external);
        return (char*)unpacked;
    }

    if (!pg_parser_mcxt_malloc(PGFUNC_VARLENA_MCXT, (void**)&result, len + 1))
    {
        return (pg_parser_Datum)0;
    }

    rmemcpy0(result, 0, PG_PARSER_VARDATA_ANY(unpacked), len);
    result[len] = '\0';
    info->valuelen = len;
    if (need_free)
    {
        pg_parser_mcxt_free(PGFUNC_VARLENA_MCXT, unpacked);
    }
    return result;
}

pg_parser_Datum textout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    pg_parser_text* txt = (pg_parser_text*)attr;

    return (pg_parser_Datum)text_to_cstring(txt, info);
}

/*
 * Convert a CHARACTER value to a C string.
 *
 * Uses the pg_parser_text conversion functions, which is only appropriate if pg_parser_BpChar
 * and pg_parser_text are equivalent types.
 */
pg_parser_Datum bpcharout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    pg_parser_text* txt = (pg_parser_text*)attr;

    return (pg_parser_Datum)text_to_cstring(txt, info);
}

/*
 * Convert a VARCHAR value to a C string.
 *
 * Uses the pg_parser_text to C string conversion function, which is only appropriate
 * if pg_parser_VarChar and pg_parser_text are equivalent types.
 */
pg_parser_Datum varcharout(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    pg_parser_text* txt = (pg_parser_text*)attr;
    return (pg_parser_Datum)text_to_cstring(txt, info);
}

/*
 * pg_node_tree_out        - output routine for type PG_NODE_TREE.
 *
 * The internal representation is the same as TEXT, so just pass it off.
 */
pg_parser_Datum pg_node_tree_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    return (pg_parser_Datum)textout(attr, info);
}

/*
 * Output.
 */
pg_parser_Datum json_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    /* we needn't detoast because text_to_cstring will handle that */
    pg_parser_text* txt = (pg_parser_text*)attr;

    return (pg_parser_Datum)text_to_cstring(txt, info);
}

pg_parser_Datum xml_out(pg_parser_Datum attr, pg_parser_extraTypoutInfo* info)
{
    pg_parser_text* txt = (pg_parser_text*)attr;

    return (pg_parser_Datum)text_to_cstring(txt, info);
}
