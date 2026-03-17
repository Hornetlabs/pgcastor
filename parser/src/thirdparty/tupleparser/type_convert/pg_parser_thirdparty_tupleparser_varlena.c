#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/common/xk_pg_parser_thirdparty_builtins.h"
#include "thirdparty/tupleparser/toast/xk_pg_parser_thirdparty_tupleparser_toast.h"
#include "thirdparty/parsernode/xk_pg_parser_thirdparty_parsernode.h"

#define PGFUNC_VARLENA_MCXT NULL

xk_pg_parser_Datum byteaout(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info)
{
    bool         is_toast = false;
    bool         need_free = false;
    xk_pg_parser_bytea       *vlena = (xk_pg_parser_bytea *) 
                                            xk_pg_parser_detoast_datum((struct xk_pg_parser_varlena *) attr,
                                                                       &is_toast,
                                                                       &need_free,
                                                                        info->zicinfo->dbtype,
                                                                        info->zicinfo->dbversion);
    char        *result;
    char        *rp;

    if (is_toast)
    {
        info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct xk_pg_parser_varatt_external);
        return (xk_pg_parser_Datum) vlena;
    }
    /* return bytea */
    if (info->zicinfo->istoast)
    {
        info->valuelen = XK_PG_PARSER_VARSIZE_ANY(vlena);
        if(!xk_pg_parser_mcxt_malloc(PGFUNC_VARLENA_MCXT, (void **) &result,
                                     info->valuelen))
            return (xk_pg_parser_Datum) 0;
        info->valueinfo = INFO_COL_IS_BYTEA;
        rmemcpy0(result, 0, vlena, info->valuelen);
    }
    /* Print hex format */
    else
    {
        if(!xk_pg_parser_mcxt_malloc(PGFUNC_VARLENA_MCXT, (void **) &result,
                                     ((int32_t)XK_PG_PARSER_VARSIZE_ANY_EXHDR(vlena)) * 2 + 2 + 1))
            return (xk_pg_parser_Datum) 0;
        rp = result;
        *rp++ = '\\';
        *rp++ = 'x';
        rp += hex_encode(XK_PG_PARSER_VARDATA_ANY(vlena), XK_PG_PARSER_VARSIZE_ANY_EXHDR(vlena), rp);
        *rp = '\0';
        info->valuelen = strlen(result);
    }
    if (need_free)
        xk_pg_parser_mcxt_free(PGFUNC_VARLENA_MCXT, vlena);
    return (xk_pg_parser_Datum) result;
}

static char *text_to_cstring(const xk_pg_parser_text *t, xk_pg_parser_extraTypoutInfo *info)
{
    xk_pg_parser_text    *unpacked = NULL;
    /* must cast away the const, unfortunately */
    int32_t  len = 0;
    bool     is_toast = false;
    char    *result;
    bool     need_free = false;

    unpacked = (xk_pg_parser_text *)xk_pg_parser_detoast_datum_packed((struct xk_pg_parser_varlena *)t,
                                                                      &is_toast, &need_free,
                                                                       info->zicinfo->dbtype,
                                                                       info->zicinfo->dbversion);
    len = (int32_t) XK_PG_PARSER_VARSIZE_ANY_EXHDR(unpacked);

    /* 捕捉到行外存储时, 直接返回行外存储值 */
    if (is_toast)
    {
        if (info != NULL)
            info->valueinfo = INFO_COL_IS_TOAST;
        info->valuelen = sizeof(struct xk_pg_parser_varatt_external);
        return (char *)unpacked;
    }

    if(!xk_pg_parser_mcxt_malloc(PGFUNC_VARLENA_MCXT, (void **) &result,
                                 len + 1))
        return (xk_pg_parser_Datum) 0;

    rmemcpy0(result, 0, XK_PG_PARSER_VARDATA_ANY(unpacked), len);
    result[len] = '\0';
    info->valuelen = len;
    if (need_free)
        xk_pg_parser_mcxt_free(PGFUNC_VARLENA_MCXT, unpacked);
    return result;
}

xk_pg_parser_Datum textout(xk_pg_parser_Datum attr, xk_pg_parser_extraTypoutInfo *info)
{
    xk_pg_parser_text *txt = (xk_pg_parser_text *) attr;

    return (xk_pg_parser_Datum) text_to_cstring(txt, info);
}

/*
 * Convert a CHARACTER value to a C string.
 *
 * Uses the xk_pg_parser_text conversion functions, which is only appropriate if xk_pg_parser_BpChar
 * and xk_pg_parser_text are equivalent types.
 */
xk_pg_parser_Datum bpcharout(xk_pg_parser_Datum attr, xk_pg_parser_extraTypoutInfo *info)
{
    xk_pg_parser_text *txt = (xk_pg_parser_text *) attr;

    return (xk_pg_parser_Datum) text_to_cstring(txt, info);
}

/*
 * Convert a VARCHAR value to a C string.
 *
 * Uses the xk_pg_parser_text to C string conversion function, which is only appropriate
 * if xk_pg_parser_VarChar and xk_pg_parser_text are equivalent types.
 */
xk_pg_parser_Datum varcharout(xk_pg_parser_Datum attr, xk_pg_parser_extraTypoutInfo *info)
{
    xk_pg_parser_text *txt = (xk_pg_parser_text *) attr;
    return (xk_pg_parser_Datum) text_to_cstring(txt, info);
}

/*
 * pg_node_tree_out        - output routine for type PG_NODE_TREE.
 *
 * The internal representation is the same as TEXT, so just pass it off.
 */
xk_pg_parser_Datum pg_node_tree_out(xk_pg_parser_Datum attr, xk_pg_parser_extraTypoutInfo *info)
{
    return (xk_pg_parser_Datum)textout(attr, info);
}

/*
 * Output.
 */
xk_pg_parser_Datum json_out(xk_pg_parser_Datum attr, xk_pg_parser_extraTypoutInfo *info)
{
    /* we needn't detoast because text_to_cstring will handle that */
    xk_pg_parser_text *txt = (xk_pg_parser_text *) attr;

    return (xk_pg_parser_Datum) text_to_cstring(txt, info);
}

xk_pg_parser_Datum xml_out(xk_pg_parser_Datum attr, xk_pg_parser_extraTypoutInfo *info)
{
    xk_pg_parser_text *txt = (xk_pg_parser_text *) attr;

    return (xk_pg_parser_Datum) text_to_cstring(txt, info);
}
