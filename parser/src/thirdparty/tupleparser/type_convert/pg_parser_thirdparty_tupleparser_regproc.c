/**
 * @file xk_pg_parser_thirdparty_tupleparser_regproc.c
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
#include "common/xk_pg_parser_translog.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "sysdict/xk_pg_parser_sysdict_getinfo.h"

#define PGFUNC_REGPROC_MCXT NULL

/*
 * regprocout        - converts proc OID to "pro_name"
 */
xk_pg_parser_Datum regprocout(xk_pg_parser_Datum attr, xk_pg_parser_extraTypoutInfo *info)
{
    uint32_t proid = (uint32_t) attr;
    char       *result = NULL;
    char       *procname = NULL;
    char       *nspname = NULL;
    xk_pg_parser_sysdicts *sysdicts = info->sysdicts;

    if (proid == xk_pg_parser_InvalidOid)
    {
        result = xk_pg_parser_mcxt_strdup("-");
        return (xk_pg_parser_Datum) result;
    }

    if (xk_pg_parser_sysdict_getProcInfoByOid(proid, sysdicts, &procname, &nspname))
    {
        if (nspname)
        {
            if (!xk_pg_parser_mcxt_malloc(PGFUNC_REGPROC_MCXT,
                                    (void**) &result,
                                    strlen(procname) + strlen(nspname) + 2))
                return (xk_pg_parser_Datum) 0;
            sprintf(result, "%s.%s", nspname, procname);
        }
        else
            result = xk_pg_parser_mcxt_strdup(procname);
    }
    else
    {
        /* If OID doesn't match any pg_proc entry, return it numerically */
        if (!xk_pg_parser_mcxt_malloc(PGFUNC_REGPROC_MCXT,
                                     (void**) &result,
                                      XK_PG_PARSER_NAMEDATALEN))
            return (xk_pg_parser_Datum) 0;
        info->valuelen = strlen(result);
        snprintf(result, XK_PG_PARSER_NAMEDATALEN, "%u", proid);
    }

    return (xk_pg_parser_Datum) result;
}

xk_pg_parser_Datum regclassout(xk_pg_parser_Datum attr)
{
    uint32_t classid = (uint32_t) attr;
    char *result = NULL;
    if (classid == xk_pg_parser_InvalidOid)
    {
        result = xk_pg_parser_mcxt_strdup("-");
        return (xk_pg_parser_Datum) result;
    }
    if (!xk_pg_parser_mcxt_malloc(PGFUNC_REGPROC_MCXT,
                                 (void**) &result,
                                  XK_PG_PARSER_NAMEDATALEN))
        return (xk_pg_parser_Datum) 0;
    snprintf(result, XK_PG_PARSER_NAMEDATALEN, "%u", classid);

    return (xk_pg_parser_Datum) result;
}
