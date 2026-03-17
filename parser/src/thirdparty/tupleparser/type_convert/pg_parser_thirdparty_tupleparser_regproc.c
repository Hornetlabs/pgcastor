/**
 * @file pg_parser_thirdparty_tupleparser_regproc.c
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
#include "sysdict/pg_parser_sysdict_getinfo.h"

#define PGFUNC_REGPROC_MCXT NULL

/*
 * regprocout        - converts proc OID to "pro_name"
 */
pg_parser_Datum regprocout(pg_parser_Datum attr, pg_parser_extraTypoutInfo *info)
{
    uint32_t proid = (uint32_t) attr;
    char       *result = NULL;
    char       *procname = NULL;
    char       *nspname = NULL;
    pg_parser_sysdicts *sysdicts = info->sysdicts;

    if (proid == pg_parser_InvalidOid)
    {
        result = pg_parser_mcxt_strdup("-");
        return (pg_parser_Datum) result;
    }

    if (pg_parser_sysdict_getProcInfoByOid(proid, sysdicts, &procname, &nspname))
    {
        if (nspname)
        {
            if (!pg_parser_mcxt_malloc(PGFUNC_REGPROC_MCXT,
                                    (void**) &result,
                                    strlen(procname) + strlen(nspname) + 2))
                return (pg_parser_Datum) 0;
            sprintf(result, "%s.%s", nspname, procname);
        }
        else
            result = pg_parser_mcxt_strdup(procname);
    }
    else
    {
        /* If OID doesn't match any pg_proc entry, return it numerically */
        if (!pg_parser_mcxt_malloc(PGFUNC_REGPROC_MCXT,
                                     (void**) &result,
                                      PG_PARSER_NAMEDATALEN))
            return (pg_parser_Datum) 0;
        info->valuelen = strlen(result);
        snprintf(result, PG_PARSER_NAMEDATALEN, "%u", proid);
    }

    return (pg_parser_Datum) result;
}

pg_parser_Datum regclassout(pg_parser_Datum attr)
{
    uint32_t classid = (uint32_t) attr;
    char *result = NULL;
    if (classid == pg_parser_InvalidOid)
    {
        result = pg_parser_mcxt_strdup("-");
        return (pg_parser_Datum) result;
    }
    if (!pg_parser_mcxt_malloc(PGFUNC_REGPROC_MCXT,
                                 (void**) &result,
                                  PG_PARSER_NAMEDATALEN))
        return (pg_parser_Datum) 0;
    snprintf(result, PG_PARSER_NAMEDATALEN, "%u", classid);

    return (pg_parser_Datum) result;
}
