/**
 * @file pg_parser_thirdparty_tupleparser_enum.c
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
#include "sysdict/pg_parser_sysdict_getinfo.h"

#define PGFUNC_ENUM_MCXT NULL

pg_parser_Datum enum_out(pg_parser_Datum attr,
                            pg_parser_extraTypoutInfo *info)
{
    uint32_t       enumval = (uint32_t) attr;
    char          *result;
    char          *enumname;

    if (pg_parser_sysdict_getEnumNameByOid(enumval, info->sysdicts, &enumname))
    {
        result = pg_parser_mcxt_strdup(enumname);
        info->valuelen = strlen(result);
        return (pg_parser_Datum) result;
    }
    else
        return (pg_parser_Datum) 0;


}

/*
 * anyenum_out		- output routine for pseudo-type ANYENUM.
 *
 * We may as well allow this, since enum_out will in fact work.
 */
pg_parser_Datum
anyenum_out(pg_parser_Datum attr,
                               pg_parser_extraTypoutInfo *info)
{
    return (pg_parser_Datum) enum_out(attr, info);
}