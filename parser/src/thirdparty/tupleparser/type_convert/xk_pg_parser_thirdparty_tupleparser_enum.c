/**
 * @file xk_pg_parser_thirdparty_tupleparser_enum.c
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
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "sysdict/xk_pg_parser_sysdict_getinfo.h"

#define PGFUNC_ENUM_MCXT NULL

xk_pg_parser_Datum enum_out(xk_pg_parser_Datum attr,
                            xk_pg_parser_extraTypoutInfo *info)
{
    uint32_t       enumval = (uint32_t) attr;
    char          *result;
    char          *enumname;

    if (xk_pg_parser_sysdict_getEnumNameByOid(enumval, info->sysdicts, &enumname))
    {
        result = xk_pg_parser_mcxt_strdup(enumname);
        info->valuelen = strlen(result);
        return (xk_pg_parser_Datum) result;
    }
    else
        return (xk_pg_parser_Datum) 0;


}

/*
 * anyenum_out		- output routine for pseudo-type ANYENUM.
 *
 * We may as well allow this, since enum_out will in fact work.
 */
xk_pg_parser_Datum
anyenum_out(xk_pg_parser_Datum attr,
                               xk_pg_parser_extraTypoutInfo *info)
{
    return (xk_pg_parser_Datum) enum_out(attr, info);
}