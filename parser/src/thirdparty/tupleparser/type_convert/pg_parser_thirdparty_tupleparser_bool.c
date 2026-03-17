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
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"

#define PGFUNC_BOOL_MCXT NULL

pg_parser_Datum
boolout(pg_parser_Datum attr)
{
    bool  b = (bool) attr;
    char *result = NULL;
    if (!pg_parser_mcxt_malloc(PGFUNC_BOOL_MCXT, (void **)&result, 6))
    {
        return (pg_parser_Datum) 0;
    }

    if (b)
        snprintf(result, 6, "%s", "true");
    else
        snprintf(result, 6, "%s", "false");
    result[5] = '\0';
    return (pg_parser_Datum) result;
}
