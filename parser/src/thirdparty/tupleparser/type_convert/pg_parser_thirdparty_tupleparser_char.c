/**
 * @file pg_parser_thirdparty_tupleparser_char.c
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

#define PGFUNC_CHAR_MCXT NULL

pg_parser_Datum charout(pg_parser_Datum attr)
{
    char  ch = (char)attr;
    char* result = NULL;
    if (!pg_parser_mcxt_malloc(PGFUNC_CHAR_MCXT, (void**)&result, 2))
    {
        return (pg_parser_Datum)0;
    }

    result[0] = ch;
    result[1] = '\0';
    return (pg_parser_Datum)result;
}