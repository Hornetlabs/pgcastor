/**
 * @file pg_parser_thirdparty_tupleparser_name.c
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

pg_parser_Datum nameout(pg_parser_Datum attr)
{
    pg_parser_Name    s = (pg_parser_Name) attr;

    return (pg_parser_Datum) pg_parser_mcxt_strdup(s->data);
}