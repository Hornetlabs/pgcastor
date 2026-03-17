/**
 * @file xk_pg_parser_thirdparty_tupleparser_name.c
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
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgfunc.h"

xk_pg_parser_Datum nameout(xk_pg_parser_Datum attr)
{
    xk_pg_parser_Name    s = (xk_pg_parser_Name) attr;

    return (xk_pg_parser_Datum) xk_pg_parser_mcxt_strdup(s->data);
}