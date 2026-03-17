/**
 * @file xk_pg_parser_thirdparty_tupleparser_pseudotypes.c
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

#define PGFUNC_PSEUDOTYPES_MCXT NULL

/*
 * cstring_out        - output routine for pseudo-type CSTRING.
 *
 * We allow this mainly so that "SELECT some_output_function(...)" does
 * what the user will expect.
 */
xk_pg_parser_Datum cstring_out(xk_pg_parser_Datum attr)
{
    char *str = (char *) attr;

    return (xk_pg_parser_Datum) xk_pg_parser_mcxt_strdup(str);
}

/*
 * void_out        - output routine for pseudo-type VOID.
 *
 * We allow this so that "SELECT function_returning_void(...)" works.
 */
xk_pg_parser_Datum void_out(xk_pg_parser_Datum attr)
{
    XK_PG_PARSER_UNUSED(attr);
    return (xk_pg_parser_Datum) xk_pg_parser_mcxt_strdup("");
}

/*
 * shell_out        - output routine for "shell" types.
 */
xk_pg_parser_Datum shell_out(xk_pg_parser_Datum attr)
{
    XK_PG_PARSER_UNUSED(attr);
    return (xk_pg_parser_Datum) NULL;
}
