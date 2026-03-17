/**
 * @file xk_pg_parser_thirdparty_tupleparser_xid.c
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

#define PGFUNC_XID_MCXT NULL

xk_pg_parser_Datum
xidout(xk_pg_parser_Datum attr)
{
    xk_pg_parser_TransactionId transactionId = (xk_pg_parser_TransactionId) attr;
    char *result = NULL;
    if (!xk_pg_parser_mcxt_malloc(PGFUNC_XID_MCXT, (void**)&result, 16))
        return (xk_pg_parser_Datum) 0;

    snprintf(result, 16, "%lu", (unsigned long) transactionId);
    return (xk_pg_parser_Datum) result;
}

/*****************************************************************************
 *     COMMAND IDENTIFIER ROUTINES                                             *
 *****************************************************************************/

/*
 *        cidout    - converts a cid to external representation.
 */
xk_pg_parser_Datum
cidout(xk_pg_parser_Datum attr)
{
    xk_pg_parser_CommandId    c = (xk_pg_parser_CommandId) attr;
    char       *result = NULL;
    if (!xk_pg_parser_mcxt_malloc(PGFUNC_XID_MCXT, (void**)&result, 16))
        return (xk_pg_parser_Datum) 0;
    snprintf(result, 16, "%lu", (unsigned long) c);
    return (xk_pg_parser_Datum) result;
}