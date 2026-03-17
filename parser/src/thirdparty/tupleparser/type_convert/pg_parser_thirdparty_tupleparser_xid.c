/**
 * @file pg_parser_thirdparty_tupleparser_xid.c
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

#define PGFUNC_XID_MCXT NULL

pg_parser_Datum
xidout(pg_parser_Datum attr)
{
    pg_parser_TransactionId transactionId = (pg_parser_TransactionId) attr;
    char *result = NULL;
    if (!pg_parser_mcxt_malloc(PGFUNC_XID_MCXT, (void**)&result, 16))
        return (pg_parser_Datum) 0;

    snprintf(result, 16, "%lu", (unsigned long) transactionId);
    return (pg_parser_Datum) result;
}

/*****************************************************************************
 *     COMMAND IDENTIFIER ROUTINES                                             *
 *****************************************************************************/

/*
 *        cidout    - converts a cid to external representation.
 */
pg_parser_Datum
cidout(pg_parser_Datum attr)
{
    pg_parser_CommandId    c = (pg_parser_CommandId) attr;
    char       *result = NULL;
    if (!pg_parser_mcxt_malloc(PGFUNC_XID_MCXT, (void**)&result, 16))
        return (pg_parser_Datum) 0;
    snprintf(result, 16, "%lu", (unsigned long) c);
    return (pg_parser_Datum) result;
}