/**
 * @file pg_parser_thirdparty_tupleparser_pglsn.c
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

#define PGFUNC_PGLSN_MCXT NULL

pg_parser_Datum pg_lsn_out(pg_parser_Datum attr)
{
    pg_parser_XLogRecPtr    lsn = (pg_parser_XLogRecPtr) attr;
    char        buf[17] = {'\0'};
    char       *result;
    uint32_t    id,
                off;

    /* Decode ID and offset */
    id = (uint32_t) (lsn >> 32);
    off = (uint32_t) lsn;

    snprintf(buf, sizeof buf, "%X/%X", id, off);
    result = pg_parser_mcxt_strdup(buf);
    return (pg_parser_Datum) result;
}
