/**
 * @file pg_parser_thirdparty_tupleparser_tid.c
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
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"

#define PGFUNC_TID_MCXT NULL

pg_parser_Datum tidout(pg_parser_Datum attr)
{
    pg_parser_ItemPointer itemPtr = (pg_parser_ItemPointer) attr;
    pg_parser_BlockNumber blockNumber;
    pg_parser_OffsetNumber offsetNumber;
    char *buf = NULL;

    blockNumber = pg_parser_ItemPointerGetBlockNumberNoCheck(itemPtr);
    offsetNumber = pg_parser_ItemPointerGetOffsetNumberNoCheck(itemPtr);

    if (!pg_parser_mcxt_malloc(PGFUNC_TID_MCXT, (void**)&buf, 32))
     return (pg_parser_Datum) 0;

    /* Perhaps someday we should output this as a record. */
    sprintf(buf, "(%u,%u)", blockNumber, offsetNumber);

    return (pg_parser_Datum) buf;
}