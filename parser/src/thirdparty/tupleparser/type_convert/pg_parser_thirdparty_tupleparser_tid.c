/**
 * @file xk_pg_parser_thirdparty_tupleparser_tid.c
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
#include "trans/transrec/xk_pg_parser_trans_transrec_itemptr.h"

#define PGFUNC_TID_MCXT NULL

xk_pg_parser_Datum tidout(xk_pg_parser_Datum attr)
{
    xk_pg_parser_ItemPointer itemPtr = (xk_pg_parser_ItemPointer) attr;
    xk_pg_parser_BlockNumber blockNumber;
    xk_pg_parser_OffsetNumber offsetNumber;
    char *buf = NULL;

    blockNumber = xk_pg_parser_ItemPointerGetBlockNumberNoCheck(itemPtr);
    offsetNumber = xk_pg_parser_ItemPointerGetOffsetNumberNoCheck(itemPtr);

    if (!xk_pg_parser_mcxt_malloc(PGFUNC_TID_MCXT, (void**)&buf, 32))
     return (xk_pg_parser_Datum) 0;

    /* Perhaps someday we should output this as a record. */
    sprintf(buf, "(%u,%u)", blockNumber, offsetNumber);

    return (xk_pg_parser_Datum) buf;
}