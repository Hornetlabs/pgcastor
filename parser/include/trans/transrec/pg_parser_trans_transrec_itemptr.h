#ifndef PG_PARSER_TRANS_TRANSREC_ITEMPTR_H
#define PG_PARSER_TRANS_TRANSREC_ITEMPTR_H

typedef struct pg_parser_BlockIdData
{
    uint16_t bi_hi;
    uint16_t bi_lo;
} pg_parser_BlockIdData;

typedef struct pg_parser_ItemPointerData
{
    pg_parser_BlockIdData ip_blkid;
    uint16_t              ip_posid;
} __attribute__((packed)) __attribute__((aligned(2))) pg_parser_ItemPointerData;

typedef pg_parser_ItemPointerData*                    pg_parser_ItemPointer;

typedef struct pg_parser_ItemIdData
{
    unsigned lp_off : 15, /* offset to tuple (from start of page) */
        lp_flags : 2,     /* state of line pointer, see below */
        lp_len : 15;      /* byte length of tuple */
} pg_parser_ItemIdData;

typedef pg_parser_ItemIdData* pg_parser_ItemId;

#define pg_parser_ItemPointerSetInvalid(pointer)                                 \
    (pg_parser_AssertMacro(((const void*)(pointer) != NULL)),                    \
     pg_parser_BlockIdSet(&((pointer)->ip_blkid), pg_parser_InvalidBlockNumber), \
     (pointer)->ip_posid = pg_parser_InvalidOffsetNumber)

#define pg_parser_PointerIsValid(pointer) ((const void*)(pointer) != NULL)

#define pg_parser_BlockIdIsValid(blockId) ((bool)pg_parser_PointerIsValid(blockId))

/*
 * pg_parser_ItemPointerSet
 * Sets a disk item pointer to the specified block and offset.
 */
#define pg_parser_ItemPointerSet(pointer, blockNumber, offNum)  \
    (pg_parser_AssertMacro(pg_parser_PointerIsValid(pointer)),  \
     pg_parser_BlockIdSet(&((pointer)->ip_blkid), blockNumber), \
     (pointer)->ip_posid = offNum)

#define pg_parser_BlockIdGetBlockNumber(blockId)               \
    (pg_parser_AssertMacro(pg_parser_BlockIdIsValid(blockId)), \
     (pg_parser_BlockNumber)(((blockId)->bi_hi << 16) | ((uint16_t)(blockId)->bi_lo)))

#define pg_parser_ItemPointerGetBlockNumberNoCheck(pointer)  (pg_parser_BlockIdGetBlockNumber(&(pointer)->ip_blkid))

#define pg_parser_ItemPointerGetOffsetNumberNoCheck(pointer) ((pointer)->ip_posid)

#define PG_PARSER_LP_UNUSED                                  0 /* unused (should always have lp_len=0) */
#define PG_PARSER_LP_NORMAL                                  1 /* used (should always have lp_len>0) */
#define PG_PARSER_LP_REDIRECT                                2 /* HOT redirect (should have lp_len=0) */
#define PG_PARSER_LP_DEAD                                    3 /* dead, may or may not have storage */

#define pg_parser_ItemIdIsUsed(itemId)                       ((itemId)->lp_flags != PG_PARSER_LP_UNUSED)

#define pg_parser_ItemIdIsRedirected(itemId)                 ((itemId)->lp_flags == PG_PARSER_LP_REDIRECT)

#define pg_parser_ItemIdIsNormal(itemId)                     ((itemId)->lp_flags == PG_PARSER_LP_NORMAL)

#define pg_parser_ItemIdHasStorage(itemId)                   ((itemId)->lp_len != 0)

#endif
