#ifndef XK_PG_PARSER_TRANS_TRANSREC_ITEMPTR_H
#define XK_PG_PARSER_TRANS_TRANSREC_ITEMPTR_H

typedef struct xk_pg_parser_BlockIdData
{
    uint16_t        bi_hi;
    uint16_t        bi_lo;
} xk_pg_parser_BlockIdData;

typedef struct xk_pg_parser_ItemPointerData
{
    xk_pg_parser_BlockIdData ip_blkid;
    uint16_t ip_posid;
}
__attribute__((packed))
__attribute__((aligned(2)))
xk_pg_parser_ItemPointerData;

typedef xk_pg_parser_ItemPointerData *xk_pg_parser_ItemPointer;

typedef struct xk_pg_parser_ItemIdData
{
    unsigned    lp_off:15,         /* offset to tuple (from start of page) */
                lp_flags:2,        /* state of line pointer, see below */
                lp_len:15;         /* byte length of tuple */
} xk_pg_parser_ItemIdData;

typedef xk_pg_parser_ItemIdData *xk_pg_parser_ItemId;

#define xk_pg_parser_ItemPointerSetInvalid(pointer) \
( \
    xk_pg_parser_AssertMacro(((const void*)(pointer) != NULL)), \
    xk_pg_parser_BlockIdSet(&((pointer)->ip_blkid), xk_pg_parser_InvalidBlockNumber), \
    (pointer)->ip_posid = xk_pg_parser_InvalidOffsetNumber \
)

#define xk_pg_parser_PointerIsValid(pointer) ((const void*)(pointer) != NULL)

#define xk_pg_parser_BlockIdIsValid(blockId) \
    ((bool) xk_pg_parser_PointerIsValid(blockId))

/*
 * xk_pg_parser_ItemPointerSet
 * Sets a disk item pointer to the specified block and offset.
 */
#define xk_pg_parser_ItemPointerSet(pointer, blockNumber, offNum) \
( \
    xk_pg_parser_AssertMacro(xk_pg_parser_PointerIsValid(pointer)), \
    xk_pg_parser_BlockIdSet(&((pointer)->ip_blkid), blockNumber), \
    (pointer)->ip_posid = offNum \
)


#define xk_pg_parser_BlockIdGetBlockNumber(blockId) \
( \
    xk_pg_parser_AssertMacro(xk_pg_parser_BlockIdIsValid(blockId)), \
    (xk_pg_parser_BlockNumber) (((blockId)->bi_hi << 16) | ((uint16_t) (blockId)->bi_lo)) \
)

#define xk_pg_parser_ItemPointerGetBlockNumberNoCheck(pointer) \
( \
    xk_pg_parser_BlockIdGetBlockNumber(&(pointer)->ip_blkid) \
)

#define xk_pg_parser_ItemPointerGetOffsetNumberNoCheck(pointer) \
( \
    (pointer)->ip_posid \
)

#define XK_PG_PARSER_LP_UNUSED        0        /* unused (should always have lp_len=0) */
#define XK_PG_PARSER_LP_NORMAL        1        /* used (should always have lp_len>0) */
#define XK_PG_PARSER_LP_REDIRECT      2        /* HOT redirect (should have lp_len=0) */
#define XK_PG_PARSER_LP_DEAD          3        /* dead, may or may not have storage */

#define xk_pg_parser_ItemIdIsUsed(itemId) \
    ((itemId)->lp_flags != XK_PG_PARSER_LP_UNUSED)

#define xk_pg_parser_ItemIdIsRedirected(itemId) \
    ((itemId)->lp_flags == XK_PG_PARSER_LP_REDIRECT)

#define xk_pg_parser_ItemIdIsNormal(itemId) \
    ((itemId)->lp_flags == XK_PG_PARSER_LP_NORMAL)

#define xk_pg_parser_ItemIdHasStorage(itemId) \
    ((itemId)->lp_len != 0)

#endif
