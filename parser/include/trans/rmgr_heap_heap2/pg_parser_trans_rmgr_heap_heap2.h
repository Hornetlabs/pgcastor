#ifndef PG_PARSER_TRANS_RMGR_HEAP_HEAP2
#define PG_PARSER_TRANS_RMGR_HEAP_HEAP2

typedef enum PG_PARSER_TRANS_RMGR_HEAP2_INFO
{
    PG_PARSER_TRANS_TRANSREC_RMGR_HEAP2_MULTI_INSERT = 0x50
} pg_parser_trans_rmgr_heap2_info;

typedef enum PG_PARSER_TRANS_RMGR_HEAP_INFO
{
    PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INSERT = 0x00,
    PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_DELETE = 0x10,
    PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_UPDATE = 0x20,
    PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_TRUNCATE = 0x30,
    PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_HOT_UPDATE = 0x40,
    PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_CONFIRM = 0x50,
    PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INPLACE = 0x70
} pg_parser_trans_rmgr_heap_info;

#define PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK 0x70
#define PG_PARSER_XLOG_HEAP_INIT_PAGE 0x80

/* Marker values for insert statements in logical mode */
#define PG_PARSER_TRANS_XLH_INSERT_ALL_VISIBLE_CLEARED (1 << 0)
#define PG_PARSER_TRANS_XLH_INSERT_LAST_IN_MULTI (1 << 1)
#define PG_PARSER_TRANS_XLH_INSERT_IS_SPECULATIVE (1 << 2)
#define PG_PARSER_TRANS_XLH_INSERT_CONTAINS_NEW_TUPLE (1 << 3)

/* heap flag for pg < 10 */
#define PG_PARSER_HEAP_HASOID 0x0008 /* has an object-id field */

#define pg_parser_HeapTupleHeaderGetOid(tup)                               \
    (((tup)->t_infomask & PG_PARSER_HEAP_HASOID)                           \
         ? *((uint32_t*)((char*)(tup) + (tup)->t_hoff - sizeof(uint32_t))) \
         : pg_parser_InvalidOid)

/* delete statement flag */
#define PG_PARSER_TRANS_XLH_DELETE_ALL_VISIBLE_CLEARED (1 << 0)
#define PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD_TUPLE (1 << 1)
#define PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD_KEY (1 << 2)
#define PG_PARSER_TRANS_XLH_DELETE_IS_SUPER (1 << 3)
#define PG_PARSER_TRANS_XLH_DELETE_IS_PARTITION_MOVE (1 << 4)

/* convenience macro for checking whether any form of old tuple was logged */
#define PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD \
    (PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD_TUPLE | PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD_KEY)

/* update statement flag */
#define PG_PARSER_TRANS_XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED (1 << 0)
/* PD_ALL_VISIBLE was cleared in the 2nd page */
#define PG_PARSER_TRANS_XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED (1 << 1)
#define PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD_TUPLE (1 << 2)
#define PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD_KEY (1 << 3)
#define PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_NEW_TUPLE (1 << 4)
#define PG_PARSER_TRANS_XLH_UPDATE_PREFIX_FROM_OLD (1 << 5)
#define PG_PARSER_TRANS_XLH_UPDATE_SUFFIX_FROM_OLD (1 << 6)

/* convenience macro for checking whether any form of old tuple was logged */
#define PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD \
    (PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD_TUPLE | PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD_KEY)

typedef struct pg_parser_xl_heap_insert
{
    uint16_t offnum; /* inserted tuple's offset */
    uint8_t  flags;

    /* pg_parser_xl_heap_header & TUPLE DATA in backup block 0 */
} pg_parser_xl_heap_insert;

typedef struct pg_parser_xl_heap_multi_insert
{
    uint8_t  flags;
    uint16_t ntuples;
    uint16_t offsets[FLEXIBLE_ARRAY_MEMBER];
} pg_parser_xl_heap_multi_insert;

typedef struct pg_parser_xl_multi_insert_tuple
{
    uint16_t datalen; /* size of tuple data that follows */
    uint16_t t_infomask2;
    uint16_t t_infomask;
    uint8_t  t_hoff;
    /* TUPLE DATA FOLLOWS AT END OF STRUCT */
} pg_parser_xl_multi_insert_tuple;

#define pg_parser_SizeOfMultiInsertTuple \
    (offsetof(pg_parser_xl_multi_insert_tuple, t_hoff) + sizeof(uint8_t))

typedef struct pg_parser_xl_heap_delete
{
    pg_parser_TransactionId xmax;         /* xmax of the deleted tuple */
    uint16_t                offnum;       /* deleted tuple's offset */
    uint8_t                 infobits_set; /* infomask bits */
    uint8_t                 flags;
} pg_parser_xl_heap_delete;

typedef struct pg_parser_xl_heap_update
{
    pg_parser_TransactionId old_xmax;         /* xmax of the old tuple */
    uint16_t                old_offnum;       /* old tuple's offset */
    uint8_t                 old_infobits_set; /* infomask bits to set on old tuple */
    uint8_t                 flags;
    pg_parser_TransactionId new_xmax;   /* xmax of the new tuple */
    uint16_t                new_offnum; /* new tuple's offset */

    /*
     * If XLOG_HEAP_CONTAINS_OLD_TUPLE or XLOG_HEAP_CONTAINS_OLD_KEY flags are
     * set, a xl_heap_header struct and tuple data for the old tuple follows.
     */
} pg_parser_xl_heap_update;

#define pg_parser_SizeOfHeapInsert (offsetof(pg_parser_xl_heap_insert, flags) + sizeof(uint8_t))

#define pg_parser_SizeOfHeapDelete (offsetof(pg_parser_xl_heap_delete, flags) + sizeof(uint8_t))

#define pg_parser_SizeOfHeapUpdate \
    (offsetof(pg_parser_xl_heap_update, new_offnum) + sizeof(uint16_t))

extern bool pg_parser_trans_rmgr_heap2_pre(pg_parser_trans_transrec_decode_XLogReaderState* state,
                                           pg_parser_translog_pre_base**                    result,
                                           int32_t* pg_parser_errno);

extern bool pg_parser_trans_rmgr_heap_pre(pg_parser_trans_transrec_decode_XLogReaderState* state,
                                          pg_parser_translog_pre_base**                    result,
                                          int32_t* pg_parser_errno);

extern bool pg_parser_trans_rmgr_heap_trans(pg_parser_trans_transrec_decode_XLogReaderState* state,
                                            pg_parser_translog_tbcolbase**                   result,
                                            int32_t* pg_parser_errno);

extern bool pg_parser_trans_rmgr_heap2_trans(pg_parser_trans_transrec_decode_XLogReaderState* state,
                                             pg_parser_translog_tbcolbase** result,
                                             int32_t*                       pg_parser_errno);

extern bool pg_parser_trans_rmgr_heap_trans_get_tuple(
    pg_parser_trans_transrec_decode_XLogReaderState* state, pg_parser_translog_tbcolbase** result,
    int32_t* pg_parser_errno);

extern bool pg_parser_trans_rmgr_heap2_trans_get_tuple(
    pg_parser_trans_transrec_decode_XLogReaderState* state, pg_parser_translog_tbcolbase** result,
    int32_t* pg_parser_errno);

bool pg_parser_check_fpw(pg_parser_trans_transrec_decode_XLogReaderState* readstate,
                         pg_parser_translog_pre_base** pg_parser_result, int32_t* pg_parser_errno,
                         int16_t dbtype);

#endif