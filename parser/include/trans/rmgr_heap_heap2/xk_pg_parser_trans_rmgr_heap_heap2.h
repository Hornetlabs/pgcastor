#ifndef XK_PG_PARSER_TRANS_RMGR_HEAP_HEAP2
#define XK_PG_PARSER_TRANS_RMGR_HEAP_HEAP2


typedef enum XK_PG_PARSER_TRANS_RMGR_HEAP2_INFO
{
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP2_MULTI_INSERT = 0x50
} xk_pg_parser_trans_rmgr_heap2_info;

typedef enum XK_PG_PARSER_TRANS_RMGR_HEAP_INFO
{
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INSERT = 0x00,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_DELETE = 0x10,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_UPDATE = 0x20,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_TRUNCATE =0x30,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_HOT_UPDATE = 0x40,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_CONFIRM =0x50,
    XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_INPLACE =0x70
} xk_pg_parser_trans_rmgr_heap_info;

#define XK_PG_PARSER_TRANS_TRANSREC_RMGR_HEAP_OPMASK        0x70
#define XK_PG_PARSER_XLOG_HEAP_INIT_PAGE 0x80

/* insert语句在logical模式下的标记值 */
#define XK_PG_PARSER_TRANS_XLH_INSERT_ALL_VISIBLE_CLEARED            (1<<0)
#define XK_PG_PARSER_TRANS_XLH_INSERT_LAST_IN_MULTI                  (1<<1)
#define XK_PG_PARSER_TRANS_XLH_INSERT_IS_SPECULATIVE                 (1<<2)
#define XK_PG_PARSER_TRANS_XLH_INSERT_CONTAINS_NEW_TUPLE             (1<<3)
#if XK_PG_VERSION_NUM >= 140000
#define XK_PG_PARSER_TRANS_XLH_INSERT_ON_TOAST_RELATION              (1<<4)

/* all_frozen_set always implies all_visible_set */
#define XK_PG_PARSER_TRANS_XLH_INSERT_ALL_FROZEN_SET                 (1<<5)
#endif

/* pg < 10时 heap 的flag */
#define XK_PG_PARSER_HEAP_HASOID 0x0008	/* has an object-id field */

#define xk_pg_parser_HeapTupleHeaderGetOid(tup) \
( \
	((tup)->t_infomask & XK_PG_PARSER_HEAP_HASOID) ? \
		*((uint32_t *) ((char *)(tup) + (tup)->t_hoff - sizeof(uint32_t))) \
	: \
		xk_pg_parser_InvalidOid \
)


/* delete 语句flag */
#define XK_PG_PARSER_TRANS_XLH_DELETE_ALL_VISIBLE_CLEARED           (1<<0)
#define XK_PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD_TUPLE            (1<<1)
#define XK_PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD_KEY              (1<<2)
#define XK_PG_PARSER_TRANS_XLH_DELETE_IS_SUPER                      (1<<3)
#define XK_PG_PARSER_TRANS_XLH_DELETE_IS_PARTITION_MOVE             (1<<4)

/* convenience macro for checking whether any form of old tuple was logged */
#define XK_PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD                        \
    (XK_PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD_TUPLE | XK_PG_PARSER_TRANS_XLH_DELETE_CONTAINS_OLD_KEY)

/* update 语句flag */
#define XK_PG_PARSER_TRANS_XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED          (1<<0)
/* PD_ALL_VISIBLE was cleared in the 2nd page */
#define XK_PG_PARSER_TRANS_XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED          (1<<1)
#define XK_PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD_TUPLE               (1<<2)
#define XK_PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD_KEY                 (1<<3)
#define XK_PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_NEW_TUPLE               (1<<4)
#define XK_PG_PARSER_TRANS_XLH_UPDATE_PREFIX_FROM_OLD                  (1<<5)
#define XK_PG_PARSER_TRANS_XLH_UPDATE_SUFFIX_FROM_OLD                  (1<<6)

/* convenience macro for checking whether any form of old tuple was logged */
#define XK_PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD                        \
    (XK_PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD_TUPLE | XK_PG_PARSER_TRANS_XLH_UPDATE_CONTAINS_OLD_KEY)

typedef struct xk_pg_parser_xl_heap_insert
{
    uint16_t        offnum;        /* inserted tuple's offset */
    uint8_t         flags;

    /* xk_pg_parser_xl_heap_header & TUPLE DATA in backup block 0 */
} xk_pg_parser_xl_heap_insert;

typedef struct xk_pg_parser_xl_heap_multi_insert
{
    uint8_t        flags;
    uint16_t       ntuples;
    uint16_t       offsets[FLEXIBLE_ARRAY_MEMBER];
} xk_pg_parser_xl_heap_multi_insert;

typedef struct xk_pg_parser_xl_multi_insert_tuple
{
    uint16_t        datalen;        /* size of tuple data that follows */
    uint16_t        t_infomask2;
    uint16_t        t_infomask;
    uint8_t         t_hoff;
    /* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xk_pg_parser_xl_multi_insert_tuple;

#define xk_pg_parser_SizeOfMultiInsertTuple (offsetof(xk_pg_parser_xl_multi_insert_tuple, t_hoff) \
                                            + sizeof(uint8_t))

typedef struct xk_pg_parser_xl_heap_delete
{
    xk_pg_parser_TransactionId xmax;            /* xmax of the deleted tuple */
    uint16_t                   offnum;          /* deleted tuple's offset */
    uint8_t                    infobits_set;    /* infomask bits */
    uint8_t                    flags;
} xk_pg_parser_xl_heap_delete;

typedef struct xk_pg_parser_xl_heap_update
{
    xk_pg_parser_TransactionId old_xmax;          /* xmax of the old tuple */
    uint16_t                   old_offnum;        /* old tuple's offset */
    uint8_t                    old_infobits_set;  /* infomask bits to set on old tuple */
    uint8_t                    flags;
    xk_pg_parser_TransactionId new_xmax;          /* xmax of the new tuple */
    uint16_t                   new_offnum;        /* new tuple's offset */

    /*
     * If XLOG_HEAP_CONTAINS_OLD_TUPLE or XLOG_HEAP_CONTAINS_OLD_KEY flags are
     * set, a xl_heap_header struct and tuple data for the old tuple follows.
     */
} xk_pg_parser_xl_heap_update;

#define xk_pg_parser_SizeOfHeapInsert (offsetof(xk_pg_parser_xl_heap_insert, flags) \
                                      + sizeof(uint8_t))

#define xk_pg_parser_SizeOfHeapDelete (offsetof(xk_pg_parser_xl_heap_delete, flags) \
                                      + sizeof(uint8_t))

#define xk_pg_parser_SizeOfHeapUpdate (offsetof(xk_pg_parser_xl_heap_update, new_offnum) \
                                      + sizeof(uint16_t))

extern bool xk_pg_parser_trans_rmgr_heap2_pre(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result, 
                            int32_t *xk_pg_parser_errno);

extern bool xk_pg_parser_trans_rmgr_heap_pre(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_pre_base **result, 
                            int32_t *xk_pg_parser_errno);

extern bool xk_pg_parser_trans_rmgr_heap_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno);

extern bool xk_pg_parser_trans_rmgr_heap2_trans(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno);

extern bool xk_pg_parser_trans_rmgr_heap_trans_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno);

extern bool xk_pg_parser_trans_rmgr_heap2_trans_get_tuple(xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                            xk_pg_parser_translog_tbcolbase **result,
                            int32_t *xk_pg_parser_errno);


bool xk_pg_parser_check_fpw(xk_pg_parser_trans_transrec_decode_XLogReaderState *readstate,
                            xk_pg_parser_translog_pre_base **xk_pg_parser_result,
                            int32_t *xk_pg_parser_errno,
                            int16_t dbtype);

#endif