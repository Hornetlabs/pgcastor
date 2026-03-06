#ifndef XK_PG_PARSER_TRANS_TRANSREC_DECODE_H
#define XK_PG_PARSER_TRANS_TRANSREC_DECODE_HY

/* BLOCK define begin*/
#define XK_PG_PARSER_TRANS_TRANSREC_XLR_MAX_BLOCK_ID            32

#define XK_PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_DATA_SHORT        255
#define XK_PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_DATA_LONG        254
#define XK_PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_ORIGIN            253

#define XK_PG_PARSER_TRANS_TRANSREC_BKPBLOCK_FORK_MASK    0x0F
#define XK_PG_PARSER_TRANS_TRANSREC_BKPBLOCK_FLAG_MASK    0xF0
#define XK_PG_PARSER_TRANS_TRANSREC_BKPBLOCK_HAS_IMAGE    0x10    /* block data is an XLogRecordBlockImage */
#define XK_PG_PARSER_TRANS_TRANSREC_BKPBLOCK_HAS_DATA    0x20
#define XK_PG_PARSER_TRANS_TRANSREC_BKPBLOCK_WILL_INIT    0x40    /* redo will re-init the page */
#define XK_PG_PARSER_TRANS_TRANSREC_BKPBLOCK_SAME_REL    0x80    /* xk_pg_parser_RelFileNode omitted, same as previous */

#define XK_PG_PARSER_TRANS_TRANSREC_BKPIMAGE_HAS_HOLE        0x01    /* page image has "hole" */
#define XK_PG_PARSER_TRANS_TRANSREC_BKPIMAGE_IS_COMPRESSED        0x02    /* page image is compressed */
#define XK_PG_PARSER_TRANS_TRANSREC_BKPIMAGE_APPLY        0x04    /* page image should be restored during
                                     * replay */

#define xk_pg_parser_XLogRecHasBlockImage(decoder, block_id) \
        ((decoder)->blocks[block_id].has_image)

#define xk_pg_parser_XLogRecGetInfo(decoder) ((decoder)->decoded_record->xl_info)

#define xk_pg_parser_XLogRecGetData(decoder) ((decoder)->main_data)
#define xk_pg_parser_XLogRecGetDataLen(decoder) ((decoder)->main_data_len)

#define xk_pg_parser_PageGetItemId(page, offsetNumber) \
    ((xk_pg_parser_ItemId) (&((xk_pg_parser_PageHeader) (page))->pd_linp[(offsetNumber) - 1]))

#define xk_pg_parser_PageIsValid(page) ((const void*)(page) != NULL)

#define xk_pg_parser_ItemIdHasStorage(itemId) \
    ((itemId)->lp_len != 0)

#define xk_pg_parser_ItemIdGetOffset(itemId) \
   ((itemId)->lp_off)

#define xk_pg_parser_PageGetItem(page, itemId) \
( \
    xk_pg_parser_AssertMacro(xk_pg_parser_PageIsValid(page)), \
    xk_pg_parser_AssertMacro(xk_pg_parser_ItemIdHasStorage(itemId)), \
    (char *)(((char *)(page)) + xk_pg_parser_ItemIdGetOffset(itemId)) \
)

#define xk_pg_parser_XLogRecGetXid(decoder) ((decoder)->decoded_record->xl_xid)

/* BLOCK define end */

/**
 * @name xk_pg_parser_XLogRecord
 * @brief 用于保存record头部信息
 */
typedef struct xk_pg_parser_XLogRecord
{
    uint32_t             xl_tot_len;        /* record长度 */
    xk_pg_parser_TransactionId        xl_xid;            /* xact id */
    xk_pg_parser_XLogRecPtr           l_prev;            /* 上一条record的偏移 */
    uint8_t              xl_info;           /* 标志位 */
    xk_pg_parser_RmgrId               xl_rmid;           /* resource manager for this record */
    /* 2 bytes of padding here, initialize to zero */
    xk_pg_parser_crc32c            xl_crc;            /* CRC for this record */

    /* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */

} xk_pg_parser_XLogRecord;

#define SizeOfXLogRecord    (offsetof(xk_pg_parser_XLogRecord, xl_crc) + sizeof(xk_pg_parser_crc32c))

/**
 * @name xk_pg_parser_DecodedBkpBlock
 * @brief 保存有block的结构
 */
typedef struct
{
    /* Is this block ref in use? */
    bool        in_use;

    /* Identify the block this refers to */
    xk_pg_parser_RelFileNode rnode;
    uint8_t    forknum;
    xk_pg_parser_BlockNumber blkno;

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    uint8_t        flags;

    /* Information on full-page image, if any */
    bool        has_image;        /* has image, even for consistency checking */
    bool        apply_image;    /* has image that should be restored */
    char       *bkp_image;
    uint16_t        hole_offset;
    uint16_t        hole_length;
    uint16_t        bimg_len;
    uint8_t        bimg_info;

    /* Buffer holding the rmgr-specific data associated with this block */
    bool        has_data;
    char       *data;
    uint16_t        data_len;
    uint16_t        data_bufsz;
} xk_pg_parser_DecodedBkpBlock;

/**
 * @name XLogReaderState
 * @brief 用于支撑record解码时的出参
 */
struct xk_pg_parser_trans_transrec_decode_XLogReaderState
{
    xk_pg_parser_XLogRecord *decoded_record; /* currently decoded record */

    char           *main_data;        /* record's main data portion */
    uint32_t        main_data_len;    /* main data portion's length */
    uint32_t        main_data_bufsz;    /* allocated size of the buffer */

    xk_pg_parser_RepOriginId record_origin;

    /* information about blocks referenced by the record. */
    xk_pg_parser_DecodedBkpBlock blocks[XK_PG_PARSER_TRANS_TRANSREC_XLR_MAX_BLOCK_ID + 1];

    int32_t         max_block_id;    /* highest block_id in use (-1 if none) */
    int32_t        *xk_pg_parser_errno;
    xk_pg_parser_translog_translog2col *trans_data;
    xk_pg_parser_translog_pre          *pre_trans_data;
};

typedef struct xk_pg_parser_trans_transrec_decode_XLogReaderState
               xk_pg_parser_trans_transrec_decode_XLogReaderState;

/* 函数声明 begin*/
extern xk_pg_parser_trans_transrec_decode_XLogReaderState 
*xk_pg_parser_trans_transrec_decode_XLogReader_Allocate(void);

extern void xk_pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(
            xk_pg_parser_trans_transrec_decode_XLogReaderState *state);

extern bool xk_pg_parser_trans_transrec_decodeXlogRecord(
                                xk_pg_parser_trans_transrec_decode_XLogReaderState *state,
                                xk_pg_parser_XLogRecord *record,
                                uint32_t BLCKSZ,
                                int32_t *xk_pg_parser_errno,
                                bool is_pre_parsing,
                                int16_t dbtype,
                                char *dbversion);

extern char *xk_pg_parser_XLogRecGetBlockData(
                            xk_pg_parser_trans_transrec_decode_XLogReaderState *record,
                            uint8_t block_id,
                            size_t *len);
/* 函数声明 end */

#endif
