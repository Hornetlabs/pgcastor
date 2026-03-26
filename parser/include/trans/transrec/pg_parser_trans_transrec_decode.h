#ifndef PG_PARSER_TRANS_TRANSREC_DECODE_H
#define PG_PARSER_TRANS_TRANSREC_DECODE_H

/* BLOCK define begin*/
#define PG_PARSER_TRANS_TRANSREC_XLR_MAX_BLOCK_ID        32

#define PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_DATA_SHORT 255
#define PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_DATA_LONG  254
#define PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_ORIGIN     253

#define PG_PARSER_TRANS_TRANSREC_BKPBLOCK_FORK_MASK      0x0F
#define PG_PARSER_TRANS_TRANSREC_BKPBLOCK_FLAG_MASK      0xF0
#define PG_PARSER_TRANS_TRANSREC_BKPBLOCK_HAS_IMAGE \
    0x10 /* block data is an XLogRecordBlockImage   \
          */
#define PG_PARSER_TRANS_TRANSREC_BKPBLOCK_HAS_DATA  0x20
#define PG_PARSER_TRANS_TRANSREC_BKPBLOCK_WILL_INIT 0x40 /* redo will re-init the page */
#define PG_PARSER_TRANS_TRANSREC_BKPBLOCK_SAME_REL \
    0x80 /* pg_parser_RelFileNode omitted, same as previous */

#define PG_PARSER_TRANS_TRANSREC_BKPIMAGE_HAS_HOLE      0x01 /* page image has "hole" */
#define PG_PARSER_TRANS_TRANSREC_BKPIMAGE_IS_COMPRESSED 0x02 /* page image is compressed */
#define PG_PARSER_TRANS_TRANSREC_BKPIMAGE_APPLY  \
    0x04 /* page image should be restored during \
          * replay */

#define pg_parser_XLogRecHasBlockImage(decoder, block_id) ((decoder)->blocks[block_id].has_image)

#define pg_parser_XLogRecGetInfo(decoder)                 ((decoder)->decoded_record->xl_info)

#define pg_parser_XLogRecGetData(decoder)                 ((decoder)->main_data)
#define pg_parser_XLogRecGetDataLen(decoder)              ((decoder)->main_data_len)

#define pg_parser_PageGetItemId(page, offsetNumber) \
    ((pg_parser_ItemId)(&((pg_parser_PageHeader)(page))->pd_linp[(offsetNumber) - 1]))

#define pg_parser_PageIsValid(page)        ((const void*)(page) != NULL)

#define pg_parser_ItemIdHasStorage(itemId) ((itemId)->lp_len != 0)

#define pg_parser_ItemIdGetOffset(itemId)  ((itemId)->lp_off)

#define pg_parser_PageGetItem(page, itemId)                     \
    (pg_parser_AssertMacro(pg_parser_PageIsValid(page)),        \
     pg_parser_AssertMacro(pg_parser_ItemIdHasStorage(itemId)), \
     (char*)(((char*)(page)) + pg_parser_ItemIdGetOffset(itemId)))

#define pg_parser_XLogRecGetXid(decoder) ((decoder)->decoded_record->xl_xid)

/* BLOCK define end */

/**
 * @name pg_parser_XLogRecord
 * @brief save record
 */
typedef struct pg_parser_XLogRecord
{
    uint32_t                xl_tot_len; /* record length */
    pg_parser_TransactionId xl_xid;     /* xact id */
    pg_parser_XLogRecPtr    l_prev;     /* offset of previous record */
    uint8_t                 xl_info;    /* flag bits */
    pg_parser_RmgrId        xl_rmid;    /* resource manager for this record */
    /* 2 bytes of padding here, initialize to zero */
    pg_parser_crc32c        xl_crc; /* CRC for this record */

    /* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */

} pg_parser_XLogRecord;

#define SizeOfXLogRecord (offsetof(pg_parser_XLogRecord, xl_crc) + sizeof(pg_parser_crc32c))

/**
 * @name pg_parser_DecodedBkpBlock
 * @brief save block
 */
typedef struct
{
    /* Is this block ref in use? */
    bool                  in_use;

    /* Identify the block this refers to */
    pg_parser_RelFileNode rnode;
    uint8_t               forknum;
    pg_parser_BlockNumber blkno;

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    uint8_t               flags;

    /* Information on full-page image, if any */
    bool                  has_image;   /* has image, even for consistency checking */
    bool                  apply_image; /* has image that should be restored */
    char*                 bkp_image;
    uint16_t              hole_offset;
    uint16_t              hole_length;
    uint16_t              bimg_len;
    uint8_t               bimg_info;

    /* Buffer holding the rmgr-specific data associated with this block */
    bool                  has_data;
    char*                 data;
    uint16_t              data_len;
    uint16_t              data_bufsz;
} pg_parser_DecodedBkpBlock;

/**
 * @name XLogReaderState
 * @brief output parameter used for record decoding
 */
struct pg_parser_XLogReaderState
{
    pg_parser_XLogRecord*            decoded_record; /* currently decoded record */

    char*                            main_data;       /* record's main data portion */
    uint32_t                         main_data_len;   /* main data portion's length */
    uint32_t                         main_data_bufsz; /* allocated size of the buffer */

    pg_parser_RepOriginId            record_origin;

    /* information about blocks referenced by the record. */
    pg_parser_DecodedBkpBlock        blocks[PG_PARSER_TRANS_TRANSREC_XLR_MAX_BLOCK_ID + 1];

    int32_t                          max_block_id; /* highest block_id in use (-1 if none) */
    int32_t*                         pg_parser_errno;
    pg_parser_translog_translog2col* trans_data;
    pg_parser_translog_pre*          pre_trans_data;
};

typedef struct pg_parser_XLogReaderState pg_parser_XLogReaderState;

/* function declaration begin*/
extern pg_parser_XLogReaderState* pg_parser_trans_transrec_decode_XLogReader_Allocate(void);

extern void pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(
    pg_parser_XLogReaderState* state);

extern bool pg_parser_trans_transrec_decodeXlogRecord(pg_parser_XLogReaderState* state,
                                                      pg_parser_XLogRecord*      record,
                                                      uint32_t                   BLCKSZ,
                                                      int32_t*                   pg_parser_errno,
                                                      bool                       is_pre_parsing,
                                                      int16_t                    dbtype,
                                                      char*                      dbversion);

extern char* pg_parser_XLogRecGetBlockData(pg_parser_XLogReaderState* record,
                                           uint8_t                    block_id,
                                           size_t*                    len);
/* function declaration end */

#endif
