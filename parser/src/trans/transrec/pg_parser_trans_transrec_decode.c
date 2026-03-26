#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "trans/transrec/pg_parser_trans_transrec_rmgr.h"

#define MAX_ERRORMSG_LEN 1000
#define DECODE_MCXT      NULL

static void ResetDecoder(pg_parser_XLogReaderState* state);

bool pg_parser_trans_transrec_decodeXlogRecord(pg_parser_XLogReaderState* state,
                                               pg_parser_XLogRecord*      record,
                                               uint32_t                   BLCKSZ,
                                               int32_t*                   pg_parser_errno,
                                               bool                       is_pre_parsing,
                                               int16_t                    dbtype,
                                               char*                      dbversion)
{
    /*
     * read next _size bytes from record buffer, but check for overrun first.
     */
#define COPY_HEADER_FIELD(_dst, _size)                                    \
    do                                                                    \
    {                                                                     \
        if (remaining < _size)                                            \
            goto pg_parser_trans_transrec_decodeXlogRecord_shortdata_err; \
        rmemcpy1(_dst, 0, ptr, _size);                                    \
        ptr += _size;                                                     \
        remaining -= _size;                                               \
    } while (0)

    char*                  ptr;
    uint32_t               remaining;
    uint32_t               datatotal;
    pg_parser_RelFileNode* rnode = NULL;
    uint8_t                block_id;

    PG_PARSER_UNUSED(is_pre_parsing);
    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);

    ResetDecoder(state);

    state->decoded_record = record;
    state->record_origin = PG_PARSER_TRANSLOG_InvalidRepOriginId;

    ptr = (char*)record;
    ptr += SizeOfXLogRecord;
    remaining = record->xl_tot_len - SizeOfXLogRecord;

    /* Decode the headers */
    datatotal = 0;
    while (remaining > datatotal)
    {
        COPY_HEADER_FIELD(&block_id, sizeof(uint8_t));

        if (block_id == PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_DATA_SHORT)
        {
            /* XLogRecordDataHeaderShort */
            uint8_t main_data_len;

            COPY_HEADER_FIELD(&main_data_len, sizeof(uint8_t));

            state->main_data_len = main_data_len;
            datatotal += main_data_len;
            break; /* by convention, the main data fragment is
                    * always last */
        }
        else if (block_id == PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_DATA_LONG)
        {
            /* XLogRecordDataHeaderLong */
            uint32_t main_data_len;

            COPY_HEADER_FIELD(&main_data_len, sizeof(uint32_t));
            state->main_data_len = main_data_len;
            datatotal += main_data_len;
            break; /* by convention, the main data fragment is
                    * always last */
        }
        else if (block_id == PG_PARSER_TRANS_TRANSREC_XLR_BLOCK_ID_ORIGIN)
        {
            COPY_HEADER_FIELD(&state->record_origin, sizeof(pg_parser_RepOriginId));
        }
        else if (block_id <= PG_PARSER_TRANS_TRANSREC_XLR_MAX_BLOCK_ID)
        {
            /* XLogRecordBlockHeader */
            pg_parser_DecodedBkpBlock* blk;
            uint8_t                    fork_flags;

            if (block_id <= state->max_block_id)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_BLOCKID_CHECK;
                goto pg_parser_trans_transrec_decodeXlogRecord_err;
            }
            state->max_block_id = block_id;

            blk = &state->blocks[block_id];
            blk->in_use = true;
            blk->apply_image = false;

            COPY_HEADER_FIELD(&fork_flags, sizeof(uint8_t));
            blk->forknum = fork_flags & PG_PARSER_TRANS_TRANSREC_BKPBLOCK_FORK_MASK;
            blk->flags = fork_flags;
            blk->has_image = ((fork_flags & PG_PARSER_TRANS_TRANSREC_BKPBLOCK_HAS_IMAGE) != 0);
            blk->has_data = ((fork_flags & PG_PARSER_TRANS_TRANSREC_BKPBLOCK_HAS_DATA) != 0);

            COPY_HEADER_FIELD(&blk->data_len, sizeof(uint16_t));
            /* cross-check that the HAS_DATA flag is set iff data_length > 0 */
            if (blk->has_data && blk->data_len == 0)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_NODATA;
                goto pg_parser_trans_transrec_decodeXlogRecord_err;
            }
            if (!blk->has_data && blk->data_len != 0)
            {
                *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_DATASET;
                goto pg_parser_trans_transrec_decodeXlogRecord_err;
            }
            datatotal += blk->data_len;

            if (blk->has_image)
            {
                COPY_HEADER_FIELD(&blk->bimg_len, sizeof(uint16_t));
                COPY_HEADER_FIELD(&blk->hole_offset, sizeof(uint16_t));
                COPY_HEADER_FIELD(&blk->bimg_info, sizeof(uint8_t));

                blk->apply_image =
                    ((blk->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_APPLY) != 0);

                if (blk->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_IS_COMPRESSED)
                {
                    if (blk->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_HAS_HOLE)
                    {
                        COPY_HEADER_FIELD(&blk->hole_length, sizeof(uint16_t));
                    }
                    else
                    {
                        blk->hole_length = 0;
                    }
                }
                else
                {
                    blk->hole_length = BLCKSZ - blk->bimg_len;
                }
                datatotal += blk->bimg_len;

                /*
                 * cross-check that hole_offset > 0, hole_length > 0 and
                 * bimg_len < BLCKSZ if the HAS_HOLE flag is set.
                 */
                if ((blk->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_HAS_HOLE) &&
                    (blk->hole_offset == 0 || blk->hole_length == 0 || blk->bimg_len == BLCKSZ))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_DATASET;
                    goto pg_parser_trans_transrec_decodeXlogRecord_err;
                }

                /*
                 * cross-check that hole_offset == 0 and hole_length == 0 if
                 * the HAS_HOLE flag is not set.
                 */
                if (!(blk->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_HAS_HOLE) &&
                    (blk->hole_offset != 0 || blk->hole_length != 0))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_HASHOLE;
                    goto pg_parser_trans_transrec_decodeXlogRecord_err;
                }

                /*
                 * cross-check that bimg_len < BLCKSZ if the IS_COMPRESSED
                 * flag is set.
                 */
                if ((blk->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_IS_COMPRESSED) &&
                    blk->bimg_len == BLCKSZ)
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_HASCOMPRESSED;
                    goto pg_parser_trans_transrec_decodeXlogRecord_err;
                }

                /*
                 * cross-check that bimg_len = BLCKSZ if neither HAS_HOLE nor
                 * IS_COMPRESSED flag is set.
                 */
                if (!(blk->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_HAS_HOLE) &&
                    !(blk->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_IS_COMPRESSED) &&
                    blk->bimg_len != BLCKSZ)
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_COMPRESSED_LENGTH;
                    goto pg_parser_trans_transrec_decodeXlogRecord_err;
                }
            }
            if (!(fork_flags & PG_PARSER_TRANS_TRANSREC_BKPBLOCK_SAME_REL))
            {
                COPY_HEADER_FIELD(&blk->rnode, sizeof(pg_parser_RelFileNode));
                rnode = &(blk->rnode);
            }
            else
            {
                if (rnode == NULL)
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_NOSAMEREL;
                    goto pg_parser_trans_transrec_decodeXlogRecord_err;
                }

                blk->rnode = *rnode;
            }
            COPY_HEADER_FIELD(&blk->blkno, sizeof(pg_parser_BlockNumber));
        }
        else
        {
            *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_INVALID_BLOCKID;
            goto pg_parser_trans_transrec_decodeXlogRecord_err;
        }
    }

    if (remaining != datatotal)
    {
        goto pg_parser_trans_transrec_decodeXlogRecord_shortdata_err;
    }

    /*
     * Ok, we've parsed the fragment headers, and verified that the total
     * length of the payload in the fragments is equal to the amount of data
     * left. Copy the data of each fragment to a separate buffer.
     *
     * We could just set up pointers into readRecordBuf, but we want to align
     * the data for the convenience of the callers. Backup images are not
     * copied, however; they don't need alignment.
     */

    /* block data first */
    for (block_id = 0; block_id <= state->max_block_id; block_id++)
    {
        pg_parser_DecodedBkpBlock* blk = &state->blocks[block_id];

        if (!blk->in_use)
        {
            continue;
        }

        if (blk->has_image)
        {
            blk->bkp_image = ptr;
            ptr += blk->bimg_len;
        }
        if (blk->has_data)
        {
            if (!blk->data || blk->data_len > blk->data_bufsz)
            {
                if (blk->data)
                {
                    pg_parser_mcxt_free(DECODE_MCXT, blk->data);
                }

                /*
                 * Force the initial request to be BLCKSZ so that we don't
                 * waste time with lots of trips through this stanza as a
                 * result of WAL compression.
                 */
                blk->data_bufsz = PG_PARSER_MAXALIGN(PG_PARSER_Max(blk->data_len, BLCKSZ));
                if (!pg_parser_mcxt_malloc(DECODE_MCXT, (void**)(&(blk->data)), blk->data_bufsz))
                {
                    *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_BLOCK;
                    return false;
                }
            }
            rmemcpy1(blk->data, 0, ptr, blk->data_len);
            ptr += blk->data_len;
        }
    }
    /* and finally, the main data */
    if (state->main_data_len > 0)
    {
        if (!state->main_data || state->main_data_len > state->main_data_bufsz)
        {
            /*
             * main_data_bufsz must be PG_PARSER_MAXALIGN'ed.  In many xlog record
             * types, we omit trailing struct padding on-disk to save a few
             * bytes; but compilers may generate accesses to the xlog struct
             * that assume that padding bytes are present.  If the palloc
             * request is not large enough to include such padding bytes then
             * we'll get valgrind complaints due to otherwise-harmless fetches
             * of the padding bytes.
             *
             * In addition, force the initial request to be reasonably large
             * so that we don't waste time with lots of trips through this
             * stanza.  BLCKSZ / 2 seems like a good compromise choice.
             */
            state->main_data_bufsz =
                PG_PARSER_MAXALIGN(PG_PARSER_Max(state->main_data_len, BLCKSZ / 2));
            if (!pg_parser_mcxt_malloc(
                    DECODE_MCXT, (void**)(&(state->main_data)), state->main_data_bufsz))
            {
                *pg_parser_errno = ERRNO_PG_PARSER_PRE_MEMERR_ALLOC_MAINDATA;
                return false;
            }
        }
        rmemcpy1(state->main_data, 0, ptr, state->main_data_len);
        ptr += state->main_data_len;
    }
    return true;

pg_parser_trans_transrec_decodeXlogRecord_shortdata_err:
    *pg_parser_errno = ERRNO_PG_PARSER_DECODE_RECORD_SHORT;
pg_parser_trans_transrec_decodeXlogRecord_err:
    return false;
}

/* Initialize state used for parsing */
pg_parser_XLogReaderState* pg_parser_trans_transrec_decode_XLogReader_Allocate(void)
{
    pg_parser_XLogReaderState* state = NULL;

    if (!pg_parser_mcxt_malloc(DECODE_MCXT, (void**)(&state), sizeof(pg_parser_XLogReaderState)))
    {
        return NULL;
    }

    state->max_block_id = -1;

    return state;
}

/* Free state related resources */
void pg_parser_trans_transrec_decode_XLogReader_XLogReaderFree(pg_parser_XLogReaderState* state)
{
    int32_t block_id;

    for (block_id = 0; block_id < PG_PARSER_TRANS_TRANSREC_XLR_MAX_BLOCK_ID; block_id++)
    {
        if (state->blocks[block_id].data)
        {
            pg_parser_mcxt_free(DECODE_MCXT, state->blocks[block_id].data);
            state->blocks[block_id].data = NULL;
        }
    }
    if (state->main_data)
    {
        pg_parser_mcxt_free(DECODE_MCXT, state->main_data);
        state->main_data = NULL;
    }
    pg_parser_mcxt_free(DECODE_MCXT, state);
}

/* Reset state status */
static void ResetDecoder(pg_parser_XLogReaderState* state)
{
    int32_t block_id;

    state->decoded_record = NULL;

    state->main_data_len = 0;

    for (block_id = 0; block_id <= state->max_block_id; block_id++)
    {
        state->blocks[block_id].in_use = false;
        state->blocks[block_id].has_image = false;
        state->blocks[block_id].has_data = false;
        state->blocks[block_id].apply_image = false;
    }
    state->max_block_id = -1;
}

char* pg_parser_XLogRecGetBlockData(pg_parser_XLogReaderState* record,
                                    uint8_t                    block_id,
                                    size_t*                    len)
{
    pg_parser_DecodedBkpBlock* bkpb;
    if (!record->blocks[block_id].in_use)
    {
        return NULL;
    }

    bkpb = &record->blocks[block_id];

    if (!bkpb->has_data)
    {
        if (len)
        {
            *len = 0;
        }
        return NULL;
    }
    else
    {
        if (len)
        {
            *len = bkpb->data_len;
        }
        return bkpb->data;
    }
}
