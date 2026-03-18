#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "trans/transrec/pg_parser_trans_transrec_decode.h"
#include "thirdparty/compress/pg_parser_thirdparty_lzcompress.h"
#include "trans/transrec/pg_parser_trans_transrec_itemptr.h"
#include "image/pg_parser_image.h"

#define IMAGE_MCXT        NULL

#define LONG_ALIGN_MASK   (sizeof(long) - 1)
#define MEMSET_LOOP_LIMIT 1024
/*
 * MemSet
 *    Exactly the same as standard library function memset(), but considerably
 *    faster for zeroing small word-aligned structures (such as parsetree nodes).
 *    This has to be a macro because the main point is to avoid function-call
 *    overhead.   However, we have also found that the loop is faster than
 *    native libc memset() on some platforms, even those with assembler
 *    memset() functions.  More research needs to be done, perhaps with
 *    MEMSET_LOOP_LIMIT tests in configure.
 */
#define MemSet(start, val, len)                                                                            \
    do                                                                                                     \
    {                                                                                                      \
        /* must be void* because we don't know if it is integer aligned yet */                             \
        void*   _vstart = (void*)(start);                                                                  \
        int32_t _val = (val);                                                                              \
        size_t  _len = (len);                                                                              \
                                                                                                           \
        if ((((uintptr_t)_vstart) & LONG_ALIGN_MASK) == 0 && (_len & LONG_ALIGN_MASK) == 0 && _val == 0 && \
            _len <= MEMSET_LOOP_LIMIT && /*                                                                \
                                          *    If MEMSET_LOOP_LIMIT == 0, optimizer should find            \
                                          *    the whole "if" false at compile time.                       \
                                          */                                                               \
            MEMSET_LOOP_LIMIT != 0)                                                                        \
        {                                                                                                  \
            long* _start = (long*)_vstart;                                                                 \
            long* _stop = (long*)((char*)_start + _len);                                                   \
            while (_start < _stop)                                                                         \
                *_start++ = 0;                                                                             \
        }                                                                                                  \
        else                                                                                               \
            rmemset1(_vstart, 0, _val, _len);                                                              \
    } while (0)

bool pg_parser_image_get_block_image(pg_parser_XLogReaderState* record,
                                     uint8_t                    block_id,
                                     char*                      page,
                                     int32_t                    block_size)
{
    pg_parser_DecodedBkpBlock* bkpb;
    char*                      ptr;
    char*                      tmp = NULL;

    if (!pg_parser_mcxt_malloc(IMAGE_MCXT, (void**)&tmp, block_size))
    {
        return false;
    }

    if (!record->blocks[block_id].in_use)
    {
        return false;
    }
    if (!record->blocks[block_id].has_image)
    {
        return false;
    }

    bkpb = &record->blocks[block_id];
    ptr = bkpb->bkp_image;

    if (bkpb->bimg_info & PG_PARSER_TRANS_TRANSREC_BKPIMAGE_IS_COMPRESSED)
    {
        /* If a backup block image is compressed, decompress it. */
        if (pg_parser_lz_decompress(ptr, bkpb->bimg_len, tmp, block_size - bkpb->hole_length, true) < 0)
        {
            return false;
        }
        ptr = tmp;
    }

    /* generate page, taking into account hole if necessary. */
    if (bkpb->hole_length == 0)
    {
        rmemcpy1(page, 0, ptr, block_size);
    }
    else
    {
        rmemcpy1(page, 0, ptr, bkpb->hole_offset);
        /* must zero-fill the hole. */
        MemSet(page + bkpb->hole_offset, 0, bkpb->hole_length);
        rmemcpy1(page + (bkpb->hole_offset + bkpb->hole_length),
                 0,
                 ptr + bkpb->hole_offset,
                 block_size - (bkpb->hole_offset + bkpb->hole_length));
    }

    pg_parser_mcxt_free(IMAGE_MCXT, tmp);

    return true;
}

#define PG_PAGE_LAYOUT_VERSION 4
#define PageSetPageSizeAndVersion(page, size, version) \
    (((pg_parser_PageHeader)(page))->pd_pagesize_version = (size) | (version))

#define PageGetMaxOffsetNumber(page)                                                      \
    (((pg_parser_PageHeader)(page))->pd_lower <= pg_parser_SizeOfPageHeaderData           \
         ? 0                                                                              \
         : ((((pg_parser_PageHeader)(page))->pd_lower - pg_parser_SizeOfPageHeaderData) / \
            sizeof(pg_parser_ItemIdData)))

pg_parser_translog_tuplecache* pg_parser_image_get_tuple_from_image(char*     page,
                                                                    uint32_t* tupcnt,
                                                                    uint32_t  pageno,
                                                                    uint8_t   debug_level)
{
    pg_parser_PageHeader           phdr = (pg_parser_PageHeader)page;
    pg_parser_translog_tuplecache* result = NULL;
    uint32_t                       i = 0, count = 0;
    uint32_t                       item_num = PageGetMaxOffsetNumber(phdr);

    for (i = 0; i < item_num; i++)
    {
        /* We only get items with NORMAL status */
        if (pg_parser_ItemIdIsNormal(&phdr->pd_linp[i]))
        {
            count++;
        }
    }
    if (count != 0)
    {
        if (!pg_parser_mcxt_malloc(IMAGE_MCXT, (void**)&result, count * sizeof(pg_parser_translog_tuplecache)))
        {
            return NULL;
        }
    }
    else
    {
        return NULL;
    }

    *tupcnt = count;
    count = 0;

    pg_parser_log_errlog(debug_level, "DEBUG: tuple num[%u]\n", item_num);

    for (i = 0; i < item_num; i++)
    {
        if (pg_parser_ItemIdIsNormal(&phdr->pd_linp[i]))
        {
            if (!pg_parser_mcxt_malloc(IMAGE_MCXT,
                                       (void**)&(result[count].m_tupledata),
                                       (int32_t)(phdr->pd_linp[i].lp_len)))
            {
                return NULL;
            }
            result[count].m_itemoffnum = i + 1;
            result[count].m_pageno = pageno;
            rmemcpy0(result[count].m_tupledata,
                     0,
                     pg_parser_PageGetItem(page, &phdr->pd_linp[i]),
                     (size_t)(phdr->pd_linp[i].lp_len));
            result[count].m_tuplelen = phdr->pd_linp[i].lp_len;
            count++;
        }
    }
    if (PG_PARSER_DEBUG_SILENCE < debug_level)
    {
        for (i = 0; i < *tupcnt; i++)
        {
            printf("DEBUG: get tuple from FPW image, itemoff[%u], pageno[%u], tuplen[%u]\n",
                   result[i].m_itemoffnum,
                   result[i].m_pageno,
                   result[i].m_tuplelen);
        }
    }
    return result;
}

pg_parser_translog_tuplecache* pg_parser_image_getTupleFromCache(pg_parser_translog_tuplecache* cache,
                                                                 uint32_t                       cnt,
                                                                 uint32_t                       off,
                                                                 uint32_t                       pageno)
{
    uint32_t                       i = 0;
    pg_parser_translog_tuplecache* result = NULL;
    for (i = 0; i < cnt; i++)
    {
        if (cache[i].m_itemoffnum == off && cache[i].m_pageno == pageno)
        {
            result = &cache[i];
        }
    }
    return result;
}

pg_parser_translog_tuplecache* pg_parser_image_get_tuple_from_image_with_dbtype(int32_t   dbtype,
                                                                                char*     dbversion,
                                                                                uint32_t  pagesize,
                                                                                char*     page,
                                                                                uint32_t* tupcnt,
                                                                                uint32_t  pageno,
                                                                                uint8_t   debug_level)
{
    PG_PARSER_UNUSED(dbtype);
    PG_PARSER_UNUSED(dbversion);
    PG_PARSER_UNUSED(pagesize);

    return pg_parser_image_get_tuple_from_image(page, tupcnt, pageno, debug_level);
}

bool check_page_have_item(char* page)
{
    pg_parser_PageHeader phdr = (pg_parser_PageHeader)page;

    uint32_t             item_num = PageGetMaxOffsetNumber(phdr);
    if (item_num > 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}
