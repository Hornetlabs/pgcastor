#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "common/xk_pg_parser_translog.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_decode.h"
#include "thirdparty/compress/xk_pg_parser_thirdparty_lzcompress.h"
#include "trans/transrec/xk_pg_parser_trans_transrec_itemptr.h"
#include "image/xk_pg_parser_image.h"

#define IMAGE_MCXT NULL

#define LONG_ALIGN_MASK (sizeof(long) - 1)
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
#define MemSet(start, val, len) \
    do \
    { \
        /* must be void* because we don't know if it is integer aligned yet */ \
        void   *_vstart = (void *) (start); \
        int32_t        _val = (val); \
        size_t    _len = (len); \
\
        if ((((uintptr_t) _vstart) & LONG_ALIGN_MASK) == 0 && \
            (_len & LONG_ALIGN_MASK) == 0 && \
            _val == 0 && \
            _len <= MEMSET_LOOP_LIMIT && \
            /* \
             *    If MEMSET_LOOP_LIMIT == 0, optimizer should find \
             *    the whole "if" false at compile time. \
             */ \
            MEMSET_LOOP_LIMIT != 0) \
        { \
            long *_start = (long *) _vstart; \
            long *_stop = (long *) ((char *) _start + _len); \
            while (_start < _stop) \
                *_start++ = 0; \
        } \
        else \
            rmemset1(_vstart, 0, _val, _len); \
    } while (0)


bool xk_pg_parser_image_get_block_image(xk_pg_parser_trans_transrec_decode_XLogReaderState *record,
                                                            uint8_t block_id,
                                                            char *page,
                                                            int32_t block_size)
{
    xk_pg_parser_DecodedBkpBlock *bkpb;
    char *ptr;
    char *tmp = NULL;

    if (!xk_pg_parser_mcxt_malloc(IMAGE_MCXT, (void **)&tmp, block_size))
        return false;

#if XK_PG_VERSION_NUM >= 150000
    if (!record->record->blocks[block_id].in_use)
        return false;
    if (!record->record->blocks[block_id].has_image)
        return false;

    bkpb = &record->record->blocks[block_id];
#else
    if (!record->blocks[block_id].in_use)
        return false;
    if (!record->blocks[block_id].has_image)
        return false;

    bkpb = &record->blocks[block_id];
#endif
    ptr = bkpb->bkp_image;

#if XK_PG_VERSION_NUM >= 150000
    if (BKPIMAGE_COMPRESSED(bkpb->bimg_info))
#else
    if (bkpb->bimg_info & XK_PG_PARSER_TRANS_TRANSREC_BKPIMAGE_IS_COMPRESSED)
#endif
    {
#if XK_PG_VERSION_NUM >= 150000
        if ((bkpb->bimg_info & BKPIMAGE_COMPRESS_PGLZ) != 0)
        {
            if (xk_pg_parser_lz_decompress(ptr, bkpb->bimg_len, tmp,
                                block_size - bkpb->hole_length, true) < 0)
                return false;
        }
        else if ((bkpb->bimg_info & BKPIMAGE_COMPRESS_LZ4) != 0)
        {
#ifdef USE_LZ4
            if (LZ4_decompress_safe(ptr, tmp,
                                    bkpb->bimg_len,
                                    block_size - bkpb->hole_length) <= 0)
                return false;
#endif
        }
#else
        /* If a backup block image is compressed, decompress it. */
#if XK_PG_VERSION_NUM >= 120000
        if (xk_pg_parser_lz_decompress(ptr, bkpb->bimg_len, tmp,
                            block_size - bkpb->hole_length, true) < 0)
#else
        if (xk_pg_parser_lz_decompress(ptr, bkpb->bimg_len, tmp,
                            block_size - bkpb->hole_length) < 0)
#endif
        {
            return false;
        }
#endif
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

    xk_pg_parser_mcxt_free(IMAGE_MCXT, tmp);

    return true;
}

#define PG_PAGE_LAYOUT_VERSION 4
#define PageSetPageSizeAndVersion(page, size, version) \
( \
    ((xk_pg_parser_PageHeader) (page))->pd_pagesize_version = (size) | (version) \
)

#define PageGetMaxOffsetNumber(page) \
    (((xk_pg_parser_PageHeader) (page))->pd_lower <= xk_pg_parser_SizeOfPageHeaderData ? 0 : \
     ((((xk_pg_parser_PageHeader) (page))->pd_lower - xk_pg_parser_SizeOfPageHeaderData) \
      / sizeof(xk_pg_parser_ItemIdData)))

xk_pg_parser_translog_tuplecache *xk_pg_parser_image_get_tuple_from_image(char *page,
                                                                          uint32_t *tupcnt,
                                                                          uint32_t pageno,
                                                                          uint8_t  debug_level)
{
    xk_pg_parser_PageHeader phdr = (xk_pg_parser_PageHeader) page;
    xk_pg_parser_translog_tuplecache *result = NULL;
    uint32_t i = 0,
            count = 0;
    uint32_t item_num = PageGetMaxOffsetNumber(phdr);

    for (i = 0; i < item_num; i++)
    {
        /* 我们只获取状态为NORMAL的item */
        if (xk_pg_parser_ItemIdIsNormal(&phdr->pd_linp[i]))
            count++;
    }
    if (count != 0)
    {
        if (!xk_pg_parser_mcxt_malloc(IMAGE_MCXT, (void **) &result, count * sizeof(xk_pg_parser_translog_tuplecache)))
            return NULL;
    }
    else
        return NULL;

    *tupcnt = count;
    count = 0;

    xk_pg_parser_log_errlog(debug_level, "DEBUG: tuple num[%u]\n", item_num);

    for (i = 0; i < item_num; i++)
    {
        if (xk_pg_parser_ItemIdIsNormal(&phdr->pd_linp[i]))
        {
            if (!xk_pg_parser_mcxt_malloc(IMAGE_MCXT, (void **) &(result[count].m_tupledata), (int32_t)(phdr->pd_linp[i].lp_len)))
                return NULL;
            result[count].m_itemoffnum = i + 1;
            result[count].m_pageno = pageno;
            rmemcpy0(result[count].m_tupledata, 0, xk_pg_parser_PageGetItem(page, &phdr->pd_linp[i]), (size_t)(phdr->pd_linp[i].lp_len));
            result[count].m_tuplelen = phdr->pd_linp[i].lp_len;
            count++;
        }
    }
    if (XK_PG_PARSER_DEBUG_SILENCE < debug_level)
    {
        for (i = 0; i < *tupcnt; i++)
            printf("DEBUG: get tuple from FPW image, itemoff[%u], pageno[%u], tuplen[%u]\n",
                    result[i].m_itemoffnum, result[i].m_pageno, result[i].m_tuplelen);
    }
    return result;
}

xk_pg_parser_translog_tuplecache *xk_pg_parser_image_getTupleFromCache(xk_pg_parser_translog_tuplecache *cache,
                                                                       uint32_t cnt,
                                                                       uint32_t off,
                                                                       uint32_t pageno)
{
    uint32_t i = 0;
    xk_pg_parser_translog_tuplecache *result = NULL;
    for (i = 0; i < cnt; i++)
    {
        if (cache[i].m_itemoffnum == off && cache[i].m_pageno == pageno)
            result = &cache[i];
    }
    return result;
}

xk_pg_parser_translog_tuplecache *xk_pg_parser_image_get_tuple_from_image_with_dbtype(
                                        int32_t dbtype,
                                        char *dbversion,
                                        uint32_t pagesize,
                                        char *page,
                                        uint32_t *tupcnt,
                                        uint32_t pageno,
                                        uint8_t  debug_level)
{
    XK_PG_PARSER_UNUSED(dbtype);
    XK_PG_PARSER_UNUSED(dbversion);
    XK_PG_PARSER_UNUSED(pagesize);

    return xk_pg_parser_image_get_tuple_from_image(page, tupcnt, pageno, debug_level);
}
bool check_page_have_item(char *page)
{
    xk_pg_parser_PageHeader phdr = (xk_pg_parser_PageHeader) page;

    uint32_t item_num = PageGetMaxOffsetNumber(phdr);
    if (item_num > 0)
        return true;
    else
        return false;
}
