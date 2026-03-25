/*-------------------------------------------------------------------------
 *
 *      SHIFT_JIS_2004 <--> UTF8
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/utf8_and_sjis2004/utf8_and_sjis2004.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_parser_os_incl.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_wchar.h"
#include "./unicode_map/shift_jis_2004_to_utf8.map"
#include "./unicode_map/utf8_to_shift_jis_2004.map"

/* ----------
 * conv_proc(
 *        INTEGER,    -- source encoding id
 *        INTEGER,    -- destination encoding id
 *        CSTRING,    -- source string (null terminated C string)
 *        CSTRING,    -- destination string (null terminated C string)
 *        INTEGER        -- source string length
 * ) returns VOID;
 * ----------
 */
void shift_jis_2004_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(SHIFT_JIS_2004, UTF8);

    LocalToUtf(src, len, dest, &shift_jis_2004_to_unicode_tree, LUmapSHIFT_JIS_2004_combined,
               conv_lengthof(LUmapSHIFT_JIS_2004_combined), NULL, SHIFT_JIS_2004);
}

void utf8_to_shift_jis_2004(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(UTF8, SHIFT_JIS_2004);

    UtfToLocal(src, len, dest, &shift_jis_2004_from_unicode_tree, ULmapSHIFT_JIS_2004_combined,
               conv_lengthof(ULmapSHIFT_JIS_2004_combined), NULL, SHIFT_JIS_2004);
}
