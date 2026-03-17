/*-------------------------------------------------------------------------
 *
 *      SJIS <--> UTF8
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/utf8_and_sjis/utf8_and_sjis.c
 *
 *-------------------------------------------------------------------------
 */
#include "xk_pg_parser_os_incl.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_wchar.h"
#include "./unicode_map/sjis_to_utf8.map"
#include "./unicode_map/utf8_to_sjis.map"


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
void sjis_to_utf8(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_SJIS, XK_UTF8);

    LocalToUtf(src, len, dest,
               &sjis_to_unicode_tree,
               NULL, 0,
               NULL,
               XK_SJIS);
}

void utf8_to_sjis(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_UTF8, XK_SJIS);

    UtfToLocal(src, len, dest,
               &sjis_from_unicode_tree,
               NULL, 0,
               NULL,
               XK_SJIS);
}
