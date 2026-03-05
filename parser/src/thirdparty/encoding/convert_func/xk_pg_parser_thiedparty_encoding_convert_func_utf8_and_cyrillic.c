/*-------------------------------------------------------------------------
 *
 *      UTF8 and Cyrillic
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/utf8_and_cyrillic/utf8_and_cyrillic.c
 *
 *-------------------------------------------------------------------------
 */
#include "xk_pg_parser_os_incl.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_wchar.h"
#include "./unicode_map/utf8_to_koi8r.map"
#include "./unicode_map/koi8r_to_utf8.map"
#include "./unicode_map/utf8_to_koi8u.map"
#include "./unicode_map/koi8u_to_utf8.map"


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

void utf8_to_koi8r(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_UTF8, XK_KOI8R);

    UtfToLocal(src, len, dest,
               &koi8r_from_unicode_tree,
               NULL, 0,
               NULL,
               XK_KOI8R);
}

void koi8r_to_utf8(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_KOI8R, XK_UTF8);

    LocalToUtf(src, len, dest,
               &koi8r_to_unicode_tree,
               NULL, 0,
               NULL,
               XK_KOI8R);
}

void utf8_to_koi8u(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_UTF8, XK_KOI8U);

    UtfToLocal(src, len, dest,
               &koi8u_from_unicode_tree,
               NULL, 0,
               NULL,
               XK_KOI8U);
}

void koi8u_to_utf8(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_KOI8U, XK_UTF8);

    LocalToUtf(src, len, dest,
               &koi8u_to_unicode_tree,
               NULL, 0,
               NULL,
               XK_KOI8U);
}
