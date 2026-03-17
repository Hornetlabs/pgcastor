/*-------------------------------------------------------------------------
 *
 *      JOHAB <--> UTF8
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/utf8_and_johab/utf8_and_johab.c
 *
 *-------------------------------------------------------------------------
 */
#include "xk_pg_parser_os_incl.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_wchar.h"
#include "./unicode_map/johab_to_utf8.map"
#include "./unicode_map/utf8_to_johab.map"


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
void johab_to_utf8(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_JOHAB, XK_UTF8);

    LocalToUtf(src, len, dest,
               &johab_to_unicode_tree,
               NULL, 0,
               NULL,
               XK_JOHAB);
}

void utf8_to_johab(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_UTF8, XK_JOHAB);

    UtfToLocal(src, len, dest,
               &johab_from_unicode_tree,
               NULL, 0,
               NULL,
               XK_JOHAB);
}
