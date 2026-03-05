/*-------------------------------------------------------------------------
 *
 *      ASCII <--> UTF8
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/utf8_and_ascii/utf8_and_ascii.c
 *
 *-------------------------------------------------------------------------
 */
#include "xk_pg_parser_os_incl.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_wchar.h"


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

void ascii_to_utf8(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_SQL_ASCII, XK_UTF8);

    /* this looks wrong, but basically we're just rejecting high-bit-set */
    xk_conv_ascii2mic(src, dest, len);
}

void utf8_to_ascii(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_UTF8, XK_SQL_ASCII);

    /* this looks wrong, but basically we're just rejecting high-bit-set */
    xk_conv_mic2ascii(src, dest, len);
}
