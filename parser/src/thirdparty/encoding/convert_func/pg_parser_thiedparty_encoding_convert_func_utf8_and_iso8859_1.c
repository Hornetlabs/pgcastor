/*-------------------------------------------------------------------------
 *
 *      ISO8859_1 <--> UTF8
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/utf8_and_iso8859_1/utf8_and_iso8859_1.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_parser_os_incl.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_wchar.h"


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

void iso8859_1_to_utf8(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t        len = str_len;
    unsigned short c;

    CHECK_ENCODING_CONVERSION_ARGS(LATIN1, UTF8);

    while (len > 0)
    {
        c = *src;
        if (c == 0)
        {
            /* report_invalid_encoding(LATIN1, (const char *) src, len); */
            break;
        }
        if (!IS_HIGHBIT_SET(c))
            *dest++ = c;
        else
        {
            *dest++ = (c >> 6) | 0xc0;
            *dest++ = (c & 0x003f) | HIGHBIT;
        }
        src++;
        len--;
    }
    *dest = '\0';
}

void utf8_to_iso8859_1(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t        len = str_len;
    unsigned short c,
                c1;

    CHECK_ENCODING_CONVERSION_ARGS(UTF8, LATIN1);

    while (len > 0)
    {
        c = *src;
        if (c == 0)
        {
            /* report_invalid_encoding(UTF8, (const char *) src, len); */
            break;
        }
        /* fast path for ASCII-subset characters */
        if (!IS_HIGHBIT_SET(c))
        {
            *dest++ = c;
            src++;
            len--;
        }
        else
        {
            int32_t l = character_utf_mblen(src);

            if (l > len || !character_utf8_islegal(src, l))
            {
                /* report_invalid_encoding(UTF8, (const char *) src, len); */
                break;
            }
            if (l != 2)
            {
                /* report_untranslatable_char(UTF8, LATIN1,
                                           (const char *) src, len); */
                break;
            }
            c1 = src[1] & 0x3f;
            c = ((c & 0x1f) << 6) | c1;
            if (c >= 0x80 && c <= 0xff)
            {
                *dest++ = (unsigned char) c;
                src += 2;
                len -= 2;
            }
            else
            {
                /* report_untranslatable_char(UTF8, LATIN1,
                                           (const char *) src, len); */
                break;
            }
        }
    }
    *dest = '\0';
}
