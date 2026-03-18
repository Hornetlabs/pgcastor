/*-------------------------------------------------------------------------
 *
 *      LATINn and MULE_INTERNAL
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c
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

void latin1_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(LATIN1, MULE_INTERNAL);

    latin2mic(src, dest, len, LC_ISO8859_1, LATIN1);
}

void mic_to_latin1(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(MULE_INTERNAL, LATIN1);

    mic2latin(src, dest, len, LC_ISO8859_1, LATIN1);
}

void latin3_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(LATIN3, MULE_INTERNAL);

    latin2mic(src, dest, len, LC_ISO8859_3, LATIN3);
}

void mic_to_latin3(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(MULE_INTERNAL, LATIN3);

    mic2latin(src, dest, len, LC_ISO8859_3, LATIN3);
}

void latin4_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(LATIN4, MULE_INTERNAL);

    latin2mic(src, dest, len, LC_ISO8859_4, LATIN4);
}

void mic_to_latin4(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(MULE_INTERNAL, LATIN4);

    mic2latin(src, dest, len, LC_ISO8859_4, LATIN4);
}
