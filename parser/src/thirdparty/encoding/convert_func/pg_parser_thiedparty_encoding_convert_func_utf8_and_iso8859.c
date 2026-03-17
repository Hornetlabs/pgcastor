/*-------------------------------------------------------------------------
 *
 *      ISO 8859 2-16 <--> UTF8
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/utf8_and_iso8859/utf8_and_iso8859.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_parser_os_incl.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_wchar.h"
#include "./unicode_map/iso8859_10_to_utf8.map"
#include "./unicode_map/iso8859_13_to_utf8.map"
#include "./unicode_map/iso8859_14_to_utf8.map"
#include "./unicode_map/iso8859_15_to_utf8.map"
#include "./unicode_map/iso8859_2_to_utf8.map"
#include "./unicode_map/iso8859_3_to_utf8.map"
#include "./unicode_map/iso8859_4_to_utf8.map"
#include "./unicode_map/iso8859_5_to_utf8.map"
#include "./unicode_map/iso8859_6_to_utf8.map"
#include "./unicode_map/iso8859_7_to_utf8.map"
#include "./unicode_map/iso8859_8_to_utf8.map"
#include "./unicode_map/iso8859_9_to_utf8.map"
#include "./unicode_map/utf8_to_iso8859_10.map"
#include "./unicode_map/utf8_to_iso8859_13.map"
#include "./unicode_map/utf8_to_iso8859_14.map"
#include "./unicode_map/utf8_to_iso8859_15.map"
#include "./unicode_map/utf8_to_iso8859_16.map"
#include "./unicode_map/utf8_to_iso8859_2.map"
#include "./unicode_map/utf8_to_iso8859_3.map"
#include "./unicode_map/utf8_to_iso8859_4.map"
#include "./unicode_map/utf8_to_iso8859_5.map"
#include "./unicode_map/utf8_to_iso8859_6.map"
#include "./unicode_map/utf8_to_iso8859_7.map"
#include "./unicode_map/utf8_to_iso8859_8.map"
#include "./unicode_map/utf8_to_iso8859_9.map"
#include "./unicode_map/iso8859_16_to_utf8.map"


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

typedef struct
{
    enc        encoding;
    const character_mb_radix_tree *map1;    /* to UTF8 map name */
    const character_mb_radix_tree *map2;    /* from UTF8 map name */
} pg_conv_map;

static const pg_conv_map maps[] = {
    {LATIN2, &iso8859_2_to_unicode_tree,
    &iso8859_2_from_unicode_tree},    /* ISO-8859-2 Latin 2 */
    {LATIN3, &iso8859_3_to_unicode_tree,
    &iso8859_3_from_unicode_tree},    /* ISO-8859-3 Latin 3 */
    {LATIN4, &iso8859_4_to_unicode_tree,
    &iso8859_4_from_unicode_tree},    /* ISO-8859-4 Latin 4 */
    {LATIN5, &iso8859_9_to_unicode_tree,
    &iso8859_9_from_unicode_tree},    /* ISO-8859-9 Latin 5 */
    {LATIN6, &iso8859_10_to_unicode_tree,
    &iso8859_10_from_unicode_tree}, /* ISO-8859-10 Latin 6 */
    {LATIN7, &iso8859_13_to_unicode_tree,
    &iso8859_13_from_unicode_tree}, /* ISO-8859-13 Latin 7 */
    {LATIN8, &iso8859_14_to_unicode_tree,
    &iso8859_14_from_unicode_tree}, /* ISO-8859-14 Latin 8 */
    {LATIN9, &iso8859_15_to_unicode_tree,
    &iso8859_15_from_unicode_tree}, /* ISO-8859-15 Latin 9 */
    {LATIN10, &iso8859_16_to_unicode_tree,
    &iso8859_16_from_unicode_tree}, /* ISO-8859-16 Latin 10 */
    {ISO_8859_5, &iso8859_5_to_unicode_tree,
    &iso8859_5_from_unicode_tree},    /* ISO-8859-5 */
    {ISO_8859_6, &iso8859_6_to_unicode_tree,
    &iso8859_6_from_unicode_tree},    /* ISO-8859-6 */
    {ISO_8859_7, &iso8859_7_to_unicode_tree,
    &iso8859_7_from_unicode_tree},    /* ISO-8859-7 */
    {ISO_8859_8, &iso8859_8_to_unicode_tree,
    &iso8859_8_from_unicode_tree},    /* ISO-8859-8 */
};

void iso8859_to_utf8(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    /* int32_t            encoding = src_str; */
    int32_t            encoding = ISO_8859_8;
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;
    int32_t            i;

    CHECK_ENCODING_CONVERSION_ARGS(-1, UTF8);

    for (i = 0; i < (int32_t) (conv_lengthof(maps)); i++)
    {
        if (encoding == (int32_t) maps[i].encoding)
        {
            LocalToUtf(src, len, dest,
                       maps[i].map1,
                       NULL, 0,
                       NULL,
                       encoding);
        }
    }
}

void utf8_to_iso8859(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    /* int32_t            encoding = PG_GETARG_INT32(1); */
    int32_t            encoding = ISO_8859_8;
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;
    int32_t            i;

    CHECK_ENCODING_CONVERSION_ARGS(UTF8, -1);

    for (i = 0; i < (int32_t) (conv_lengthof(maps)); i++)
    {
        if (encoding == (int32_t) maps[i].encoding)
        {
            UtfToLocal(src, len, dest,
                       maps[i].map2,
                       NULL, 0,
                       NULL,
                       encoding);
        }
    }
}
