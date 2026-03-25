/*-------------------------------------------------------------------------
 *
 *      WIN <--> UTF8
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/utf8_and_win/utf8_and_win.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_parser_os_incl.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_wchar.h"
#include "./unicode_map/utf8_to_win1250.map"
#include "./unicode_map/utf8_to_win1251.map"
#include "./unicode_map/utf8_to_win1252.map"
#include "./unicode_map/utf8_to_win1253.map"
#include "./unicode_map/utf8_to_win1254.map"
#include "./unicode_map/utf8_to_win1255.map"
#include "./unicode_map/utf8_to_win1256.map"
#include "./unicode_map/utf8_to_win1257.map"
#include "./unicode_map/utf8_to_win1258.map"
#include "./unicode_map/utf8_to_win866.map"
#include "./unicode_map/utf8_to_win874.map"
#include "./unicode_map/win1250_to_utf8.map"
#include "./unicode_map/win1251_to_utf8.map"
#include "./unicode_map/win1252_to_utf8.map"
#include "./unicode_map/win1253_to_utf8.map"
#include "./unicode_map/win1254_to_utf8.map"
#include "./unicode_map/win1255_to_utf8.map"
#include "./unicode_map/win1256_to_utf8.map"
#include "./unicode_map/win1257_to_utf8.map"
#include "./unicode_map/win866_to_utf8.map"
#include "./unicode_map/win874_to_utf8.map"
#include "./unicode_map/win1258_to_utf8.map"

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
    enc                            encoding;
    const character_mb_radix_tree* map1; /* to UTF8 map name */
    const character_mb_radix_tree* map2; /* from UTF8 map name */
} pg_conv_map;

static const pg_conv_map maps[] = {
    {WIN866, &win866_to_unicode_tree, &win866_from_unicode_tree},
    {WIN874, &win874_to_unicode_tree, &win874_from_unicode_tree},
    {WIN1250, &win1250_to_unicode_tree, &win1250_from_unicode_tree},
    {WIN1251, &win1251_to_unicode_tree, &win1251_from_unicode_tree},
    {WIN1252, &win1252_to_unicode_tree, &win1252_from_unicode_tree},
    {WIN1253, &win1253_to_unicode_tree, &win1253_from_unicode_tree},
    {WIN1254, &win1254_to_unicode_tree, &win1254_from_unicode_tree},
    {WIN1255, &win1255_to_unicode_tree, &win1255_from_unicode_tree},
    {WIN1256, &win1256_to_unicode_tree, &win1256_from_unicode_tree},
    {WIN1257, &win1257_to_unicode_tree, &win1257_from_unicode_tree},
    {WIN1258, &win1258_to_unicode_tree, &win1258_from_unicode_tree},
};

void win_to_utf8(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    /* int32_t            encoding = PG_GETARG_INT32(0); */
    int32_t        encoding = WIN1258;
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;
    int32_t        i;

    CHECK_ENCODING_CONVERSION_ARGS(-1, UTF8);

    for (i = 0; i < (int32_t)(conv_lengthof(maps)); i++)
    {
        if (encoding == (int32_t)maps[i].encoding)
        {
            LocalToUtf(src, len, dest, maps[i].map1, NULL, 0, NULL, encoding);
        }
    }
}

void utf8_to_win(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    /* int32_t            encoding = PG_GETARG_INT32(1); */
    int32_t        encoding = WIN1258;
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;
    int32_t        i;

    CHECK_ENCODING_CONVERSION_ARGS(UTF8, -1);

    for (i = 0; i < (int32_t)(conv_lengthof(maps)); i++)
    {
        if (encoding == (int32_t)maps[i].encoding)
        {
            UtfToLocal(src, len, dest, maps[i].map2, NULL, 0, NULL, encoding);
        }
    }
}
