/**
 * @file pg_parser_thirdparty_encode.c
 * @author bytesync
 * @brief
 * @version 0.1
 * @date 2023-08-03
 *
 * @copyright Copyright (c) 2023
 *
 */
#include <ctype.h>
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"

/* HEX */
static const char hextbl[] = "0123456789abcdef";

unsigned hex_encode(const char* src, unsigned len, char* dst)
{
    const char* end = src + len;

    while (src < end)
    {
        *dst++ = hextbl[(*src >> 4) & 0xF];
        *dst++ = hextbl[*src & 0xF];
        src++;
    }
    return len * 2;
}
