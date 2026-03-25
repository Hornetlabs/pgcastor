/**
 * @file pg_parser_thiedparty_encoding_convert_func_ascii_and_mic.c
 * @author bytesync
 * @brief
 * @version 0.1
 * @date 2023-08-10
 *
 * @copyright Copyright (c) 2023
 *
 */
#include "pg_parser_os_incl.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_wchar.h"

void ascii_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(SQL_ASCII, MULE_INTERNAL);

    conv_ascii2mic(src, dest, len);
}

void mic_to_ascii(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(MULE_INTERNAL, SQL_ASCII);

    conv_mic2ascii(src, dest, len);
}
