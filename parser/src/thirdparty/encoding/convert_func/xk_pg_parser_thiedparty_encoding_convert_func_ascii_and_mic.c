/**
 * @file xk_pg_parser_thiedparty_encoding_convert_func_ascii_and_mic.c
 * @author bytesync
 * @brief 
 * @version 0.1
 * @date 2023-08-10
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include "xk_pg_parser_os_incl.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_wchar.h"

void ascii_to_mic(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_SQL_ASCII, XK_MULE_INTERNAL);

    xk_conv_ascii2mic(src, dest, len);

}

void mic_to_ascii(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t            len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(XK_MULE_INTERNAL, XK_SQL_ASCII);

    xk_conv_mic2ascii(src, dest, len);

}
