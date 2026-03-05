#ifndef xk_pg_parser_thirdparty_encoding_CONV_H
#define xk_pg_parser_thirdparty_encoding_CONV_H

/*
 * 定义函数指针, 为字符集转换函数提供函数原型。
 */
typedef void (*xk_pg_parser_Local_PGEncoding) (unsigned char *src_str,
                                               unsigned char *dest_str,
                                               int32_t str_len);

typedef enum xk_enc
{
XK_SQL_ASCII = 0,           /* SQL/ASCII */
XK_EUC_JP,                  /* EUC for Japanese */
XK_EUC_CN,                  /* EUC for Chinese */
XK_EUC_KR,                  /* EUC for Korean */
XK_EUC_TW,                  /* EUC for Taiwan */
XK_EUC_JIS_2004,            /* EUC-JIS-2004 */
XK_UTF8,                    /* Unicode UTF8 */
XK_MULE_INTERNAL,           /* Mule internal code */
XK_LATIN1,                  /* ISO-8859-1 Latin 1 */
XK_LATIN2,                  /* ISO-8859-2 Latin 2 */
XK_LATIN3,                  /* ISO-8859-3 Latin 3 */
XK_LATIN4,                  /* ISO-8859-4 Latin 4 */
XK_LATIN5,                  /* ISO-8859-9 Latin 5 */
XK_LATIN6,                  /* ISO-8859-10 Latin6 */
XK_LATIN7,                  /* ISO-8859-13 Latin7 */
XK_LATIN8,                  /* ISO-8859-14 Latin8 */
XK_LATIN9,                  /* ISO-8859-15 Latin9 */
XK_LATIN10,                 /* ISO-8859-16 Latin10 */
XK_WIN1256,                 /* windows-1256 */
XK_WIN1258,                 /* Windows-1258 */
XK_WIN866,                  /* (MS-DOS CP866) */
XK_WIN874,                  /* windows-874 */
XK_KOI8R,                   /* KOI8-R */
XK_WIN1251,                 /* windows-1251 */
XK_WIN1252,                 /* windows-1252 */
XK_ISO_8859_5,              /* ISO-8859-5 */
XK_ISO_8859_6,              /* ISO-8859-6 */
XK_ISO_8859_7,              /* ISO-8859-7 */
XK_ISO_8859_8,              /* ISO-8859-8 */
XK_WIN1250,                 /* windows-1250 */
XK_WIN1253,                 /* windows-1253 */
XK_WIN1254,                 /* windows-1254 */
XK_WIN1255,                 /* windows-1255 */
XK_WIN1257,                 /* windows-1257 */
XK_KOI8U,                   /* KOI8-U */
/* PG_ENCODING_BE_LAST points to the above entry */
/* followings are for client encoding only */
XK_SJIS,                    /* Shift JIS (Windows-932) */
XK_BIG5,                    /* Big5 (Windows-950) */
XK_GBK,                     /* GBK (Windows-936) */
XK_UHC,                     /* UHC (Windows-949) */
XK_GB18030,                 /* GB18030 */
XK_JOHAB,                   /* EUC for Korean JOHAB */
XK_SHIFT_JIS_2004,          /* Shift-JIS-2004 */
_XK_LAST_ENCODING_          /* mark only */

} xk_enc;

typedef struct xk_enc2gettext
{
    xk_enc encoding;
    const char *stand_name;
    const char *pg_name;
} xk_enc2gettext;

/*
 * 存储所有字符集转换函数。
 */
typedef struct
{
    xk_enc src;                               /* source encoding name */
    xk_enc dest;                              /* target encoding name */
    xk_pg_parser_Local_PGEncoding   func;     /* pointer to compiled function */
} xk_pg_parser_FmgrBuiltinEncoding;


extern char *xk_pg_parser_encoding_convert(char *src_str,
                                           bool *needfree,
                                           char *dest_encoding,
                                           char *src_encoding);

#endif
