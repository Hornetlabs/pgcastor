#ifndef pg_parser_thirdparty_encoding_CONV_H
#define pg_parser_thirdparty_encoding_CONV_H

/*
 * Define function pointers, provide function prototypes for charset conversion functions.
 */
typedef void (*pg_parser_Local_PGEncoding)(unsigned char* src_str, unsigned char* dest_str,
                                           int32_t str_len);

typedef enum enc
{
    SQL_ASCII = 0, /* SQL/ASCII */
    EUC_JP,        /* EUC for Japanese */
    EUC_CN,        /* EUC for Chinese */
    EUC_KR,        /* EUC for Korean */
    EUC_TW,        /* EUC for Taiwan */
    EUC_JIS_2004,  /* EUC-JIS-2004 */
    UTF8,          /* Unicode UTF8 */
    MULE_INTERNAL, /* Mule internal code */
    LATIN1,        /* ISO-8859-1 Latin 1 */
    LATIN2,        /* ISO-8859-2 Latin 2 */
    LATIN3,        /* ISO-8859-3 Latin 3 */
    LATIN4,        /* ISO-8859-4 Latin 4 */
    LATIN5,        /* ISO-8859-9 Latin 5 */
    LATIN6,        /* ISO-8859-10 Latin6 */
    LATIN7,        /* ISO-8859-13 Latin7 */
    LATIN8,        /* ISO-8859-14 Latin8 */
    LATIN9,        /* ISO-8859-15 Latin9 */
    LATIN10,       /* ISO-8859-16 Latin10 */
    WIN1256,       /* windows-1256 */
    WIN1258,       /* Windows-1258 */
    WIN866,        /* (MS-DOS CP866) */
    WIN874,        /* windows-874 */
    KOI8R,         /* KOI8-R */
    WIN1251,       /* windows-1251 */
    WIN1252,       /* windows-1252 */
    ISO_8859_5,    /* ISO-8859-5 */
    ISO_8859_6,    /* ISO-8859-6 */
    ISO_8859_7,    /* ISO-8859-7 */
    ISO_8859_8,    /* ISO-8859-8 */
    WIN1250,       /* windows-1250 */
    WIN1253,       /* windows-1253 */
    WIN1254,       /* windows-1254 */
    WIN1255,       /* windows-1255 */
    WIN1257,       /* windows-1257 */
    KOI8U,         /* KOI8-U */
    /* PG_ENCODING_BE_LAST points to the above entry */
    /* followings are for client encoding only */
    SJIS,           /* Shift JIS (Windows-932) */
    BIG5,           /* Big5 (Windows-950) */
    GBK,            /* GBK (Windows-936) */
    UHC,            /* UHC (Windows-949) */
    GB18030,        /* GB18030 */
    JOHAB,          /* EUC for Korean JOHAB */
    SHIFT_JIS_2004, /* Shift-JIS-2004 */
    _LAST_ENCODING_ /* mark only */

} enc;

typedef struct enc2gettext
{
    enc         encoding;
    const char* stand_name;
    const char* pg_name;
} enc2gettext;

/*
 * Store all charset conversion functions.
 */
typedef struct
{
    enc                        src;  /* source encoding name */
    enc                        dest; /* target encoding name */
    pg_parser_Local_PGEncoding func; /* pointer to compiled function */
} pg_parser_FmgrBuiltinEncoding;

extern char* pg_parser_encoding_convert(char* src_str, bool* needfree, char* dest_encoding,
                                        char* src_encoding);

#endif
