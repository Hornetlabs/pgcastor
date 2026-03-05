#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/xk_pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/common/xk_pg_parser_thirdparty_builtins.h"

/* 
 * 目前PG支持的字符集在最坏的情况下, 转换后的结果是4比1的增长
 * 在这里使用最坏的情况, 可以确保所有的字符集转换后不会溢出
 */
#define XK_CONV_MAX_CONVERSION_GROWTH  4
#define XK_CONV_MCXT NULL

const xk_enc2gettext xk_enc2gettext_tbl[] =
{
    {XK_SQL_ASCII,      "US-ASCII"      , "SQL_ASCII"},
    {XK_UTF8,           "UTF-8"         , "UTF8"},
    {XK_LATIN1,         "LATIN1"        , "LATIN1"},
    {XK_LATIN2,         "LATIN2"        , "LATIN2"},
    {XK_LATIN3,         "LATIN3"        , "LATIN3"},
    {XK_LATIN4,         "LATIN4"        , "LATIN4"},
    {XK_ISO_8859_5,     "ISO-8859-5"    , "ISO_8859_5"},
    {XK_ISO_8859_6,     "ISO_8859-6"    , "ISO_8859_6"},
    {XK_ISO_8859_7,     "ISO-8859-7"    , "ISO_8859_7"},
    {XK_ISO_8859_8,     "ISO-8859-8"    , "ISO_8859_8"},
    {XK_LATIN5,         "LATIN5"        , "LATIN5"},
    {XK_LATIN6,         "LATIN6"        , "LATIN6"},
    {XK_LATIN7,         "LATIN7"        , "LATIN7"},
    {XK_LATIN8,         "LATIN8"        , "LATIN8"},
    {XK_LATIN9,         "LATIN-9"       , "LATIN9"},
    {XK_LATIN10,        "LATIN10"       , "LATIN10"},
    {XK_KOI8R,          "KOI8-R"        , "KOI8R"},
    {XK_KOI8U,          "KOI8-U"        , "KOI8U"},
    {XK_WIN1250,        "CP1250"        , "WIN1250"},
    {XK_WIN1251,        "CP1251"        , "WIN1251"},
    {XK_WIN1252,        "CP1252"        , "WIN1252"},
    {XK_WIN1253,        "CP1253"        , "WIN1253"},
    {XK_WIN1254,        "CP1254"        , "WIN1254"},
    {XK_WIN1255,        "CP1255"        , "WIN1255"},
    {XK_WIN1256,        "CP1256"        , "WIN1256"},
    {XK_WIN1257,        "CP1257"        , "WIN1257"},
    {XK_WIN1258,        "CP1258"        , "WIN1258"},
    {XK_WIN866,         "CP866"         , "WIN866"},
    {XK_WIN874,         "CP874"         , "WIN874"},
    {XK_EUC_CN,         "EUC-CN"        , "EUC_CN"},
    {XK_EUC_JP,         "EUC-JP"        , "EUC_JP"},
    {XK_EUC_KR,         "EUC-KR"        , "EUC_KR"},
    {XK_EUC_TW,         "EUC-TW"        , "EUC_TW"},
    {XK_EUC_JIS_2004,   "EUC_JIS_2004"  , "EUC_JIS_2004"},
    {XK_SJIS,           "SHIFT-JIS"     , "SHIFT_JIS_2004"},
    {XK_BIG5,           "BIG5"          , "BIG5"},
    {XK_GBK,            "GBK"           , "GBK"},
    {XK_UHC,            "UHC"           , "UHC"},
    {XK_GB18030,        "GB18030"       , "GB18030"},
    {XK_JOHAB,          "JOHAB"         , "JOHAB"},
    {XK_SHIFT_JIS_2004, "SHIFT_JISX0213", "SHIFT_JISX0213"},
    {XK_MULE_INTERNAL,  "MULE_INTERNAL" , "MULE_INTERNAL"}
};

const int32_t xk_enc2gettext_tbl_num = (sizeof(xk_enc2gettext_tbl)
                                             / sizeof(xk_enc2gettext));

/**
 * @brief 保存现有的转换对应规则 源字符集 目标字符集 转换函数
 * 
 */
const xk_pg_parser_FmgrBuiltinEncoding xk_pg_parser_fmgr_encoding_builtins[] = {
    { XK_SQL_ASCII      , XK_MULE_INTERNAL      , ascii_to_mic },
    { XK_MULE_INTERNAL  , XK_SQL_ASCII          , mic_to_ascii },
    { XK_KOI8R          , XK_MULE_INTERNAL      , koi8r_to_mic },
    { XK_MULE_INTERNAL  , XK_KOI8R              , mic_to_koi8r },
    { XK_ISO_8859_5     , XK_MULE_INTERNAL      , iso_to_mic },
    { XK_MULE_INTERNAL  , XK_ISO_8859_5         , mic_to_iso },
    { XK_WIN1251        , XK_MULE_INTERNAL      , win1251_to_mic },
    { XK_MULE_INTERNAL  , XK_WIN1251            , mic_to_win1251 },
    { XK_WIN866         , XK_MULE_INTERNAL      , win866_to_mic },
    { XK_MULE_INTERNAL  , XK_WIN866             , mic_to_win866 },
    { XK_KOI8R          , XK_WIN1251            , koi8r_to_win1251 },
    { XK_WIN1251        , XK_KOI8R              , win1251_to_koi8r },
    { XK_KOI8R          , XK_WIN866             , koi8r_to_win866 },
    { XK_WIN866         , XK_KOI8R              , win866_to_koi8r },
    { XK_WIN866         , XK_WIN1251            , win866_to_win1251 },
    { XK_WIN1251        , XK_WIN866             , win1251_to_win866 },
    { XK_ISO_8859_5     , XK_KOI8R              , iso_to_koi8r },
    { XK_KOI8R          , XK_ISO_8859_5         , koi8r_to_iso },
    { XK_ISO_8859_5     , XK_WIN1251            , iso_to_win1251 },
    { XK_WIN1251        , XK_ISO_8859_5         , win1251_to_iso },
    { XK_ISO_8859_5     , XK_WIN866             , iso_to_win866 },
    { XK_WIN866         , XK_ISO_8859_5         , win866_to_iso },
    { XK_EUC_CN         , XK_MULE_INTERNAL      , euc_cn_to_mic },
    { XK_MULE_INTERNAL  , XK_EUC_CN             , mic_to_euc_cn },
    { XK_EUC_JP         , XK_SJIS               , euc_jp_to_sjis },
    { XK_SJIS           , XK_EUC_JP             , sjis_to_euc_jp },
    { XK_EUC_JP         , XK_MULE_INTERNAL      , euc_jp_to_mic },
    { XK_SJIS           , XK_MULE_INTERNAL      , sjis_to_mic },
    { XK_MULE_INTERNAL  , XK_EUC_JP             , mic_to_euc_jp },
    { XK_MULE_INTERNAL  , XK_SJIS               , mic_to_sjis },
    { XK_EUC_KR         , XK_MULE_INTERNAL      , euc_kr_to_mic },
    { XK_MULE_INTERNAL  , XK_EUC_KR             , mic_to_euc_kr },
    { XK_EUC_TW         , XK_BIG5               , euc_tw_to_big5 },
    { XK_BIG5           , XK_EUC_TW             , big5_to_euc_tw },
    { XK_EUC_TW         , XK_MULE_INTERNAL      , euc_tw_to_mic },
    { XK_BIG5           , XK_MULE_INTERNAL      , big5_to_mic },
    { XK_MULE_INTERNAL  , XK_EUC_TW             , mic_to_euc_tw },
    { XK_MULE_INTERNAL  , XK_BIG5               , mic_to_big5 },
    { XK_LATIN2         , XK_MULE_INTERNAL      , latin2_to_mic },
    { XK_MULE_INTERNAL  , XK_LATIN2             , mic_to_latin2 },
    { XK_WIN1250        , XK_MULE_INTERNAL      , win1250_to_mic },
    { XK_MULE_INTERNAL  , XK_WIN1250            , mic_to_win1250 },
    { XK_LATIN2         , XK_WIN1250            , latin2_to_win1250 },
    { XK_WIN1250        , XK_LATIN2             , win1250_to_latin2 },
    { XK_LATIN1         , XK_MULE_INTERNAL      , latin1_to_mic },
    { XK_MULE_INTERNAL  , XK_LATIN1             , mic_to_latin1 },
    { XK_LATIN3         , XK_MULE_INTERNAL      , latin3_to_mic },
    { XK_MULE_INTERNAL  , XK_LATIN3             , mic_to_latin3 },
    { XK_LATIN4         , XK_MULE_INTERNAL      , latin4_to_mic },
    { XK_MULE_INTERNAL  , XK_LATIN4             , mic_to_latin4 },
    { XK_SQL_ASCII      , XK_UTF8               , ascii_to_utf8 },
    { XK_UTF8           , XK_SQL_ASCII          , utf8_to_ascii },
    { XK_BIG5           , XK_UTF8               , big5_to_utf8 },
    { XK_UTF8           , XK_BIG5               , utf8_to_big5 },
    { XK_UTF8           , XK_KOI8R              , utf8_to_koi8r },
    { XK_KOI8R          , XK_UTF8               , koi8r_to_utf8 },
    { XK_UTF8           , XK_KOI8U              , utf8_to_koi8u },
    { XK_KOI8U          , XK_UTF8               , koi8u_to_utf8 },
    { XK_UTF8           , XK_WIN866             , utf8_to_win },
    { XK_WIN866         , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN874             , utf8_to_win },
    { XK_WIN874         , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1250            , utf8_to_win },
    { XK_WIN1250        , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN874             , utf8_to_win },
    { XK_WIN874         , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1251            , utf8_to_win },
    { XK_WIN1251        , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1252            , utf8_to_win },
    { XK_WIN1252        , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1253            , utf8_to_win },
    { XK_WIN1253        , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1254            , utf8_to_win },
    { XK_WIN1254        , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1255            , utf8_to_win },
    { XK_WIN1255        , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1256            , utf8_to_win },
    { XK_WIN1256        , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1257            , utf8_to_win },
    { XK_WIN1257        , XK_UTF8               , win_to_utf8 },
    { XK_UTF8           , XK_WIN1258            , utf8_to_win },
    { XK_WIN1258        , XK_UTF8               , win_to_utf8 },
    { XK_EUC_CN         , XK_UTF8               , euc_cn_to_utf8 },
    { XK_UTF8           , XK_EUC_CN             , utf8_to_euc_cn },
    { XK_EUC_JP         , XK_UTF8               , euc_jp_to_utf8 },
    { XK_UTF8           , XK_EUC_JP             , utf8_to_euc_jp },
    { XK_EUC_KR         , XK_UTF8               , euc_kr_to_utf8 },
    { XK_UTF8           , XK_EUC_KR             , utf8_to_euc_kr },
    { XK_EUC_TW         , XK_UTF8               , euc_tw_to_utf8 },
    { XK_UTF8           , XK_EUC_TW             , utf8_to_euc_tw },
    { XK_GB18030        , XK_UTF8               , gb18030_to_utf8 },
    { XK_UTF8           , XK_GB18030            , utf8_to_gb18030 },
    { XK_GBK            , XK_UTF8               , gbk_to_utf8 },
    { XK_UTF8           , XK_GBK                , utf8_to_gbk },
    { XK_UTF8           , XK_LATIN2             , utf8_to_iso8859 },
    { XK_LATIN2         , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_LATIN3             , utf8_to_iso8859 },
    { XK_LATIN3         , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_LATIN4             , utf8_to_iso8859 },
    { XK_LATIN4         , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_LATIN5             , utf8_to_iso8859 },
    { XK_LATIN5         , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_LATIN6             , utf8_to_iso8859 },
    { XK_LATIN6         , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_LATIN7             , utf8_to_iso8859 },
    { XK_LATIN7         , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_LATIN8             , utf8_to_iso8859 },
    { XK_LATIN8         , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_LATIN9             , utf8_to_iso8859 },
    { XK_LATIN9         , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_LATIN10            , utf8_to_iso8859 },
    { XK_LATIN10        , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_ISO_8859_5         , utf8_to_iso8859 },
    { XK_ISO_8859_5     , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_ISO_8859_6         , utf8_to_iso8859 },
    { XK_ISO_8859_6     , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_ISO_8859_7         , utf8_to_iso8859 },
    { XK_ISO_8859_7     , XK_UTF8               , iso8859_to_utf8 },
    { XK_UTF8           , XK_ISO_8859_8         , utf8_to_iso8859 },
    { XK_ISO_8859_8     , XK_UTF8               , iso8859_to_utf8 },
    { XK_LATIN1         , XK_UTF8               , iso8859_1_to_utf8 },
    { XK_UTF8           , XK_LATIN1             , utf8_to_iso8859_1 },
    { XK_JOHAB          , XK_UTF8               , johab_to_utf8 },
    { XK_UTF8           , XK_JOHAB              , utf8_to_johab },
    { XK_SJIS           , XK_UTF8               , sjis_to_utf8 },
    { XK_UTF8           , XK_SJIS               , utf8_to_sjis },
    { XK_UHC            , XK_UTF8               , uhc_to_utf8 },
    { XK_UTF8           , XK_UHC                , utf8_to_uhc },
    { XK_EUC_JIS_2004   , XK_UTF8               , euc_jis_2004_to_utf8 },
    { XK_UTF8           , XK_EUC_JIS_2004       , utf8_to_euc_jis_2004 },
    { XK_SHIFT_JIS_2004 , XK_UTF8               , shift_jis_2004_to_utf8 },
    { XK_UTF8           , XK_SHIFT_JIS_2004     , utf8_to_shift_jis_2004 },
    { XK_EUC_JIS_2004   , XK_SHIFT_JIS_2004     , euc_jis_2004_to_shift_jis_2004 },
    { XK_SHIFT_JIS_2004 , XK_EUC_JIS_2004       , shift_jis_2004_to_euc_jis_2004 }
};

const int32_t xk_pg_parser_fmgr_encoding_nbuiltins = (sizeof(xk_pg_parser_fmgr_encoding_builtins)
                                                     / sizeof(xk_pg_parser_FmgrBuiltinEncoding));

static int32_t xk_pg_parser_encoding_getEncodingNum(char *name)
{
    int32_t i = 0;
    for (i = 0; i < xk_enc2gettext_tbl_num; i++)
    {
        if (!strcmp(xk_enc2gettext_tbl[i].pg_name, name)
         || !strcmp(xk_enc2gettext_tbl[i].stand_name, name))
            return xk_enc2gettext_tbl[i].encoding;
    }

    return -1;
}

static xk_pg_parser_FmgrBuiltinEncoding *xk_pg_parser_getEncodingConvFunc(int32_t dest_encode, int32_t src_encode)
{
    xk_pg_parser_FmgrBuiltinEncoding *res = NULL;
    int32_t i = 0;

    if (0 > dest_encode || 0 > src_encode)
        return NULL;

    for (i = 0; i < xk_pg_parser_fmgr_encoding_nbuiltins; i++)
    {
        res = (xk_pg_parser_FmgrBuiltinEncoding *) (&xk_pg_parser_fmgr_encoding_builtins[i]);
        if (dest_encode == (int32_t) res->dest && src_encode == (int32_t) res->src)
            break;
        res = NULL;
    }

    return res;
}

char *xk_pg_parser_encoding_convert(char *src_str,
                                    bool *needfree,
                                    char *dest_encoding,
                                    char *src_encoding)
{
    xk_pg_parser_FmgrBuiltinEncoding *res = NULL;
    char *result = NULL;
    int32_t src_len = strlen(src_str);
    int32_t dest_len = src_len * XK_CONV_MAX_CONVERSION_GROWTH;

    int32_t dest_encode = xk_pg_parser_encoding_getEncodingNum(dest_encoding);
    int32_t src_encode = xk_pg_parser_encoding_getEncodingNum(src_encoding);
    if (dest_encode == src_encode)
        return src_str;

    if (!xk_pg_parser_mcxt_malloc(XK_CONV_MCXT, (void **) &result, dest_len + 1))
        return NULL;

    res = xk_pg_parser_getEncodingConvFunc(dest_encode, src_encode);
    if (!res)
    {
        xk_pg_parser_mcxt_free(XK_CONV_MCXT, result);
        return NULL;
    }

    res->func((unsigned char *)src_str, (unsigned char *)result, src_len);
    *needfree = true;
    return result;
}
