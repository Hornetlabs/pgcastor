#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"

/* 
 * 目前PG支持的字符集在最坏的情况下, 转换后的结果是4比1的增长
 * 在这里使用最坏的情况, 可以确保所有的字符集转换后不会溢出
 */
#define CONV_MAX_CONVERSION_GROWTH  4
#define CONV_MCXT NULL

const enc2gettext enc2gettext_tbl[] =
{
    {SQL_ASCII,      "US-ASCII"      , "SQL_ASCII"},
    {UTF8,           "UTF-8"         , "UTF8"},
    {LATIN1,         "LATIN1"        , "LATIN1"},
    {LATIN2,         "LATIN2"        , "LATIN2"},
    {LATIN3,         "LATIN3"        , "LATIN3"},
    {LATIN4,         "LATIN4"        , "LATIN4"},
    {ISO_8859_5,     "ISO-8859-5"    , "ISO_8859_5"},
    {ISO_8859_6,     "ISO_8859-6"    , "ISO_8859_6"},
    {ISO_8859_7,     "ISO-8859-7"    , "ISO_8859_7"},
    {ISO_8859_8,     "ISO-8859-8"    , "ISO_8859_8"},
    {LATIN5,         "LATIN5"        , "LATIN5"},
    {LATIN6,         "LATIN6"        , "LATIN6"},
    {LATIN7,         "LATIN7"        , "LATIN7"},
    {LATIN8,         "LATIN8"        , "LATIN8"},
    {LATIN9,         "LATIN-9"       , "LATIN9"},
    {LATIN10,        "LATIN10"       , "LATIN10"},
    {KOI8R,          "KOI8-R"        , "KOI8R"},
    {KOI8U,          "KOI8-U"        , "KOI8U"},
    {WIN1250,        "CP1250"        , "WIN1250"},
    {WIN1251,        "CP1251"        , "WIN1251"},
    {WIN1252,        "CP1252"        , "WIN1252"},
    {WIN1253,        "CP1253"        , "WIN1253"},
    {WIN1254,        "CP1254"        , "WIN1254"},
    {WIN1255,        "CP1255"        , "WIN1255"},
    {WIN1256,        "CP1256"        , "WIN1256"},
    {WIN1257,        "CP1257"        , "WIN1257"},
    {WIN1258,        "CP1258"        , "WIN1258"},
    {WIN866,         "CP866"         , "WIN866"},
    {WIN874,         "CP874"         , "WIN874"},
    {EUC_CN,         "EUC-CN"        , "EUC_CN"},
    {EUC_JP,         "EUC-JP"        , "EUC_JP"},
    {EUC_KR,         "EUC-KR"        , "EUC_KR"},
    {EUC_TW,         "EUC-TW"        , "EUC_TW"},
    {EUC_JIS_2004,   "EUC_JIS_2004"  , "EUC_JIS_2004"},
    {SJIS,           "SHIFT-JIS"     , "SHIFT_JIS_2004"},
    {BIG5,           "BIG5"          , "BIG5"},
    {GBK,            "GBK"           , "GBK"},
    {UHC,            "UHC"           , "UHC"},
    {GB18030,        "GB18030"       , "GB18030"},
    {JOHAB,          "JOHAB"         , "JOHAB"},
    {SHIFT_JIS_2004, "SHIFT_JISX0213", "SHIFT_JISX0213"},
    {MULE_INTERNAL,  "MULE_INTERNAL" , "MULE_INTERNAL"}
};

const int32_t enc2gettext_tbl_num = (sizeof(enc2gettext_tbl)
                                             / sizeof(enc2gettext));

/**
 * @brief 保存现有的转换对应规则 源字符集 目标字符集 转换函数
 * 
 */
const pg_parser_FmgrBuiltinEncoding pg_parser_fmgr_encoding_builtins[] = {
    { SQL_ASCII      , MULE_INTERNAL      , ascii_to_mic },
    { MULE_INTERNAL  , SQL_ASCII          , mic_to_ascii },
    { KOI8R          , MULE_INTERNAL      , koi8r_to_mic },
    { MULE_INTERNAL  , KOI8R              , mic_to_koi8r },
    { ISO_8859_5     , MULE_INTERNAL      , iso_to_mic },
    { MULE_INTERNAL  , ISO_8859_5         , mic_to_iso },
    { WIN1251        , MULE_INTERNAL      , win1251_to_mic },
    { MULE_INTERNAL  , WIN1251            , mic_to_win1251 },
    { WIN866         , MULE_INTERNAL      , win866_to_mic },
    { MULE_INTERNAL  , WIN866             , mic_to_win866 },
    { KOI8R          , WIN1251            , koi8r_to_win1251 },
    { WIN1251        , KOI8R              , win1251_to_koi8r },
    { KOI8R          , WIN866             , koi8r_to_win866 },
    { WIN866         , KOI8R              , win866_to_koi8r },
    { WIN866         , WIN1251            , win866_to_win1251 },
    { WIN1251        , WIN866             , win1251_to_win866 },
    { ISO_8859_5     , KOI8R              , iso_to_koi8r },
    { KOI8R          , ISO_8859_5         , koi8r_to_iso },
    { ISO_8859_5     , WIN1251            , iso_to_win1251 },
    { WIN1251        , ISO_8859_5         , win1251_to_iso },
    { ISO_8859_5     , WIN866             , iso_to_win866 },
    { WIN866         , ISO_8859_5         , win866_to_iso },
    { EUC_CN         , MULE_INTERNAL      , euc_cn_to_mic },
    { MULE_INTERNAL  , EUC_CN             , mic_to_euc_cn },
    { EUC_JP         , SJIS               , euc_jp_to_sjis },
    { SJIS           , EUC_JP             , sjis_to_euc_jp },
    { EUC_JP         , MULE_INTERNAL      , euc_jp_to_mic },
    { SJIS           , MULE_INTERNAL      , sjis_to_mic },
    { MULE_INTERNAL  , EUC_JP             , mic_to_euc_jp },
    { MULE_INTERNAL  , SJIS               , mic_to_sjis },
    { EUC_KR         , MULE_INTERNAL      , euc_kr_to_mic },
    { MULE_INTERNAL  , EUC_KR             , mic_to_euc_kr },
    { EUC_TW         , BIG5               , euc_tw_to_big5 },
    { BIG5           , EUC_TW             , big5_to_euc_tw },
    { EUC_TW         , MULE_INTERNAL      , euc_tw_to_mic },
    { BIG5           , MULE_INTERNAL      , big5_to_mic },
    { MULE_INTERNAL  , EUC_TW             , mic_to_euc_tw },
    { MULE_INTERNAL  , BIG5               , mic_to_big5 },
    { LATIN2         , MULE_INTERNAL      , latin2_to_mic },
    { MULE_INTERNAL  , LATIN2             , mic_to_latin2 },
    { WIN1250        , MULE_INTERNAL      , win1250_to_mic },
    { MULE_INTERNAL  , WIN1250            , mic_to_win1250 },
    { LATIN2         , WIN1250            , latin2_to_win1250 },
    { WIN1250        , LATIN2             , win1250_to_latin2 },
    { LATIN1         , MULE_INTERNAL      , latin1_to_mic },
    { MULE_INTERNAL  , LATIN1             , mic_to_latin1 },
    { LATIN3         , MULE_INTERNAL      , latin3_to_mic },
    { MULE_INTERNAL  , LATIN3             , mic_to_latin3 },
    { LATIN4         , MULE_INTERNAL      , latin4_to_mic },
    { MULE_INTERNAL  , LATIN4             , mic_to_latin4 },
    { SQL_ASCII      , UTF8               , ascii_to_utf8 },
    { UTF8           , SQL_ASCII          , utf8_to_ascii },
    { BIG5           , UTF8               , big5_to_utf8 },
    { UTF8           , BIG5               , utf8_to_big5 },
    { UTF8           , KOI8R              , utf8_to_koi8r },
    { KOI8R          , UTF8               , koi8r_to_utf8 },
    { UTF8           , KOI8U              , utf8_to_koi8u },
    { KOI8U          , UTF8               , koi8u_to_utf8 },
    { UTF8           , WIN866             , utf8_to_win },
    { WIN866         , UTF8               , win_to_utf8 },
    { UTF8           , WIN874             , utf8_to_win },
    { WIN874         , UTF8               , win_to_utf8 },
    { UTF8           , WIN1250            , utf8_to_win },
    { WIN1250        , UTF8               , win_to_utf8 },
    { UTF8           , WIN874             , utf8_to_win },
    { WIN874         , UTF8               , win_to_utf8 },
    { UTF8           , WIN1251            , utf8_to_win },
    { WIN1251        , UTF8               , win_to_utf8 },
    { UTF8           , WIN1252            , utf8_to_win },
    { WIN1252        , UTF8               , win_to_utf8 },
    { UTF8           , WIN1253            , utf8_to_win },
    { WIN1253        , UTF8               , win_to_utf8 },
    { UTF8           , WIN1254            , utf8_to_win },
    { WIN1254        , UTF8               , win_to_utf8 },
    { UTF8           , WIN1255            , utf8_to_win },
    { WIN1255        , UTF8               , win_to_utf8 },
    { UTF8           , WIN1256            , utf8_to_win },
    { WIN1256        , UTF8               , win_to_utf8 },
    { UTF8           , WIN1257            , utf8_to_win },
    { WIN1257        , UTF8               , win_to_utf8 },
    { UTF8           , WIN1258            , utf8_to_win },
    { WIN1258        , UTF8               , win_to_utf8 },
    { EUC_CN         , UTF8               , euc_cn_to_utf8 },
    { UTF8           , EUC_CN             , utf8_to_euc_cn },
    { EUC_JP         , UTF8               , euc_jp_to_utf8 },
    { UTF8           , EUC_JP             , utf8_to_euc_jp },
    { EUC_KR         , UTF8               , euc_kr_to_utf8 },
    { UTF8           , EUC_KR             , utf8_to_euc_kr },
    { EUC_TW         , UTF8               , euc_tw_to_utf8 },
    { UTF8           , EUC_TW             , utf8_to_euc_tw },
    { GB18030        , UTF8               , gb18030_to_utf8 },
    { UTF8           , GB18030            , utf8_to_gb18030 },
    { GBK            , UTF8               , gbk_to_utf8 },
    { UTF8           , GBK                , utf8_to_gbk },
    { UTF8           , LATIN2             , utf8_to_iso8859 },
    { LATIN2         , UTF8               , iso8859_to_utf8 },
    { UTF8           , LATIN3             , utf8_to_iso8859 },
    { LATIN3         , UTF8               , iso8859_to_utf8 },
    { UTF8           , LATIN4             , utf8_to_iso8859 },
    { LATIN4         , UTF8               , iso8859_to_utf8 },
    { UTF8           , LATIN5             , utf8_to_iso8859 },
    { LATIN5         , UTF8               , iso8859_to_utf8 },
    { UTF8           , LATIN6             , utf8_to_iso8859 },
    { LATIN6         , UTF8               , iso8859_to_utf8 },
    { UTF8           , LATIN7             , utf8_to_iso8859 },
    { LATIN7         , UTF8               , iso8859_to_utf8 },
    { UTF8           , LATIN8             , utf8_to_iso8859 },
    { LATIN8         , UTF8               , iso8859_to_utf8 },
    { UTF8           , LATIN9             , utf8_to_iso8859 },
    { LATIN9         , UTF8               , iso8859_to_utf8 },
    { UTF8           , LATIN10            , utf8_to_iso8859 },
    { LATIN10        , UTF8               , iso8859_to_utf8 },
    { UTF8           , ISO_8859_5         , utf8_to_iso8859 },
    { ISO_8859_5     , UTF8               , iso8859_to_utf8 },
    { UTF8           , ISO_8859_6         , utf8_to_iso8859 },
    { ISO_8859_6     , UTF8               , iso8859_to_utf8 },
    { UTF8           , ISO_8859_7         , utf8_to_iso8859 },
    { ISO_8859_7     , UTF8               , iso8859_to_utf8 },
    { UTF8           , ISO_8859_8         , utf8_to_iso8859 },
    { ISO_8859_8     , UTF8               , iso8859_to_utf8 },
    { LATIN1         , UTF8               , iso8859_1_to_utf8 },
    { UTF8           , LATIN1             , utf8_to_iso8859_1 },
    { JOHAB          , UTF8               , johab_to_utf8 },
    { UTF8           , JOHAB              , utf8_to_johab },
    { SJIS           , UTF8               , sjis_to_utf8 },
    { UTF8           , SJIS               , utf8_to_sjis },
    { UHC            , UTF8               , uhc_to_utf8 },
    { UTF8           , UHC                , utf8_to_uhc },
    { EUC_JIS_2004   , UTF8               , euc_jis_2004_to_utf8 },
    { UTF8           , EUC_JIS_2004       , utf8_to_euc_jis_2004 },
    { SHIFT_JIS_2004 , UTF8               , shift_jis_2004_to_utf8 },
    { UTF8           , SHIFT_JIS_2004     , utf8_to_shift_jis_2004 },
    { EUC_JIS_2004   , SHIFT_JIS_2004     , euc_jis_2004_to_shift_jis_2004 },
    { SHIFT_JIS_2004 , EUC_JIS_2004       , shift_jis_2004_to_euc_jis_2004 }
};

const int32_t pg_parser_fmgr_encoding_nbuiltins = (sizeof(pg_parser_fmgr_encoding_builtins)
                                                     / sizeof(pg_parser_FmgrBuiltinEncoding));

static int32_t pg_parser_encoding_getEncodingNum(char *name)
{
    int32_t i = 0;
    for (i = 0; i < enc2gettext_tbl_num; i++)
    {
        if (!strcmp(enc2gettext_tbl[i].pg_name, name)
         || !strcmp(enc2gettext_tbl[i].stand_name, name))
            return enc2gettext_tbl[i].encoding;
    }

    return -1;
}

static pg_parser_FmgrBuiltinEncoding *pg_parser_getEncodingConvFunc(int32_t dest_encode, int32_t src_encode)
{
    pg_parser_FmgrBuiltinEncoding *res = NULL;
    int32_t i = 0;

    if (0 > dest_encode || 0 > src_encode)
        return NULL;

    for (i = 0; i < pg_parser_fmgr_encoding_nbuiltins; i++)
    {
        res = (pg_parser_FmgrBuiltinEncoding *) (&pg_parser_fmgr_encoding_builtins[i]);
        if (dest_encode == (int32_t) res->dest && src_encode == (int32_t) res->src)
            break;
        res = NULL;
    }

    return res;
}

char *pg_parser_encoding_convert(char *src_str,
                                    bool *needfree,
                                    char *dest_encoding,
                                    char *src_encoding)
{
    pg_parser_FmgrBuiltinEncoding *res = NULL;
    char *result = NULL;
    int32_t src_len = strlen(src_str);
    int32_t dest_len = src_len * CONV_MAX_CONVERSION_GROWTH;

    int32_t dest_encode = pg_parser_encoding_getEncodingNum(dest_encoding);
    int32_t src_encode = pg_parser_encoding_getEncodingNum(src_encoding);
    if (dest_encode == src_encode)
        return src_str;

    if (!pg_parser_mcxt_malloc(CONV_MCXT, (void **) &result, dest_len + 1))
        return NULL;

    res = pg_parser_getEncodingConvFunc(dest_encode, src_encode);
    if (!res)
    {
        pg_parser_mcxt_free(CONV_MCXT, result);
        return NULL;
    }

    res->func((unsigned char *)src_str, (unsigned char *)result, src_len);
    *needfree = true;
    return result;
}
