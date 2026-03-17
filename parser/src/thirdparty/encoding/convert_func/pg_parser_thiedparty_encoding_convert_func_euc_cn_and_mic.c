/*-------------------------------------------------------------------------
 *
 *      EUC_CN and MULE_INTERNAL
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/euc_cn_and_mic/euc_cn_and_mic.c
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

static void euc_cn2mic(const unsigned char *euc, unsigned char *p, int32_t len);
static void mic2euc_cn(const unsigned char *mic, unsigned char *p, int32_t len);

void euc_cn_to_mic(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(EUC_CN, MULE_INTERNAL);

    euc_cn2mic(src, dest, len);
}

void mic_to_euc_cn(unsigned char *src_str, unsigned char *dest_str, int32_t str_len)
{
    unsigned char *src = src_str;
    unsigned char *dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(MULE_INTERNAL, EUC_CN);

    mic2euc_cn(src, dest, len);
}

/*
 * EUC_CN ---> MIC
 */
static void euc_cn2mic(const unsigned char *euc, unsigned char *p, int32_t len)
{
    int32_t c1;

    while (len > 0)
    {
        c1 = *euc;
        if (IS_HIGHBIT_SET(c1))
        {
            if (len < 2 || !IS_HIGHBIT_SET(euc[1]))
            {
                /* report_invalid_encoding(EUC_CN, (const char *) euc, len); */
                break;
            }
            *p++ = LC_GB2312_80;
            *p++ = c1;
            *p++ = euc[1];
            euc += 2;
            len -= 2;
        }
        else
        {                        /* should be ASCII */
            if (c1 == 0)
            {
                /* report_invalid_encoding(EUC_CN, (const char *) euc, len); */
                break;
            }
            *p++ = c1;
            euc++;
            len--;
        }
    }
    *p = '\0';
}

/*
 * MIC ---> EUC_CN
 */
static void mic2euc_cn(const unsigned char *mic, unsigned char *p, int32_t len)
{
    int32_t            c1;

    while (len > 0)
    {
        c1 = *mic;
        if (IS_HIGHBIT_SET(c1))
        {
            if (c1 != LC_GB2312_80)
            {
                /* report_untranslatable_char(MULE_INTERNAL, EUC_CN,
                                           (const char *) mic, len); */
                break;
            }
            if (len < 3 || !IS_HIGHBIT_SET(mic[1]) || !IS_HIGHBIT_SET(mic[2]))
            {
                /* report_invalid_encoding(MULE_INTERNAL,
                                        (const char *) mic, len); */
                break;
            }
            mic++;
            *p++ = *mic++;
            *p++ = *mic++;
            len -= 3;
        }
        else
        {                        /* should be ASCII */
            if (c1 == 0)
            {
                /* report_invalid_encoding(MULE_INTERNAL,
                                        (const char *) mic, len); */
                break;
            }
            *p++ = c1;
            mic++;
            len--;
        }
    }
    *p = '\0';
}
