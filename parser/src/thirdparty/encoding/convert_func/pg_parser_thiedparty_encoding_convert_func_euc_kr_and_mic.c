/*-------------------------------------------------------------------------
 *
 *      EUC_KR and MULE_INTERNAL
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c
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

static void euc_kr2mic(const unsigned char* euc, unsigned char* p, int32_t len);
static void mic2euc_kr(const unsigned char* mic, unsigned char* p, int32_t len);

void euc_kr_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(EUC_KR, MULE_INTERNAL);

    euc_kr2mic(src, dest, len);
}

void mic_to_euc_kr(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(MULE_INTERNAL, EUC_KR);

    mic2euc_kr(src, dest, len);
}

/*
 * EUC_KR ---> MIC
 */
static void euc_kr2mic(const unsigned char* euc, unsigned char* p, int32_t len)
{
    int32_t c1;
    int32_t l;

    while (len > 0)
    {
        c1 = *euc;
        if (IS_HIGHBIT_SET(c1))
        {
            l = character_encoding_verifymb(EUC_KR, (const char*)euc, len);
            if (l != 2)
            {
                /* report_invalid_encoding(EUC_KR,
                                        (const char *) euc, len); */
                break;
            }
            *p++ = LC_KS5601;
            *p++ = c1;
            *p++ = euc[1];
            euc += 2;
            len -= 2;
        }
        else
        { /* should be ASCII */
            if (c1 == 0)
            {
                /* report_invalid_encoding(EUC_KR,
                                        (const char *) euc, len); */
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
 * MIC ---> EUC_KR
 */
static void mic2euc_kr(const unsigned char* mic, unsigned char* p, int32_t len)
{
    int32_t c1;
    int32_t l;

    while (len > 0)
    {
        c1 = *mic;
        if (!IS_HIGHBIT_SET(c1))
        {
            /* ASCII */
            if (c1 == 0)
            {
                /* report_invalid_encoding(MULE_INTERNAL,
                                        (const char *) mic, len); */
                break;
            }
            *p++ = c1;
            mic++;
            len--;
            continue;
        }
        l = character_encoding_verifymb(MULE_INTERNAL, (const char*)mic, len);
        if (l < 0)
        {
            /* report_invalid_encoding(MULE_INTERNAL,
                                    (const char *) mic, len); */
            break;
        }
        if (c1 == LC_KS5601)
        {
            *p++ = mic[1];
            *p++ = mic[2];
        }
        else
        {
            /* report_untranslatable_char(MULE_INTERNAL, EUC_KR,
                                       (const char *) mic, len); */
            break;
        }
        mic += l;
        len -= l;
    }
    *p = '\0';
}
