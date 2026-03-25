/*-------------------------------------------------------------------------
 *
 *      EUC_TW, BIG5 and MULE_INTERNAL
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_convfunc.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_wchar.h"

#define ENCODING_GROWTH_RATE 4
#define EUCTW_BIG5_MCXT NULL

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

static void big52mic(const unsigned char* big5, unsigned char* p, int32_t len);
static void mic2big5(const unsigned char* mic, unsigned char* p, int32_t len);
static void euc_tw2mic(const unsigned char* euc, unsigned char* p, int32_t len);
static void mic2euc_tw(const unsigned char* mic, unsigned char* p, int32_t len);

void euc_tw_to_big5(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;
    unsigned char* buf;

    CHECK_ENCODING_CONVERSION_ARGS(EUC_TW, BIG5);
    pg_parser_mcxt_malloc(EUCTW_BIG5_MCXT, (void**)&buf, len * ENCODING_GROWTH_RATE + 1);
    euc_tw2mic(src, buf, len);
    mic2big5(buf, dest, strlen((char*)buf));
    pg_parser_mcxt_free(EUCTW_BIG5_MCXT, buf);
}

void big5_to_euc_tw(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;
    unsigned char* buf;

    CHECK_ENCODING_CONVERSION_ARGS(BIG5, EUC_TW);

    pg_parser_mcxt_malloc(EUCTW_BIG5_MCXT, (void**)&buf, len * ENCODING_GROWTH_RATE + 1);
    big52mic(src, buf, len);
    mic2euc_tw(buf, dest, strlen((char*)buf));
    pg_parser_mcxt_free(EUCTW_BIG5_MCXT, buf);
}

void euc_tw_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(EUC_TW, MULE_INTERNAL);

    euc_tw2mic(src, dest, len);
}

void mic_to_euc_tw(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(MULE_INTERNAL, EUC_TW);

    mic2euc_tw(src, dest, len);
}

void big5_to_mic(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(BIG5, MULE_INTERNAL);

    big52mic(src, dest, len);
}

void mic_to_big5(unsigned char* src_str, unsigned char* dest_str, int32_t str_len)
{
    unsigned char* src = src_str;
    unsigned char* dest = dest_str;
    int32_t        len = str_len;

    CHECK_ENCODING_CONVERSION_ARGS(MULE_INTERNAL, BIG5);

    mic2big5(src, dest, len);
}

/*
 * EUC_TW ---> MIC
 */
static void euc_tw2mic(const unsigned char* euc, unsigned char* p, int32_t len)
{
    int32_t c1;
    int32_t l;

    while (len > 0)
    {
        c1 = *euc;
        if (IS_HIGHBIT_SET(c1))
        {
            l = character_encoding_verifymb(EUC_TW, (const char*)euc, len);
            if (l < 0)
            {
                /* report_invalid_encoding(EUC_TW,
                                        (const char *) euc, len); */
                break;
            }
            if (c1 == SS2)
            {
                c1 = euc[1]; /* plane No. */
                if (c1 == 0xa1)
                {
                    *p++ = LC_CNS11643_1;
                }
                else if (c1 == 0xa2)
                {
                    *p++ = LC_CNS11643_2;
                }
                else
                {
                    /* other planes are MULE private charsets */
                    *p++ = LCPRV2_B;
                    *p++ = c1 - 0xa3 + LC_CNS11643_3;
                }
                *p++ = euc[2];
                *p++ = euc[3];
            }
            else
            { /* CNS11643-1 */
                *p++ = LC_CNS11643_1;
                *p++ = c1;
                *p++ = euc[1];
            }
            euc += l;
            len -= l;
        }
        else
        { /* should be ASCII */
            if (c1 == 0)
            {
                /* report_invalid_encoding(EUC_TW,
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
 * MIC ---> EUC_TW
 */
static void mic2euc_tw(const unsigned char* mic, unsigned char* p, int32_t len)
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
        if (c1 == LC_CNS11643_1)
        {
            *p++ = mic[1];
            *p++ = mic[2];
        }
        else if (c1 == LC_CNS11643_2)
        {
            *p++ = SS2;
            *p++ = 0xa2;
            *p++ = mic[1];
            *p++ = mic[2];
        }
        else if (c1 == LCPRV2_B && mic[1] >= LC_CNS11643_3 && mic[1] <= LC_CNS11643_7)
        {
            *p++ = SS2;
            *p++ = mic[1] - LC_CNS11643_3 + 0xa3;
            *p++ = mic[2];
            *p++ = mic[3];
        }
        else
        {
            /* report_untranslatable_char(MULE_INTERNAL, EUC_TW,
                                       (const char *) mic, len); */
        }
        mic += l;
        len -= l;
    }
    *p = '\0';
}

/*
 * Big5 ---> MIC
 */
static void big52mic(const unsigned char* big5, unsigned char* p, int32_t len)
{
    unsigned short c1;
    unsigned short big5buf, cnsBuf;
    unsigned char  lc;
    int32_t        l;

    while (len > 0)
    {
        c1 = *big5;
        if (!IS_HIGHBIT_SET(c1))
        {
            /* ASCII */
            if (c1 == 0)
            {
                /* report_invalid_encoding(BIG5,
                                        (const char *) big5, len); */
                break;
            }
            *p++ = c1;
            big5++;
            len--;
            continue;
        }
        l = character_encoding_verifymb(BIG5, (const char*)big5, len);
        if (l < 0)
        {
            /* report_invalid_encoding(BIG5,
                                    (const char *) big5, len); */
            break;
        }
        big5buf = (c1 << 8) | big5[1];
        cnsBuf = BIG5toCNS(big5buf, &lc);
        if (lc != 0)
        {
            /* Planes 3 and 4 are MULE private charsets */
            if (lc == LC_CNS11643_3 || lc == LC_CNS11643_4)
            {
                *p++ = LCPRV2_B;
            }
            *p++ = lc; /* Plane No. */
            *p++ = (cnsBuf >> 8) & 0x00ff;
            *p++ = cnsBuf & 0x00ff;
        }
        else
        {
            /* report_untranslatable_char(BIG5, MULE_INTERNAL,
                                       (const char *) big5, len); */
            break;
        }
        big5 += l;
        len -= l;
    }
    *p = '\0';
}

/*
 * MIC ---> Big5
 */
static void mic2big5(const unsigned char* mic, unsigned char* p, int32_t len)
{
    unsigned short c1;
    unsigned short big5buf, cnsBuf;
    int32_t        l;

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
        if (c1 == LC_CNS11643_1 || c1 == LC_CNS11643_2 || c1 == LCPRV2_B)
        {
            if (c1 == LCPRV2_B)
            {
                c1 = mic[1]; /* get plane no. */
                cnsBuf = (mic[2] << 8) | mic[3];
            }
            else
            {
                cnsBuf = (mic[1] << 8) | mic[2];
            }
            big5buf = CNStoBIG5(cnsBuf, c1);
            if (big5buf == 0)
            {
                /* report_untranslatable_char(MULE_INTERNAL, BIG5,
                                           (const char *) mic, len); */
                break;
            }
            *p++ = (big5buf >> 8) & 0x00ff;
            *p++ = big5buf & 0x00ff;
        }
        else
        {
            /* report_untranslatable_char(MULE_INTERNAL, BIG5,
                                       (const char *) mic, len); */
            break;
        }
        mic += l;
        len -= l;
    }
    *p = '\0';
}
