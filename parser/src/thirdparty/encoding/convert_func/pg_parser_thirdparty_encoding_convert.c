/**
 * @file pg_parser_thirdparty_encoding_convert.c
 * @author bytesync
 * @brief
 * @version 0.1
 * @date 2023-08-10
 *
 * @copyright Copyright (c) 2023
 *
 */
#include "pg_parser_os_incl.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_wchar.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_conv.h"
#include "thirdparty/encoding/pg_parser_thirdparty_encoding_convfunc.h"

/*
 * local2local: a generic single byte charset encoding
 * conversion between two ASCII-superset encodings.
 *
 * l points to the source string of length len
 * p is the output area (must be large enough!)
 * src_encoding is the PG identifier for the source encoding
 * dest_encoding is the PG identifier for the target encoding
 * tab holds conversion entries for the source charset
 * starting from 128 (0x80). each entry in the table holds the corresponding
 * code point for the target charset, or 0 if there is no equivalent code.
 */

void local2local(const unsigned char* l,
                 unsigned char*       p,
                 int32_t              len,
                 int32_t              src_encoding,
                 int32_t              dest_encoding,
                 const unsigned char* tab)
{
    unsigned char c1, c2;

    while (len > 0)
    {
        c1 = *l;
        if (c1 == 0)
        {
            /* todo error handling */
            return;
        }
        if (!IS_HIGHBIT_SET(c1))
        {
            *p++ = c1;
        }
        else
        {
            c2 = tab[c1 - HIGHBIT];
            if (c2)
            {
                *p++ = c2;
            }
            else
            {
                /* todo error handling */
                return;
            }
        }
        l++;
        len--;
    }
    *p = '\0';
}

/*
 * LATINn ---> MIC when the charset's local codes map directly to MIC
 *
 * l points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 */
void latin2mic(const unsigned char* l, unsigned char* p, int32_t len, int32_t lc, int32_t encoding)
{
    int32_t c1;

    while (len > 0)
    {
        c1 = *l;
        if (c1 == 0)
        {
            /* todo error handling */
            return;
        }
        if (IS_HIGHBIT_SET(c1))
        {
            *p++ = lc;
        }
        *p++ = c1;
        l++;
        len--;
    }
    *p = '\0';
}

/*
 * MIC ---> LATINn when the charset's local codes map directly to MIC
 *
 * mic points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 */
void mic2latin(
    const unsigned char* mic, unsigned char* p, int32_t len, int32_t lc, int32_t encoding)
{
    int32_t c1;

    while (len > 0)
    {
        c1 = *mic;
        if (c1 == 0)
        {
            /* todo error handling */
            return;
        }
        if (!IS_HIGHBIT_SET(c1))
        {
            /* easy for ASCII */
            *p++ = c1;
            mic++;
            len--;
        }
        else
        {
            int32_t l = character_mic_mblen(mic);

            if (len < l)
            {
                /* todo error handling */
                return;
            }
            if (l != 2 || c1 != lc || !IS_HIGHBIT_SET(mic[1]))
            {
                /* todo error handling */
                return;
            }
            *p++ = mic[1];
            mic += 2;
            len -= 2;
        }
    }
    *p = '\0';
}

/*
 * ASCII ---> MIC
 *
 * While ordinarily SQL_ASCII encoding is forgiving of high-bit-set
 * characters, here we must take a hard line because we don't know
 * the appropriate MIC equivalent.
 */
void conv_ascii2mic(const unsigned char* l, unsigned char* p, int32_t len)
{
    int32_t c1;

    while (len > 0)
    {
        c1 = *l;
        if (c1 == 0 || IS_HIGHBIT_SET(c1))
        {
            /* todo error handling */
            return;
        }
        *p++ = c1;
        l++;
        len--;
    }
    *p = '\0';
}

/*
 * MIC ---> ASCII
 */
void conv_mic2ascii(const unsigned char* mic, unsigned char* p, int32_t len)
{
    int32_t c1;

    while (len > 0)
    {
        c1 = *mic;
        if (c1 == 0 || IS_HIGHBIT_SET(c1))
        {
            /* todo error handling */
            return;
        }
        *p++ = c1;
        mic++;
        len--;
    }
    *p = '\0';
}

/*
 * latin2mic_with_table: a generic single byte charset encoding
 * conversion from a local charset to the mule internal code.
 *
 * l points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 * tab holds conversion entries for the local charset
 * starting from 128 (0x80). each entry in the table holds the corresponding
 * code point for the mule encoding, or 0 if there is no equivalent code.
 */
void latin2mic_with_table(const unsigned char* l,
                          unsigned char*       p,
                          int32_t              len,
                          int32_t              lc,
                          int32_t              encoding,
                          const unsigned char* tab)
{
    unsigned char c1, c2;

    while (len > 0)
    {
        c1 = *l;
        if (c1 == 0)
        {
            /* todo error handling */
            return;
        }
        if (!IS_HIGHBIT_SET(c1))
        {
            *p++ = c1;
        }
        else
        {
            c2 = tab[c1 - HIGHBIT];
            if (c2)
            {
                *p++ = lc;
                *p++ = c2;
            }
            else
            {
                /* todo error handling */
                return;
            }
        }
        l++;
        len--;
    }
    *p = '\0';
}

/*
 * mic2latin_with_table: a generic single byte charset encoding
 * conversion from the mule internal code to a local charset.
 *
 * mic points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 * tab holds conversion entries for the mule internal code's second byte,
 * starting from 128 (0x80). each entry in the table holds the corresponding
 * code point for the local charset, or 0 if there is no equivalent code.
 */
void mic2latin_with_table(const unsigned char* mic,
                          unsigned char*       p,
                          int32_t              len,
                          int32_t              lc,
                          int32_t              encoding,
                          const unsigned char* tab)
{
    unsigned char c1, c2;

    while (len > 0)
    {
        c1 = *mic;
        if (c1 == 0)
        {
            /* todo error handling */
            return;
        }
        if (!IS_HIGHBIT_SET(c1))
        {
            /* easy for ASCII */
            *p++ = c1;
            mic++;
            len--;
        }
        else
        {
            int32_t l = character_mic_mblen(mic);

            if (len < l)
            {
                /* todo error handling */
                return;
            }
            if (l != 2 || c1 != lc || !IS_HIGHBIT_SET(mic[1]) || (c2 = tab[mic[1] - HIGHBIT]) == 0)
            {
                /* todo error handling */
                return;
            }
            *p++ = c2;
            mic += 2;
            len -= 2;
        }
    }
    *p = '\0';
}

/*
 * comparison routine for bsearch()
 * this routine is intended for combined UTF8 -> local code
 */
static int32_t compare3(const void* p1, const void* p2)
{
    uint32_t s1, s2, d1, d2;

    s1 = *(const uint32_t*)p1;
    s2 = *((const uint32_t*)p1 + 1);
    d1 = ((const character_utf_to_local_combined*)p2)->utf1;
    d2 = ((const character_utf_to_local_combined*)p2)->utf2;
    return (s1 > d1 || (s1 == d1 && s2 > d2)) ? 1 : ((s1 == d1 && s2 == d2) ? 0 : -1);
}

/*
 * comparison routine for bsearch()
 * this routine is intended for local code -> combined UTF8
 */
static int32_t compare4(const void* p1, const void* p2)
{
    uint32_t v1, v2;

    v1 = *(const uint32_t*)p1;
    v2 = ((const character_local_to_utf_combined*)p2)->code;
    return (v1 > v2) ? 1 : ((v1 == v2) ? 0 : -1);
}

/*
 * store 32bit character representation into multibyte stream
 */
static inline unsigned char* store_coded_char(unsigned char* dest, uint32_t code)
{
    if (code & 0xff000000)
    {
        *dest++ = code >> 24;
    }
    if (code & 0x00ff0000)
    {
        *dest++ = code >> 16;
    }
    if (code & 0x0000ff00)
    {
        *dest++ = code >> 8;
    }
    if (code & 0x000000ff)
    {
        *dest++ = code;
    }
    return dest;
}

/*
 * Convert a character using a conversion radix tree.
 *
 * 'l' is the length of the input character in bytes, and b1-b4 are
 * the input character's bytes.
 */
static inline uint32_t conv_mb_radix_conv(const character_mb_radix_tree* rt,
                                          int32_t                        l,
                                          unsigned char                  b1,
                                          unsigned char                  b2,
                                          unsigned char                  b3,
                                          unsigned char                  b4)
{
    if (l == 4)
    {
        /* 4-byte code */

        /* check code validity */
        if (b1 < rt->b4_1_lower || b1 > rt->b4_1_upper || b2 < rt->b4_2_lower ||
            b2 > rt->b4_2_upper || b3 < rt->b4_3_lower || b3 > rt->b4_3_upper ||
            b4 < rt->b4_4_lower || b4 > rt->b4_4_upper)
        {
            return 0;
        }

        /* perform lookup */
        if (rt->chars32)
        {
            uint32_t idx = rt->b4root;

            idx = rt->chars32[b1 + idx - rt->b4_1_lower];
            idx = rt->chars32[b2 + idx - rt->b4_2_lower];
            idx = rt->chars32[b3 + idx - rt->b4_3_lower];
            return rt->chars32[b4 + idx - rt->b4_4_lower];
        }
        else
        {
            uint16_t idx = rt->b4root;

            idx = rt->chars16[b1 + idx - rt->b4_1_lower];
            idx = rt->chars16[b2 + idx - rt->b4_2_lower];
            idx = rt->chars16[b3 + idx - rt->b4_3_lower];
            return rt->chars16[b4 + idx - rt->b4_4_lower];
        }
    }
    else if (l == 3)
    {
        /* 3-byte code */

        /* check code validity */
        if (b2 < rt->b3_1_lower || b2 > rt->b3_1_upper || b3 < rt->b3_2_lower ||
            b3 > rt->b3_2_upper || b4 < rt->b3_3_lower || b4 > rt->b3_3_upper)
        {
            return 0;
        }

        /* perform lookup */
        if (rt->chars32)
        {
            uint32_t idx = rt->b3root;

            idx = rt->chars32[b2 + idx - rt->b3_1_lower];
            idx = rt->chars32[b3 + idx - rt->b3_2_lower];
            return rt->chars32[b4 + idx - rt->b3_3_lower];
        }
        else
        {
            uint16_t idx = rt->b3root;

            idx = rt->chars16[b2 + idx - rt->b3_1_lower];
            idx = rt->chars16[b3 + idx - rt->b3_2_lower];
            return rt->chars16[b4 + idx - rt->b3_3_lower];
        }
    }
    else if (l == 2)
    {
        /* 2-byte code */

        /* check code validity - first byte */
        if (b3 < rt->b2_1_lower || b3 > rt->b2_1_upper || b4 < rt->b2_2_lower ||
            b4 > rt->b2_2_upper)
        {
            return 0;
        }

        /* perform lookup */
        if (rt->chars32)
        {
            uint32_t idx = rt->b2root;

            idx = rt->chars32[b3 + idx - rt->b2_1_lower];
            return rt->chars32[b4 + idx - rt->b2_2_lower];
        }
        else
        {
            uint16_t idx = rt->b2root;

            idx = rt->chars16[b3 + idx - rt->b2_1_lower];
            return rt->chars16[b4 + idx - rt->b2_2_lower];
        }
    }
    else if (l == 1)
    {
        /* 1-byte code */

        /* check code validity - first byte */
        if (b4 < rt->b1_lower || b4 > rt->b1_upper)
        {
            return 0;
        }

        /* perform lookup */
        if (rt->chars32)
        {
            return rt->chars32[b4 + rt->b1root - rt->b1_lower];
        }
        else
        {
            return rt->chars16[b4 + rt->b1root - rt->b1_lower];
        }
    }
    return 0; /* shouldn't happen */
}

/*
 * UTF8 ---> local code
 *
 * utf: input string in UTF8 encoding (need not be null-terminated)
 * len: length of input string (in bytes)
 * iso: pointer to the output area (must be large enough!)
          (output string will be null-terminated)
 * map: conversion map for single characters
 * cmap: conversion map for combined characters
 *          (optional, pass NULL if none)
 * cmapsize: number of entries in the conversion map for combined characters
 *          (optional, pass 0 if none)
 * conv_func: algorithmic encoding conversion function
 *          (optional, pass NULL if none)
 * encoding: PG identifier for the local encoding
 *
 * For each character, the cmap (if provided) is consulted first; if no match,
 * the map is consulted next; if still no match, the conv_func (if provided)
 * is applied.  An error is raised if no match is found.
 *
 * See pg_wchar.h for more details about the data structures used here.
 */
void UtfToLocal(const unsigned char*                   utf,
                int32_t                                len,
                unsigned char*                         iso,
                const character_mb_radix_tree*         map,
                const character_utf_to_local_combined* cmap,
                int32_t                                cmapsize,
                utf_local_conversion_func              conv_func,
                int32_t                                encoding)
{
    uint32_t                               iutf;
    int32_t                                l;
    const character_utf_to_local_combined* cp;

    if (!CHARACTER_VALID_ENCODING(encoding))
    {
        /* todo error handling */
        return;
    }

    for (; len > 0; len -= l)
    {
        unsigned char b1 = 0;
        unsigned char b2 = 0;
        unsigned char b3 = 0;
        unsigned char b4 = 0;

        /* "break" cases all represent errors */
        if (*utf == '\0')
        {
            break;
        }

        l = character_utf_mblen(utf);
        if (len < l)
        {
            break;
        }

        if (!character_utf8_islegal(utf, l))
        {
            break;
        }

        if (l == 1)
        {
            /* ASCII case is easy, assume it's one-to-one conversion */
            *iso++ = *utf++;
            continue;
        }

        /* collect coded char of length l */
        if (l == 2)
        {
            b3 = *utf++;
            b4 = *utf++;
        }
        else if (l == 3)
        {
            b2 = *utf++;
            b3 = *utf++;
            b4 = *utf++;
        }
        else if (l == 4)
        {
            b1 = *utf++;
            b2 = *utf++;
            b3 = *utf++;
            b4 = *utf++;
        }
        else
        {
            /* todo error handling */
            return;
        }
        iutf = (b1 << 24 | b2 << 16 | b3 << 8 | b4);

        /* First, try with combined map if possible */
        if (cmap && len > l)
        {
            const unsigned char* utf_save = utf;
            int32_t              len_save = len;
            int32_t              l_save = l;

            /* collect next character, same as above */
            len -= l;

            l = character_utf_mblen(utf);
            if (len < l)
            {
                break;
            }

            if (!character_utf8_islegal(utf, l))
            {
                break;
            }

            /* We assume ASCII character cannot be in combined map */
            if (l > 1)
            {
                uint32_t iutf2;
                uint32_t cutf[2];

                if (l == 2)
                {
                    iutf2 = *utf++ << 8;
                    iutf2 |= *utf++;
                }
                else if (l == 3)
                {
                    iutf2 = *utf++ << 16;
                    iutf2 |= *utf++ << 8;
                    iutf2 |= *utf++;
                }
                else if (l == 4)
                {
                    iutf2 = *utf++ << 24;
                    iutf2 |= *utf++ << 16;
                    iutf2 |= *utf++ << 8;
                    iutf2 |= *utf++;
                }
                else
                {
                    /* todo error handling */
                    return;
                }

                cutf[0] = iutf;
                cutf[1] = iutf2;
                cp = bsearch(
                    cutf, cmap, cmapsize, sizeof(character_utf_to_local_combined), compare3);

                if (cp)
                {
                    iso = store_coded_char(iso, cp->code);
                    continue;
                }
            }

            /* fail, so back up to reprocess second character next time */
            utf = utf_save;
            len = len_save;
            l = l_save;
        }

        /* Now check ordinary map */
        if (map)
        {
            uint32_t converted = conv_mb_radix_conv(map, l, b1, b2, b3, b4);

            if (converted)
            {
                iso = store_coded_char(iso, converted);
                continue;
            }
        }

        /* if there's a conversion function, try that */
        if (conv_func)
        {
            uint32_t converted = (*conv_func)(iutf);

            if (converted)
            {
                iso = store_coded_char(iso, converted);
                continue;
            }
        }

        /* failed to translate this character */
        /* todo error handling */
        return;
    }

    /* if we broke out of loop early, must be invalid input */
    if (len > 0)
    {
        /* todo error handling */
        return;
    }

    *iso = '\0';
}

/*
 * local code ---> UTF8
 *
 * iso: input string in local encoding (need not be null-terminated)
 * len: length of input string (in bytes)
 * utf: pointer to the output area (must be large enough!)
          (output string will be null-terminated)
 * map: conversion map for single characters
 * cmap: conversion map for combined characters
 *          (optional, pass NULL if none)
 * cmapsize: number of entries in the conversion map for combined characters
 *          (optional, pass 0 if none)
 * conv_func: algorithmic encoding conversion function
 *          (optional, pass NULL if none)
 * encoding: PG identifier for the local encoding
 *
 * For each character, the map is consulted first; if no match, the cmap
 * (if provided) is consulted next; if still no match, the conv_func
 * (if provided) is applied.  An error is raised if no match is found.
 *
 * See pg_wchar.h for more details about the data structures used here.
 */
void LocalToUtf(const unsigned char*                   iso,
                int32_t                                len,
                unsigned char*                         utf,
                const character_mb_radix_tree*         map,
                const character_local_to_utf_combined* cmap,
                int32_t                                cmapsize,
                utf_local_conversion_func              conv_func,
                int32_t                                encoding)
{
    uint32_t                               iiso;
    int32_t                                l;
    const character_local_to_utf_combined* cp;

    if (!CHARACTER_VALID_ENCODING(encoding))
    {
        /* todo error handling */
        return;
    }

    for (; len > 0; len -= l)
    {
        unsigned char b1 = 0;
        unsigned char b2 = 0;
        unsigned char b3 = 0;
        unsigned char b4 = 0;

        /* "break" cases all represent errors */
        if (*iso == '\0')
        {
            break;
        }

        if (!IS_HIGHBIT_SET(*iso))
        {
            /* ASCII case is easy, assume it's one-to-one conversion */
            *utf++ = *iso++;
            l = 1;
            continue;
        }

        l = character_encoding_verifymb(encoding, (const char*)iso, len);
        if (l < 0)
        {
            break;
        }

        /* collect coded char of length l */
        if (l == 1)
        {
            b4 = *iso++;
        }
        else if (l == 2)
        {
            b3 = *iso++;
            b4 = *iso++;
        }
        else if (l == 3)
        {
            b2 = *iso++;
            b3 = *iso++;
            b4 = *iso++;
        }
        else if (l == 4)
        {
            b1 = *iso++;
            b2 = *iso++;
            b3 = *iso++;
            b4 = *iso++;
        }
        else
        {
            /* todo error handling */
            return;
        }
        iiso = (b1 << 24 | b2 << 16 | b3 << 8 | b4);

        if (map)
        {
            uint32_t converted = conv_mb_radix_conv(map, l, b1, b2, b3, b4);

            if (converted)
            {
                utf = store_coded_char(utf, converted);
                continue;
            }

            /* If there's a combined character map, try that */
            if (cmap)
            {
                cp = bsearch(
                    &iiso, cmap, cmapsize, sizeof(character_local_to_utf_combined), compare4);

                if (cp)
                {
                    utf = store_coded_char(utf, cp->utf1);
                    utf = store_coded_char(utf, cp->utf2);
                    continue;
                }
            }
        }

        /* if there's a conversion function, try that */
        if (conv_func)
        {
            uint32_t converted = (*conv_func)(iiso);

            if (converted)
            {
                utf = store_coded_char(utf, converted);
                continue;
            }
        }

        /* failed to translate this character */
        /* todo error handling */
        return;
    }

    /* if we broke out of loop early, must be invalid input */
    if (len > 0)
    {
        /* todo error handling */
        return;
    }

    *utf = '\0';
}
