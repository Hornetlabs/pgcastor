/**
 * @file xk_pg_parser_thirdparty_tupleparser_network.c
 * @author bytesync
 * @brief 
 * @version 0.1
 * @date 2023-08-03
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgfunc.h"

#define PGFUNC_NETWORK_MCXT NULL

#define PGSQL_AF_INET  (AF_INET + 0)
#define PGSQL_AF_INET6 (AF_INET + 1)

#define NS_IN6ADDRSZ 16
#define NS_INT16SZ 2

#define SPRINTF(x) ((size_t)sprintf x)

#define ip_family(inetptr) \
    (((inet_struct *) XK_PG_PARSER_VARDATA_ANY(inetptr))->family)

#define ip_bits(inetptr) \
    (((inet_struct *) XK_PG_PARSER_VARDATA_ANY(inetptr))->bits)

#define ip_addr(inetptr) \
    (((inet_struct *) XK_PG_PARSER_VARDATA_ANY(inetptr))->ipaddr)

typedef struct
{
    unsigned char family;        /* PGSQL_AF_INET or PGSQL_AF_INET6 */
    unsigned char bits;          /* number of bits in netmask */
    unsigned char ipaddr[16];    /* up to 128 bits of address */
} inet_struct;

typedef struct
{
    char        vl_len_[4];      /* Do not touch this field directly! */
    inet_struct inet_data;
} inet;

static char *xk_pg_parser_inet_net_ntop(int32_t af, const void *src, int32_t bits, char *dst, size_t size);
static int32_t decoct(const unsigned char *src,int32_t bytes, char *dst, size_t size);

static char *inet_net_ntop_ipv4(const unsigned char *src,
                                int32_t bits,
                                char *dst,
                                size_t size);

static char *inet_net_ntop_ipv6(const unsigned char *src,
                                int32_t bits,
                                char *dst,
                                size_t size);

static int32_t decoct(const unsigned char *src,int32_t bytes, char *dst, size_t size)
{
    char       *odst = dst;
    char       *t;
    int32_t         b;

    for (b = 1; b <= bytes; b++)
    {
        if (size <= sizeof "255.")
            return (0);
        t = dst;
        dst += SPRINTF((dst, "%u", *src++));
        if (b != bytes)
        {
            *dst++ = '.';
            *dst = '\0';
        }
        size -= (size_t) (dst - t);
    }
    return (dst - odst);
}

/*
 * static char *
 * inet_net_ntop_ipv4(src, bits, dst, size)
 *    convert IPv4 network address from network to presentation format.
 *    "src"'s size is determined from its "af".
 * return:
 *    pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *    network byte order assumed.  this means 192.5.5.240/28 has
 *    0b11110000 in its fourth octet.
 * author:
 *    Paul Vixie (ISC), October 1998
 */
static char *inet_net_ntop_ipv4(const unsigned char *src,
                                int32_t bits,
                                char *dst,
                                size_t size)
{
    char       *odst = dst;
    char       *t;
    int32_t         len = 4;
    int32_t         b;

    if (bits < 0 || bits > 32)
    {
        errno = EINVAL;
        return (NULL);
    }

    /* Always format all four octets, regardless of mask length. */
    for (b = len; b > 0; b--)
    {
        if (size <= sizeof ".255")
            goto network_emsgsize;
        t = dst;
        if (dst != odst)
            *dst++ = '.';
        dst += SPRINTF((dst, "%u", *src++));
        size -= (size_t) (dst - t);
    }

    /* don't print masklen if 32 bits */
    if (bits != 32)
    {
        if (size <= sizeof "/32")
            goto network_emsgsize;
        dst += SPRINTF((dst, "/%u", bits));
    }

    return (odst);

network_emsgsize:
    errno = EMSGSIZE;
    return (NULL);
}

static char *inet_net_ntop_ipv6(const unsigned char *src,
                                int32_t bits,
                                char *dst,
                                size_t size)
{
    /*
     * Note that int32_t and int16_t need only be "at least" large enough to
     * contain a value of the specified size.  On some systems, like Crays,
     * there is no such thing as an integer variable with 16 bits. Keep this
     * in mind if you think this function should have been coded to use
     * pointer overlays.  All the world's not a VAX.
     */
    char        tmp[sizeof "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255/128"];
    char       *tp;
    struct
    {
        int32_t base,
            len;
    }best, cur;
    uint32_t words[NS_IN6ADDRSZ / NS_INT16SZ];
    int32_t            i;

    if ((bits < -1) || (bits > 128))
    {
        errno = EINVAL;
        return (NULL);
    }

    /*
     * Preprocess: Copy the input (bytewise) array into a wordwise array. Find
     * the longest run of 0x00's in src[] for :: shorthanding.
     */
    rmemset1(words, 0, '\0', sizeof words);
    for (i = 0; i < NS_IN6ADDRSZ; i++)
        words[i / 2] |= (src[i] << ((1 - (i % 2)) << 3));
    best.base = -1;
    cur.base = -1;
    best.len = 0;
    cur.len = 0;
    for (i = 0; i < (NS_IN6ADDRSZ / NS_INT16SZ); i++)
    {
        if (words[i] == 0)
        {
            if (cur.base == -1)
                cur.base = i, cur.len = 1;
            else
                cur.len++;
        }
        else
        {
            if (cur.base != -1)
            {
                if (best.base == -1 || cur.len > best.len)
                    best = cur;
                cur.base = -1;
            }
        }
    }
    if (cur.base != -1)
    {
        if (best.base == -1 || cur.len > best.len)
            best = cur;
    }
    if (best.base != -1 && best.len < 2)
        best.base = -1;

    /*
     * Format the result.
     */
    tp = tmp;
    for (i = 0; i < (NS_IN6ADDRSZ / NS_INT16SZ); i++)
    {
        /* Are we inside the best run of 0x00's? */
        if (best.base != -1 && i >= best.base &&
            i < (best.base + best.len))
        {
            if (i == best.base)
                *tp++ = ':';
            continue;
        }
        /* Are we following an initial run of 0x00s or any real hex? */
        if (i != 0)
            *tp++ = ':';
        /* Is this address an encapsulated IPv4? */
        if (i == 6 && best.base == 0 && (best.len == 6 ||
                                         (best.len == 7 && words[7] != 0x0001) ||
                                         (best.len == 5 && words[5] == 0xffff)))
        {
            int32_t n;

            n = decoct(src + 12, 4, tp, sizeof tmp - (tp - tmp));
            if (n == 0)
            {
                errno = EMSGSIZE;
                return (NULL);
            }
            tp += strlen(tp);
            break;
        }
        tp += SPRINTF((tp, "%x", words[i]));
    }

    /* Was it a trailing run of 0x00's? */
    if (best.base != -1 && (best.base + best.len) ==
        (NS_IN6ADDRSZ / NS_INT16SZ))
        *tp++ = ':';
    *tp = '\0';

    if (bits != -1 && bits != 128)
        tp += SPRINTF((tp, "/%u", bits));

    /*
     * Check for overflow, copy, and we're done.
     */
    if ((size_t) (tp - tmp) > size)
    {
        errno = EMSGSIZE;
        return (NULL);
    }
    strcpy(dst, tmp);
    return (dst);
}

/*
 * char *
 * xk_pg_parser_inet_net_ntop(af, src, bits, dst, size)
 *    convert host/network address from network to presentation format.
 *    "src"'s size is determined from its "af".
 * return:
 *    pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *    192.5.5.1/28 has a nonzero host part, which means it isn't a network
 *    as called for by inet_net_pton() but it can be a host address with
 *    an included netmask.
 * author:
 *    Paul Vixie (ISC), October 1998
 */
static char *xk_pg_parser_inet_net_ntop(int32_t af, const void *src, int32_t bits, char *dst, size_t size)
{
    /*
     * We need to cover both the address family constants used by the PG inet
     * type (PGSQL_AF_INET and PGSQL_AF_INET6) and those used by the system
     * libraries (AF_INET and AF_INET6).  We can safely assume PGSQL_AF_INET
     * == AF_INET, but the INET6 constants are very likely to be different. If
     * AF_INET6 isn't defined, silently ignore it.
     */
    switch (af)
    {
        case PGSQL_AF_INET:
            return (inet_net_ntop_ipv4(src, bits, dst, size));
        case PGSQL_AF_INET6:
#if defined(AF_INET6) && AF_INET6 != PGSQL_AF_INET6
        case AF_INET6:
#endif
            return (inet_net_ntop_ipv6(src, bits, dst, size));
        default:
            errno = EAFNOSUPPORT;
            return (NULL);
    }
}

/*
 * Common INET/CIDR output routine
 */
static char *network_out(inet *src, bool is_cidr)
{
    char        tmp[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];
    char       *dst;
    int32_t         len;

    dst = xk_pg_parser_inet_net_ntop(ip_family(src), ip_addr(src), ip_bits(src),
                                     tmp, sizeof(tmp));
    if (dst == NULL)
    {
        /* ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("could not format inet value: %m"))); */
        return NULL;
    }

    /* For CIDR, add /n if not present */
    if (is_cidr && strchr(tmp, '/') == NULL)
    {
        len = strlen(tmp);
        snprintf(tmp + len, sizeof(tmp) - len, "/%u", ip_bits(src));
    }

    return xk_pg_parser_mcxt_strdup(tmp);
}

xk_pg_parser_Datum inet_out(xk_pg_parser_Datum attr)
{
    inet       *src = (inet *) attr;

    return (xk_pg_parser_Datum) network_out(src, false);
}

xk_pg_parser_Datum cidr_out(xk_pg_parser_Datum attr)
{
    inet       *src = (inet *) attr;

    return (xk_pg_parser_Datum) network_out(src, true);
}