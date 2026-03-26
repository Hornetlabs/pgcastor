/**
 * @file pg_parser_thirdparty_tupleparser_mac.c
 * @author bytesync
 * @brief
 * @version 0.1
 * @date 2023-08-03
 *
 * @copyright Copyright (c) 2023
 *
 */
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"

#define PGFUNC_MAC_MCXT NULL

/*
 *    This is the internal storage format for MAC addresses:
 */
typedef struct macaddr
{
    unsigned char a;
    unsigned char b;
    unsigned char c;
    unsigned char d;
    unsigned char e;
    unsigned char f;
} macaddr;

typedef struct macaddr8
{
    unsigned char a;
    unsigned char b;
    unsigned char c;
    unsigned char d;
    unsigned char e;
    unsigned char f;
    unsigned char g;
    unsigned char h;
} macaddr8;

/*
 *    MAC address output function.  Fixed format.
 */

pg_parser_Datum macaddr_out(pg_parser_Datum attr)
{
    macaddr* addr = (macaddr*)attr;
    char*    result = NULL;

    if (!pg_parser_mcxt_malloc(PGFUNC_MAC_MCXT, (void**)&result, 32))
    {
        return (pg_parser_Datum)0;
    }

    snprintf(result,
             32,
             "%02x:%02x:%02x:%02x:%02x:%02x",
             addr->a,
             addr->b,
             addr->c,
             addr->d,
             addr->e,
             addr->f);

    return (pg_parser_Datum)result;
}

pg_parser_Datum macaddr8_out(pg_parser_Datum attr)
{
    macaddr8* addr = (macaddr8*)attr;
    char*     result;

    if (!pg_parser_mcxt_malloc(PGFUNC_MAC_MCXT, (void**)&result, 32))
    {
        return (pg_parser_Datum)0;
    }

    snprintf(result,
             32,
             "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
             addr->a,
             addr->b,
             addr->c,
             addr->d,
             addr->e,
             addr->f,
             addr->g,
             addr->h);

    return (pg_parser_Datum)result;
}