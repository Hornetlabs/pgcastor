/**
 * @file pg_parser_thirdparty_tupleparser_uuid.c
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
#include "thirdparty/stringinfo/pg_parser_thirdparty_stringinfo.h"

#define PGFUNC_UUID_MCXT NULL

/* uuid size in bytes */
#define UUID_LEN 16

typedef struct pg_uuid_t
{
    unsigned char data[UUID_LEN];
} pg_uuid_t;

pg_parser_Datum
uuid_out(pg_parser_Datum attr)
{
    pg_uuid_t  *uuid = (pg_uuid_t *) attr;
    static const char hex_chars[] = "0123456789abcdef";
    pg_parser_StringInfoData buf;
    int32_t            i;
    char *result = NULL;

    pg_parser_initStringInfo(&buf);
    for (i = 0; i < UUID_LEN; i++)
    {
        int32_t            hi;
        int32_t            lo;

        /*
         * We print uuid values as a string of 8, 4, 4, 4, and then 12
         * hexadecimal characters, with each group is separated by a hyphen
         * ("-"). Therefore, add the hyphens at the appropriate places here.
         */
        if (i == 4 || i == 6 || i == 8 || i == 10)
            pg_parser_appendStringInfoChar(&buf, '-');

        hi = uuid->data[i] >> 4;
        lo = uuid->data[i] & 0x0F;

        pg_parser_appendStringInfoChar(&buf, hex_chars[hi]);
        pg_parser_appendStringInfoChar(&buf, hex_chars[lo]);
    }
    result = pg_parser_mcxt_strdup(buf.data);
    pg_parser_mcxt_free(PGFUNC_UUID_MCXT, buf.data);
    return (pg_parser_Datum) result;
}