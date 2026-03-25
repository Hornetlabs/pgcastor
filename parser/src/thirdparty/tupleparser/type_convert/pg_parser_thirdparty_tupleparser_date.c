/**
 * @file pg_parser_thirdparty_tupleparser_date.c
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
#include "thirdparty/time/time/pg_parser_thirdparty_timezone_time.h"
#include "thirdparty/time/date/pg_parser_thirdparty_timezone_date.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"

#define PGFUNC_DATE_MCXT NULL

static void EncodeDateOnly(struct pg_parser_tm* tm, int32_t style, char* str, int32_t DateOrder);
static void EncodeSpecialDate(DateADT dt, char* str);
static void j2date(int32_t jd, int32_t* year, int32_t* month, int32_t* day);

/* date_out()
 * Given internal format date, convert to pg_parser_text string.
 */
pg_parser_Datum date_out(pg_parser_Datum attr)
{
    DateADT             date = (DateADT)attr;
    char*               result;
    struct pg_parser_tm tt, *tm = &tt;
    char                buf[MAXDATELEN + 1];
    int32_t             DateStyle = USE_ISO_DATES;
    int32_t             DateOrder = DATEORDER_MDY;
    if (DATE_NOT_FINITE(date))
    {
        EncodeSpecialDate(date, buf);
    }
    else
    {
        j2date(date + PG_PARSER_EPOCH_JDATE, &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
        EncodeDateOnly(tm, DateStyle, buf, DateOrder);
    }

    result = pg_parser_mcxt_strdup(buf);
    return (pg_parser_Datum)result;
}

/*
 * Convert reserved date values to string.
 */
static void EncodeSpecialDate(DateADT dt, char* str)
{
    if (DATE_IS_NOBEGIN(dt))
    {
        strcpy(str, EARLY);
    }
    else if (DATE_IS_NOEND(dt))
    {
        strcpy(str, LATE);
    }
    else /* shouldn't happen */
    {
        return;
    }
}

static void j2date(int32_t jd, int32_t* year, int32_t* month, int32_t* day)
{
    uint32_t julian;
    uint32_t quad;
    uint32_t extra;
    int32_t  y;

    julian = jd;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
    y += quad * 4;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % MONTHS_PER_YEAR + 1;

    return;
} /* j2date() */

/* EncodeDateOnly()
 * Encode date as local time.
 */
static void EncodeDateOnly(struct pg_parser_tm* tm, int32_t style, char* str, int32_t DateOrder)
{
    switch (style)
    {
        case USE_ISO_DATES:
        case USE_XSD_DATES:
            /* compatible with ISO date formats */
            str = numutils_ltostr_zeropad(str, (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1),
                                          4);
            *str++ = '-';
            str = numutils_ltostr_zeropad(str, tm->tm_mon, 2);
            *str++ = '-';
            str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
            break;

        case USE_SQL_DATES:
            /* compatible with Oracle/Ingres date formats */
            if (DateOrder == DATEORDER_DMY)
            {
                str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
                *str++ = '/';
                str = numutils_ltostr_zeropad(str, tm->tm_mon, 2);
            }
            else
            {
                str = numutils_ltostr_zeropad(str, tm->tm_mon, 2);
                *str++ = '/';
                str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
            }
            *str++ = '/';
            str = numutils_ltostr_zeropad(str, (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1),
                                          4);
            break;

        case USE_GERMAN_DATES:
            /* German-style date format */
            str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
            *str++ = '.';
            str = numutils_ltostr_zeropad(str, tm->tm_mon, 2);
            *str++ = '.';
            str = numutils_ltostr_zeropad(str, (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1),
                                          4);
            break;

        case USE_POSTGRES_DATES:
        default:
            /* traditional date-only style for Postgres */
            if (DateOrder == DATEORDER_DMY)
            {
                str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
                *str++ = '-';
                str = numutils_ltostr_zeropad(str, tm->tm_mon, 2);
            }
            else
            {
                str = numutils_ltostr_zeropad(str, tm->tm_mon, 2);
                *str++ = '-';
                str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
            }
            *str++ = '-';
            str = numutils_ltostr_zeropad(str, (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1),
                                          4);
            break;
    }

    if (tm->tm_year <= 0)
    {
        rmemcpy1(str, 0, " BC", 3); /* Don't copy NUL */
        str += 3;
    }
    *str = '\0';
}
