/**
 * @file xk_pg_parser_thirdparty_tupleparser_char.c
 * @author bytesync
 * @brief 
 * @version 0.1
 * @date 2023-08-03
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include "xk_pg_parser_os_incl.h"
#include "xk_pg_parser_app_incl.h"
#include "thirdparty/tupleparser/common/xk_pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/time/time/xk_pg_parser_thirdparty_timezone_time.h"
#include "thirdparty/time/date/xk_pg_parser_thirdparty_timezone_date.h"
#include "thirdparty/time/time/xk_pg_parser_thirdparty_timezone_timestamp.h"
#include "thirdparty/common/xk_pg_parser_thirdparty_builtins.h"

#define PGFUNC_CHAR_MCXT NULL

#define Abs(x) ((x) >= 0 ? (x) : -(x))

static int32_t time2tm(TimeADT time, struct xk_pg_parser_tm *tm, fsec_t *fsec);
static char *AppendSeconds(char *cp, int32_t sec, fsec_t fsec, int32_t precision, bool fillzeros);
static char *EncodeTimezone(char *str, int32_t tz, int32_t style);

static void EncodeTimeOnly(struct xk_pg_parser_tm *tm,
                           fsec_t fsec,
                           bool print_tz,
                           int32_t tz,
                           int32_t style,
                           char *str);

xk_pg_parser_Datum time_out(xk_pg_parser_Datum attr)
{
    TimeADT        time = (TimeADT) attr;
    char          *result;
    struct xk_pg_parser_tm   tt,
                  *tm = &tt;
    fsec_t         fsec;
    char           buf[MAXDATELEN + 1];
    int32_t        DateStyle = USE_ISO_DATES;

    time2tm(time, tm, &fsec);
    EncodeTimeOnly(tm, fsec, false, 0, DateStyle, buf);

    result = xk_pg_parser_mcxt_strdup(buf);
    return (xk_pg_parser_Datum) result;
}

/*****************************************************************************
 *     Time ADT
 *****************************************************************************/


/* time2tm()
 * Convert time data type to POSIX time structure.
 *
 * For dates within the range of pg_time_t, convert to the local time zone.
 * If out of this range, leave as UTC (in practice that could only happen
 * if pg_time_t is just 32 bits) - thomas 97/05/27
 */
static int32_t time2tm(TimeADT time, struct xk_pg_parser_tm *tm, fsec_t *fsec)
{
    tm->tm_hour = time / USECS_PER_HOUR;
    time -= tm->tm_hour * USECS_PER_HOUR;
    tm->tm_min = time / USECS_PER_MINUTE;
    time -= tm->tm_min * USECS_PER_MINUTE;
    tm->tm_sec = time / USECS_PER_SEC;
    time -= tm->tm_sec * USECS_PER_SEC;
    *fsec = time;
    return 0;
}

/* EncodeTimeOnly()
 * Encode time fields only.
 *
 * tm and fsec are the value to encode, print_tz determines whether to include
 * a time zone (the difference between time and timetz types), tz is the
 * numeric time zone offset, style is the date style, str is where to write the
 * output.
 */
static void EncodeTimeOnly(struct xk_pg_parser_tm *tm, fsec_t fsec, bool print_tz, int32_t tz, int32_t style, char *str)
{
    str = xk_numutils_ltostr_zeropad(str, tm->tm_hour, 2);
    *str++ = ':';
    str = xk_numutils_ltostr_zeropad(str, tm->tm_min, 2);
    *str++ = ':';
    str = AppendSeconds(str, tm->tm_sec, fsec, MAX_TIME_PRECISION, true);
    if (print_tz)
        str = EncodeTimezone(str, tz, style);
    *str = '\0';
}

/*
 * Append seconds and fractional seconds (if any) at *cp.
 *
 * precision is the max number of fraction digits, fillzeros says to
 * pad to two integral-seconds digits.
 *
 * Returns a pointer to the new end of string.  No NUL terminator is put
 * there; callers are responsible for NUL terminating str themselves.
 *
 * Note that any sign is stripped from the input seconds values.
 */
static char *AppendSeconds(char *cp, int32_t sec, fsec_t fsec, int32_t precision, bool fillzeros)
{
    if (fillzeros)
        cp = xk_numutils_ltostr_zeropad(cp, Abs(sec), 2);
    else
        cp = xk_numutils_ltostr(cp, Abs(sec));

    /* fsec_t is just an int32 */
    if (fsec != 0)
    {
        int32_t        value = Abs(fsec);
        char       *end = &cp[precision + 1];
        bool        gotnonzero = false;

        *cp++ = '.';

        /*
         * Append the fractional seconds part.  Note that we don't want any
         * trailing zeros here, so since we're building the number in reverse
         * we'll skip appending zeros until we've output a non-zero digit.
         */
        while (precision--)
        {
            int32_t        oldval = value;
            int32_t        remainder;

            value /= 10;
            remainder = oldval - value * 10;

            /* check if we got a non-zero */
            if (remainder)
                gotnonzero = true;

            if (gotnonzero)
                cp[precision] = '0' + remainder;
            else
                end = &cp[precision];
        }

        /*
         * If we still have a non-zero value then precision must have not been
         * enough to print the number.  We punt the problem to pg_ltostr(),
         * which will generate a correct answer in the minimum valid width.
         */
        if (value)
            return xk_numutils_ltostr(cp, Abs(fsec));

        return end;
    }
    else
        return cp;
}

/* EncodeTimezone()
 *        Copies representation of a numeric timezone offset to str.
 *
 * Returns a pointer to the new end of string.  No NUL terminator is put
 * there; callers are responsible for NUL terminating str themselves.
 */
static char *EncodeTimezone(char *str, int32_t tz, int32_t style)
{
    int32_t     hour,
                min,
                sec;

    sec = abs(tz);
    min = sec / SECS_PER_MINUTE;
    sec -= min * SECS_PER_MINUTE;
    hour = min / MINS_PER_HOUR;
    min -= hour * MINS_PER_HOUR;

    /* TZ is negated compared to sign we wish to display ... */
    *str++ = (tz <= 0 ? '+' : '-');

    if (sec != 0)
    {
        str = xk_numutils_ltostr_zeropad(str, hour, 2);
        *str++ = ':';
        str = xk_numutils_ltostr_zeropad(str, min, 2);
        *str++ = ':';
        str = xk_numutils_ltostr_zeropad(str, sec, 2);
    }
    else if (min != 0 || style == USE_XSD_DATES)
    {
        str = xk_numutils_ltostr_zeropad(str, hour, 2);
        *str++ = ':';
        str = xk_numutils_ltostr_zeropad(str, min, 2);
    }
    else
        str = xk_numutils_ltostr_zeropad(str, hour, 2);
    return str;
}
