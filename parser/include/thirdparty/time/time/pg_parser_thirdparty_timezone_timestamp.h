/**
 * @file                xk_pg_parser_thirdparty_timezone_timestamp.h
 * @author              ByteSynch
 * @brief               定义 timestamp/timestamp with timezone 类型的数据结构和宏函数
 * @version             0.1
 * @date                2023-09-18
 *
 * @copyright           Copyright (c) 2023
 *
 */

#ifndef XK_PG_PARSER_THIRDPARTY_TIMEZONE_TIMESTAMP_H
#define XK_PG_PARSER_THIRDPARTY_TIMEZONE_TIMESTAMP_H

typedef int64_t                     Timestamp;
typedef int64_t                     TimestampTz;
typedef int64_t                     TimeOffset;
typedef int32_t                     fsec_t;                         /* fractional seconds (in microseconds) */
typedef int32_t                     AbsoluteTime;
typedef int32_t                     RelativeTime;

#define INVALID_ABSTIME             ((AbsoluteTime)0x7FFFFFFE)      /* 2147483647 (2^31 - 1) */
#define NOEND_ABSTIME               ((AbsoluteTime)0x7FFFFFFC)      /* 2147483645 (2^31 - 3) */
#define NOSTART_ABSTIME             ((AbsoluteTime)INT_MIN)         /* -2147483648 */

typedef struct

{
    TimeOffset                      time;           /* all time units other than days, months and years */
    int32_t                         day;            /* days, after time for alignment */
    int32_t                         month;          /* months and years, after time for alignment */
} Interval;

/* Limits on the "precision" option (typmod) for these data types */
#define MAX_TIMESTAMP_PRECISION     6
#define MAX_INTERVAL_PRECISION      6

/*
 * Assorted constants for datetime-related calculations
 */

#define DAYS_PER_YEAR               365.25    /* assumes leap year every four years */
#define MONTHS_PER_YEAR             12
/*
 *    DAYS_PER_MONTH is very imprecise.  The more accurate value is
 *    365.2425/12 = 30.436875, or '30 days 10:29:06'.  Right now we only
 *    return an integral number of days, but someday perhaps we should
 *    also return a 'time' value to be used as well.  ISO 8601 suggests
 *    30 days.
 */
#define DAYS_PER_MONTH              30        /* assumes exactly 30 days per month */
#define HOURS_PER_DAY               24        /* assume no daylight savings time changes */

/*
 *    This doesn't adjust for uneven daylight savings time intervals or leap
 *    seconds, and it crudely estimates leap years.  A more accurate value
 *    for days per years is 365.2422.
 */
#define SECS_PER_YEAR               (36525 * 864)    /* avoid floating-point computation */
#define SECS_PER_DAY                86400
#define SECS_PER_HOUR               3600
#define SECS_PER_MINUTE             60
#define MINS_PER_HOUR               60

#define USECS_PER_DAY               XK_PG_PARSER_INT64CONST(86400000000)
#define USECS_PER_HOUR              XK_PG_PARSER_INT64CONST(3600000000)
#define USECS_PER_MINUTE            XK_PG_PARSER_INT64CONST(60000000)
#define USECS_PER_SEC               XK_PG_PARSER_INT64CONST(1000000)

#define NSECS_PER_DAY    XK_PG_PARSER_INT64CONST(86400000000000)
#define NSECS_PER_HOUR   XK_PG_PARSER_INT64CONST(3600000000000)
#define NSECS_PER_MINUTE XK_PG_PARSER_INT64CONST(60000000000)
#define NSECS_PER_SEC    XK_PG_PARSER_INT64CONST(1000000000)
#define NSECS_PER_USEC   XK_PG_PARSER_INT64CONST(1000)

/*
 * We allow numeric timezone offsets up to 15:59:59 either way from Greenwich.
 * Currently, the record holders for wackiest offsets in actual use are zones
 * Asia/Manila, at -15:56:00 until 1844, and America/Metlakatla, at +15:13:42
 * until 1867.  If we were to reject such values we would fail to dump and
 * restore old timestamptz values with these zone settings.
 */
#define MAX_TZDISP_HOUR             15    /* maximum allowed hour part */
#define TZDISP_LIMIT                ((MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR)

/*
 * DT_NOBEGIN represents timestamp -infinity; DT_NOEND represents +infinity
 */
#define DT_NOBEGIN                  XK_PG_PARSER_INT64_MIN
#define DT_NOEND                    XK_PG_PARSER_INT64_MAX

#define TIMESTAMP_NOBEGIN(j)    \
    do {(j) = DT_NOBEGIN;} while (0)

#define TIMESTAMP_IS_NOBEGIN(j) ((j) == DT_NOBEGIN)

#define TIMESTAMP_NOEND(j)        \
    do {(j) = DT_NOEND;} while (0)

#define TIMESTAMP_IS_NOEND(j)       ((j) == DT_NOEND)

#define TIMESTAMP_NOT_FINITE(j)     (TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j))

#define JULIAN_MINYEAR              (-4713)
#define JULIAN_MINMONTH             (11)
#define JULIAN_MINDAY               (24)
#define JULIAN_MAXYEAR              (5874898)
#define JULIAN_MAXMONTH             (6)
#define JULIAN_MAXDAY               (3)

#define IS_VALID_JULIAN(y,m,d) \
    (((y) > JULIAN_MINYEAR || \
      ((y) == JULIAN_MINYEAR && ((m) >= JULIAN_MINMONTH))) && \
     ((y) < JULIAN_MAXYEAR || \
      ((y) == JULIAN_MAXYEAR && ((m) < JULIAN_MAXMONTH))))

/* Julian-date equivalents of Day 0 in Unix and Postgres reckoning */
#define UNIX_EPOCH_JDATE            2440588 /* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE        2451545 /* == date2j(2000, 1, 1) */

/* First allowed date, and first disallowed date, in Julian-date form */
#define DATETIME_MIN_JULIAN         (0)
#define DATE_END_JULIAN             (2147483494)    /* == date2j(JULIAN_MAXYEAR, 1, 1) */
#define TIMESTAMP_END_JULIAN        (109203528)     /* == date2j(294277, 1, 1) */

/* Timestamp limits */
#define MIN_TIMESTAMP               XK_PG_PARSER_INT64CONST(-211813488000000000)
/* == (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY */
#define END_TIMESTAMP               XK_PG_PARSER_INT64CONST(9223371331200000000)
/* == (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY */

/* Range-check a date (given in Postgres, not Julian, numbering) */
#define IS_VALID_DATE(d) \
    ((DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= (d) && \
     (d) < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE))

/* Range-check a timestamp */
#define IS_VALID_TIMESTAMP(t)       (MIN_TIMESTAMP <= (t) && (t) < END_TIMESTAMP)

typedef struct
{
    int32_t                     status;
    AbsoluteTime                data[2];
} TimeIntervalData;

typedef TimeIntervalData*       TimeInterval;

#endif /* XK_PG_PARSER_THIRDPARTY_TIMEZONE_TIMESTAMP_H */
