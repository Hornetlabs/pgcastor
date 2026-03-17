/**
 * @file pg_parser_thirdparty_tupleparser_timestamp.c
 * @author bytesync
 * @brief 
 * @version 0.1
 * @date 2023-08-03
 * 
 * @copyright Copyright (c) 2023
 * 
 */
#include <sys/types.h>
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "common/pg_parser_translog.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgfunc.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_pgsfunc.h"
#include "thirdparty/time/time/pg_parser_thirdparty_timezone_timestamp.h"
#include "thirdparty/time/time/pg_parser_thirdparty_timezone_time.h"
#include "thirdparty/time/date/pg_parser_thirdparty_timezone_date.h"
#include "thirdparty/time/timezone/pg_parser_thirdparty_timezone_tz.h"
#include "thirdparty/time/timezone/pg_parser_thirdparty_timezone_tzfile.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_private.h"
#include "thirdparty/common/pg_parser_thirdparty_builtins.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"
#include "thirdparty/tupleparser/common/pg_parser_thirdparty_tupleparser_fmgr.h"

#define PGFUNC_TIMESTAMP_MCXT NULL

#define TWOS_COMPLEMENT(t) ((t) ~ (t) 0 < 0)
#define TZDEFRULESTRING ",M3.2.0,M11.1.0"

#define INTSTYLE_POSTGRES           0
#define INTSTYLE_POSTGRES_VERBOSE   1
#define INTSTYLE_SQL_STANDARD       2
#define INTSTYLE_ISO_8601           3


#define TIMESTAMP_MASK(b) (1 << (b))
#define INTERVAL_MASK(b) (1 << (b))

/* Macros to handle packing and unpacking the typmod field for intervals */
#define INTERVAL_FULL_RANGE (0x7FFF)
#define INTERVAL_RANGE_MASK (0x7FFF)
#define INTERVAL_FULL_PRECISION (0xFFFF)
#define INTERVAL_PRECISION_MASK (0xFFFF)
#define INTERVAL_TYPMOD(p,r) ((((r) & INTERVAL_RANGE_MASK) << 16) | ((p) & INTERVAL_PRECISION_MASK))
#define INTERVAL_PRECISION(t) ((t) & INTERVAL_PRECISION_MASK)
#define INTERVAL_RANGE(t) (((t) >> 16) & INTERVAL_RANGE_MASK)

/* TMODULO()
 * Like FMODULO(), but work on the timestamp datatype (now always int64_t).
 * We assume that int64_t follows the C99 semantics for division (negative
 * quotients truncate towards zero).
 */
#define TMODULO(t,q,u) \
do { \
    (q) = ((t) / (u)); \
    if ((q) != 0) (t) -= ((q) * (u)); \
} while(0)

#define unconstify(underlying_type, expr) \
    ((underlying_type) (expr))

#define Abs(x) ((x) >= 0 ? (x) : -(x))

#define SAMESIGN(a,b) (((a) < 0) == ((b) < 0))

/* Input buffer for data read from a compiled tz file.  */
union input_buffer
{
    /* The first part of the buffer, interpreted as a header.  */
    struct time_tzhead tzhead;

    /* The entire buffer.  */
    char buf[2 * sizeof(struct time_tzhead)
            + 2 * sizeof(struct pg_parser_time_state)
            + 4 * TIME_TZ_MAX_TIMES];
};

/* Local storage needed for 'tzloadbody'.  */
union local_storage
{
    /* The results of analyzing the file's contents after it is opened.  */
    struct file_analysis
    {
        /* The input buffer.  */
        union input_buffer u;

        /* A temporary state used for parsing a TZ string in the file.  */
        struct pg_parser_time_state st;
    } u;

    /* We don't need the "fullname" member */
};

enum r_type
{
    JULIAN_DAY,              /* Jn = Julian day */
    DAY_OF_YEAR,          /* n = day of year */
    MONTH_NTH_DAY_OF_WEEK /* Mm.n.d = month, week, day of week */
};

struct rule
{
    enum r_type r_type; /* type of rule */
    int32_t r_day;            /* day number of rule */
    int32_t r_week;            /* week number of rule */
    int32_t r_mon;            /* month number of rule */
    int32_t r_time;        /* transition time of rule */
};

static const int32_t year_lengths[2] = {
    TIME_DAYSPERNYEAR, TIME_DAYSPERLYEAR};

static const int32_t mon_lengths[2][TIME_MONSPERYEAR] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};

const char *const months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
"Jul", "Aug", "Sep", "Oct", "Nov", "Dec", NULL};

const char *const days[] = {"Sunday", "Monday", "Tuesday", "Wednesday",
"Thursday", "Friday", "Saturday", NULL};


#define WILDABBR "   "
static const char wildabbr[] = WILDABBR;

static void EncodeSpecialTimestamp(Timestamp dt, char *str);

static int32_t timestamp2tm(Timestamp dt,
                        int32_t *tzp,
                        struct pg_parser_tm *tm,
                        fsec_t *fsec,
                        const char **tzn,
                        pg_parser_tz *attimezone,
                        pg_parser_extraTypoutInfo *info);

static int32_t tzload(const char *name,
                      char *canonname,
                      struct pg_parser_time_state *sp,
                      bool doextend,
                      pg_parser_extraTypoutInfo *info);

static int32_t tzloadbody(char const *name,
                      char *canonname,
                      struct pg_parser_time_state *sp,
                      bool doextend,
                      union local_storage *lsp,
                      pg_parser_extraTypoutInfo *info);

static bool typesequiv(const struct pg_parser_time_state *sp, int32_t a, int32_t b);

static bool differ_by_repeat(const pg_parser_time_t t1, const pg_parser_time_t t0);

bool tzparse(const char *name,
             struct pg_parser_time_state *sp,
             bool lastditch,
             pg_parser_extraTypoutInfo *info);

static const char *getnum(const char *strp, int32_t *const nump, const int32_t min, const int32_t max);
static const char *getsecs(const char *strp, int32_t *const secsp);
static const char *getoffset(const char *strp, int32_t *const offsetp);
static const char *getqzname(const char *strp, const int32_t delim);
static const char *getzname(const char *strp);
static const char *getrule(const char *strp, struct rule *const rulep);
static void init_ttinfo(struct time_ttinfo *s,
                        int32_t utoff,
                        bool isdst,
                        int32_t desigidx);
static bool increment_overflow_time(pg_parser_time_t *tp, int32_t j);
static int32_t transtime(const int32_t year,
                         const struct rule *const rulep,
                         const int32_t offset);
void j2date(int32_t jd, int32_t *year, int32_t *month, int32_t *day);
void dt2time(Timestamp jd, int32_t *hour, int32_t *min, int32_t *sec, fsec_t *fsec);

static struct pg_parser_tm *localsub(struct pg_parser_time_state const *sp,
                                        pg_parser_time_t const *timep,
                                        struct pg_parser_tm *const tmp);
static struct pg_parser_tm *gmtsub(pg_parser_time_t const *timep,
                                      int32_t offset,
                                      struct pg_parser_tm *tmp);
static bool increment_overflow(int32_t *ip, int32_t j);
static int32_t leaps_thru_end_of(const int32_t y);
static int32_t leaps_thru_end_of_nonneg(int32_t y);
void EncodeDateTime(struct pg_parser_tm *tm,
                    fsec_t fsec,
                    bool print_tz,
                    int32_t tz,
                    const char *tzn,
                    int32_t style,
                    char *str);
static char *EncodeTimezone(char *str, int32_t tz, int32_t style);
static char *AppendSeconds(char *cp,
                           int32_t sec,
                           fsec_t fsec,
                           int32_t precision,
                           bool fillzeros);
static int32_t date2j(int32_t y, int32_t m, int32_t d);
static int32_t j2day(int32_t date);
static int32_t interval2tm(Interval span, struct pg_parser_tm *tm, fsec_t *fsec);
static int32_t dsinterval2tm(Interval span, struct pg_parser_tm *tm, fsec_t *fsec);
static void EncodeInterval(struct pg_parser_tm *tm,
                           fsec_t fsec,
                           int32_t style,
                           char *str);
static char *AddISO8601IntPart(char *cp, int32_t value, char units);
static char *AddPostgresIntPart(char *cp,
                                int32_t value,
                                const char *units,
                                bool *is_zero,
                                bool *is_before);
static int32_t timetz2tm(TimeTzADT *time,
                         struct pg_parser_tm *tm,
                         fsec_t *fsec,
                         int32_t *tzp);

static char *AddVerboseIntPart(char *cp,
                               int32_t value,
                               const char *units,
                               bool *is_zero, bool *is_before);

static int64_t detzcode64(const char *const codep);

static int64_t leapcorr(struct pg_parser_time_state const *sp, pg_parser_time_t t);
static char *AppendTimestampSeconds(char *cp, struct pg_parser_tm *tm, fsec_t fsec);

static void EncodeTimeOnly(struct pg_parser_tm *tm,
                           fsec_t fsec,
                           bool print_tz,
                           int32_t tz,
                           int32_t style,
                           char *str);

/* timestamptz_out()
 * Convert a timestamp to external form.
 */
pg_parser_Datum timestamptz_out(pg_parser_Datum attr,
                                   pg_parser_extraTypoutInfo *info)
{
    TimestampTz dt = (Timestamp) attr;
    char       *result;
    int32_t         tz;
    struct pg_parser_tm tt,
               *tm = &tt;
    fsec_t      fsec;
    const char *tzn;
    char        buf[MAXDATELEN + 1];

    if (TIMESTAMP_NOT_FINITE(dt))
        EncodeSpecialTimestamp(dt, buf);
    else if (timestamp2tm(dt, &tz, tm, &fsec, &tzn, NULL, info) == 0)
        EncodeDateTime(tm, fsec, true, tz, tzn, USE_ISO_DATES, buf);
    else
        return (pg_parser_Datum) 0;

    result = pg_parser_mcxt_strdup(buf);
    info->valuelen = strlen(result);
    return (pg_parser_Datum) result;
}

char *pg_parser_timestamptz_to_str(int64_t t)
{
    char           buf[MAXDATELEN + 1];
    int            tz;
    struct pg_parser_tm tt,
               *tm = &tt;
    fsec_t        fsec;
    const char *tzn;
    pg_parser_extraTypoutInfo info;

    if (!pg_parser_mcxt_malloc(PGFUNC_TIMESTAMP_MCXT,
                                 (void**)&(info.zicinfo),
                                  sizeof(pg_parser_translog_convertinfo_with_zic)))
        strcpy(buf, "UNKNOWN");

    if (!pg_parser_mcxt_malloc(PGFUNC_TIMESTAMP_MCXT,
                                 (void**)&(info.zicinfo->convertinfo),
                                  sizeof(pg_parser_translog_convertinfo)))
        strcpy(buf, "UNKNOWN");

    info.zicinfo->convertinfo->m_tzname = "CST";
    if (TIMESTAMP_NOT_FINITE(t))
        EncodeSpecialTimestamp(t, buf);
    else if (timestamp2tm(t, &tz, tm, &fsec, &tzn, NULL, &info) == 0)
        EncodeDateTime(tm, fsec, true, tz, tzn, USE_ISO_DATES, buf);
    else
        strcpy(buf, "(timestamp out of range)");

    if (info.zicinfo)
    {
        if (info.zicinfo->convertinfo)
            pg_parser_mcxt_free(PGFUNC_TIMESTAMP_MCXT, info.zicinfo->convertinfo);
        if (info.zicinfo->zicdata)
            pg_parser_mcxt_free(PGFUNC_TIMESTAMP_MCXT, info.zicinfo->zicdata);
        pg_parser_mcxt_free(PGFUNC_TIMESTAMP_MCXT, info.zicinfo);
    }

    return pg_parser_mcxt_strdup(buf);
}

/* interval_out()
 * Convert a time span to external form.
 */
pg_parser_Datum interval_out(pg_parser_Datum attr)
{
    Interval   *span = (Interval *) attr;
    char       *result;
    struct pg_parser_tm tt,
               *tm = &tt;
    fsec_t        fsec;
    char        buf[MAXDATELEN + 1];

    if (interval2tm(*span, tm, &fsec) != 0)
    {
        /* elog(ERROR, "could not convert interval to tm"); */
        return (pg_parser_Datum) 0;
    }

    EncodeInterval(tm, fsec, INTSTYLE_ISO_8601, buf);

    result = pg_parser_mcxt_strdup(buf);
    return (pg_parser_Datum) result;
}

pg_parser_Datum dsinterval_out(pg_parser_Datum attr)
{
    Interval   *span = (Interval *) attr;
    char       *result;
    struct pg_parser_tm tt,
               *tm = &tt;
    fsec_t        fsec;
    char        buf[MAXDATELEN + 1];

    if (dsinterval2tm(*span, tm, &fsec) != 0)
    {
        /* elog(ERROR, "could not convert interval to tm"); */
        return (pg_parser_Datum) 0;
    }

    EncodeInterval(tm, fsec, INTSTYLE_ISO_8601, buf);

    result = pg_parser_mcxt_strdup(buf);
    return (pg_parser_Datum) result;
}

/* timestamp_out()
 * Convert a timestamp to external form.
 */
pg_parser_Datum timestamp_out(pg_parser_Datum attr,
                                 pg_parser_extraTypoutInfo *info)
{
    Timestamp    timestamp = (Timestamp) attr;
    char        *result;
    struct pg_parser_tm tt,
                *tm = &tt;
    fsec_t       fsec;
    char         buf[MAXDATELEN + 1];

    if (TIMESTAMP_NOT_FINITE(timestamp))
        EncodeSpecialTimestamp(timestamp, buf);
    else if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL, info) == 0)
        EncodeDateTime(tm, fsec, false, 0, NULL, USE_ISO_DATES, buf);
    else
    {
        /* ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("timestamp out of range"))); */
        return (pg_parser_Datum) NULL;
    }

    result = pg_parser_mcxt_strdup(buf);
    info->valuelen = strlen(result);
    return (pg_parser_Datum) result;
}

/*****************************************************************************
 *     Time With Time Zone ADT
 *****************************************************************************/

pg_parser_Datum
timetz_out(pg_parser_Datum attr)
{
    TimeTzADT  *time = (TimeTzADT *) attr;
    char       *result;
    struct pg_parser_tm tt,
               *tm = &tt;
    fsec_t      fsec;
    int32_t     tz;
    char        buf[MAXDATELEN + 1];
    int32_t     DateStyle = USE_ISO_DATES;
    timetz2tm(time, tm, &fsec, &tz);
    EncodeTimeOnly(tm, fsec, true, tz, DateStyle, buf);

    result = pg_parser_mcxt_strdup(buf);
    return (pg_parser_Datum) result;
}

/* timetz2tm()
 * Convert TIME WITH TIME ZONE data type to POSIX time structure.
 */
static int32_t timetz2tm(TimeTzADT *time,
                         struct pg_parser_tm *tm,
                         fsec_t *fsec,
                         int32_t *tzp)
{
    TimeOffset    trem = time->time;

    tm->tm_hour = trem / USECS_PER_HOUR;
    trem -= tm->tm_hour * USECS_PER_HOUR;
    tm->tm_min = trem / USECS_PER_MINUTE;
    trem -= tm->tm_min * USECS_PER_MINUTE;
    tm->tm_sec = trem / USECS_PER_SEC;
    *fsec = trem - tm->tm_sec * USECS_PER_SEC;

    if (tzp != NULL)
        *tzp = time->zone;

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
static void EncodeTimeOnly(struct pg_parser_tm *tm,
                           fsec_t fsec,
                           bool print_tz,
                           int32_t tz,
                           int32_t style,
                           char *str)
{
    str = numutils_ltostr_zeropad(str, tm->tm_hour, 2);
    *str++ = ':';
    str = numutils_ltostr_zeropad(str, tm->tm_min, 2);
    *str++ = ':';
    str = AppendSeconds(str, tm->tm_sec, fsec, MAX_TIME_PRECISION, true);
    if (print_tz)
        str = EncodeTimezone(str, tz, style);
    *str = '\0';
}

/* EncodeInterval()
 * Interpret time structure as a delta time and convert to string.
 *
 * Support "traditional Postgres" and ISO-8601 styles.
 * Actually, afaik ISO does not address time interval formatting,
 *    but this looks similar to the spec for absolute date/time.
 * - thomas 1998-04-30
 *
 * Actually, afaik, ISO 8601 does specify formats for "time
 * intervals...[of the]...format with time-unit designators", which
 * are pretty ugly.  The format looks something like
 *       P1Y1M1DT1H1M1.12345S
 * but useful for exchanging data with computers instead of humans.
 * - ron 2003-07-14
 *
 * And ISO's SQL 2008 standard specifies standards for
 * "year-month literal"s (that look like '2-3') and
 * "day-time literal"s (that look like ('4 5:6:7')
 */
static void EncodeInterval(struct pg_parser_tm *tm,
                           fsec_t fsec,
                           int32_t style,
                           char *str)
{
    char       *cp = str;
    int32_t     year = tm->tm_year;
    int32_t     mon = tm->tm_mon;
    int32_t     mday = tm->tm_mday;
    int32_t     hour = tm->tm_hour;
    int32_t     min = tm->tm_min;
    int32_t     sec = tm->tm_sec;
    bool        is_before = false;
    bool        is_zero = true;

    /*
     * The sign of year and month are guaranteed to match, since they are
     * stored internally as "month". But we'll need to check for is_before and
     * is_zero when determining the signs of day and hour/minute/seconds
     * fields.
     */
    switch (style)
    {
            /* SQL Standard interval format */
        case INTSTYLE_SQL_STANDARD:
            {
                bool        has_negative = year < 0 || mon < 0 ||
                mday < 0 || hour < 0 ||
                min < 0 || sec < 0 || fsec < 0;
                bool        has_positive = year > 0 || mon > 0 ||
                mday > 0 || hour > 0 ||
                min > 0 || sec > 0 || fsec > 0;
                bool        has_year_month = year != 0 || mon != 0;
                bool        has_day_time = mday != 0 || hour != 0 ||
                min != 0 || sec != 0 || fsec != 0;
                bool        has_day = mday != 0;
                bool        sql_standard_value = !(has_negative && has_positive) &&
                !(has_year_month && has_day_time);

                /*
                 * SQL Standard wants only 1 "<sign>" preceding the whole
                 * interval ... but can't do that if mixed signs.
                 */
                if (has_negative && sql_standard_value)
                {
                    *cp++ = '-';
                    year = -year;
                    mon = -mon;
                    mday = -mday;
                    hour = -hour;
                    min = -min;
                    sec = -sec;
                    fsec = -fsec;
                }

                if (!has_negative && !has_positive)
                {
                    sprintf(cp, "0");
                }
                else if (!sql_standard_value)
                {
                    /*
                     * For non sql-standard interval values, force outputting
                     * the signs to avoid ambiguities with intervals with
                     * mixed sign components.
                     */
                    char        year_sign = (year < 0 || mon < 0) ? '-' : '+';
                    char        day_sign = (mday < 0) ? '-' : '+';
                    char        sec_sign = (hour < 0 || min < 0 ||
                                            sec < 0 || fsec < 0) ? '-' : '+';

                    sprintf(cp, "%c%d-%d %c%d %c%d:%02d:",
                            year_sign, abs(year), abs(mon),
                            day_sign, abs(mday),
                            sec_sign, abs(hour), abs(min));
                    cp += strlen(cp);
                    cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
                    *cp = '\0';
                }
                else if (has_year_month)
                {
                    sprintf(cp, "%d-%d", year, mon);
                }
                else if (has_day)
                {
                    sprintf(cp, "%d %d:%02d:", mday, hour, min);
                    cp += strlen(cp);
                    cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
                    *cp = '\0';
                }
                else
                {
                    sprintf(cp, "%d:%02d:", hour, min);
                    cp += strlen(cp);
                    cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
                    *cp = '\0';
                }
            }
            break;

            /* ISO 8601 "time-intervals by duration only" */
        case INTSTYLE_ISO_8601:
            /* special-case zero to avoid printing nothing */
            if (year == 0 && mon == 0 && mday == 0 &&
                hour == 0 && min == 0 && sec == 0 && fsec == 0)
            {
                sprintf(cp, "PT0S");
                break;
            }
            *cp++ = 'P';
            cp = AddISO8601IntPart(cp, year, 'Y');
            cp = AddISO8601IntPart(cp, mon, 'M');
            cp = AddISO8601IntPart(cp, mday, 'D');
            if (hour != 0 || min != 0 || sec != 0 || fsec != 0)
                *cp++ = 'T';
            cp = AddISO8601IntPart(cp, hour, 'H');
            cp = AddISO8601IntPart(cp, min, 'M');
            if (sec != 0 || fsec != 0)
            {
                if (sec < 0 || fsec < 0)
                    *cp++ = '-';
                cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
                *cp++ = 'S';
                *cp++ = '\0';
            }
            break;

            /* Compatible with postgresql < 8.4 when DateStyle = 'iso' */
        case INTSTYLE_POSTGRES:
            cp = AddPostgresIntPart(cp, year, "year", &is_zero, &is_before);

            /*
             * Ideally we should spell out "month" like we do for "year" and
             * "day".  However, for backward compatibility, we can't easily
             * fix this.  bjm 2011-05-24
             */
            cp = AddPostgresIntPart(cp, mon, "mon", &is_zero, &is_before);
            cp = AddPostgresIntPart(cp, mday, "day", &is_zero, &is_before);
            if (is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0)
            {
                bool        minus = (hour < 0 || min < 0 || sec < 0 || fsec < 0);

                sprintf(cp, "%s%s%02d:%02d:",
                        is_zero ? "" : " ",
                        (minus ? "-" : (is_before ? "+" : "")),
                        abs(hour), abs(min));
                cp += strlen(cp);
                cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
                *cp = '\0';
            }
            break;

            /* Compatible with postgresql < 8.4 when DateStyle != 'iso' */
        case INTSTYLE_POSTGRES_VERBOSE:
        default:
            strcpy(cp, "@");
            cp++;
            cp = AddVerboseIntPart(cp, year, "year", &is_zero, &is_before);
            cp = AddVerboseIntPart(cp, mon, "mon", &is_zero, &is_before);
            cp = AddVerboseIntPart(cp, mday, "day", &is_zero, &is_before);
            cp = AddVerboseIntPart(cp, hour, "hour", &is_zero, &is_before);
            cp = AddVerboseIntPart(cp, min, "min", &is_zero, &is_before);
            if (sec != 0 || fsec != 0)
            {
                *cp++ = ' ';
                if (sec < 0 || (sec == 0 && fsec < 0))
                {
                    if (is_zero)
                        is_before = true;
                    else if (!is_before)
                        *cp++ = '-';
                }
                else if (is_before)
                    *cp++ = '-';
                cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
                sprintf(cp, " sec%s",
                        (abs(sec) != 1 || fsec != 0) ? "s" : "");
                is_zero = false;
            }
            /* identically zero? then put in a unitless zero... */
            if (is_zero)
                strcat(cp, " 0");
            if (is_before)
                strcat(cp, " ago");
            break;
    }
}

/* Append a verbose-style interval field, but only if value isn't zero */
static char *AddVerboseIntPart(char *cp,
                               int32_t value,
                               const char *units,
                               bool *is_zero, bool *is_before)
{
    if (value == 0)
        return cp;
    /* first nonzero value sets is_before */
    if (*is_zero)
    {
        *is_before = (value < 0);
        value = abs(value);
    }
    else if (*is_before)
        value = -value;
    sprintf(cp, " %d %s%s", value, units, (value == 1) ? "" : "s");
    *is_zero = false;
    return cp + strlen(cp);
}

/* Append a postgres-style interval field, but only if value isn't zero */
static char *AddPostgresIntPart(char *cp,
                                int32_t value,
                                const char *units,
                                bool *is_zero,
                                bool *is_before)
{
    if (value == 0)
        return cp;
    sprintf(cp, "%s%s%d %s%s",
            (!*is_zero) ? " " : "",
            (*is_before && value > 0) ? "+" : "",
            value,
            units,
            (value != 1) ? "s" : "");

    /*
     * Each nonzero field sets is_before for (only) the next one.  This is a
     * tad bizarre but it's how it worked before...
     */
    *is_before = (value < 0);
    *is_zero = false;
    return cp + strlen(cp);
}

/*
 * Helper functions to avoid duplicated code in EncodeInterval.
 */

/* Append an ISO-8601-style interval field, but only if value isn't zero */
static char *AddISO8601IntPart(char *cp, int32_t value, char units)
{
    if (value == 0)
        return cp;
    sprintf(cp, "%d%c", value, units);
    return cp + strlen(cp);
}

/* interval2tm()
 * Convert an interval data type to a tm structure.
 */
static int32_t interval2tm(Interval span, struct pg_parser_tm *tm, fsec_t *fsec)
{
    TimeOffset    time;
    TimeOffset    tfrac;

    tm->tm_year = span.month / MONTHS_PER_YEAR;
    tm->tm_mon = span.month % MONTHS_PER_YEAR;
    tm->tm_mday = span.day;
    time = span.time;

    tfrac = time / USECS_PER_HOUR;
    time -= tfrac * USECS_PER_HOUR;
    tm->tm_hour = tfrac;
    if (!SAMESIGN(tm->tm_hour, tfrac))
    {
        /* ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("interval out of range"))); */
        return -1;
    }
    tfrac = time / USECS_PER_MINUTE;
    time -= tfrac * USECS_PER_MINUTE;
    tm->tm_min = tfrac;
    tfrac = time / USECS_PER_SEC;
    *fsec = time - (tfrac * USECS_PER_SEC);
    tm->tm_sec = tfrac;

    return 0;
}

/* dsinterval2tm()
 * Convert an dsinterval data type to a tm structure.
 */
static int32_t dsinterval2tm(Interval span, struct pg_parser_tm *tm, fsec_t *fsec)
{
    TimeOffset    time;
    TimeOffset    tfrac;

    tm->tm_year = span.month / MONTHS_PER_YEAR;
    tm->tm_mon = span.month % MONTHS_PER_YEAR;
    tm->tm_mday = span.day;
    time = span.time;

    tfrac = time / NSECS_PER_HOUR;
    time -= tfrac * NSECS_PER_HOUR;
    tm->tm_hour = tfrac;
    if (!SAMESIGN(tm->tm_hour, tfrac))
    {
        /* ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("interval out of range"))); */
        return -1;
    }
    tfrac = time / NSECS_PER_MINUTE;
    time -= tfrac * NSECS_PER_MINUTE;
    tm->tm_min = tfrac;
    tfrac = time / NSECS_PER_SEC;
    *fsec = time - (tfrac * NSECS_PER_SEC);
    tm->tm_sec = tfrac;

    return 0;
}

/* EncodeSpecialTimestamp()
 * Convert reserved timestamp data type to string.
 */
static void EncodeSpecialTimestamp(Timestamp dt, char *str)
{
    if (TIMESTAMP_IS_NOBEGIN(dt))
        strcpy(str, EARLY);
    else if (TIMESTAMP_IS_NOEND(dt))
        strcpy(str, LATE);
    else                        /* shouldn't happen */
        /* elog(ERROR, "invalid argument for EncodeSpecialTimestamp"); */
        return;
}

void j2date(int32_t jd, int32_t *year, int32_t *month, int32_t *day)
{
    uint32_t julian;
    uint32_t quad;
    uint32_t extra;
    int32_t          y;

    julian = jd;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
        + 123;
    y += quad * 4;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % MONTHS_PER_YEAR + 1;

    return;
}

void dt2time(Timestamp jd, int32_t *hour, int32_t *min, int32_t *sec, fsec_t *fsec)
{
    TimeOffset    time;

    time = jd;

    *hour = time / USECS_PER_HOUR;
    time -= (*hour) * USECS_PER_HOUR;
    *min = time / USECS_PER_MINUTE;
    time -= (*min) * USECS_PER_MINUTE;
    *sec = time / USECS_PER_SEC;
    *fsec = time - (*sec * USECS_PER_SEC);
}

/*
 * Normalize logic courtesy Paul Eggert.
 */

static bool increment_overflow(int32_t *ip, int32_t j)
{
    int32_t const i = *ip;

    /*----------
     * If i >= 0 there can only be overflow if i + j > INT_MAX
     * or if j > INT_MAX - i; given i >= 0, INT_MAX - i cannot overflow.
     * If i < 0 there can only be overflow if i + j < INT_MIN
     * or if j < INT_MIN - i; given i < 0, INT_MIN - i cannot overflow.
     *----------
     */
    if ((i >= 0) ? (j > INT_MAX - i) : (j < INT_MIN - i))
        return true;
    *ip += j;
    return false;
}

/*
 * Return the number of leap years through the end of the given year
 * where, to make the math easy, the answer for year zero is defined as zero.
 */

static int32_t leaps_thru_end_of_nonneg(int32_t y)
{
    return y / 4 - y / 100 + y / 400;
}

static int32_t leaps_thru_end_of(const int32_t y)
{
    return (y < 0
                ? -1 - leaps_thru_end_of_nonneg(-1 - y)
                : leaps_thru_end_of_nonneg(y));
}

static struct pg_parser_tm *timesub(const pg_parser_time_t *timep,
                                       int32_t offset,
                                       const struct pg_parser_time_state *sp,
                                       struct pg_parser_tm *tmp)
{
    const struct time_lsinfo *lp;
    pg_parser_time_t tdays;
    int32_t idays; /* unsigned would be so 2003 */
    int64_t rem;
    int32_t y;
    const int32_t *ip;
    int64_t corr;
    bool hit;
    int32_t i;

    corr = 0;
    hit = false;
    i = (sp == NULL) ? 0 : sp->leapcnt;
    while (--i >= 0)
    {
        lp = &sp->lsis[i];
        if (*timep >= lp->ls_trans)
        {
            corr = lp->ls_corr;
            hit = (*timep == lp->ls_trans && (i == 0 ? 0 : lp[-1].ls_corr) < corr);
            break;
        }
    }
    y = TIME_EPOCH_YEAR;
    tdays = *timep / TIME_SECSPERDAY;
    rem = *timep % TIME_SECSPERDAY;
    while (tdays < 0 || tdays >= year_lengths[time_isleap(y)])
    {
        int32_t newy;
        pg_parser_time_t tdelta;
        int32_t idelta;
        int32_t leapdays;

        tdelta = tdays / TIME_DAYSPERLYEAR;
        if (!((!TIME_TYPE_SIGNED(pg_parser_time_t)
                || INT_MIN <= tdelta) && tdelta <= INT_MAX))
            goto timestamp_out_of_range;
        idelta = tdelta;
        if (idelta == 0)
            idelta = (tdays < 0) ? -1 : 1;
        newy = y;
        if (increment_overflow(&newy, idelta))
            goto timestamp_out_of_range;
        leapdays = leaps_thru_end_of(newy - 1) -
                   leaps_thru_end_of(y - 1);
        tdays -= ((pg_parser_time_t)newy - y) * TIME_DAYSPERNYEAR;
        tdays -= leapdays;
        y = newy;
    }

    /*
     * Given the range, we can now fearlessly cast...
     */
    idays = tdays;
    rem += offset - corr;
    while (rem < 0)
    {
        rem += TIME_SECSPERDAY;
        --idays;
    }
    while (rem >= TIME_SECSPERDAY)
    {
        rem -= TIME_SECSPERDAY;
        ++idays;
    }
    while (idays < 0)
    {
        if (increment_overflow(&y, -1))
            goto timestamp_out_of_range;
        idays += year_lengths[time_isleap(y)];
    }
    while (idays >= year_lengths[time_isleap(y)])
    {
        idays -= year_lengths[time_isleap(y)];
        if (increment_overflow(&y, 1))
            goto timestamp_out_of_range;
    }
    tmp->tm_year = y;
    if (increment_overflow(&tmp->tm_year, -TIME_TM_YEAR_BASE))
        goto timestamp_out_of_range;
    tmp->tm_yday = idays;

    /*
     * The "extra" mods below avoid overflow problems.
     */
    tmp->tm_wday = TIME_EPOCH_WDAY +
                   ((y - TIME_EPOCH_YEAR) % TIME_DAYSPERWEEK) *
                       (TIME_DAYSPERNYEAR % TIME_DAYSPERWEEK) +
                   leaps_thru_end_of(y - 1) -
                   leaps_thru_end_of(TIME_EPOCH_YEAR - 1) +
                   idays;
    tmp->tm_wday %= TIME_DAYSPERWEEK;
    if (tmp->tm_wday < 0)
        tmp->tm_wday += TIME_DAYSPERWEEK;
    tmp->tm_hour = (int32_t)(rem / TIME_SECSPERHOUR);
    rem %= TIME_SECSPERHOUR;
    tmp->tm_min = (int32_t)(rem / TIME_SECSPERMIN);

    /*
     * A positive leap second requires a special representation. This uses
     * "... ??:59:60" et seq.
     */
    tmp->tm_sec = (int32_t)(rem % TIME_SECSPERMIN) + hit;
    ip = mon_lengths[time_isleap(y)];
    for (tmp->tm_mon = 0; idays >= ip[tmp->tm_mon]; ++(tmp->tm_mon))
        idays -= ip[tmp->tm_mon];
    tmp->tm_mday = (int32_t)(idays + 1);
    tmp->tm_isdst = 0;
    tmp->tm_gmtoff = offset;
    return tmp;

timestamp_out_of_range:
    errno = EOVERFLOW;
    return NULL;
}

/*
 * gmtsub is to gmtime as localsub is to localtime.
 *
 * Except we have a private "struct state" for GMT, so no sp is passed in.
 */

static struct pg_parser_tm *gmtsub(pg_parser_time_t const *timep,
                                      int32_t offset,
                                      struct pg_parser_tm *tmp)
{
    struct pg_parser_tm *result;

    /* GMT timezone state data is kept here */
    static struct pg_parser_time_state *gmtptr = NULL;

    if (gmtptr == NULL)
    {
        /* Allocate on first use */
        if (!pg_parser_mcxt_malloc(PGFUNC_TIMESTAMP_MCXT,
                                      (void **)&gmtptr,
                                      sizeof(struct pg_parser_time_state)))
            return NULL; /* errno should be set by malloc */
                         /* gmtload(gmtptr); */
    }

    result = timesub(timep, offset, gmtptr, tmp);

    /*
     * Could get fancy here and deliver something such as "+xx" or "-xx" if
     * offset is non-zero, but this is no time for a treasure hunt.
     */
    if (offset != 0)
        tmp->tm_zone = wildabbr;
    else
        tmp->tm_zone = gmtptr->chars;

    return result;
}

/*
 * The easy way to behave "as if no library function calls" localtime
 * is to not call it, so we drop its guts into "localsub", which can be
 * freely called. (And no, the PANS doesn't require the above behavior,
 * but it *is* desirable.)
 */
static struct pg_parser_tm *localsub(struct pg_parser_time_state const *sp,
                                        pg_parser_time_t const *timep,
                                        struct pg_parser_tm *const tmp)
{
    const struct time_ttinfo *ttisp;
    int32_t i;
    struct pg_parser_tm *result;
    const pg_parser_time_t t = *timep;

    if (sp == NULL)
        return gmtsub(timep, 0, tmp);
    if ((sp->goback && t < sp->ats[0]) ||
        (sp->goahead && t > sp->ats[sp->timecnt - 1]))
    {
        pg_parser_time_t newt = t;
        pg_parser_time_t seconds;
        pg_parser_time_t years;

        if (t < sp->ats[0])
            seconds = sp->ats[0] - t;
        else
            seconds = t - sp->ats[sp->timecnt - 1];
        --seconds;
        years = (seconds / TIME_SECSPERREPEAT + 1) * TIME_YEARSPERREPEAT;
        seconds = years * TIME_AVGSECSPERYEAR;
        if (t < sp->ats[0])
            newt += seconds;
        else
            newt -= seconds;
        if (newt < sp->ats[0] ||
            newt > sp->ats[sp->timecnt - 1])
            return NULL; /* "cannot happen" */
        result = localsub(sp, &newt, tmp);
        if (result)
        {
            int64_t newy;

            newy = result->tm_year;
            if (t < sp->ats[0])
                newy -= years;
            else
                newy += years;
            if (!(INT_MIN <= newy && newy <= INT_MAX))
                return NULL;
            result->tm_year = newy;
        }
        return result;
    }
    if (sp->timecnt == 0 || t < sp->ats[0])
    {
        i = sp->defaulttype;
    }
    else
    {
        int32_t lo = 1;
        int32_t hi = sp->timecnt;

        while (lo < hi)
        {
            int32_t mid = (lo + hi) >> 1;

            if (t < sp->ats[mid])
                hi = mid;
            else
                lo = mid + 1;
        }
        i = (int32_t)sp->types[lo - 1];
    }
    ttisp = &sp->ttis[i];

    /*
     * To get (wrong) behavior that's compatible with System V Release 2.0
     * you'd replace the statement below with t += ttisp->tt_utoff;
     * timesub(&t, 0L, sp, tmp);
     */
    result = timesub(&t, ttisp->tt_utoff, sp, tmp);
    if (result)
    {
        result->tm_isdst = ttisp->tt_isdst;
        result->tm_zone = unconstify(char *, &sp->chars[ttisp->tt_desigidx]);
    }
    return result;
}

/*
 * timestamp2tm() - Convert timestamp data type to POSIX time structure.
 *
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 * Returns:
 *     0 on success
 *    -1 on out of range
 *
 * If attimezone is NULL, the global timezone setting will be used.
 */
static int32_t timestamp2tm(Timestamp dt,
                        int32_t *tzp,
                        struct pg_parser_tm *tm,
                        fsec_t *fsec,
                        const char **tzn,
                        pg_parser_tz *attimezone,
                        pg_parser_extraTypoutInfo *info)
{
    Timestamp    date;
    Timestamp    time;
    pg_parser_time_t    utime;
    char *tzname = info->zicinfo->convertinfo->m_tzname;

    /* Use session timezone if caller asks for default */
    if (attimezone == NULL && tzp)
    {
        pg_parser_mcxt_malloc(PGFUNC_TIMESTAMP_MCXT,
                                (void**)&attimezone,
                                 sizeof(pg_parser_tz));
        rmemcpy1(attimezone->TZname, 0, tzname, strlen(tzname));
        tzload(attimezone->TZname, NULL, &attimezone->state, true, info);
        tzparse(attimezone->TZname, &attimezone->state, false, info);
    }
    time = dt;
    TMODULO(time, date, USECS_PER_DAY);

    if (time < PG_PARSER_INT64CONST(0))
    {
        time += USECS_PER_DAY;
        date -= 1;
    }

    /* add offset to go from J2000 back to standard Julian date */
    date += POSTGRES_EPOCH_JDATE;

    /* Julian day routine does not work for negative Julian days */
    if (date < 0 || date > (Timestamp) INT_MAX)
        return -1;

    j2date((int32_t) date, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
    dt2time(time, &tm->tm_hour, &tm->tm_min, &tm->tm_sec, fsec);

    /* Done if no TZ conversion wanted */
    if (tzp == NULL)
    {
        tm->tm_isdst = -1;
        tm->tm_gmtoff = 0;
        tm->tm_zone = NULL;
        if (tzn != NULL)
            *tzn = NULL;
        return 0;
    }

    /*
     * If the time falls within the range of pg_time_t, use pg_localtime() to
     * rotate to the local time zone.
     *
     * First, convert to an integral timestamp, avoiding possibly
     * platform-specific roundoff-in-wrong-direction errors, and adjust to
     * Unix epoch.  Then see if we can convert to pg_time_t without loss. This
     * coding avoids hardwiring any assumptions about the width of pg_time_t,
     * so it should behave sanely on machines without int64_t.
     */
    dt = (dt - *fsec) / USECS_PER_SEC +
        (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY;
    utime = (pg_parser_time_t) dt;
    if ((Timestamp) utime == dt)
    {
        struct pg_parser_tm temp ={'\0'} ;
        struct pg_parser_tm *tx = localsub(&attimezone->state, &utime, &temp);

        tm->tm_year = tx->tm_year + 1900;
        tm->tm_mon = tx->tm_mon + 1;
        tm->tm_mday = tx->tm_mday;
        tm->tm_hour = tx->tm_hour;
        tm->tm_min = tx->tm_min;
        tm->tm_sec = tx->tm_sec;
        tm->tm_isdst = tx->tm_isdst;
        tm->tm_gmtoff = tx->tm_gmtoff;
        tm->tm_zone = tx->tm_zone;
        *tzp = -tm->tm_gmtoff;
        if (tzn != NULL)
            *tzn = tm->tm_zone;
    }
    else
    {
        /*
         * When out of range of pg_time_t, treat as GMT
         */
        *tzp = 0;
        /* Mark this as *no* time zone available */
        tm->tm_isdst = -1;
        tm->tm_gmtoff = 0;
        tm->tm_zone = NULL;
        if (tzn != NULL)
            *tzn = NULL;
    }

    if (attimezone)
        pg_parser_mcxt_free(PGFUNC_TIMESTAMP_MCXT, attimezone);

    return 0;
}

/* Load tz data from the file named NAME into *SP.  Read extended
 * format if DOEXTEND.  Return 0 on success, an errno value on failure.
 * PG: If "canonname" is not NULL, then on success the canonical spelling of
 * given name is stored there (the buffer must be > TZ_STRLEN_MAX bytes!).
 */
static int32_t tzload(const char *name,
                  char *canonname,
                  struct pg_parser_time_state *sp,
                  bool doextend,
                  pg_parser_extraTypoutInfo *info)
{
    union local_storage *lsp = NULL;
    if(!pg_parser_mcxt_malloc(PGFUNC_TIMESTAMP_MCXT, (void**) &lsp, sizeof *lsp))
    {
        return -1;
    }
    else
    {
        int32_t err = tzloadbody(name, canonname, sp, doextend, lsp, info);

        pg_parser_mcxt_free(PGFUNC_TIMESTAMP_MCXT, lsp);
        return err;
    }
}

static int32_t detzcode(const char *const codep)
{
    int32_t result;
    int32_t i;
    int32_t one = 1;
    int32_t halfmaxval = one << (32 - 2);
    int32_t maxval = halfmaxval - 1 + halfmaxval;
    int32_t minval = -1 - maxval;

    result = codep[0] & 0x7f;
    for (i = 1; i < 4; ++i)
        result = (result << 8) | (codep[i] & 0xff);

    if (codep[0] & 0x80)
    {
        /*
         * Do two's-complement negation even on non-two's-complement machines.
         * If the result would be minval - 1, return minval.
         */
        result -= !TWOS_COMPLEMENT(int32_t) && result != 0;
        result += minval;
    }
    return result;
}

static int64_t detzcode64(const char *const codep)
{
    uint64_t       result;
    int32_t        i;
    int64_t        one = 1;
    int64_t        halfmaxval = one << (64 - 2);
    int64_t        maxval = halfmaxval - 1 + halfmaxval;
    int64_t        minval = -TWOS_COMPLEMENT(int64_t) - maxval;

    result = codep[0] & 0x7f;
    for (i = 1; i < 8; ++i)
        result = (result << 8) | (codep[i] & 0xff);

    if (codep[0] & 0x80)
    {
        /*
         * Do two's-complement negation even on non-two's-complement machines.
         * If the result would be minval - 1, return minval.
         */
        result -= !TWOS_COMPLEMENT(int64_t) && result != 0;
        result += minval;
    }
    return result;
}

static int64_t leapcorr(struct pg_parser_time_state const *sp, pg_parser_time_t t)
{
    struct time_lsinfo const *lp;
    int32_t i;

    i = sp->leapcnt;
    while (--i >= 0)
    {
        lp = &sp->lsis[i];
        if (t >= lp->ls_trans)
            return lp->ls_corr;
    }
    return 0;
}

/* Load tz data from the file named NAME into *SP.  Read extended
 * format if DOEXTEND.  Use *LSP for temporary storage.  Return 0 on
 * success, an errno value on failure.
 * PG: If "canonname" is not NULL, then on success the canonical spelling of
 * given name is stored there (the buffer must be > TZ_STRLEN_MAX bytes!).
 */
static int32_t tzloadbody(char const *name,
                      char *canonname,
                      struct pg_parser_time_state *sp,
                      bool doextend,
                      union local_storage *lsp,
                      pg_parser_extraTypoutInfo *info)
{
    int32_t i;
    /* int32_t fid; */
    int32_t stored;
    ssize_t nread;
    union input_buffer *up = &lsp->u.u;
    int32_t tzheadsize = sizeof(struct time_tzhead);
    pg_parser_StringInfoData strdata = {'\0'};

    PG_PARSER_UNUSED(canonname);

    sp->goback = sp->goahead = false;

    if (!name)
    {
        name = TIME_TZDEFAULT;
        if (!name)
            return EINVAL;
    }

    if (name[0] == ':')
        ++name;

    if (!info->zicinfo->zicdata || !info->zicinfo->ziclen)
    {
        char *tzdata_temp = NULL;
        pg_parser_initStringInfo(&strdata);
        pg_parser_zic_get_tzdata(info->zicinfo->convertinfo->m_tzname, &strdata);
        tzdata_temp = pg_parser_mcxt_strdup(strdata.data);
        info->zicinfo->zicdata = tzdata_temp;
        info->zicinfo->ziclen = strlen(tzdata_temp);
        pg_parser_mcxt_free(PGFUNC_TIMESTAMP_MCXT, strdata.data);
    }

    nread = info->zicinfo->ziclen;
    rmemcpy1(up->buf, 0, info->zicinfo->zicdata, nread);


    for (stored = 4; stored <= 8; stored *= 2)
    {
        int32_t ttisstdcnt = detzcode(up->tzhead.tzh_ttisstdcnt);
        int32_t ttisutcnt = detzcode(up->tzhead.tzh_ttisutcnt);
        int64_t prevtr = 0;
        int32_t prevcorr = 0;
        int32_t leapcnt = detzcode(up->tzhead.tzh_leapcnt);
        int32_t timecnt = detzcode(up->tzhead.tzh_timecnt);
        int32_t typecnt = detzcode(up->tzhead.tzh_typecnt);
        int32_t charcnt = detzcode(up->tzhead.tzh_charcnt);
        char const *p = up->buf + tzheadsize;

        /*
         * Although tzfile(5) currently requires typecnt to be nonzero,
         * support future formats that may allow zero typecnt in files that
         * have a TZ string and no transitions.
         */
        if (!(0 <= leapcnt && leapcnt < TIME_TZ_MAX_LEAPS && 0 <= typecnt &&
              typecnt < TIME_TZ_MAX_TYPES && 0 <= timecnt && timecnt < TIME_TZ_MAX_TIMES &&
              0 <= charcnt && charcnt < TIME_TZ_MAX_CHARS &&
              (ttisstdcnt == typecnt || ttisstdcnt == 0) &&
              (ttisutcnt == typecnt || ttisutcnt == 0)))
            return EINVAL;
        if (nread < (tzheadsize                  /* struct tzhead */
                     + timecnt * stored          /* ats */
                     + timecnt                  /* types */
                     + typecnt * 6              /* ttinfos */
                     + charcnt                  /* chars */
                     + leapcnt * (stored + 4) /* lsinfos */
                     + ttisstdcnt              /* ttisstds */
                     + ttisutcnt))              /* ttisuts */
            return EINVAL;
        sp->leapcnt = leapcnt;
        sp->timecnt = timecnt;
        sp->typecnt = typecnt;
        sp->charcnt = charcnt;

        /*
         * Read transitions, discarding those out of pg_time_t range. But
         * pretend the last transition before TIME_T_MIN occurred at
         * TIME_T_MIN.
         */
        timecnt = 0;
        for (i = 0; i < sp->timecnt; ++i)
        {
            int64_t at = stored == 4 ? detzcode(p) : detzcode64(p);

            sp->types[i] = at <= TIME_TIME_T_MAX;
            if (sp->types[i])
            {
                pg_parser_time_t attime = ((TIME_TYPE_SIGNED(pg_parser_time_t) ? at < TIME_TIME_T_MIN : at < 0)
                                        ? TIME_TIME_T_MIN
                                        : at);

                if (timecnt && attime <= sp->ats[timecnt - 1])
                {
                    if (attime < sp->ats[timecnt - 1])
                        return EINVAL;
                    sp->types[i - 1] = 0;
                    timecnt--;
                }
                sp->ats[timecnt++] = attime;
            }
            p += stored;
        }

        timecnt = 0;
        for (i = 0; i < sp->timecnt; ++i)
        {
            unsigned char typ = *p++;

            if (sp->typecnt <= typ)
                return EINVAL;
            if (sp->types[i])
                sp->types[timecnt++] = typ;
        }
        sp->timecnt = timecnt;
        for (i = 0; i < sp->typecnt; ++i)
        {
            struct time_ttinfo *ttisp;
            unsigned char isdst,
                desigidx;

            ttisp = &sp->ttis[i];
            ttisp->tt_utoff = detzcode(p);
            p += 4;
            isdst = *p++;
            if (!(isdst < 2))
                return EINVAL;
            ttisp->tt_isdst = isdst;
            desigidx = *p++;
            if (!(desigidx < sp->charcnt))
                return EINVAL;
            ttisp->tt_desigidx = desigidx;
        }
        for (i = 0; i < sp->charcnt; ++i)
            sp->chars[i] = *p++;
        sp->chars[i] = '\0'; /* ensure '\0' at end */

        /* Read leap seconds, discarding those out of pg_time_t range.  */
        leapcnt = 0;
        for (i = 0; i < sp->leapcnt; ++i)
        {
            int64_t tr = stored == 4 ? detzcode(p) : detzcode64(p);
            int32_t corr = detzcode(p + stored);

            p += stored + 4;
            /* Leap seconds cannot occur before the Epoch.  */
            if (tr < 0)
                return EINVAL;
            if (tr <= TIME_TIME_T_MAX)
            {
                /*
                 * Leap seconds cannot occur more than once per UTC month, and
                 * UTC months are at least 28 days long (minus 1 second for a
                 * negative leap second).  Each leap second's correction must
                 * differ from the previous one's by 1 second.
                 */
                if (tr - prevtr < 28 * TIME_SECSPERDAY - 1 || (corr != prevcorr - 1 && corr != prevcorr + 1))
                    return EINVAL;
                sp->lsis[leapcnt].ls_trans = prevtr = tr;
                sp->lsis[leapcnt].ls_corr = prevcorr = corr;
                leapcnt++;
            }
        }
        sp->leapcnt = leapcnt;

        for (i = 0; i < sp->typecnt; ++i)
        {
            struct time_ttinfo *ttisp;

            ttisp = &sp->ttis[i];
            if (ttisstdcnt == 0)
                ttisp->tt_ttisstd = false;
            else
            {
                if (*p != true && *p != false)
                    return EINVAL;
                ttisp->tt_ttisstd = *p++;
            }
        }
        for (i = 0; i < sp->typecnt; ++i)
        {
            struct time_ttinfo *ttisp;

            ttisp = &sp->ttis[i];
            if (ttisutcnt == 0)
                ttisp->tt_ttisut = false;
            else
            {
                if (*p != true && *p != false)
                    return EINVAL;
                ttisp->tt_ttisut = *p++;
            }
        }

        /*
         * If this is an old file, we're done.
         */
        if (up->tzhead.tzh_version[0] == '\0')
            break;
        nread -= p - up->buf;
        memmove(up->buf, p, nread);
    }
    if (doextend && nread > 2 &&
        up->buf[0] == '\n' && up->buf[nread - 1] == '\n' &&
        sp->typecnt + 2 <= TIME_TZ_MAX_TYPES)
    {
        struct pg_parser_time_state *ts = &lsp->u.st;

        up->buf[nread - 1] = '\0';
        if (tzparse(&up->buf[1], ts, false, info))
        {
            /*
             * Attempt to reuse existing abbreviations. Without this,
             * America/Anchorage would be right on the edge after 2037 when
             * TZ_MAX_CHARS is 50, as sp->charcnt equals 40 (for LMT AST AWT
             * APT AHST AHDT YST AKDT AKST) and ts->charcnt equals 10 (for
             * AKST AKDT).  Reusing means sp->charcnt can stay 40 in this
             * example.
             */
            int32_t gotabbr = 0;
            int32_t charcnt = sp->charcnt;

            for (i = 0; i < ts->typecnt; i++)
            {
                char *tsabbr = ts->chars + ts->ttis[i].tt_desigidx;
                int32_t j;

                for (j = 0; j < charcnt; j++)
                    if (strcmp(sp->chars + j, tsabbr) == 0)
                    {
                        ts->ttis[i].tt_desigidx = j;
                        gotabbr++;
                        break;
                    }
                if (!(j < charcnt))
                {
                    int32_t tsabbrlen = strlen(tsabbr);

                    if (j + tsabbrlen < TIME_TZ_MAX_CHARS)
                    {
                        strcpy(sp->chars + j, tsabbr);
                        charcnt = j + tsabbrlen + 1;
                        ts->ttis[i].tt_desigidx = j;
                        gotabbr++;
                    }
                }
            }
            if (gotabbr == ts->typecnt)
            {
                sp->charcnt = charcnt;

                /*
                 * Ignore any trailing, no-op transitions generated by zic as
                 * they don't help here and can run afoul of bugs in zic 2016j
                 * or earlier.
                 */
                while (1 < sp->timecnt && (sp->types[sp->timecnt - 1] == sp->types[sp->timecnt - 2]))
                    sp->timecnt--;

                for (i = 0; i < ts->timecnt; i++)
                    if (sp->timecnt == 0 || (sp->ats[sp->timecnt - 1] < ts->ats[i] + leapcorr(sp, ts->ats[i])))
                        break;
                while (i < ts->timecnt && sp->timecnt < TIME_TZ_MAX_TIMES)
                {
                    sp->ats[sp->timecnt] = ts->ats[i] + leapcorr(sp, ts->ats[i]);
                    sp->types[sp->timecnt] = (sp->typecnt + ts->types[i]);
                    sp->timecnt++;
                    i++;
                }
                for (i = 0; i < ts->typecnt; i++)
                    sp->ttis[sp->typecnt++] = ts->ttis[i];
            }
        }
    }
    if (sp->typecnt == 0)
        return EINVAL;
    if (sp->timecnt > 1)
    {
        for (i = 1; i < sp->timecnt; ++i)
            if (typesequiv(sp, sp->types[i], sp->types[0]) &&
                differ_by_repeat(sp->ats[i], sp->ats[0]))
            {
                sp->goback = true;
                break;
            }
        for (i = sp->timecnt - 2; i >= 0; --i)
            if (typesequiv(sp, sp->types[sp->timecnt - 1],
                           sp->types[i]) &&
                differ_by_repeat(sp->ats[sp->timecnt - 1],
                                 sp->ats[i]))
            {
                sp->goahead = true;
                break;
            }
    }

    /*
     * Infer sp->defaulttype from the data.  Although this default type is
     * always zero for data from recent tzdb releases, things are trickier for
     * data from tzdb 2018e or earlier.
     *
     * The first set of heuristics work around bugs in 32-bit data generated
     * by tzdb 2013c or earlier.  The workaround is for zones like
     * Australia/Macquarie where timestamps before the first transition have a
     * time type that is not the earliest standard-time type.  See:
     * https://mm.icann.org/pipermail/tz/2013-May/019368.html
     */

    /*
     * If type 0 is unused in transitions, it's the type to use for early
     * times.
     */
    for (i = 0; i < sp->timecnt; ++i)
        if (sp->types[i] == 0)
            break;
    i = i < sp->timecnt ? -1 : 0;

    /*
     * Absent the above, if there are transition times and the first
     * transition is to a daylight time find the standard type less than and
     * closest to the type of the first transition.
     */
    if (i < 0 && sp->timecnt > 0 && sp->ttis[sp->types[0]].tt_isdst)
    {
        i = sp->types[0];
        while (--i >= 0)
            if (!sp->ttis[i].tt_isdst)
                break;
    }

    /*
     * The next heuristics are for data generated by tzdb 2018e or earlier,
     * for zones like EST5EDT where the first transition is to DST.
     */

    /*
     * If no result yet, find the first standard type. If there is none, punt
     * to type zero.
     */
    if (i < 0)
    {
        i = 0;
        while (sp->ttis[i].tt_isdst)
            if (++i >= sp->typecnt)
            {
                i = 0;
                break;
            }
    }

    /*
     * A simple 'sp->defaulttype = 0;' would suffice here if we didn't have to
     * worry about 2018e-or-earlier data.  Even simpler would be to remove the
     * defaulttype member and just use 0 in its place.
     */
    sp->defaulttype = i;

    return 0;
}

static bool typesequiv(const struct pg_parser_time_state *sp, int32_t a, int32_t b)
{
    bool result;

    if (sp == NULL ||
        a < 0 || a >= sp->typecnt ||
        b < 0 || b >= sp->typecnt)
        result = false;
    else
    {
        const struct time_ttinfo *ap = &sp->ttis[a];
        const struct time_ttinfo *bp = &sp->ttis[b];

        result = (ap->tt_utoff == bp->tt_utoff && ap->tt_isdst == bp->tt_isdst && ap->tt_ttisstd == bp->tt_ttisstd && ap->tt_ttisut == bp->tt_ttisut && (strcmp(&sp->chars[ap->tt_desigidx], &sp->chars[bp->tt_desigidx]) == 0));
    }
    return result;
}

static bool differ_by_repeat(const pg_parser_time_t t1, const pg_parser_time_t t0)
{
    if ((TIME_TYPE_BIT(pg_parser_time_t)
        - TIME_TYPE_SIGNED(pg_parser_time_t))
        < TIME_SECSPERREPEAT_BITS)
        return 0;
    return t1 - t0 == TIME_SECSPERREPEAT;
}

bool tzparse(const char *name,
             struct pg_parser_time_state *sp,
             bool lastditch,
             pg_parser_extraTypoutInfo *info)
{
    const char *stdname;
    const char *dstname = NULL;
    size_t stdlen;
    size_t dstlen;
    size_t charcnt;
    int32_t stdoffset;
    int32_t dstoffset;
    char *cp;
    /* bool load_ok; */
    static struct pg_parser_time_state *tzdefrules_s = NULL;

    stdname = name;
    if (lastditch)
    {
        /* Unlike IANA, don't assume name is exactly "GMT" */
        stdlen = strlen(name); /* length of standard zone name */
        name += stdlen;
        stdoffset = 0;
    }
    else
    {
        if (*name == '<')
        {
            name++;
            stdname = name;
            name = getqzname(name, '>');
            if (*name != '>')
                return false;
            stdlen = name - stdname;
            name++;
        }
        else
        {
            name = getzname(name);
            stdlen = name - stdname;
        }
        if (*name == '\0') /* we allow empty STD abbrev, unlike IANA */
            return false;
        name = getoffset(name, &stdoffset);
        if (name == NULL)
            return false;
    }
    charcnt = stdlen + 1;
    if (sizeof sp->chars < charcnt)
        return false;

    /*
     * The IANA code always tries tzload(TZDEFRULES) here.  We do not want to
     * do that; it would be bad news in the lastditch case, where we can't
     * assume pg_open_tzfile() is sane yet.  Moreover, the only reason to do
     * it unconditionally is to absorb the TZDEFRULES zone's leap second info,
     * which we don't want to do anyway.  Without that, we only need to load
     * TZDEFRULES if the zone name specifies DST but doesn't incorporate a
     * POSIX-style transition date rule, which is not a common case.
     */
    sp->goback = sp->goahead = false; /* simulate failed tzload() */
    sp->leapcnt = 0;                  /* intentionally assume no leap seconds */

    if (*name != '\0')
    {
        if (*name == '<')
        {
            dstname = ++name;
            name = getqzname(name, '>');
            if (*name != '>')
                return false;
            dstlen = name - dstname;
            name++;
        }
        else
        {
            dstname = name;
            name = getzname(name);
            dstlen = name - dstname; /* length of DST abbr. */
        }
        if (!dstlen)
            return false;
        charcnt += dstlen + 1;
        if (sizeof sp->chars < charcnt)
            return false;
        if (*name != '\0' && *name != ',' && *name != ';')
        {
            name = getoffset(name, &dstoffset);
            if (name == NULL)
                return false;
        }
        else
            dstoffset = stdoffset - TIME_SECSPERHOUR;
        if (*name == '\0')
        {
            /*
             * The POSIX zone name does not provide a transition-date rule.
             * Here we must load the TZDEFRULES zone, if possible, to serve as
             * source data for the transition dates.  Unlike the IANA code, we
             * try to cache the data so it's only loaded once.
             */
            if (tzload(TIME_TZDEFRULES, NULL, tzdefrules_s, false, info) == 0)
                rmemcpy1(sp, 0, tzdefrules_s, sizeof(struct pg_parser_time_state));
            else
            {
                /* If we can't load TZDEFRULES, fall back to hard-wired rule */
                name = TZDEFRULESTRING;
            }
        }
        if (*name == ',' || *name == ';')
        {
            struct rule start;
            struct rule end;
            int32_t year;
            int32_t yearlim;
            int32_t timecnt;
            pg_parser_time_t janfirst;
            int32_t janoffset = 0;
            int32_t yearbeg;

            ++name;
            if ((name = getrule(name, &start)) == NULL)
                return false;
            if (*name++ != ',')
                return false;
            if ((name = getrule(name, &end)) == NULL)
                return false;
            if (*name != '\0')
                return false;
            sp->typecnt = 2; /* standard time and DST */

            /*
             * Two transitions per year, from EPOCH_YEAR forward.
             */
            init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
            init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
            sp->defaulttype = 0;
            timecnt = 0;
            janfirst = 0;
            yearbeg = TIME_EPOCH_YEAR;

            do
            {
                int32_t yearsecs = year_lengths[time_isleap(yearbeg - 1)] * TIME_SECSPERDAY;

                yearbeg--;
                if (increment_overflow_time(&janfirst, -yearsecs))
                {
                    janoffset = -yearsecs;
                    break;
                }
            } while (TIME_EPOCH_YEAR - TIME_YEARSPERREPEAT / 2 < yearbeg);

            yearlim = yearbeg + TIME_YEARSPERREPEAT + 1;
            for (year = yearbeg; year < yearlim; year++)
            {
                int32_t
                    starttime = transtime(year, &start, stdoffset),
                    endtime = transtime(year, &end, dstoffset);
                int32_t
                    yearsecs = (year_lengths[time_isleap(year)] * TIME_SECSPERDAY);
                bool reversed = endtime < starttime;

                if (reversed)
                {
                    int32_t swap = starttime;

                    starttime = endtime;
                    endtime = swap;
                }
                if (reversed || (starttime < endtime && (endtime - starttime < (yearsecs + (stdoffset - dstoffset)))))
                {
                    if (TIME_TZ_MAX_TIMES - 2 < timecnt)
                        break;
                    sp->ats[timecnt] = janfirst;
                    if (!increment_overflow_time(&sp->ats[timecnt],
                                                 janoffset + starttime))
                        sp->types[timecnt++] = !reversed;
                    sp->ats[timecnt] = janfirst;
                    if (!increment_overflow_time(&sp->ats[timecnt],
                                                 janoffset + endtime))
                    {
                        sp->types[timecnt++] = reversed;
                        yearlim = year + TIME_YEARSPERREPEAT + 1;
                    }
                }
                if (increment_overflow_time(&janfirst, janoffset + yearsecs))
                    break;
                janoffset = 0;
            }
            sp->timecnt = timecnt;
            if (!timecnt)
            {
                sp->ttis[0] = sp->ttis[1];
                sp->typecnt = 1; /* Perpetual DST.  */
            }
            else if (TIME_YEARSPERREPEAT < year - yearbeg)
                sp->goback = sp->goahead = true;
        }
        else
        {
            int32_t theirstdoffset;
            int32_t theirdstoffset;
            int32_t theiroffset;
            bool isdst;
            int32_t i;
            int32_t j;

            if (*name != '\0')
                return false;

            /*
             * Initial values of theirstdoffset and theirdstoffset.
             */
            theirstdoffset = 0;
            for (i = 0; i < sp->timecnt; ++i)
            {
                j = sp->types[i];
                if (!sp->ttis[j].tt_isdst)
                {
                    theirstdoffset =
                        -sp->ttis[j].tt_utoff;
                    break;
                }
            }
            theirdstoffset = 0;
            for (i = 0; i < sp->timecnt; ++i)
            {
                j = sp->types[i];
                if (sp->ttis[j].tt_isdst)
                {
                    theirdstoffset =
                        -sp->ttis[j].tt_utoff;
                    break;
                }
            }

            /*
             * Initially we're assumed to be in standard time.
             */
            isdst = false;
            theiroffset = theirstdoffset;

            /*
             * Now juggle transition times and types tracking offsets as you
             * do.
             */
            for (i = 0; i < sp->timecnt; ++i)
            {
                j = sp->types[i];
                sp->types[i] = sp->ttis[j].tt_isdst;
                if (sp->ttis[j].tt_ttisut)
                {
                    /* No adjustment to transition time */
                }
                else
                {
                    /*
                     * If daylight saving time is in effect, and the
                     * transition time was not specified as standard time, add
                     * the daylight saving time offset to the transition time;
                     * otherwise, add the standard time offset to the
                     * transition time.
                     */
                    /*
                     * Transitions from DST to DDST will effectively disappear
                     * since POSIX provides for only one DST offset.
                     */
                    if (isdst && !sp->ttis[j].tt_ttisstd)
                    {
                        sp->ats[i] += dstoffset -
                                      theirdstoffset;
                    }
                    else
                    {
                        sp->ats[i] += stdoffset -
                                      theirstdoffset;
                    }
                }
                theiroffset = -sp->ttis[j].tt_utoff;
                if (sp->ttis[j].tt_isdst)
                    theirdstoffset = theiroffset;
                else
                    theirstdoffset = theiroffset;
            }

            /*
             * Finally, fill in ttis.
             */
            init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
            init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
            sp->typecnt = 2;
            sp->defaulttype = 0;
        }
    }
    else
    {
        dstlen = 0;
        sp->typecnt = 1; /* only standard time */
        sp->timecnt = 0;
        init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
        sp->defaulttype = 0;
    }
    sp->charcnt = charcnt;
    cp = sp->chars;
    rmemcpy1(cp, 0, stdname, stdlen);
    cp += stdlen;
    *cp++ = '\0';
    if (dstlen != 0)
    {
        rmemcpy1(cp, 0, dstname, dstlen);
        *(cp + dstlen) = '\0';
    }
    return true;
}

/*
 * Given a pointer into an extended timezone string, scan until the ending
 * delimiter of the time zone abbreviation is located.
 * Return a pointer to the delimiter.
 *
 * As with getzname above, the legal character set is actually quite
 * restricted, with other characters producing undefined results.
 * We don't do any checking here; checking is done later in common-case code.
 */

static const char *getqzname(const char *strp, const int32_t delim)
{
    int32_t c;

    while ((c = *strp) != '\0' && c != delim)
        ++strp;
    return strp;
}

/*
 * Given a pointer into a timezone string, scan until a character that is not
 * a valid character in a time zone abbreviation is found.
 * Return a pointer to that character.
 */

static const char *getzname(const char *strp)
{
    char c;

    while ((c = *strp) != '\0' && !time_is_digit(c) && c != ',' && c != '-' &&
           c != '+')
        ++strp;
    return strp;
}

/*
 * Given a pointer into a timezone string, extract an offset, in
 * [+-]hh[:mm[:ss]] form, from the string.
 * If any error occurs, return NULL.
 * Otherwise, return a pointer to the first character not part of the time.
 */

static const char *getoffset(const char *strp, int32_t *const offsetp)
{
    bool neg = false;

    if (*strp == '-')
    {
        neg = true;
        ++strp;
    }
    else if (*strp == '+')
        ++strp;
    strp = getsecs(strp, offsetp);
    if (strp == NULL)
        return NULL; /* illegal time */
    if (neg)
        *offsetp = -*offsetp;
    return strp;
}

/*
 * Given a pointer into a timezone string, extract a number of seconds,
 * in hh[:mm[:ss]] form, from the string.
 * If any error occurs, return NULL.
 * Otherwise, return a pointer to the first character not part of the number
 * of seconds.
 */

static const char *getsecs(const char *strp, int32_t *const secsp)
{
    int32_t num;

    /*
     * 'HOURSPERDAY * DAYSPERWEEK - 1' allows quasi-Posix rules like
     * "M10.4.6/26", which does not conform to Posix, but which specifies the
     * equivalent of "02:00 on the first Sunday on or after 23 Oct".
     */
    strp = getnum(strp, &num, 0, TIME_HOURSPERDAY * TIME_DAYSPERWEEK - 1);
    if (strp == NULL)
        return NULL;
    *secsp = num * (int32_t)TIME_SECSPERHOUR;
    if (*strp == ':')
    {
        ++strp;
        strp = getnum(strp, &num, 0, TIME_MINSPERHOUR - 1);
        if (strp == NULL)
            return NULL;
        *secsp += num * TIME_SECSPERMIN;
        if (*strp == ':')
        {
            ++strp;
            /* 'SECSPERMIN' allows for leap seconds.  */
            strp = getnum(strp, &num, 0, TIME_SECSPERMIN);
            if (strp == NULL)
                return NULL;
            *secsp += num;
        }
    }
    return strp;
}

/*
 * Given a pointer into a timezone string, extract a number from that string.
 * Check that the number is within a specified range; if it is not, return
 * NULL.
 * Otherwise, return a pointer to the first character not part of the number.
 */

static const char *getnum(const char *strp, int32_t *const nump, const int32_t min, const int32_t max)
{
    char c;
    int32_t num;

    if (strp == NULL || !time_is_digit(c = *strp))
        return NULL;
    num = 0;
    do
    {
        num = num * 10 + (c - '0');
        if (num > max)
            return NULL; /* illegal value */
        c = *++strp;
    } while (time_is_digit(c));
    if (num < min)
        return NULL; /* illegal value */
    *nump = num;
    return strp;
}

/*
 * Given a pointer into a timezone string, extract a rule in the form
 * date[/time]. See POSIX section 8 for the format of "date" and "time".
 * If a valid rule is not found, return NULL.
 * Otherwise, return a pointer to the first character not part of the rule.
 */

static const char *getrule(const char *strp, struct rule *const rulep)
{
    if (*strp == 'J')
    {
        /*
         * Julian day.
         */
        rulep->r_type = JULIAN_DAY;
        ++strp;
        strp = getnum(strp, &rulep->r_day, 1, TIME_DAYSPERNYEAR);
    }
    else if (*strp == 'M')
    {
        /*
         * Month, week, day.
         */
        rulep->r_type = MONTH_NTH_DAY_OF_WEEK;
        ++strp;
        strp = getnum(strp, &rulep->r_mon, 1, TIME_MONSPERYEAR);
        if (strp == NULL)
            return NULL;
        if (*strp++ != '.')
            return NULL;
        strp = getnum(strp, &rulep->r_week, 1, 5);
        if (strp == NULL)
            return NULL;
        if (*strp++ != '.')
            return NULL;
        strp = getnum(strp, &rulep->r_day, 0, TIME_DAYSPERWEEK - 1);
    }
    else if (time_is_digit(*strp))
    {
        /*
         * Day of year.
         */
        rulep->r_type = DAY_OF_YEAR;
        strp = getnum(strp, &rulep->r_day, 0, TIME_DAYSPERLYEAR - 1);
    }
    else
        return NULL; /* invalid format */
    if (strp == NULL)
        return NULL;
    if (*strp == '/')
    {
        /*
         * Time specified.
         */
        ++strp;
        strp = getoffset(strp, &rulep->r_time);
    }
    else
        rulep->r_time = 2 * TIME_SECSPERHOUR; /* default = 2:00:00 */
    return strp;
}

/* Initialize *S to a value based on UTOFF, ISDST, and DESIGIDX.  */
static void init_ttinfo(struct time_ttinfo *s,
                        int32_t utoff,
                        bool isdst,
                        int32_t desigidx)
{
    s->tt_utoff = utoff;
    s->tt_isdst = isdst;
    s->tt_desigidx = desigidx;
    s->tt_ttisstd = false;
    s->tt_ttisut = false;
}

static bool increment_overflow_time(pg_parser_time_t *tp, int32_t j)
{
    /*----------
     * This is like
     * 'if (! (TIME_T_MIN <= *tp + j && *tp + j <= TIME_T_MAX)) ...',
     * except that it does the right thing even if *tp + j would overflow.
     *----------
     */
    if (!(j < 0
              ? (TIME_TYPE_SIGNED(pg_parser_time_t) ? TIME_TIME_T_MIN - j <= *tp : -1 - j < *tp)
              : *tp <= TIME_TIME_T_MAX - j))
        return true;
    *tp += j;
    return false;
}

/*
 * Given a year, a rule, and the offset from UT at the time that rule takes
 * effect, calculate the year-relative time that rule takes effect.
 */

static int32_t transtime(const int32_t year,
                         const struct rule *const rulep,
                         const int32_t offset)
{
    bool leapyear;
    int32_t value;
    int32_t i;
    int32_t d,
        m1,
        yy0,
        yy1,
        yy2,
        dow;

    TIME_INITIALIZE(value);
    leapyear = time_isleap(year);
    switch (rulep->r_type)
    {

    case JULIAN_DAY:

        /*
         * Jn - Julian day, 1 == January 1, 60 == March 1 even in leap
         * years. In non-leap years, or if the day number is 59 or less,
         * just add SECSPERDAY times the day number-1 to the time of
         * January 1, midnight, to get the day.
         */
        value = (rulep->r_day - 1) * TIME_SECSPERDAY;
        if (leapyear && rulep->r_day >= 60)
            value += TIME_SECSPERDAY;
        break;

    case DAY_OF_YEAR:

        /*
         * n - day of year. Just add SECSPERDAY times the day number to
         * the time of January 1, midnight, to get the day.
         */
        value = rulep->r_day * TIME_SECSPERDAY;
        break;

    case MONTH_NTH_DAY_OF_WEEK:

        /*
         * Mm.n.d - nth "dth day" of month m.
         */

        /*
         * Use Zeller's Congruence to get day-of-week of first day of
         * month.
         */
        m1 = (rulep->r_mon + 9) % 12 + 1;
        yy0 = (rulep->r_mon <= 2) ? (year - 1) : year;
        yy1 = yy0 / 100;
        yy2 = yy0 % 100;
        dow = ((26 * m1 - 2) / 10 +
               1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) %
              7;
        if (dow < 0)
            dow += TIME_DAYSPERWEEK;

        /*
         * "dow" is the day-of-week of the first day of the month. Get the
         * day-of-month (zero-origin) of the first "dow" day of the month.
         */
        d = rulep->r_day - dow;
        if (d < 0)
            d += TIME_DAYSPERWEEK;
        for (i = 1; i < rulep->r_week; ++i)
        {
            if (d + TIME_DAYSPERWEEK >=
                mon_lengths[(int32_t)leapyear][rulep->r_mon - 1])
                break;
            d += TIME_DAYSPERWEEK;
        }

        /*
         * "d" is the day-of-month (zero-origin) of the day we want.
         */
        value = d * TIME_SECSPERDAY;
        for (i = 0; i < rulep->r_mon - 1; ++i)
            value += mon_lengths[(int32_t)leapyear][i] * TIME_SECSPERDAY;
        break;
    }

    /*
     * "value" is the year-relative time of 00:00:00 UT on the day in
     * question. To get the year-relative time of the specified local time on
     * that day, add the transition time and the current offset from UT.
     */
    return value + rulep->r_time + offset;
}

/*
 * Variant of above that's specialized to timestamp case.
 *
 * Returns a pointer to the new end of string.  No NUL terminator is put
 * there; callers are responsible for NUL terminating str themselves.
 */
static char *AppendTimestampSeconds(char *cp, struct pg_parser_tm *tm, fsec_t fsec)
{
    return AppendSeconds(cp, tm->tm_sec, fsec, MAX_TIMESTAMP_PRECISION, true);
}

/* EncodeDateTime()
 * Encode date and time interpreted as local time.
 *
 * tm and fsec are the value to encode, print_tz determines whether to include
 * a time zone (the difference between timestamp and timestamptz types), tz is
 * the numeric time zone offset, tzn is the textual time zone, which if
 * specified will be used instead of tz by some styles, style is the date
 * style, str is where to write the output.
 *
 * Supported date styles:
 *    Postgres - day mon hh:mm:ss yyyy tz
 *    SQL - mm/dd/yyyy hh:mm:ss.ss tz
 *    ISO - yyyy-mm-dd hh:mm:ss+/-tz
 *    German - dd.mm.yyyy hh:mm:ss tz
 *    XSD - yyyy-mm-ddThh:mm:ss.ss+/-tz
 */
void EncodeDateTime(struct pg_parser_tm *tm,
                    fsec_t fsec,
                    bool print_tz,
                    int32_t tz,
                    const char *tzn,
                    int32_t style,
                    char *str)
{
    int32_t            day;

    /*
     * Negative tm_isdst means we have no valid time zone translation.
     */
    int32_t DateOrder = DATEORDER_MDY;

    if (tm->tm_isdst < 0)
        print_tz = false;

    switch (style)
    {
        case USE_ISO_DATES:
        case USE_XSD_DATES:
            /* Compatible with ISO-8601 date formats */
            str = numutils_ltostr_zeropad(str,
                                    (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
            *str++ = '-';
            str = numutils_ltostr_zeropad(str, tm->tm_mon, 2);
            *str++ = '-';
            str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
            *str++ = (style == USE_ISO_DATES) ? ' ' : 'T';
            str = numutils_ltostr_zeropad(str, tm->tm_hour, 2);
            *str++ = ':';
            str = numutils_ltostr_zeropad(str, tm->tm_min, 2);
            *str++ = ':';
            str = AppendSeconds(str, tm->tm_sec, fsec, MAX_TIMESTAMP_PRECISION, true);
            if (print_tz)
                str = EncodeTimezone(str, tz, style);
            break;

        case USE_SQL_DATES:
            /* Compatible with Oracle/Ingres date formats */
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
            str = numutils_ltostr_zeropad(str,
                                    (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
            *str++ = ' ';
            str = numutils_ltostr_zeropad(str, tm->tm_hour, 2);
            *str++ = ':';
            str = numutils_ltostr_zeropad(str, tm->tm_min, 2);
            *str++ = ':';
            str = AppendSeconds(str, tm->tm_sec, fsec, MAX_TIMESTAMP_PRECISION, true);

            /*
             * Note: the uses of %.*s in this function would be risky if the
             * timezone names ever contain non-ASCII characters.  However, all
             * TZ abbreviations in the IANA database are plain ASCII.
             */
            if (print_tz)
            {
                if (tzn)
                {
                    sprintf(str, " %.*s", 10, tzn);
                    str += strlen(str);
                }
                else
                    str = EncodeTimezone(str, tz, style);
            }
            break;

        case USE_GERMAN_DATES:
            /* German variant on European style */
            str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
            *str++ = '.';
            str = numutils_ltostr_zeropad(str, tm->tm_mon, 2);
            *str++ = '.';
            str = numutils_ltostr_zeropad(str,
                                    (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
            *str++ = ' ';
            str = numutils_ltostr_zeropad(str, tm->tm_hour, 2);
            *str++ = ':';
            str = numutils_ltostr_zeropad(str, tm->tm_min, 2);
            *str++ = ':';
            str = AppendSeconds(str, tm->tm_sec, fsec, MAX_TIMESTAMP_PRECISION, true);

            if (print_tz)
            {
                if (tzn)
                {
                    sprintf(str, " %.*s", 10, tzn);
                    str += strlen(str);
                }
                else
                    str = EncodeTimezone(str, tz, style);
            }
            break;

        case USE_POSTGRES_DATES:
        default:
            /* Backward-compatible with traditional Postgres abstime dates */
            day = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
            tm->tm_wday = j2day(day);
            rmemcpy1(str, 0, days[tm->tm_wday], 3);
            str += 3;
            *str++ = ' ';
            if (DateOrder == DATEORDER_DMY)
            {
                str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
                *str++ = ' ';
                rmemcpy1(str, 0, months[tm->tm_mon - 1], 3);
                str += 3;
            }
            else
            {
                rmemcpy1(str, 0, months[tm->tm_mon - 1], 3);
                str += 3;
                *str++ = ' ';
                str = numutils_ltostr_zeropad(str, tm->tm_mday, 2);
            }
            *str++ = ' ';
            str = numutils_ltostr_zeropad(str, tm->tm_hour, 2);
            *str++ = ':';
            str = numutils_ltostr_zeropad(str, tm->tm_min, 2);
            *str++ = ':';
            str = AppendTimestampSeconds(str, tm, fsec);
            *str++ = ' ';
            str = numutils_ltostr_zeropad(str,
                                    (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);

            if (print_tz)
            {
                if (tzn)
                {
                    sprintf(str, " %.*s", 10, tzn);
                    str += strlen(str);
                }
                else
                {
                    /*
                     * We have a time zone, but no string version. Use the
                     * numeric form, but be sure to include a leading space to
                     * avoid formatting something which would be rejected by
                     * the date/time parser later. - thomas 2001-10-19
                     */
                    *str++ = ' ';
                    str = EncodeTimezone(str, tz, style);
                }
            }
            break;
    }

    if (tm->tm_year <= 0)
    {
        rmemcpy1(str, 0, " BC", 3);    /* Don't copy NUL */
        str += 3;
    }
    *str = '\0';
}

/* EncodeTimezone()
 *        Copies representation of a numeric timezone offset to str.
 *
 * Returns a pointer to the new end of string.  No NUL terminator is put
 * there; callers are responsible for NUL terminating str themselves.
 */
static char *EncodeTimezone(char *str, int32_t tz, int32_t style)
{
    int32_t            hour,
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
        str = numutils_ltostr_zeropad(str, hour, 2);
        *str++ = ':';
        str = numutils_ltostr_zeropad(str, min, 2);
        *str++ = ':';
        str = numutils_ltostr_zeropad(str, sec, 2);
    }
    else if (min != 0 || style == USE_XSD_DATES)
    {
        str = numutils_ltostr_zeropad(str, hour, 2);
        *str++ = ':';
        str = numutils_ltostr_zeropad(str, min, 2);
    }
    else
        str = numutils_ltostr_zeropad(str, hour, 2);
    return str;
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
static char *AppendSeconds(char *cp,
                           int32_t sec,
                           fsec_t fsec,
                           int32_t precision,
                           bool fillzeros)
{
    if (fillzeros)
        cp = numutils_ltostr_zeropad(cp, Abs(sec), 2);
    else
        cp = numutils_ltostr(cp, Abs(sec));

    /* fsec_t is just an int32_t */
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
            return numutils_ltostr(cp, Abs(fsec));

        return end;
    }
    else
        return cp;
}

/*
 * Calendar time to Julian date conversions.
 * Julian date is commonly used in astronomical applications,
 *    since it is numerically accurate and computationally simple.
 * The algorithms here will accurately convert between Julian day
 *    and calendar date for all non-negative Julian days
 *    (i.e. from Nov 24, -4713 on).
 *
 * Rewritten to eliminate overflow problems. This now allows the
 * routines to work correctly for all Julian day counts from
 * 0 to 2147483647    (Nov 24, -4713 to Jun 3, 5874898) assuming
 * a 32-bit integer. Longer types should also work to the limits
 * of their precision.
 *
 * Actually, date2j() will work sanely, in the sense of producing
 * valid negative Julian dates, significantly before Nov 24, -4713.
 * We rely on it to do so back to Nov 1, -4713; see IS_VALID_JULIAN()
 * and associated commentary in timestamp.h.
 */

static int32_t date2j(int32_t y, int32_t m, int32_t d)
{
    int32_t            julian;
    int32_t            century;

    if (m > 2)
    {
        m += 1;
        y += 4800;
    }
    else
    {
        m += 13;
        y += 4799;
    }

    century = y / 100;
    julian = y * 365 - 32167;
    julian += y / 4 - century + century / 4;
    julian += 7834 * m / 256 + d;

    return julian;
}

/*
 * j2day - convert Julian date to day-of-week (0..6 == Sun..Sat)
 *
 * Note: various places use the locution j2day(date - 1) to produce a
 * result according to the convention 0..6 = Mon..Sun.  This is a bit of
 * a crock, but will work as long as the computation here is just a modulo.
 */
static int32_t j2day(int32_t date)
{
    date += 1;
    date %= 7;
    /* Cope if division truncates towards zero, as it probably does */
    if (date < 0)
        date += 7;

    return date;
}
