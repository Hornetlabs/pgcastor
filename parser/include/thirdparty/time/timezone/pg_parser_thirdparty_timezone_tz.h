/**
 * @file                xk_pg_parser_thirdparty_timezone_tz.h
 * @author              bytesync
 * @brief               定义时区使用的数据类型
 * @version             0.1
 * @date                2023-07-26
 *
 * @copyright           Copyright (c) 2023
 *
 */

#ifndef XK_PG_PARSER_THIRDPARTY_TIMEZONE_TZ
#define XK_PG_PARSER_THIRDPARTY_TIMEZONE_TZ

#include "thirdparty/time/time/xk_pg_parser_thirdparty_timezone_time.h"
#include "thirdparty/time/timezone/xk_pg_parser_thirdparty_timezone_tzfile.h"

#define XK_TIME_SMALLEST(a, b)          (((a) < (b)) ? (a) : (b))
#define XK_TIME_BIGGEST(a, b)           (((a) > (b)) ? (a) : (b))

struct xk_time_ttinfo
{                                       /* time type information */
    int32_t             tt_utoff;       /* UT offset in seconds */
    bool                tt_isdst;       /* used to set tm_isdst */
    int32_t             tt_desigidx;    /* abbreviation list index */
    bool                tt_ttisstd;     /* transition is std time */
    bool                tt_ttisut;      /* transition is UT */
};

struct xk_time_lsinfo
{                                               /* leap second information */
    xk_pg_parser_time_t      ls_trans;       /* transition time */
    int64_t                     ls_corr;        /* correction to apply */
};

struct xk_pg_parser_time_state
{
    int32_t                     leapcnt;
    int32_t                     timecnt;
    int32_t                     typecnt;
    int32_t                     charcnt;
    bool                        goback;
    bool                        goahead;
    xk_pg_parser_time_t      ats[XK_TIME_TZ_MAX_TIMES];
    unsigned char               types[XK_TIME_TZ_MAX_TIMES];
    struct xk_time_ttinfo       ttis[XK_TIME_TZ_MAX_TYPES];
    char                        chars[XK_TIME_BIGGEST(XK_TIME_BIGGEST(XK_TIME_TZ_MAX_CHARS + 1, 4),
                                                                      (2 * (XK_TIME_TZ_STRLEN_MAX + 1)))];
    struct xk_time_lsinfo       lsis[XK_TIME_TZ_MAX_LEAPS];

    /*
     * The time type to use for early times or if no transitions. It is always
     * zero for recent tzdb releases. It might be nonzero for data from tzdb
     * 2018e or earlier.
     */
    int32_t                     defaulttype;
};


struct xk_pg_parser_tz
{
    /* TZname contains the canonically-cased name of the timezone */
    char                                TZname[XK_TIME_TZ_STRLEN_MAX + 1];
    struct xk_pg_parser_time_state   state;
};

#endif /* XK_PG_PARSER_THIRDPARTY_TIMEZONE_TZ */
