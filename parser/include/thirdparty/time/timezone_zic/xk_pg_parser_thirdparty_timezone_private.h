/**
 * @file                xk_pg_parser_thirdparty_timezone_private.h
 * @author              bytesync
 * @brief               定义时区使用一些数据结构和宏函数
 * @version             0.1
 * @date                2023-07-26
 *
 * @copyright           Copyright (c) 2023
 *
 */

#ifndef XK_PG_PARSER_THIRDPARTY_TIMEZONE_PRIVATE_H
#define XK_PG_PARSER_THIRDPARTY_TIMEZONE_PRIVATE_H

#include <limits.h>            /* for CHAR_BIT et al. */
#include <sys/wait.h>          /* for WIFEXITED and WEXITSTATUS */
#include <unistd.h>            /* for F_OK and R_OK */

#include "thirdparty/time/time/xk_pg_parser_thirdparty_timezone_time.h"

/* This string was in the Factory zone through version 2016f.  */
#define XK_TIME_GRANDPARENTED     "Local time zone must be set--see zic manual page"
#define XK_TIME_ENOTSUP           EINVAL

#define XK_TIME_EOVERFLOW         EINVAL

/* Unlike <ctype.h>'s isdigit, this also works if c < 0 | c > UCHAR_MAX. */
#define xk_time_is_digit(c)       ((unsigned)(c) - '0' <= 9)

/*
 * Finally, some convenience items.
 */
#define XK_TIME_TYPE_BIT(type)      (sizeof (type) * CHAR_BIT)
#define XK_TIME_TYPE_SIGNED(type)   (((type) -1) < 0)
#define XK_TIME_TWOS_COMPLEMENT(t)  ((t) ~ (t) 0 < 0)
#define XK_TIME_MAXVAL(t, b)                                      \
  ((t) (((t) 1 << ((b) - 1 - XK_TIME_TYPE_SIGNED(t)))             \
    - 1 + ((t) 1 << ((b) - 1 - XK_TIME_TYPE_SIGNED(t)))))
#define XK_TIME_MINVAL(t, b)                        \
  ((t) (XK_TIME_TYPE_SIGNED(t) ? - XK_TIME_TWOS_COMPLEMENT(t) - XK_TIME_MAXVAL(t, b) : 0))

/* The extreme time values, assuming no padding.  */
#define XK_TIME_TIME_T_MIN        XK_TIME_MINVAL(xk_pg_parser_time_t, XK_TIME_TYPE_BIT(xk_pg_parser_time_t))
#define XK_TIME_TIME_T_MAX        XK_TIME_MAXVAL(xk_pg_parser_time_t, XK_TIME_TYPE_BIT(xk_pg_parser_time_t))
#define XK_TIME_INITIALIZE(x)     ((x) = 0)

#define XK_TIME_YEARSPERREPEAT        400     /* years before a Gregorian repeat */
#define XK_TIME_SECSPERMIN            60
#define XK_TIME_MINSPERHOUR           60
#define XK_TIME_HOURSPERDAY           24
#define XK_TIME_DAYSPERWEEK           7
#define XK_TIME_DAYSPERNYEAR          365
#define XK_TIME_DAYSPERLYEAR          366
#define XK_TIME_SECSPERHOUR           (XK_TIME_SECSPERMIN * XK_TIME_MINSPERHOUR)
#define XK_TIME_SECSPERDAY            ((int32_t) XK_TIME_SECSPERHOUR * XK_TIME_HOURSPERDAY)
#define XK_TIME_MONSPERYEAR           12

#define XK_TIME_TM_SUNDAY             0
#define XK_TIME_TM_MONDAY             1
#define XK_TIME_TM_TUESDAY            2
#define XK_TIME_TM_WEDNESDAY          3
#define XK_TIME_TM_THURSDAY           4
#define XK_TIME_TM_FRIDAY             5
#define XK_TIME_TM_SATURDAY           6

#define XK_TIME_TM_JANUARY            0
#define XK_TIME_TM_FEBRUARY           1
#define XK_TIME_TM_MARCH              2
#define XK_TIME_TM_APRIL              3
#define XK_TIME_TM_MAY                4
#define XK_TIME_TM_JUNE               5
#define XK_TIME_TM_JULY               6
#define XK_TIME_TM_AUGUST             7
#define XK_TIME_TM_SEPTEMBER          8
#define XK_TIME_TM_OCTOBER            9
#define XK_TIME_TM_NOVEMBER           10
#define XK_TIME_TM_DECEMBER           11

#define XK_TIME_TM_YEAR_BASE          1900

#define XK_TIME_EPOCH_YEAR            1970
#define XK_TIME_EPOCH_WDAY            XK_TIME_TM_THURSDAY

#define xk_time_isleap(y)             (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))
#define xk_time_isleap_sum(a, b)      xk_time_isleap((a) % 400 + (b) % 400)

#define XK_TIME_AVGSECSPERYEAR        31556952L
#define XK_TIME_SECSPERREPEAT \
  ((int64_t) XK_TIME_YEARSPERREPEAT * (int64_t) XK_TIME_AVGSECSPERYEAR)
#define XK_TIME_SECSPERREPEAT_BITS    34    /* ceil(log2(XK_TIME_SECSPERREPEAT)) */

#endif                            /* XK_PG_PARSER_THIRDPARTY_TIMEZONE_PRIVATE_H */
