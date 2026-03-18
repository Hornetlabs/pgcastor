/**
 * @file                pg_parser_thirdparty_timezone_private.h
 * @author              bytesync
 * @brief               Define some data structures and macro functions used by timezone
 * @version             0.1
 * @date                2023-07-26
 *
 * @copyright           Copyright (c) 2023
 *
 */

#ifndef PG_PARSER_THIRDPARTY_TIMEZONE_PRIVATE_H
#define PG_PARSER_THIRDPARTY_TIMEZONE_PRIVATE_H

#include <limits.h>   /* for CHAR_BIT et al. */
#include <sys/wait.h> /* for WIFEXITED and WEXITSTATUS */
#include <unistd.h>   /* for F_OK and R_OK */

#include "thirdparty/time/time/pg_parser_thirdparty_timezone_time.h"

/* This string was in the Factory zone through version 2016f.  */
#define TIME_GRANDPARENTED "Local time zone must be set--see zic manual page"
#define TIME_ENOTSUP       EINVAL

#define TIME_EOVERFLOW     EINVAL

/* Unlike <ctype.h>'s isdigit, this also works if c < 0 | c > UCHAR_MAX. */
#define time_is_digit(c) ((unsigned)(c) - '0' <= 9)

/*
 * Finally, some convenience items.
 */
#define TIME_TYPE_BIT(type)     (sizeof(type) * CHAR_BIT)
#define TIME_TYPE_SIGNED(type)  (((type) - 1) < 0)
#define TIME_TWOS_COMPLEMENT(t) ((t) ~(t)0 < 0)
#define TIME_MAXVAL(t, b) \
    ((t)(((t)1 << ((b) - 1 - TIME_TYPE_SIGNED(t))) - 1 + ((t)1 << ((b) - 1 - TIME_TYPE_SIGNED(t)))))
#define TIME_MINVAL(t, b) ((t)(TIME_TYPE_SIGNED(t) ? -TIME_TWOS_COMPLEMENT(t) - TIME_MAXVAL(t, b) : 0))

/* The extreme time values, assuming no padding.  */
#define TIME_TIME_T_MIN         TIME_MINVAL(pg_parser_time_t, TIME_TYPE_BIT(pg_parser_time_t))
#define TIME_TIME_T_MAX         TIME_MAXVAL(pg_parser_time_t, TIME_TYPE_BIT(pg_parser_time_t))
#define TIME_INITIALIZE(x)      ((x) = 0)

#define TIME_YEARSPERREPEAT     400 /* years before a Gregorian repeat */
#define TIME_SECSPERMIN         60
#define TIME_MINSPERHOUR        60
#define TIME_HOURSPERDAY        24
#define TIME_DAYSPERWEEK        7
#define TIME_DAYSPERNYEAR       365
#define TIME_DAYSPERLYEAR       366
#define TIME_SECSPERHOUR        (TIME_SECSPERMIN * TIME_MINSPERHOUR)
#define TIME_SECSPERDAY         ((int32_t)TIME_SECSPERHOUR * TIME_HOURSPERDAY)
#define TIME_MONSPERYEAR        12

#define TIME_TM_SUNDAY          0
#define TIME_TM_MONDAY          1
#define TIME_TM_TUESDAY         2
#define TIME_TM_WEDNESDAY       3
#define TIME_TM_THURSDAY        4
#define TIME_TM_FRIDAY          5
#define TIME_TM_SATURDAY        6

#define TIME_TM_JANUARY         0
#define TIME_TM_FEBRUARY        1
#define TIME_TM_MARCH           2
#define TIME_TM_APRIL           3
#define TIME_TM_MAY             4
#define TIME_TM_JUNE            5
#define TIME_TM_JULY            6
#define TIME_TM_AUGUST          7
#define TIME_TM_SEPTEMBER       8
#define TIME_TM_OCTOBER         9
#define TIME_TM_NOVEMBER        10
#define TIME_TM_DECEMBER        11

#define TIME_TM_YEAR_BASE       1900

#define TIME_EPOCH_YEAR         1970
#define TIME_EPOCH_WDAY         TIME_TM_THURSDAY

#define time_isleap(y)          (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))
#define time_isleap_sum(a, b)   time_isleap((a) % 400 + (b) % 400)

#define TIME_AVGSECSPERYEAR     31556952L
#define TIME_SECSPERREPEAT      ((int64_t)TIME_YEARSPERREPEAT * (int64_t)TIME_AVGSECSPERYEAR)
#define TIME_SECSPERREPEAT_BITS 34 /* ceil(log2(TIME_SECSPERREPEAT)) */

#endif /* PG_PARSER_THIRDPARTY_TIMEZONE_PRIVATE_H */
