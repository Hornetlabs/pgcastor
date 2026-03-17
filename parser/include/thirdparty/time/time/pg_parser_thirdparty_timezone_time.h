/**
 * @file                pg_parser_thirdparty_timezone_time.h
 * @author              bytesync
 * @brief               定义 time/time with timezone 类型使用的数据结构
 * @version 0.1
 * @date 2023-07-26
 *
 * @copyright Copyright (c) 2023
 *
 */
#ifndef PG_PARSER_THIRDPARTY_TIMEZONE_TIME_H
#define PG_PARSER_THIRDPARTY_TIMEZONE_TIME_H

typedef int64_t pg_parser_time_t;

struct pg_parser_tm
{
    int32_t                     tm_sec;
    int32_t                     tm_min;
    int32_t                     tm_hour;
    int32_t                     tm_mday;
    int32_t                     tm_mon;         /* origin 1, not 0! */
    int32_t                     tm_year;        /* relative to 1900 */
    int32_t                     tm_wday;
    int32_t                     tm_yday;
    int32_t                     tm_isdst;
    int64_t                     tm_gmtoff;
    const char*                 tm_zone;
};

typedef struct pg_parser_tz pg_parser_tz;

/* Maximum length of a timezone name (not including trailing null) */
#define TIME_TZ_STRLEN_MAX   255

#endif                            /* PG_PARSER_THIRDPARTY_TIMEZONE_TIME_H */
