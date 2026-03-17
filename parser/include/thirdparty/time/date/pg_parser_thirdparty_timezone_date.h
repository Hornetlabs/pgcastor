/**
 * @file                pg_parser_thirdparty_timezone_date.h
 * @author              ByteSynch
 * @brief               定义 Date 类型使用的数据结构以及
 * @version 0.1
 * @date 2023-09-18
 *
 * @copyright Copyright (c) 2023
 *
 */

#ifndef PG_PARSER_THIRDPARTY_TIMEZONE_DATE_H
#define PG_PARSER_THIRDPARTY_TIMEZONE_DATE_H

#define MAXDATELEN                      128
#define PG_PARSER_INT32_MIN       (-0x7FFFFFFF-1)
#define PG_PARSER_INT32_MAX       (0x7FFFFFFF)

#define MONTHS_PER_YEAR                 12
#define EARLY                           "-infinity"
#define LATE                            "infinity"
#define INVALID                         "invalid"

#define USE_POSTGRES_DATES              0
#define USE_ISO_DATES                   1
#define USE_SQL_DATES                   2
#define USE_GERMAN_DATES                3
#define USE_XSD_DATES                   4

/* valid DateOrder values */
#define DATEORDER_YMD                   0
#define DATEORDER_DMY                   1
#define DATEORDER_MDY                   2

#define MONTH                           1
#define YEAR                            2
#define DAY                             3
#define HOUR                            10
#define MINUTE                          11
#define SECOND                          12

#define MAX_TIME_PRECISION              6

typedef int32_t                         DateADT;
typedef int64_t                         TimeADT;

typedef struct
{
    TimeADT                 time;       /* all time units other than months and years */
    int32_t                 zone;       /* numeric time zone, in seconds */
} TimeTzADT;

/*
 * Infinity and minus infinity must be the max and min values of DateADT.
 */
#define DATEVAL_NOBEGIN                 ((DateADT) PG_PARSER_INT32_MIN)
#define DATEVAL_NOEND                   ((DateADT) PG_PARSER_INT32_MAX)

#define DATE_NOBEGIN(j)                 ((j) = DATEVAL_NOBEGIN)
#define DATE_IS_NOBEGIN(j)              ((j) == DATEVAL_NOBEGIN)
#define DATE_NOEND(j)                   ((j) = DATEVAL_NOEND)
#define DATE_IS_NOEND(j)                ((j) == DATEVAL_NOEND)
#define DATE_NOT_FINITE(j)              (DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j))

#define PG_PARSER_EPOCH_JDATE     2451545 /* == date2j(2000, 1, 1) */

#endif /* PG_PARSER_THIRDPARTY_TIMEZONE_DATE_H */
