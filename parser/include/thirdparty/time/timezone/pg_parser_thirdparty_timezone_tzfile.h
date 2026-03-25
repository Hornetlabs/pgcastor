/**
 * @file                pg_parser_thirdparty_timezone_tzfile.h
 * @author              ByteSynch
 * @brief               Define data structures for timezone information output by zic functions
 * @version             0.1
 * @date                2023-09-18
 *
 * @copyright           Copyright (c) 2023
 *
 */

#ifndef PG_PARSER_THIRDPARTY_TIMEZONE_TZFILE_H
#define PG_PARSER_THIRDPARTY_TIMEZONE_TZFILE_H

/*
 * Information about time zone files.
 */

#define TIME_TZDEFAULT "/etc/localtime"
#define TIME_TZDEFRULES "posixrules"

/*
 * Each file begins with. . .
 */

#define TIME_TZ_MAGIC "TZif"

struct time_tzhead
{
    char tzh_magic[4];      /* TIME_TZ_MAGIC */
    char tzh_version[1];    /* '\0' or '2' or '3' as of 2013 */
    char tzh_reserved[15];  /* reserved; must be zero */
    char tzh_ttisutcnt[4];  /* coded number of trans. time flags */
    char tzh_ttisstdcnt[4]; /* coded number of trans. time flags */
    char tzh_leapcnt[4];    /* coded number of leap seconds */
    char tzh_timecnt[4];    /* coded number of transition times */
    char tzh_typecnt[4];    /* coded number of local time types */
    char tzh_charcnt[4];    /* coded number of abbr. chars */
};

#define TIME_TZ_MAX_TIMES 2000

/* This must be at least 17 for Europe/Samara and Europe/Vilnius.  */
#define TIME_TZ_MAX_TYPES 256 /* Limited by what (unsigned char)'s can hold */

#define TIME_TZ_MAX_CHARS 50 /* Maximum number of abbreviation characters */
                             /* (limited by what unsigned chars can hold) */

#define TIME_TZ_MAX_LEAPS 50 /* Maximum number of leap second corrections */

#endif /* PG_PARSER_THIRDPARTY_TIMEZONE_TZFILE_H */
