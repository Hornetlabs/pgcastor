/**
 * @file                pg_parser_thirdparty_timezone_zic.c
 * @author              bytesync
 * @brief               Implementation of zic function
 * @version             0.1
 * @date                2023-07-26
 *
 * @copyright           Copyright (c) 2023
 *
 */

/* include */
#include "pg_parser_os_incl.h"
#include "pg_parser_app_incl.h"
#include "mcxt/pg_parser_mcxt.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_tzdata_info.h"
#include "thirdparty/time/timezone_zic/pg_parser_thirdparty_timezone_private.h"
#include "thirdparty/time/timezone/pg_parser_thirdparty_timezone_tzfile.h"

#define ZIC_MCXT NULL

/* typedef begin*/
typedef int64_t zic_t;
typedef int32_t lineno_t;

/* define */

#if __WORDSIZE == 64
#define TIME_INT64CONST(x)  ((int64_t)(x))
#define TIME_UINT64CONST(x) ((uint64_t)(x))
#else
#define TIME_INT64CONST(x)  ((int64)x##LL)
#define TIME_UINT64CONST(x) ((uint64)x##ULL)
#endif

#define TIME_INT32_MAX (0x7FFFFFFF)

#define TIME_INT32_MIN (-0x7FFFFFFF - 1)
#define TIME_INT32_MAX (0x7FFFFFFF)

#define TIME_INT64_MIN (-TIME_INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define TIME_INT64_MAX TIME_INT64CONST(0x7FFFFFFFFFFFFFFF)

#define ZIC_MIN        TIME_INT64_MIN
#define ZIC_MAX        TIME_INT64_MAX
/*
 * Which fields are which on a Zone continuation line.
 */
#define ZFC_STDOFF      0
#define ZFC_RULE        1
#define ZFC_FORMAT      2
#define ZFC_TILYEAR     3
#define ZFC_TILMONTH    4
#define ZFC_TILDAY      5
#define ZFC_TILTIME     6
#define ZONEC_MINFIELDS 3
#define ZONEC_MAXFIELDS 7

/*
 * Which fields are which on a Zone line.
 */

#define ZF_NAME        1
#define ZF_STDOFF      2
#define ZF_RULE        3
#define ZF_FORMAT      4
#define ZF_TILYEAR     5
#define ZF_TILMONTH    6
#define ZF_TILDAY      7
#define ZF_TILTIME     8
#define ZONE_MINFIELDS 5
#define ZONE_MAXFIELDS 9

/*
 * Year synonyms.
 */

#define YR_MINIMUM 0
#define YR_MAXIMUM 1
#define YR_ONLY    2

/*
 * Line codes.
 */

#define LC_RULE    0
#define LC_ZONE    1
#define LC_LINK    2
#define LC_LEAP    3
#define LC_EXPIRES 4

/*
 * Which files are which on a Rule line.
 */

#define RF_NAME     1
#define RF_LOYEAR   2
#define RF_HIYEAR   3
#define RF_COMMAND  4
#define RF_MONTH    5
#define RF_DAY      6
#define RF_TOD      7
#define RF_SAVE     8
#define RF_ABBRVAR  9
#define RULE_FIELDS 10

/*
 * Which fields are which on a Link line.
 */

#define LF_TARGET   1
#define LF_LINKNAME 2
#define LINK_FIELDS 3

/*
 *    r_dycode        r_dayofmonth    r_wday
 */

#define DC_DOM               0 /* 1..31 */ /* unused */
#define DC_DOWGEQ            1 /* 1..31 */ /* 0..6 (Sun..Sat) */
#define DC_DOWLEQ            2 /* 1..31 */ /* 0..6 (Sun..Sat) */

#define TIME_T_BITS_IN_FILE  64

#define ZIC_VERSION_PRE_2013 '2'
#define ZIC_VERSION          '3'

#ifndef ZIC_MAX_ABBR_LEN_WO_WARN
#define ZIC_MAX_ABBR_LEN_WO_WARN 6
#endif /* !defined ZIC_MAX_ABBR_LEN_WO_WARN */

/* The minimum and maximum values representable in a TZif file.  */
static zic_t const min_time = TIME_MINVAL(zic_t, TIME_T_BITS_IN_FILE);
static zic_t const max_time = TIME_MAXVAL(zic_t, TIME_T_BITS_IN_FILE);

/* The minimum, and one less than the maximum, values specified by
   the -r option.  These default to MIN_TIME and MAX_TIME.  */
static zic_t       lo_time = TIME_MINVAL(zic_t, TIME_T_BITS_IN_FILE);
static zic_t       hi_time = TIME_MAXVAL(zic_t, TIME_T_BITS_IN_FILE);

/* enum, struct, union */
typedef enum zic_percent_z_len_bound
{
    PERCENT_Z_LEN_BOUND = sizeof "+995959" - 1
} ZIC_PERCENT_Z_LEN_BOUND;

struct lookup
{
    const char*   l_word;
    const int32_t l_value;
};

struct rule
{
    const char* r_filename;
    lineno_t    r_linenum;
    const char* r_name;
    zic_t       r_loyear; /* for example, 1986 */
    zic_t       r_hiyear; /* for example, 1986 */
    bool        r_lowasnum;
    bool        r_hiwasnum;
    int32_t     r_month;  /* 0..11 */
    int32_t     r_dycode; /* see below */
    int32_t     r_dayofmonth;
    int32_t     r_wday;
    zic_t       r_tod;      /* time from midnight */
    bool        r_todisstd; /* is r_tod standard time? */
    bool        r_todisut;  /* is r_tod UT? */
    bool        r_isdst;    /* is this daylight saving time? */
    zic_t       r_save;     /* offset from standard time */
    const char* r_abbrvar;  /* variable part of abbreviation */
    bool        r_todo;     /* a rule to do (used in outzone) */
    zic_t       r_temp;     /* used in outzone */
};

struct zone
{
    const char*  z_filename;
    lineno_t     z_linenum;
    const char*  z_name;
    zic_t        z_stdoff;
    char*        z_rule;
    const char*  z_format;
    char         z_format_specifier;
    bool         z_isdst;
    zic_t        z_save;
    struct rule* z_rules;
    ptrdiff_t    z_nrules;
    struct rule  z_untilrule;
    zic_t        z_untiltime;
};

struct link
{
    const char* l_filename;
    lineno_t    l_linenum;
    const char* l_target;
    const char* l_linkname;
};

struct attype
{
    zic_t         at;
    bool          dontmerge;
    unsigned char type;
};

/* For multithreading, put all global variables into a structure. */
struct zicinfo
{
    int32_t        charcnt;
    const char*    filename;
    int32_t        leapcnt;
    bool           leapseen;
    zic_t          leapminyear;
    zic_t          leapmaxyear;
    lineno_t       linenum;
    int32_t        max_abbrvar_len;
    int32_t        max_format_len;
    zic_t          max_year;
    zic_t          min_year;
    bool           print_abbrevs;
    zic_t          print_cutoff;
    const char*    rfilename;
    lineno_t       rlinenum;
    ptrdiff_t      timecnt;
    ptrdiff_t      timecnt_alloc;
    int32_t        typecnt;
    struct rule*   rules;
    ptrdiff_t      nrules; /* number of rules */
    ptrdiff_t      nrules_alloc;
    struct zone*   zones;
    ptrdiff_t      nzones; /* number of zones */
    ptrdiff_t      nzones_alloc;
    struct link*   links;
    ptrdiff_t      nlinks;
    ptrdiff_t      nlinks_alloc;
    zic_t          utoffs[TIME_TZ_MAX_TYPES];
    char           isdsts[TIME_TZ_MAX_TYPES];
    unsigned char  desigidx[TIME_TZ_MAX_TYPES];
    bool           ttisstds[TIME_TZ_MAX_TYPES];
    bool           ttisuts[TIME_TZ_MAX_TYPES];
    char           chars[TIME_TZ_MAX_CHARS];
    zic_t          trans[TIME_TZ_MAX_LEAPS];
    zic_t          corr[TIME_TZ_MAX_LEAPS];
    char           roll[TIME_TZ_MAX_LEAPS];
    struct attype* attypes;
    int32_t        bloat;
};

struct timerange
{
    int32_t   defaulttype;
    ptrdiff_t base, count;
    int32_t   leapbase, leapcount;
};

static struct lookup const lasts[] = {
    {"last-Sunday",    TIME_TM_SUNDAY   },
    {"last-Monday",    TIME_TM_MONDAY   },
    {"last-Tuesday",   TIME_TM_TUESDAY  },
    {"last-Wednesday", TIME_TM_WEDNESDAY},
    {"last-Thursday",  TIME_TM_THURSDAY },
    {"last-Friday",    TIME_TM_FRIDAY   },
    {"last-Saturday",  TIME_TM_SATURDAY },
    {NULL,             0                }
};

static struct lookup const wday_names[] = {
    {"Sunday",    TIME_TM_SUNDAY   },
    {"Monday",    TIME_TM_MONDAY   },
    {"Tuesday",   TIME_TM_TUESDAY  },
    {"Wednesday", TIME_TM_WEDNESDAY},
    {"Thursday",  TIME_TM_THURSDAY },
    {"Friday",    TIME_TM_FRIDAY   },
    {"Saturday",  TIME_TM_SATURDAY },
    {NULL,        0                }
};

static struct lookup const mon_names[] = {
    {"January",   TIME_TM_JANUARY  },
    {"February",  TIME_TM_FEBRUARY },
    {"March",     TIME_TM_MARCH    },
    {"April",     TIME_TM_APRIL    },
    {"May",       TIME_TM_MAY      },
    {"June",      TIME_TM_JUNE     },
    {"July",      TIME_TM_JULY     },
    {"August",    TIME_TM_AUGUST   },
    {"September", TIME_TM_SEPTEMBER},
    {"October",   TIME_TM_OCTOBER  },
    {"November",  TIME_TM_NOVEMBER },
    {"December",  TIME_TM_DECEMBER },
    {NULL,        0                }
};

static struct lookup const begin_years[] = {
    {"minimum", YR_MINIMUM},
    {"maximum", YR_MAXIMUM},
    {NULL,      0         }
};

static struct lookup const end_years[] = {
    {"minimum", YR_MINIMUM},
    {"maximum", YR_MAXIMUM},
    {"only",    YR_ONLY   },
    {NULL,      0         }
};

static const int32_t len_months[2][TIME_MONSPERYEAR] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

static const int32_t       len_years[2] = {TIME_DAYSPERNYEAR, TIME_DAYSPERLYEAR};

static struct lookup const zi_line_codes[] = {
    {"Rule", LC_RULE},
    {"Zone", LC_ZONE},
    {"Link", LC_LINK},
    {NULL,   0      }
};

/* If true, work around a bug in Qt 5.6.1 and earlier, which mishandles
   TZif files whose POSIX-TZ-style strings contain '<'; see
   QTBUG-53071 <https://bugreports.qt.io/browse/QTBUG-53071>.  This
   workaround will no longer be needed when Qt 5.6.1 and earlier are
   obsolete, say in the year 2021.  */
#ifndef WORK_AROUND_QTBUG_53071
enum
{
    WORK_AROUND_QTBUG_53071 = true
};
#endif

/* static function statement */
static char** getfields(char* cp);
static void infile(const char* name, struct zicinfo* local_zicinfo);
static void memory_exhausted(const char* msg);
static size_t size_product(size_t nitems, size_t itemsize);
static bool is_space(char a);
static bool inzcont(char** fields, int32_t nfields, struct zicinfo* local_zicinfo);
static bool inzsub(char** fields, int32_t nfields, bool iscont, struct zicinfo* local_zicinfo);
static bool namecheck(const char* name);
static bool componentcheck(char const* name, char const* component, char const* component_end);
static char* ecpyalloc(char const* str);
static void* memcheck(void* ptr);
static zic_t gethms(char const* string, char const* errstring);
static zic_t oadd(zic_t t1, zic_t t2);
static void time_overflow(void);
static void rulesub(struct rule* rp,
                    const char*  loyearp,
                    const char*  hiyearp,
                    const char*  typep,
                    const char*  monthp,
                    const char*  dayp,
                    const char*  timep);
static const struct lookup* byword(const char* word, const struct lookup* table);
static bool ciprefix(char const* abbr, char const* word);
static char lowerit(char a);
static bool ciequal(const char* ap, const char* bp);
static zic_t rpytime(const struct rule* rp, zic_t wantedy);
static zic_t tadd(zic_t t1, zic_t t2);
static void* growalloc(void* ptr, size_t itemsize, ptrdiff_t nitems, ptrdiff_t* nitems_alloc);
static void* erealloc(void* ptr, size_t size);
static void inrule(char** fields, int32_t nfields, struct zicinfo* local_zicinfo);
static zic_t getsave(char*, bool*);
static bool inzone(char** fields, int32_t nfields, struct zicinfo* local_zicinfo);
static void inlink(char** fields, int32_t nfields, struct zicinfo* local_zicinfo);
static void associate(struct zicinfo* local_zicinfo);
static int32_t rcomp(const void* cp1, const void* cp2);
static void eat(char const* name, lineno_t num, struct zicinfo* local_zicinfo);
static void eats(char const* name, lineno_t num, char const* rname, lineno_t rnum, struct zicinfo* local_zicinfo);
static void outzone(const struct zone*   zpfirst,
                    ptrdiff_t            zonecount,
                    pg_parser_StringInfo local_tzdata,
                    struct zicinfo*      local_zicinfo);
static void* emalloc(size_t size);
static void updateminmax(const zic_t x, struct zicinfo* local_zicinfo);
static bool want_bloat(struct zicinfo* local_zicinfo);
static size_t doabbr(char* abbr, struct zone const* zp, char const* letters, bool isdst, zic_t save, bool doquotes);
static char const* abbroffset(char* buf, zic_t offset);
static bool is_alpha(char a);
static int32_t addtype(zic_t, char const*, bool, bool, bool, struct zicinfo*);
static void newabbr(const char* string, struct zicinfo* local_zicinfo);
static void addtt(zic_t starttime, int32_t type, struct zicinfo* local_zicinfo);
static void writezone(const char* const    string,
                      char                 version,
                      int32_t              defaulttype,
                      pg_parser_StringInfo local_tzdata,
                      struct zicinfo*      local_zicinfo);
static int32_t atcomp(const void* avp, const void* bvp);
static struct timerange limitrange(struct timerange     r,
                                   zic_t                lo,
                                   zic_t                hi,
                                   zic_t const*         ats,
                                   unsigned char const* types,
                                   struct zicinfo*      local_zicinfo);
static void convert(const int32_t val, char* const buf);
static void convert64(const zic_t val, char* const buf);
static void loop_append_char(int32_t loop, char* arr, pg_parser_StringInfo local_tzdata);
static void puttzcode(const int32_t val, pg_parser_StringInfo local_tzdata);
static void puttzcodepass(zic_t val, int32_t pass, pg_parser_StringInfo local_tzdata);
static void free_data(struct zicinfo* local_zicinfo);
static int32_t stringzone(char* result, struct zone const* zpfirst, ptrdiff_t zonecount);
static int32_t rule_cmp(struct rule const* a, struct rule const* b);
static int32_t stringoffset(char* result, zic_t offset);
static int32_t stringrule(char* result, struct rule* const rp, zic_t save, zic_t stdoff);
static void local_qsort(void* a, size_t n, size_t es, int (*cmp)(const void*, const void*));

/* static functions */

static int32_t stringrule(char* result, struct rule* const rp, zic_t save, zic_t stdoff)
{
    zic_t   tod = rp->r_tod;
    int32_t compat = 0;

    if (rp->r_dycode == DC_DOM)
    {
        int32_t month, total;

        if (rp->r_dayofmonth == 29 && rp->r_month == TIME_TM_FEBRUARY)
        {
            return -1;
        }
        total = 0;
        for (month = 0; month < rp->r_month; ++month)
        {
            total += len_months[0][month];
        }
        /* Omit the "J" in Jan and Feb, as that's shorter.  */
        if (rp->r_month <= 1)
        {
            result += sprintf(result, "%d", total + rp->r_dayofmonth - 1);
        }
        else
        {
            result += sprintf(result, "J%d", total + rp->r_dayofmonth);
        }
    }
    else
    {
        int32_t week;
        int32_t wday = rp->r_wday;
        int32_t wdayoff;

        if (rp->r_dycode == DC_DOWGEQ)
        {
            wdayoff = (rp->r_dayofmonth - 1) % TIME_DAYSPERWEEK;
            if (wdayoff)
            {
                compat = 2013;
            }
            wday -= wdayoff;
            tod += wdayoff * TIME_SECSPERDAY;
            week = 1 + (rp->r_dayofmonth - 1) / TIME_DAYSPERWEEK;
        }
        else if (rp->r_dycode == DC_DOWLEQ)
        {
            if (rp->r_dayofmonth == len_months[1][rp->r_month])
            {
                week = 5;
            }
            else
            {
                wdayoff = rp->r_dayofmonth % TIME_DAYSPERWEEK;
                if (wdayoff)
                {
                    compat = 2013;
                }
                wday -= wdayoff;
                tod += wdayoff * TIME_SECSPERDAY;
                week = rp->r_dayofmonth / TIME_DAYSPERWEEK;
            }
        }
        else
        {
            return -1; /* "cannot happen" */
        }
        if (wday < 0)
        {
            wday += TIME_DAYSPERWEEK;
        }
        result += sprintf(result, "M%d.%d.%d", rp->r_month + 1, week, wday);
    }
    if (rp->r_todisut)
    {
        tod += stdoff;
    }
    if (rp->r_todisstd && !rp->r_isdst)
    {
        tod += save;
    }
    if (tod != 2 * TIME_SECSPERMIN * TIME_MINSPERHOUR)
    {
        *result++ = '/';
        if (!stringoffset(result, tod))
        {
            return -1;
        }
        if (tod < 0)
        {
            if (compat < 2013)
            {
                compat = 2013;
            }
        }
        else if (TIME_SECSPERDAY <= tod)
        {
            if (compat < 1994)
            {
                compat = 1994;
            }
        }
    }
    return compat;
}

static int32_t stringoffset(char* result, zic_t offset)
{
    int32_t hours;
    int32_t minutes;
    int32_t seconds;
    bool    negative = offset < 0;
    int32_t len = negative;

    if (negative)
    {
        offset = -offset;
        result[0] = '-';
    }
    seconds = offset % TIME_SECSPERMIN;
    offset /= TIME_SECSPERMIN;
    minutes = offset % TIME_MINSPERHOUR;
    offset /= TIME_MINSPERHOUR;
    hours = offset;
    if (hours >= TIME_HOURSPERDAY * TIME_DAYSPERWEEK)
    {
        result[0] = '\0';
        return 0;
    }
    len += sprintf(result + len, "%d", hours);
    if (minutes != 0 || seconds != 0)
    {
        len += sprintf(result + len, ":%02d", minutes);
        if (seconds != 0)
        {
            len += sprintf(result + len, ":%02d", seconds);
        }
    }
    return len;
}

static int32_t rule_cmp(struct rule const* a, struct rule const* b)
{
    if (!a)
    {
        return -!!b;
    }
    if (!b)
    {
        return 1;
    }
    if (a->r_hiyear != b->r_hiyear)
    {
        return a->r_hiyear < b->r_hiyear ? -1 : 1;
    }
    if (a->r_month - b->r_month != 0)
    {
        return a->r_month - b->r_month;
    }
    return a->r_dayofmonth - b->r_dayofmonth;
}

static int32_t stringzone(char* result, struct zone const* zpfirst, ptrdiff_t zonecount)
{
    const struct zone* zp = NULL;
    struct rule*       rp = NULL;
    struct rule*       stdrp = NULL;
    struct rule*       dstrp = NULL;
    ptrdiff_t          i;
    const char*        abbrvar = NULL;
    int32_t            compat = 0;
    int32_t            c;
    size_t             len;
    int32_t            offsetlen;
    struct rule        stdr, dstr;

    result[0] = '\0';

    /*
     * Internet RFC 8536 section 5.1 says to use an empty TZ string if future
     * timestamps are truncated.
     */
    if (hi_time < max_time)
    {
        return -1;
    }

    zp = zpfirst + zonecount - 1;
    stdrp = dstrp = NULL;
    for (i = 0; i < zp->z_nrules; ++i)
    {
        rp = &zp->z_rules[i];
        if (rp->r_hiwasnum || rp->r_hiyear != ZIC_MAX)
        {
            continue;
        }
        if (!rp->r_isdst)
        {
            if (stdrp == NULL)
            {
                stdrp = rp;
            }
            else
            {
                return -1;
            }
        }
        else
        {
            if (dstrp == NULL)
            {
                dstrp = rp;
            }
            else
            {
                return -1;
            }
        }
    }
    if (stdrp == NULL && dstrp == NULL)
    {
        /*
         * There are no rules running through "max". Find the latest std rule
         * in stdabbrrp and latest rule of any type in stdrp.
         */
        struct rule* stdabbrrp = NULL;

        for (i = 0; i < zp->z_nrules; ++i)
        {
            rp = &zp->z_rules[i];
            if (!rp->r_isdst && rule_cmp(stdabbrrp, rp) < 0)
            {
                stdabbrrp = rp;
            }
            if (rule_cmp(stdrp, rp) < 0)
            {
                stdrp = rp;
            }
        }
        if (stdrp != NULL && stdrp->r_isdst)
        {
            /* Perpetual DST.  */
            dstr.r_month = TIME_TM_JANUARY;
            dstr.r_dycode = DC_DOM;
            dstr.r_dayofmonth = 1;
            dstr.r_tod = 0;
            dstr.r_todisstd = dstr.r_todisut = false;
            dstr.r_isdst = stdrp->r_isdst;
            dstr.r_save = stdrp->r_save;
            dstr.r_abbrvar = stdrp->r_abbrvar;
            stdr.r_month = TIME_TM_DECEMBER;
            stdr.r_dycode = DC_DOM;
            stdr.r_dayofmonth = 31;
            stdr.r_tod = TIME_SECSPERDAY + stdrp->r_save;
            stdr.r_todisstd = stdr.r_todisut = false;
            stdr.r_isdst = false;
            stdr.r_save = 0;
            stdr.r_abbrvar = (stdabbrrp ? stdabbrrp->r_abbrvar : "");
            dstrp = &dstr;
            stdrp = &stdr;
        }
    }
    if (stdrp == NULL && (zp->z_nrules != 0 || zp->z_isdst))
    {
        return -1;
    }
    abbrvar = (stdrp == NULL) ? "" : stdrp->r_abbrvar;
    len = doabbr(result, zp, abbrvar, false, 0, true);
    offsetlen = stringoffset(result + len, -zp->z_stdoff);
    if (!offsetlen)
    {
        result[0] = '\0';
        return -1;
    }
    len += offsetlen;
    if (dstrp == NULL)
    {
        return compat;
    }
    len += doabbr(result + len, zp, dstrp->r_abbrvar, dstrp->r_isdst, dstrp->r_save, true);
    if (dstrp->r_save != TIME_SECSPERMIN * TIME_MINSPERHOUR)
    {
        offsetlen = stringoffset(result + len, -(zp->z_stdoff + dstrp->r_save));
        if (!offsetlen)
        {
            result[0] = '\0';
            return -1;
        }
        len += offsetlen;
    }
    result[len++] = ',';
    c = stringrule(result + len, dstrp, dstrp->r_save, zp->z_stdoff);
    if (c < 0)
    {
        result[0] = '\0';
        return -1;
    }
    if (compat < c)
    {
        compat = c;
    }
    len += strlen(result + len);
    result[len++] = ',';
    c = stringrule(result + len, stdrp, dstrp->r_save, zp->z_stdoff);
    if (c < 0)
    {
        result[0] = '\0';
        return -1;
    }
    if (compat < c)
    {
        compat = c;
    }
    return compat;
}

static void free_data(struct zicinfo* local_zicinfo)
{
    int32_t i;

    if (NULL == local_zicinfo)
    {
        return;
    }

    local_zicinfo->filename = NULL;
    for (i = 0; i < local_zicinfo->nrules; ++i)
    {
        struct rule* tmp = &local_zicinfo->rules[i];
        pg_parser_mcxt_free(ZIC_MCXT, (void*)tmp->r_name);
        pg_parser_mcxt_free(ZIC_MCXT, (void*)tmp->r_abbrvar);
    }
    pg_parser_mcxt_free(ZIC_MCXT, local_zicinfo->rules);

    for (i = 0; i < local_zicinfo->nzones; ++i)
    {
        struct zone* tmp = &local_zicinfo->zones[i];
        pg_parser_mcxt_free(ZIC_MCXT, (void*)tmp->z_rule);
        pg_parser_mcxt_free(ZIC_MCXT, (void*)tmp->z_format);
        pg_parser_mcxt_free(ZIC_MCXT, (void*)tmp->z_name);
        tmp->z_rules = NULL;
    }
    pg_parser_mcxt_free(ZIC_MCXT, local_zicinfo->zones);

    for (i = 0; i < local_zicinfo->nlinks; ++i)
    {
        struct link* tmp = &local_zicinfo->links[i];
        pg_parser_mcxt_free(ZIC_MCXT, (void*)tmp->l_target);
        pg_parser_mcxt_free(ZIC_MCXT, (void*)tmp->l_linkname);
    }
    pg_parser_mcxt_free(ZIC_MCXT, (void*)local_zicinfo->links);

    pg_parser_mcxt_free(ZIC_MCXT, (void*)local_zicinfo->attypes);

    pg_parser_mcxt_free(ZIC_MCXT, (void*)local_zicinfo);
    local_zicinfo = NULL;
}

static void puttzcodepass(zic_t val, int32_t pass, pg_parser_StringInfo local_tzdata)
{
    if (pass == 1)
    {
        puttzcode(val, local_tzdata);
    }
    else
    {
        char buf[8];

        convert64(val, buf);
        loop_append_char(sizeof buf, buf, local_tzdata);
    }
}

static void puttzcode(const int32_t val, pg_parser_StringInfo local_tzdata)
{
    char buf[4];

    convert(val, buf);
    loop_append_char(sizeof buf, buf, local_tzdata);
}

static void loop_append_char(int32_t loop, char* arr, pg_parser_StringInfo local_tzdata)
{
    int32_t i;

    for (i = 0; i < loop; ++i)
    {
        pg_parser_appendStringInfoChar(local_tzdata, arr[i]);
    }
}

static void convert(const int32_t val, char* const buf)
{
    int32_t              i;
    int32_t              shift;
    unsigned char* const b = (unsigned char*)buf;

    for (i = 0, shift = 24; i < 4; ++i, shift -= 8)
    {
        b[i] = val >> shift;
    }
}

static void convert64(const zic_t val, char* const buf)
{
    int32_t              i;
    int32_t              shift;
    unsigned char* const b = (unsigned char*)buf;

    for (i = 0, shift = 56; i < 8; ++i, shift -= 8)
    {
        b[i] = val >> shift;
    }
}

static struct timerange limitrange(struct timerange     r,
                                   zic_t                lo,
                                   zic_t                hi,
                                   zic_t const*         ats,
                                   unsigned char const* types,
                                   struct zicinfo*      local_zicinfo)
{
    while (0 < r.count && ats[r.base] < lo)
    {
        r.defaulttype = types[r.base];
        r.count--;
        r.base++;
    }
    while (0 < r.leapcount && local_zicinfo->trans[r.leapbase] < lo)
    {
        r.leapcount--;
        r.leapbase++;
    }

    if (hi < ZIC_MAX)
    {
        while (0 < r.count && hi + 1 < ats[r.base + r.count - 1])
        {
            r.count--;
        }
        while (0 < r.leapcount && hi + 1 < local_zicinfo->trans[r.leapbase + r.leapcount - 1])
        {
            r.leapcount--;
        }
    }

    return r;
}

static int32_t atcomp(const void* avp, const void* bvp)
{
    const zic_t a = ((const struct attype*)avp)->at;
    const zic_t b = ((const struct attype*)bvp)->at;

    return (a < b) ? -1 : (a > b);
}

static void addtt(zic_t starttime, int32_t type, struct zicinfo* local_zicinfo)
{
    local_zicinfo->attypes = (struct attype*)growalloc(local_zicinfo->attypes,
                                                       sizeof *local_zicinfo->attypes,
                                                       local_zicinfo->timecnt,
                                                       &local_zicinfo->timecnt_alloc);
    local_zicinfo->attypes[local_zicinfo->timecnt].at = starttime;
    local_zicinfo->attypes[local_zicinfo->timecnt].dontmerge = false;
    local_zicinfo->attypes[local_zicinfo->timecnt].type = type;
    ++local_zicinfo->timecnt;
}

static void newabbr(const char* string, struct zicinfo* local_zicinfo)
{
    int32_t i;

    if (strcmp(string, TIME_GRANDPARENTED) != 0)
    {
        const char* cp = NULL;
        const char* mp = NULL;

        cp = string;
        mp = NULL;
        while (is_alpha(*cp) || ('0' <= *cp && *cp <= '9') || *cp == '-' || *cp == '+')
        {
            ++cp;
        }
        if (cp - string > ZIC_MAX_ABBR_LEN_WO_WARN)
        {
            mp = "time zone abbreviation has too many characters";
        }
        if (*cp != '\0')
        {
            mp = "time zone abbreviation differs from POSIX standard";
        }
        if (mp != NULL)
        {
            fprintf(stderr, "%s (%s)", mp, string);
        }
    }
    i = strlen(string) + 1;
    if (local_zicinfo->charcnt + i > TIME_TZ_MAX_CHARS)
    {
        fprintf(stderr, "too many, or too long, time zone abbreviations");
        return;
    }
    strcpy(&local_zicinfo->chars[local_zicinfo->charcnt], string);
    local_zicinfo->charcnt += i;
}

static int32_t addtype(zic_t           utoff,
                       char const*     abbr,
                       bool            isdst,
                       bool            ttisstd,
                       bool            ttisut,
                       struct zicinfo* local_zicinfo)
{
    int32_t i, j;

    if (!(-1L - 2147483647L <= utoff && utoff <= 2147483647L))
    {
        fprintf(stderr, "UT offset out of range");
        return -1;
    }
    if (!want_bloat(local_zicinfo))
    {
        ttisstd = ttisut = false;
    }

    for (j = 0; j < local_zicinfo->charcnt; ++j)
    {
        if (strcmp(&local_zicinfo->chars[j], abbr) == 0)
        {
            break;
        }
    }
    if (j == local_zicinfo->charcnt)
    {
        newabbr(abbr, local_zicinfo);
    }
    else
    {
        /* If there's already an entry, return its index.  */
        for (i = 0; i < local_zicinfo->typecnt; i++)
        {
            if (utoff == local_zicinfo->utoffs[i] && isdst == local_zicinfo->isdsts[i] &&
                j == local_zicinfo->desigidx[i] && ttisstd == local_zicinfo->ttisstds[i] &&
                ttisut == local_zicinfo->ttisuts[i])
            {
                return i;
            }
        }
    }

    /*
     * There isn't one; add a new one, unless there are already too many.
     */
    if (local_zicinfo->typecnt >= TIME_TZ_MAX_TYPES)
    {
        fprintf(stderr, "too many local time types");
        return -1;
    }
    i = local_zicinfo->typecnt++;
    local_zicinfo->utoffs[i] = utoff;
    local_zicinfo->isdsts[i] = isdst;
    local_zicinfo->ttisstds[i] = ttisstd;
    local_zicinfo->ttisuts[i] = ttisut;
    local_zicinfo->desigidx[i] = j;
    return i;
}

/* Is A an alphabetic character in the C locale?  */
static bool is_alpha(char a)
{
    switch (a)
    {
        default:
            return false;
        case 'A':
        case 'B':
        case 'C':
        case 'D':
        case 'E':
        case 'F':
        case 'G':
        case 'H':
        case 'I':
        case 'J':
        case 'K':
        case 'L':
        case 'M':
        case 'N':
        case 'O':
        case 'P':
        case 'Q':
        case 'R':
        case 'S':
        case 'T':
        case 'U':
        case 'V':
        case 'W':
        case 'X':
        case 'Y':
        case 'Z':
        case 'a':
        case 'b':
        case 'c':
        case 'd':
        case 'e':
        case 'f':
        case 'g':
        case 'h':
        case 'i':
        case 'j':
        case 'k':
        case 'l':
        case 'm':
        case 'n':
        case 'o':
        case 'p':
        case 'q':
        case 'r':
        case 's':
        case 't':
        case 'u':
        case 'v':
        case 'w':
        case 'x':
        case 'y':
        case 'z':
            return true;
    }
}

static char const* abbroffset(char* buf, zic_t offset)
{
    char    sign = '+';
    int32_t seconds, minutes;

    if (offset < 0)
    {
        offset = -offset;
        sign = '-';
    }

    seconds = offset % TIME_SECSPERMIN;
    offset /= TIME_SECSPERMIN;
    minutes = offset % TIME_MINSPERHOUR;
    offset /= TIME_MINSPERHOUR;
    if (100 <= offset)
    {
        fprintf(stderr, "\%\%z UT offset magnitude exceeds 99:59:59");
        return "%z";
    }
    else
    {
        char* p = buf;

        *p++ = sign;
        *p++ = '0' + offset / 10;
        *p++ = '0' + offset % 10;
        if (minutes | seconds)
        {
            *p++ = '0' + minutes / 10;
            *p++ = '0' + minutes % 10;
            if (seconds)
            {
                *p++ = '0' + seconds / 10;
                *p++ = '0' + seconds % 10;
            }
        }
        *p = '\0';
        return buf;
    }
}

static size_t doabbr(char* abbr, struct zone const* zp, char const* letters, bool isdst, zic_t save, bool doquotes)
{
    char*       cp = NULL;
    char*       slashp = NULL;
    size_t      len;
    char const* format = zp->z_format;

    slashp = (char*)strchr(format, '/');
    if (slashp == NULL)
    {
        char letterbuf[PERCENT_Z_LEN_BOUND + 1];

        if (zp->z_format_specifier == 'z')
        {
            letters = abbroffset(letterbuf, zp->z_stdoff + save);
        }
        else if (!letters)
        {
            letters = "%s";
        }
        sprintf(abbr, format, letters);
    }
    else if (isdst)
    {
        strcpy(abbr, slashp + 1);
    }
    else
    {
        rmemcpy1(abbr, 0, format, slashp - format);
        abbr[slashp - format] = '\0';
    }
    len = strlen(abbr);
    if (!doquotes)
    {
        return len;
    }
    for (cp = abbr; is_alpha(*cp); cp++)
    {
        continue;
    }
    if (len > 0 && *cp == '\0')
    {
        return len;
    }
    abbr[len + 2] = '\0';
    abbr[len + 1] = '>';
    memmove(abbr + 1, abbr, len);
    abbr[0] = '<';

    return len + 2;
}

static bool want_bloat(struct zicinfo* local_zicinfo)
{
    return 0 <= local_zicinfo->bloat;
}

static void updateminmax(const zic_t x, struct zicinfo* local_zicinfo)
{
    if (local_zicinfo->min_year > x)
    {
        local_zicinfo->min_year = x;
    }
    if (local_zicinfo->max_year < x)
    {
        local_zicinfo->max_year = x;
    }
}

static void* emalloc(size_t size)
{
    char* ptr = NULL;

    pg_parser_mcxt_malloc(ZIC_MCXT, (void**)(&ptr), size);

    return memcheck(ptr);
}

/*
 * Error handling.
 */
static void eats(char const* name, lineno_t num, char const* rname, lineno_t rnum, struct zicinfo* local_zicinfo)
{
    local_zicinfo->filename = name;
    local_zicinfo->linenum = num;
    local_zicinfo->rfilename = rname;
    local_zicinfo->rlinenum = rnum;
}

static void eat(char const* name, lineno_t num, struct zicinfo* local_zicinfo)
{
    eats(name, num, NULL, -1, local_zicinfo);
}

/*
 * Sort by rule name.
 */
static int32_t rcomp(const void* cp1, const void* cp2)
{
    return strcmp(((const struct rule*)cp1)->r_name, ((const struct rule*)cp2)->r_name);
}

static void associate(struct zicinfo* local_zicinfo)
{
    struct zone* zp = NULL;
    struct rule* rp = NULL;
    ptrdiff_t    i, j, base, out;

    ptrdiff_t    nrules = local_zicinfo->nrules;
    struct rule* rules = local_zicinfo->rules;
    ptrdiff_t    nzones = local_zicinfo->nzones;
    struct zone* zones = local_zicinfo->zones;

    if (nrules != 0)
    {
        local_qsort(rules, nrules, sizeof *rules, rcomp);
        for (i = 0; i < nrules - 1; ++i)
        {
            if (strcmp(rules[i].r_name, rules[i + 1].r_name) != 0)
            {
                continue;
            }
            if (strcmp(rules[i].r_filename, rules[i + 1].r_filename) == 0)
            {
                continue;
            }
            eat(rules[i].r_filename, rules[i].r_linenum, local_zicinfo);
            /*warning(_("same rule name in multiple files"));*/
            eat(rules[i + 1].r_filename, rules[i + 1].r_linenum, local_zicinfo);
            /*warning(_("same rule name in multiple files"));*/
            for (j = i + 2; j < nrules; ++j)
            {
                if (strcmp(rules[i].r_name, rules[j].r_name) != 0)
                {
                    break;
                }
                if (strcmp(rules[i].r_filename, rules[j].r_filename) == 0)
                {
                    continue;
                }
                if (strcmp(rules[i + 1].r_filename, rules[j].r_filename) == 0)
                {
                    continue;
                }
                break;
            }
            i = j - 1;
        }
    }
    for (i = 0; i < nzones; ++i)
    {
        zp = &zones[i];
        zp->z_rules = NULL;
        zp->z_nrules = 0;
    }
    for (base = 0; base < nrules; base = out)
    {
        rp = &rules[base];
        for (out = base + 1; out < nrules; ++out)
        {
            if (strcmp(rp->r_name, rules[out].r_name) != 0)
            {
                break;
            }
        }
        for (i = 0; i < nzones; ++i)
        {
            zp = &zones[i];
            if (strcmp(zp->z_rule, rp->r_name) != 0)
            {
                continue;
            }
            zp->z_rules = rp;
            zp->z_nrules = out - base;
        }
    }
    for (i = 0; i < nzones; ++i)
    {
        zp = &zones[i];
        if (zp->z_nrules == 0)
        {
            /*
             * Maybe we have a local standard time offset.
             */
            eat(zp->z_filename, zp->z_linenum, local_zicinfo);
            zp->z_save = getsave(zp->z_rule, &zp->z_isdst);

            /*
             * Note, though, that if there's no rule, a '%s' in the format is
             * a bad thing.
             */
            if (zp->z_format_specifier == 's')
            {
                fprintf(stderr, "%%s in ruleless zone");
            }
        }
    }
}

static void inlink(char** fields, int32_t nfields, struct zicinfo* local_zicinfo)
{
    struct link l;

    if (nfields != LINK_FIELDS)
    {
        fprintf(stderr, "wrong number of fields on Link line");
        return;
    }
    if (*fields[LF_TARGET] == '\0')
    {
        fprintf(stderr, "blank TARGET field on Link line");
        return;
    }
    if (!namecheck(fields[LF_LINKNAME]))
    {
        return;
    }
    l.l_filename = local_zicinfo->filename;
    l.l_linenum = local_zicinfo->linenum;
    l.l_target = ecpyalloc(fields[LF_TARGET]);
    l.l_linkname = ecpyalloc(fields[LF_LINKNAME]);
    local_zicinfo->links = (struct link*)growalloc(local_zicinfo->links,
                                                   sizeof *local_zicinfo->links,
                                                   local_zicinfo->nlinks,
                                                   &local_zicinfo->nlinks_alloc);
    local_zicinfo->links[local_zicinfo->nlinks++] = l;
}

static bool inzone(char** fields, int32_t nfields, struct zicinfo* local_zicinfo)
{
    ptrdiff_t    i;
    struct zone* zones = local_zicinfo->zones;
    ptrdiff_t    nzones = local_zicinfo->nzones;

    if (nfields < ZONE_MINFIELDS || nfields > ZONE_MAXFIELDS)
    {
        fprintf(stderr, "wrong number of fields on Zone line");
        return false;
    }
    for (i = 0; i < nzones; ++i)
    {
        if (zones[i].z_name != NULL && strcmp(zones[i].z_name, fields[ZF_NAME]) == 0)
        {
            fprintf(stderr,
                    "duplicate zone name %s"
                    " (file \"%s\", line %d)",
                    fields[ZF_NAME],
                    zones[i].z_filename,
                    zones[i].z_linenum);
            return false;
        }
    }

    return inzsub(fields, nfields, false, local_zicinfo);
}

static zic_t getsave(char* field, bool* isdst)
{
    int32_t dst = -1;
    zic_t   save;
    size_t  fieldlen = strlen(field);

    if (fieldlen != 0)
    {
        char* ep = field + fieldlen - 1;

        switch (*ep)
        {
            case 'd':
                dst = 1;
                *ep = '\0';
                break;
            case 's':
                dst = 0;
                *ep = '\0';
                break;
        }
    }
    save = gethms(field, "invalid saved time");
    *isdst = dst < 0 ? save != 0 : dst;

    return save;
}

static void inrule(char** fields, int32_t nfields, struct zicinfo* local_zicinfo)
{
    struct rule r;

    if (nfields != RULE_FIELDS)
    {
        fprintf(stderr, "wrong number of fields on Rule line");
        return;
    }
    switch (*fields[RF_NAME])
    {
        case '\0':
        case ' ':
        case '\f':
        case '\n':
        case '\r':
        case '\t':
        case '\v':
        case '+':
        case '-':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            fprintf(stderr, "Invalid rule name \"%s\"", fields[RF_NAME]);
            return;
    }
    r.r_filename = local_zicinfo->filename;
    r.r_linenum = local_zicinfo->linenum;
    r.r_save = getsave(fields[RF_SAVE], &r.r_isdst);
    rulesub(&r,
            fields[RF_LOYEAR],
            fields[RF_HIYEAR],
            fields[RF_COMMAND],
            fields[RF_MONTH],
            fields[RF_DAY],
            fields[RF_TOD]);
    r.r_name = ecpyalloc(fields[RF_NAME]);
    r.r_abbrvar = ecpyalloc(fields[RF_ABBRVAR]);
    if ((uint32_t)local_zicinfo->max_abbrvar_len < strlen(r.r_abbrvar))
    {
        local_zicinfo->max_abbrvar_len = strlen(r.r_abbrvar);
    }
    local_zicinfo->rules = (struct rule*)growalloc(local_zicinfo->rules,
                                                   sizeof *local_zicinfo->rules,
                                                   local_zicinfo->nrules,
                                                   &local_zicinfo->nrules_alloc);
    local_zicinfo->rules[local_zicinfo->nrules++] = r;
}

static void* erealloc(void* ptr, size_t size)
{
    pg_parser_mcxt_realloc(ZIC_MCXT, (void**)&ptr, size);
    return (void*)ptr;
}

static void* growalloc(void* ptr, size_t itemsize, ptrdiff_t nitems, ptrdiff_t* nitems_alloc)
{
    if (nitems < *nitems_alloc)
    {
        return ptr;
    }
    else
    {
        ptrdiff_t nitems_max = PTRDIFF_MAX - WORK_AROUND_QTBUG_53071;
        ptrdiff_t amax = nitems_max;

        if ((amax - 1) / 3 * 2 < *nitems_alloc)
        {
            memory_exhausted("integer overflow");
        }
        *nitems_alloc += (*nitems_alloc >> 1) + 1;
        return erealloc(ptr, size_product(*nitems_alloc, itemsize));
    }
}

static zic_t tadd(zic_t t1, zic_t t2)
{
    if (t1 < 0)
    {
        if (t2 < min_time - t1)
        {
            if (t1 != min_time)
            {
                time_overflow();
            }
            return min_time;
        }
    }
    else
    {
        if (max_time - t1 < t2)
        {
            if (t1 != max_time)
            {
                time_overflow();
            }
            return max_time;
        }
    }

    return t1 + t2;
}

/*
 * Given a rule, and a year, compute the date (in seconds since January 1,
 * 1970, 00:00 LOCAL time) in that year that the rule refers to.
 */

static zic_t rpytime(const struct rule* rp, zic_t wantedy)
{
    int32_t m, i;
    zic_t   dayoff; /* with a nod to Margaret O. */
    zic_t   t, y;

    if (wantedy == ZIC_MIN)
    {
        return min_time;
    }
    if (wantedy == ZIC_MAX)
    {
        return max_time;
    }
    dayoff = 0;
    m = TIME_TM_JANUARY;
    y = TIME_EPOCH_YEAR;
    if (y < wantedy)
    {
        wantedy -= y;
        dayoff = (wantedy / TIME_YEARSPERREPEAT) * (TIME_SECSPERREPEAT / TIME_SECSPERDAY);
        wantedy %= TIME_YEARSPERREPEAT;
        wantedy += y;
    }
    else if (wantedy < 0)
    {
        dayoff = (wantedy / TIME_YEARSPERREPEAT) * (TIME_SECSPERREPEAT / TIME_SECSPERDAY);
        wantedy %= TIME_YEARSPERREPEAT;
    }
    while (wantedy != y)
    {
        if (wantedy > y)
        {
            i = len_years[time_isleap(y)];
            ++y;
        }
        else
        {
            --y;
            i = -len_years[time_isleap(y)];
        }
        dayoff = oadd(dayoff, i);
    }
    while (m != rp->r_month)
    {
        i = len_months[time_isleap(y)][m];
        dayoff = oadd(dayoff, i);
        ++m;
    }
    i = rp->r_dayofmonth;
    if (m == TIME_TM_FEBRUARY && i == 29 && !time_isleap(y))
    {
        if (rp->r_dycode == DC_DOWLEQ)
        {
            --i;
        }
        else
        {
            fprintf(stderr, "use of 2/29 in non leap-year");
            return -1;
        }
    }
    --i;
    dayoff = oadd(dayoff, i);
    if (rp->r_dycode == DC_DOWGEQ || rp->r_dycode == DC_DOWLEQ)
    {
        zic_t wday;

#define TIME_LDAYSPERWEEK ((zic_t)TIME_DAYSPERWEEK)
        wday = TIME_EPOCH_WDAY;

        /*
         * Don't trust mod of negative numbers.
         */
        if (dayoff >= 0)
        {
            wday = (wday + dayoff) % TIME_LDAYSPERWEEK;
        }
        else
        {
            wday -= ((-dayoff) % TIME_LDAYSPERWEEK);
            if (wday < 0)
            {
                wday += TIME_LDAYSPERWEEK;
            }
        }
        while (wday != rp->r_wday)
        {
            if (rp->r_dycode == DC_DOWGEQ)
            {
                dayoff = oadd(dayoff, 1);
                if (++wday >= TIME_LDAYSPERWEEK)
                {
                    wday = 0;
                }
                ++i;
            }
            else
            {
                dayoff = oadd(dayoff, -1);
                if (--wday < 0)
                {
                    wday = TIME_LDAYSPERWEEK - 1;
                }
                --i;
            }
        }
        if (i < 0 || i >= len_months[time_isleap(y)][m])
        {
            /* nothing to do */
        }
    }
    if (dayoff < min_time / TIME_SECSPERDAY)
    {
        return min_time;
    }
    if (dayoff > max_time / TIME_SECSPERDAY)
    {
        return max_time;
    }
    t = (zic_t)dayoff * TIME_SECSPERDAY;

    return tadd(t, rp->r_tod);
}

/* case-insensitive equality */
static bool ciequal(const char* ap, const char* bp)
{
    while (lowerit(*ap) == lowerit(*bp++))
    {
        if (*ap++ == '\0')
        {
            return true;
        }
    }

    return false;
}

static char lowerit(char a)
{
    switch (a)
    {
        default:
            return a;
        case 'A':
            return 'a';
        case 'B':
            return 'b';
        case 'C':
            return 'c';
        case 'D':
            return 'd';
        case 'E':
            return 'e';
        case 'F':
            return 'f';
        case 'G':
            return 'g';
        case 'H':
            return 'h';
        case 'I':
            return 'i';
        case 'J':
            return 'j';
        case 'K':
            return 'k';
        case 'L':
            return 'l';
        case 'M':
            return 'm';
        case 'N':
            return 'n';
        case 'O':
            return 'o';
        case 'P':
            return 'p';
        case 'Q':
            return 'q';
        case 'R':
            return 'r';
        case 'S':
            return 's';
        case 'T':
            return 't';
        case 'U':
            return 'u';
        case 'V':
            return 'v';
        case 'W':
            return 'w';
        case 'X':
            return 'x';
        case 'Y':
            return 'y';
        case 'Z':
            return 'z';
    }
}

static bool ciprefix(char const* abbr, char const* word)
{
    do
    {
        if (!*abbr)
        {
            return true;
        }
    } while (lowerit(*abbr++) == lowerit(*word++));

    return false;
}

static const struct lookup* byword(const char* word, const struct lookup* table)
{
    const struct lookup* foundlp = NULL;
    const struct lookup* lp = NULL;

    if (word == NULL || table == NULL)
    {
        return NULL;
    }

    /*
     * If TABLE is LASTS and the word starts with "last" followed by a
     * non-'-', skip the "last" and look in WDAY_NAMES instead. Warn about any
     * usage of the undocumented prefix "last-".
     */
    if (table == lasts && ciprefix("last", word) && word[4])
    {
        if (word[4] == '-')
        {
            fprintf(stderr, "\"%s\" is undocumented; use \"last%s\" instead", word, word + 5);
        }
        else
        {
            word += 4;
            table = wday_names;
        }
    }

    /*
     * Look for exact match.
     */
    for (lp = table; lp->l_word != NULL; ++lp)
    {
        if (ciequal(word, lp->l_word))
        {
            return lp;
        }
    }

    /*
     * Look for inexact match.
     */
    foundlp = NULL;
    for (lp = table; lp->l_word != NULL; ++lp)
    {
        if (ciprefix(word, lp->l_word))
        {
            if (foundlp == NULL)
            {
                foundlp = lp;
            }
            else
            {
                return NULL; /* multiple inexact matches */
            }
        }
    }

    return foundlp;
}

static void rulesub(struct rule* rp,
                    const char*  loyearp,
                    const char*  hiyearp,
                    const char*  typep,
                    const char*  monthp,
                    const char*  dayp,
                    const char*  timep)
{
    const struct lookup* lp = NULL;
    const char*          cp = NULL;
    char*                dp = NULL;
    char*                ep = NULL;
    char                 xs;

    /* PG: year_tmp is to avoid sscanf portability issues */
    int32_t              year_tmp;

    if ((lp = byword(monthp, mon_names)) == NULL)
    {
        fprintf(stderr, "invalid month name");
        return;
    }
    rp->r_month = lp->l_value;
    rp->r_todisstd = false;
    rp->r_todisut = false;
    dp = ecpyalloc(timep);
    if (*dp != '\0')
    {
        ep = dp + strlen(dp) - 1;
        switch (lowerit(*ep))
        {
            case 's': /* Standard */
                rp->r_todisstd = true;
                rp->r_todisut = false;
                *ep = '\0';
                break;
            case 'w': /* Wall */
                rp->r_todisstd = false;
                rp->r_todisut = false;
                *ep = '\0';
                break;
            case 'g': /* Greenwich */
            case 'u': /* Universal */
            case 'z': /* Zulu */
                rp->r_todisstd = true;
                rp->r_todisut = true;
                *ep = '\0';
                break;
        }
    }
    rp->r_tod = gethms(dp, "invalid time of day");
    pg_parser_mcxt_free(ZIC_MCXT, (void*)dp);

    /*
     * Year work.
     */
    cp = loyearp;
    lp = byword(cp, begin_years);
    rp->r_lowasnum = lp == NULL;
    if (!rp->r_lowasnum)
    {
        switch (lp->l_value)
        {
            case YR_MINIMUM:
                rp->r_loyear = ZIC_MIN;
                break;
            case YR_MAXIMUM:
                rp->r_loyear = ZIC_MAX;
                break;
            default: /* "cannot happen" */
                fprintf(stderr, "panic: Invalid l_value %d\n", lp->l_value);
                return;
        }
    }
    else if (sscanf(cp, "%d%c", &year_tmp, &xs) == 1)
    {
        rp->r_loyear = year_tmp;
    }
    else
    {
        fprintf(stderr, "invalid starting year");
        return;
    }
    cp = hiyearp;
    lp = byword(cp, end_years);
    rp->r_hiwasnum = lp == NULL;
    if (!rp->r_hiwasnum)
    {
        switch (lp->l_value)
        {
            case YR_MINIMUM:
                rp->r_hiyear = ZIC_MIN;
                break;
            case YR_MAXIMUM:
                rp->r_hiyear = ZIC_MAX;
                break;
            case YR_ONLY:
                rp->r_hiyear = rp->r_loyear;
                break;
            default: /* "cannot happen" */
                fprintf(stderr, "panic: Invalid l_value %d\n", lp->l_value);
                return;
        }
    }
    else if (sscanf(cp, "%d%c", &year_tmp, &xs) == 1)
    {
        rp->r_hiyear = year_tmp;
    }
    else
    {
        fprintf(stderr, "invalid ending year");
        return;
    }
    if (rp->r_loyear > rp->r_hiyear)
    {
        fprintf(stderr, "starting year greater than ending year");
        return;
    }
    if (*typep != '\0')
    {
        fprintf(stderr, "year type \"%s\" is unsupported; use \"-\" instead", typep);
        return;
    }

    /*
     * Day work. Accept things such as: 1 lastSunday last-Sunday
     * (undocumented; warn about this) Sun<=20 Sun>=7
     */
    dp = ecpyalloc(dayp);
    if ((lp = byword(dp, lasts)) != NULL)
    {
        rp->r_dycode = DC_DOWLEQ;
        rp->r_wday = lp->l_value;
        rp->r_dayofmonth = len_months[1][rp->r_month];
    }
    else
    {
        if ((ep = strchr(dp, '<')) != NULL)
        {
            rp->r_dycode = DC_DOWLEQ;
        }
        else if ((ep = strchr(dp, '>')) != NULL)
        {
            rp->r_dycode = DC_DOWGEQ;
        }
        else
        {
            ep = dp;
            rp->r_dycode = DC_DOM;
        }
        if (rp->r_dycode != DC_DOM)
        {
            *ep++ = 0;
            if (*ep++ != '=')
            {
                fprintf(stderr, "invalid day of month");
                pg_parser_mcxt_free(ZIC_MCXT, (void*)dp);
                return;
            }
            if ((lp = byword(dp, wday_names)) == NULL)
            {
                fprintf(stderr, "invalid weekday name");
                pg_parser_mcxt_free(ZIC_MCXT, (void*)dp);
                return;
            }
            rp->r_wday = lp->l_value;
        }
        if (sscanf(ep, "%d%c", &rp->r_dayofmonth, &xs) != 1 || rp->r_dayofmonth <= 0 ||
            (rp->r_dayofmonth > len_months[1][rp->r_month]))
        {
            fprintf(stderr, "invalid day of month");
            pg_parser_mcxt_free(ZIC_MCXT, (void*)dp);
            return;
        }
    }
    pg_parser_mcxt_free(ZIC_MCXT, (void*)dp);
}

static void time_overflow(void)
{
    fprintf(stderr, "time overflow");

    return;
}

static zic_t oadd(zic_t t1, zic_t t2)
{
    if (t1 < 0 ? t2 < ZIC_MIN - t1 : ZIC_MAX - t1 < t2)
    {
        time_overflow();
    }

    return t1 + t2;
}

static zic_t gethms(char const* string, char const* errstring)
{
    /* PG: make hh be int32_t not zic_t to avoid sscanf portability issues */
    int32_t hh;
    int32_t sign, mm = 0, ss = 0;
    char    hhx, mmx, ssx, xr = '0', xs;
    int32_t tenths = 0;
    bool    ok = true;

    if (string == NULL || *string == '\0')
    {
        return 0;
    }
    if (*string == '-')
    {
        sign = -1;
        ++string;
    }
    else
    {
        sign = 1;
    }
    switch (
        sscanf(string, "%d%c%d%c%d%c%1d%*[0]%c%*[0123456789]%c", &hh, &hhx, &mm, &mmx, &ss, &ssx, &tenths, &xr, &xs))
    {
        default:
            ok = false;
            break;
        case 8:
            ok = '0' <= xr && xr <= '9';
            /* fallthrough */
        case 7:
            ok &= ssx == '.';
            /* fallthrough */
        case 5:
            ok &= mmx == ':';
            /* fallthrough */
        case 3:
            ok &= hhx == ':';
            /* fallthrough */
        case 1:
            break;
    }
    if (!ok)
    {
        fprintf(stderr, "%s", errstring);

        return 0;
    }
    if (hh < 0 || mm < 0 || mm >= TIME_MINSPERHOUR || ss < 0 || ss > TIME_SECSPERMIN)
    {
        fprintf(stderr, "%s", errstring);

        return 0;
    }
    /* Some compilers warn that this test is unsatisfiable for 32-bit ints */
#if INT_MAX > TIME_INT32_MAX
    if (ZIC_MAX / TIME_SECSPERHOUR < hh)
    {
        fprintf(stderr, "time overflow");
        return 0;
    }
#endif
    ss += 5 + ((ss ^ 1) & (xr == '0')) <= tenths; /* Round to even.  */

    return oadd(sign * (zic_t)hh * TIME_SECSPERHOUR, sign * (mm * TIME_SECSPERMIN + ss));
}

static void* memcheck(void* ptr)
{
    if (ptr == NULL)
    {
        memory_exhausted(strerror(errno));
    }

    return ptr;
}

static char* ecpyalloc(char const* str)
{
    return (char*)memcheck(pg_parser_mcxt_strdup(str));
}

static bool componentcheck(char const* name, char const* component, char const* component_end)
{
    enum
    {
        component_len_max = 14
    };

    ptrdiff_t component_len = component_end - component;

    if (component_len == 0)
    {
        if (!*name)
        {
            fprintf(stderr, "empty file name");
        }
        else
        {
            fprintf(stderr,
                    component == name ? "file name '%s' begins with '/'"
                    : *component_end  ? "file name '%s' contains '//'"
                                      : "file name '%s' ends with '/'",
                    name);
        }
        return false;
    }
    if (0 < component_len && component_len <= 2 && component[0] == '.' && component_end[-1] == '.')
    {
        int32_t len = component_len;

        fprintf(stderr, "file name '%s' contains '%.*s' component", name, len, component);
        return false;
    }

    return true;
}

static bool inzcont(char** fields, int32_t nfields, struct zicinfo* local_zicinfo)
{
    if (nfields < ZONEC_MINFIELDS || nfields > ZONEC_MAXFIELDS)
    {
        fprintf(stderr, "wrong number of fields on Zone continuation line");
        return false;
    }

    return inzsub(fields, nfields, true, local_zicinfo);
}

static bool namecheck(const char* name)
{
    char const* cp = NULL;
    char const* component = name;

    for (cp = name; *cp; cp++)
    {
        unsigned char c = *cp;

        if (c == '/')
        {
            if (!componentcheck(name, component, cp))
            {
                return false;
            }
            component = cp + 1;
        }
    }

    return componentcheck(name, component, cp);
}

static bool inzsub(char** fields, int32_t nfields, bool iscont, struct zicinfo* local_zicinfo)
{
    char*       cp = NULL;
    char*       cp1 = NULL;
    struct zone z;
    int32_t     i_stdoff, i_rule, i_format;
    int32_t     i_untilyear, i_untilmonth;
    int32_t     i_untilday, i_untiltime;
    bool        hasuntil;

    if (iscont)
    {
        i_stdoff = ZFC_STDOFF;
        i_rule = ZFC_RULE;
        i_format = ZFC_FORMAT;
        i_untilyear = ZFC_TILYEAR;
        i_untilmonth = ZFC_TILMONTH;
        i_untilday = ZFC_TILDAY;
        i_untiltime = ZFC_TILTIME;
        z.z_name = NULL;
    }
    else if (!namecheck(fields[ZF_NAME]))
    {
        return false;
    }
    else
    {
        i_stdoff = ZF_STDOFF;
        i_rule = ZF_RULE;
        i_format = ZF_FORMAT;
        i_untilyear = ZF_TILYEAR;
        i_untilmonth = ZF_TILMONTH;
        i_untilday = ZF_TILDAY;
        i_untiltime = ZF_TILTIME;
        z.z_name = ecpyalloc(fields[ZF_NAME]);
    }
    z.z_filename = local_zicinfo->filename;
    z.z_linenum = local_zicinfo->linenum;
    z.z_stdoff = gethms(fields[i_stdoff], "invalid UT offset");
    if ((cp = strchr(fields[i_format], '%')) != NULL)
    {
        if ((*++cp != 's' && *cp != 'z') || strchr(cp, '%') || strchr(fields[i_format], '/'))
        {
            fprintf(stderr, "invalid abbreviation format");
            return false;
        }
    }
    z.z_rule = ecpyalloc(fields[i_rule]);
    z.z_format = cp1 = ecpyalloc(fields[i_format]);
    z.z_format_specifier = cp ? *cp : '\0';
    if (z.z_format_specifier == 'z')
    {
        cp1[cp - fields[i_format]] = 's';
    }
    if ((uint32_t)local_zicinfo->max_format_len < strlen(z.z_format))
    {
        local_zicinfo->max_format_len = strlen(z.z_format);
    }
    hasuntil = nfields > i_untilyear;
    if (hasuntil)
    {
        z.z_untilrule.r_filename = local_zicinfo->filename;
        z.z_untilrule.r_linenum = local_zicinfo->linenum;
        rulesub(&z.z_untilrule,
                fields[i_untilyear],
                "only",
                "",
                (nfields > i_untilmonth) ? fields[i_untilmonth] : "Jan",
                (nfields > i_untilday) ? fields[i_untilday] : "1",
                (nfields > i_untiltime) ? fields[i_untiltime] : "0");
        z.z_untiltime = rpytime(&z.z_untilrule, z.z_untilrule.r_loyear);
        if (iscont && local_zicinfo->nzones > 0 && z.z_untiltime > min_time && z.z_untiltime < max_time &&
            local_zicinfo->zones[local_zicinfo->nzones - 1].z_untiltime > min_time &&
            local_zicinfo->zones[local_zicinfo->nzones - 1].z_untiltime < max_time &&
            local_zicinfo->zones[local_zicinfo->nzones - 1].z_untiltime >= z.z_untiltime)
        {
            fprintf(stderr, "Zone continuation line end time is not after end time of previous line");
            return false;
        }
    }
    local_zicinfo->zones = (struct zone*)growalloc(local_zicinfo->zones,
                                                   sizeof *local_zicinfo->zones,
                                                   local_zicinfo->nzones,
                                                   &local_zicinfo->nzones_alloc);
    local_zicinfo->zones[local_zicinfo->nzones++] = z;

    /*
     * If there was an UNTIL field on this line, there's more information
     * about the zone on the next line.
     */
    return hasuntil;
}

static bool is_space(char a)
{
    switch (a)
    {
        default:
            return false;
        case ' ':
        case '\f':
        case '\n':
        case '\r':
        case '\t':
        case '\v':
            return true;
    }
}

static void memory_exhausted(const char* msg)
{
    fprintf(stderr, "[pg_parser_zic] Memory exhausted: %s\n", msg);
}

static size_t size_product(size_t nitems, size_t itemsize)
{
    if (SIZE_MAX / itemsize < nitems)
    {
        memory_exhausted("size overflow");
    }

    return nitems * itemsize;
}

static char** getfields(char* cp)
{
    char*   dp = NULL;
    char**  array = NULL;
    int32_t nsubs;

    if (cp == NULL)
    {
        return NULL;
    }

    array = (char**)emalloc(size_product(strlen(cp) + 1, sizeof *array));

    nsubs = 0;
    for (;;)
    {
        while (is_space(*cp))
        {
            ++cp;
        }
        if (*cp == '\0' || *cp == '#')
        {
            break;
        }
        array[nsubs++] = dp = cp;
        do
        {
            if ((*dp = *cp++) != '"')
            {
                ++dp;
            }
            else
            {
                while ((*dp = *cp++) != '"')
                {
                    if (*dp != '\0')
                    {
                        ++dp;
                    }
                    else
                    {
                        fprintf(stderr, ("[pg_parser_zic] Odd number of quotation marks"));
                        return NULL;
                    }
                }
            }
        } while (*cp && *cp != '#' && !is_space(*cp));
        if (is_space(*cp))
        {
            ++cp;
        }
        *dp = '\0';
    }
    array[nsubs] = NULL;

    return array;
}

static void infile(const char* name, struct zicinfo* local_zicinfo)
{
    char**               fields = NULL;
    const struct lookup* lp = NULL;
    int32_t              nfields;
    bool                 wantcont;
    lineno_t             num;
    char                 buf[BUFSIZ];
    char                 nada = '\0';
    char**               tzdata = NULL;
    lineno_t             tzdataSize = 0;

    tzdata = pg_parser_get_tzdata_info(name, &tzdataSize);
    if (NULL == tzdata)
    {
        return;
    }

    wantcont = false;
    for (num = 1;; ++num)
    {
        eat(name, num, local_zicinfo);
        if (num > tzdataSize - 1)
        {
            break;
        }
        snprintf(buf, 512, "%s", tzdata[num - 1]);
        fields = getfields(buf);
        nfields = 0;
        while (fields[nfields] != NULL)
        {
            if (strcmp(fields[nfields], "-") == 0)
            {
                fields[nfields] = &nada;
            }
            ++nfields;
        }
        if (nfields == 0)
        {
            /* nothing to do */
        }
        else if (wantcont)
        {
            wantcont = inzcont(fields, nfields, local_zicinfo);
        }
        else
        {
            struct lookup const* line_codes = zi_line_codes;

            lp = byword(fields[0], line_codes);
            if (lp == NULL)
            {
                fprintf(stderr, "input line of unknown type");
            }
            else
            {
                switch (lp->l_value)
                {
                    case LC_RULE:
                        inrule(fields, nfields, local_zicinfo);
                        wantcont = false;
                        break;
                    case LC_ZONE:
                        wantcont = inzone(fields, nfields, local_zicinfo);
                        break;
                    case LC_LINK:
                        inlink(fields, nfields, local_zicinfo);
                        wantcont = false;
                        break;
                    default: /* "cannot happen" */
                        fprintf(stderr, "panic: Invalid l_value %d\n", lp->l_value);
                        return;
                }
            }
        }
        pg_parser_mcxt_free(ZIC_MCXT, (void*)fields);
    }
    if (wantcont)
    {
        fprintf(stderr, "expected continuation line not found");
    }
}

static void outzone(const struct zone*   zpfirst,
                    ptrdiff_t            zonecount,
                    pg_parser_StringInfo local_tzdata,
                    struct zicinfo*      local_zicinfo)
{
    const struct zone* zp = NULL;
    struct rule*       rp = NULL;
    ptrdiff_t          i, j;
    bool               usestart, useuntil;
    zic_t              starttime, untiltime;
    zic_t              stdoff;
    zic_t              save;
    zic_t              year;
    zic_t              startoff;
    bool               startttisstd;
    bool               startttisut;
    int32_t            type;
    char*              startbuf = NULL;
    char*              ab = NULL;
    char*              envvar = NULL;
    int32_t            max_abbr_len;
    int32_t            max_envvar_len;
    bool               prodstic; /* all rules are min to max */
    int32_t            compat;
    bool               do_extend;
    char               version;
    ptrdiff_t          lastatmax = -1;
    zic_t              one = 1;
    zic_t              y2038_boundary = one << 31;
    zic_t              max_year0;
    int32_t            defaulttype = -1;

    max_abbr_len = 2 + local_zicinfo->max_format_len + local_zicinfo->max_abbrvar_len;
    max_envvar_len = 2 * max_abbr_len + 5 * 9;

    startbuf = (char*)emalloc(max_abbr_len + 1);

    ab = (char*)emalloc(max_abbr_len + 1);

    envvar = (char*)emalloc(max_envvar_len + 1);

    TIME_INITIALIZE(untiltime);
    TIME_INITIALIZE(starttime);

    /*
     * Now. . .finally. . .generate some useful data!
     */
    local_zicinfo->timecnt = 0;
    local_zicinfo->typecnt = 0;
    local_zicinfo->charcnt = 0;
    prodstic = zonecount == 1;

    /*
     * Thanks to Earl Chew for noting the need to unconditionally initialize
     * startttisstd.
     */
    startttisstd = false;
    startttisut = false;
    local_zicinfo->min_year = local_zicinfo->max_year = TIME_EPOCH_YEAR;
    if (local_zicinfo->leapseen)
    {
        updateminmax(local_zicinfo->leapminyear, local_zicinfo);
        updateminmax(local_zicinfo->leapmaxyear + (local_zicinfo->leapmaxyear < ZIC_MAX), local_zicinfo);
    }
    for (i = 0; i < zonecount; ++i)
    {
        zp = &zpfirst[i];
        if (i < zonecount - 1)
        {
            updateminmax(zp->z_untilrule.r_loyear, local_zicinfo);
        }
        for (j = 0; j < zp->z_nrules; ++j)
        {
            rp = &zp->z_rules[j];
            if (rp->r_lowasnum)
            {
                updateminmax(rp->r_loyear, local_zicinfo);
            }
            if (rp->r_hiwasnum)
            {
                updateminmax(rp->r_hiyear, local_zicinfo);
            }
            if (rp->r_lowasnum || rp->r_hiwasnum)
            {
                prodstic = false;
            }
        }
    }

    /*
     * Generate lots of data if a rule can't cover all future times.
     */
    compat = stringzone(envvar, zpfirst, zonecount);
    version = compat < 2013 ? ZIC_VERSION_PRE_2013 : ZIC_VERSION;
    do_extend = compat < 0;
    if (do_extend)
    {
        /*
         * Search through a couple of extra years past the obvious 400, to
         * avoid edge cases.  For example, suppose a non-POSIX rule applies
         * from 2012 onwards and has transitions in March and September, plus
         * some one-off transitions in November 2013.  If zic looked only at
         * the last 400 years, it would set max_year=2413, with the intent
         * that the 400 years 2014 through 2413 will be repeated.  The last
         * transition listed in the tzfile would be in 2413-09, less than 400
         * years after the last one-off transition in 2013-11.  Two years
         * might be overkill, but with the kind of edge cases available we're
         * not sure that one year would suffice.
         */
        enum
        {
            years_of_observations = TIME_YEARSPERREPEAT + 2
        };

        if (local_zicinfo->min_year >= ZIC_MIN + years_of_observations)
        {
            local_zicinfo->min_year -= years_of_observations;
        }
        else
        {
            local_zicinfo->min_year = ZIC_MIN;
        }
        if (local_zicinfo->max_year <= ZIC_MAX - years_of_observations)
        {
            local_zicinfo->max_year += years_of_observations;
        }
        else
        {
            local_zicinfo->max_year = ZIC_MAX;
        }

        /*
         * Regardless of any of the above, for a "proDSTic" zone which
         * specifies that its rules always have and always will be in effect,
         * we only need one cycle to define the zone.
         */
        if (prodstic)
        {
            local_zicinfo->min_year = 1900;
            local_zicinfo->max_year = local_zicinfo->min_year + years_of_observations;
        }
    }
    max_year0 = local_zicinfo->max_year;
    if (want_bloat(local_zicinfo))
    {
        /*
         * For the benefit of older systems, generate data from 1900 through
         * 2038.
         */
        if (local_zicinfo->min_year > 1900)
        {
            local_zicinfo->min_year = 1900;
        }
        if (local_zicinfo->max_year < 2038)
        {
            local_zicinfo->max_year = 2038;
        }
    }

    for (i = 0; i < zonecount; ++i)
    {
        struct rule* prevrp = NULL;

        /*
         * A guess that may well be corrected later.
         */
        save = 0;
        zp = &zpfirst[i];
        usestart = i > 0 && (zp - 1)->z_untiltime > min_time;
        useuntil = i < (zonecount - 1);
        if (useuntil && zp->z_untiltime <= min_time)
        {
            continue;
        }
        stdoff = zp->z_stdoff;
        eat(zp->z_filename, zp->z_linenum, local_zicinfo);
        *startbuf = '\0';
        startoff = zp->z_stdoff;
        if (zp->z_nrules == 0)
        {
            save = zp->z_save;
            doabbr(startbuf, zp, NULL, zp->z_isdst, save, false);
            type = addtype(oadd(zp->z_stdoff, save), startbuf, zp->z_isdst, startttisstd, startttisut, local_zicinfo);
            if (usestart)
            {
                addtt(starttime, type, local_zicinfo);
                usestart = false;
            }
            else
            {
                defaulttype = type;
            }
        }
        else
        {
            for (year = local_zicinfo->min_year; year <= local_zicinfo->max_year; ++year)
            {
                if (useuntil && year > zp->z_untilrule.r_hiyear)
                {
                    break;
                }

                /*
                 * Mark which rules to do in the current year. For those to
                 * do, calculate rpytime(rp, year); The former TYPE field was
                 * also considered here.
                 */
                for (j = 0; j < zp->z_nrules; ++j)
                {
                    rp = &zp->z_rules[j];
                    eats(zp->z_filename, zp->z_linenum, rp->r_filename, rp->r_linenum, local_zicinfo);
                    rp->r_todo = year >= rp->r_loyear && year <= rp->r_hiyear;
                    if (rp->r_todo)
                    {
                        rp->r_temp = rpytime(rp, year);
                        rp->r_todo = (rp->r_temp < y2038_boundary || year <= max_year0);
                    }
                }
                for (;;)
                {
                    ptrdiff_t k;
                    zic_t     jtime, ktime;
                    zic_t     offset;

                    TIME_INITIALIZE(ktime);
                    if (useuntil)
                    {
                        /*
                         * Turn untiltime into UT assuming the current stdoff
                         * and save values.
                         */
                        untiltime = zp->z_untiltime;
                        if (!zp->z_untilrule.r_todisut)
                        {
                            untiltime = tadd(untiltime, -stdoff);
                        }
                        if (!zp->z_untilrule.r_todisstd)
                        {
                            untiltime = tadd(untiltime, -save);
                        }
                    }

                    /*
                     * Find the rule (of those to do, if any) that takes
                     * effect earliest in the year.
                     */
                    k = -1;
                    for (j = 0; j < zp->z_nrules; ++j)
                    {
                        rp = &zp->z_rules[j];
                        if (!rp->r_todo)
                        {
                            continue;
                        }
                        eats(zp->z_filename, zp->z_linenum, rp->r_filename, rp->r_linenum, local_zicinfo);
                        offset = rp->r_todisut ? 0 : stdoff;
                        if (!rp->r_todisstd)
                        {
                            offset = oadd(offset, save);
                        }
                        jtime = rp->r_temp;
                        if (jtime == min_time || jtime == max_time)
                        {
                            continue;
                        }
                        jtime = tadd(jtime, -offset);
                        if (k < 0 || jtime < ktime)
                        {
                            k = j;
                            ktime = jtime;
                        }
                        else if (jtime == ktime)
                        {
                            char const* dup_rules_msg = "two rules for same instant";

                            eats(zp->z_filename, zp->z_linenum, rp->r_filename, rp->r_linenum, local_zicinfo);
                            fprintf(stderr, "%s", dup_rules_msg);
                            rp = &zp->z_rules[k];
                            eats(zp->z_filename, zp->z_linenum, rp->r_filename, rp->r_linenum, local_zicinfo);
                            fprintf(stderr, "%s", dup_rules_msg);
                        }
                    }
                    if (k < 0)
                    {
                        break; /* go on to next year */
                    }
                    rp = &zp->z_rules[k];
                    rp->r_todo = false;
                    if (useuntil && ktime >= untiltime)
                    {
                        break;
                    }
                    save = rp->r_save;
                    if (usestart && ktime == starttime)
                    {
                        usestart = false;
                    }
                    if (usestart)
                    {
                        if (ktime < starttime)
                        {
                            startoff = oadd(zp->z_stdoff, save);
                            doabbr(startbuf, zp, rp->r_abbrvar, rp->r_isdst, rp->r_save, false);
                            continue;
                        }
                        if (*startbuf == '\0' && startoff == oadd(zp->z_stdoff, save))
                        {
                            doabbr(startbuf, zp, rp->r_abbrvar, rp->r_isdst, rp->r_save, false);
                        }
                    }
                    eats(zp->z_filename, zp->z_linenum, rp->r_filename, rp->r_linenum, local_zicinfo);
                    doabbr(ab, zp, rp->r_abbrvar, rp->r_isdst, rp->r_save, false);
                    offset = oadd(zp->z_stdoff, rp->r_save);
                    if (!want_bloat(local_zicinfo) && !useuntil && !do_extend && prevrp && rp->r_hiyear == ZIC_MAX &&
                        prevrp->r_hiyear == ZIC_MAX)
                    {
                        break;
                    }
                    type = addtype(offset, ab, rp->r_isdst, rp->r_todisstd, rp->r_todisut, local_zicinfo);
                    if (defaulttype < 0 && !rp->r_isdst)
                    {
                        defaulttype = type;
                    }
                    if (rp->r_hiyear == ZIC_MAX && !(0 <= lastatmax && ktime < local_zicinfo->attypes[lastatmax].at))
                    {
                        lastatmax = local_zicinfo->timecnt;
                    }
                    addtt(ktime, type, local_zicinfo);
                    prevrp = rp;
                }
            }
        }
        if (usestart)
        {
            if (*startbuf == '\0' && zp->z_format != NULL && strchr(zp->z_format, '%') == NULL &&
                strchr(zp->z_format, '/') == NULL)
            {
                strcpy(startbuf, zp->z_format);
            }
            eat(zp->z_filename, zp->z_linenum, local_zicinfo);
            if (*startbuf == '\0')
            {
                fprintf(stderr, "cannot determine time zone abbreviation to use just after until time");
            }
            else
            {
                bool isdst = startoff != zp->z_stdoff;

                type = addtype(startoff, startbuf, isdst, startttisstd, startttisut, local_zicinfo);
                if (defaulttype < 0 && !isdst)
                {
                    defaulttype = type;
                }
                addtt(starttime, type, local_zicinfo);
            }
        }

        /*
         * Now we may get to set starttime for the next zone line.
         */
        if (useuntil)
        {
            startttisstd = zp->z_untilrule.r_todisstd;
            startttisut = zp->z_untilrule.r_todisut;
            starttime = zp->z_untiltime;
            if (!startttisstd)
            {
                starttime = tadd(starttime, -save);
            }
            if (!startttisut)
            {
                starttime = tadd(starttime, -stdoff);
            }
        }
    }
    if (defaulttype < 0)
    {
        defaulttype = 0;
    }
    if (0 <= lastatmax)
    {
        local_zicinfo->attypes[lastatmax].dontmerge = true;
    }
    if (do_extend)
    {
        /*
         * If we're extending the explicitly listed observations for 400 years
         * because we can't fill the POSIX-TZ field, check whether we actually
         * ended up explicitly listing observations through that period.  If
         * there aren't any near the end of the 400-year period, add a
         * redundant one at the end of the final year, to make it clear that
         * we are claiming to have definite knowledge of the lack of
         * transitions up to that point.
         */
        struct rule    xr;
        struct attype* lastat;

        xr.r_month = TIME_TM_JANUARY;
        xr.r_dycode = DC_DOM;
        xr.r_dayofmonth = 1;
        xr.r_tod = 0;
        for (lastat = local_zicinfo->attypes, i = 1; i < local_zicinfo->timecnt; i++)
        {
            if (local_zicinfo->attypes[i].at > lastat->at)
            {
                lastat = &local_zicinfo->attypes[i];
            }
        }
        if (!lastat || lastat->at < rpytime(&xr, local_zicinfo->max_year - 1))
        {
            addtt(rpytime(&xr, local_zicinfo->max_year + 1), lastat ? lastat->type : defaulttype, local_zicinfo);
            local_zicinfo->attypes[local_zicinfo->timecnt - 1].dontmerge = true;
        }
    }
    writezone(envvar, version, defaulttype, local_tzdata, local_zicinfo);
    pg_parser_mcxt_free(ZIC_MCXT, (void*)startbuf);
    pg_parser_mcxt_free(ZIC_MCXT, (void*)ab);
    pg_parser_mcxt_free(ZIC_MCXT, (void*)envvar);
}

static void writezone(const char* const    string,
                      char                 version,
                      int32_t              defaulttype,
                      pg_parser_StringInfo local_tzdata,
                      struct zicinfo*      local_zicinfo)
{
    ptrdiff_t          i, j;
    int32_t            pass;
    struct time_tzhead tzh;
    zic_t              one = 1;
    zic_t              y2038_boundary = one << 31;
    ptrdiff_t          nats = local_zicinfo->timecnt + WORK_AROUND_QTBUG_53071;

    /*
     * Allocate the ATS and TYPES arrays via a single malloc, as this is a bit
     * faster.
     */
    zic_t*             ats = (zic_t*)emalloc(PG_PARSER_MAXALIGN(size_product(nats, sizeof *ats + 1)));

    void*              typesptr = ats + nats;
    unsigned char*     types = (unsigned char*)typesptr;
    struct timerange   rangeall, range32, range64;

    /*
     * Sort.
     */
    if (local_zicinfo->timecnt > 1)
    {
        local_qsort(local_zicinfo->attypes, local_zicinfo->timecnt, sizeof *local_zicinfo->attypes, atcomp);
    }

    /*
     * Optimize.
     */
    {
        ptrdiff_t fromi, toi;

        toi = 0;
        fromi = 0;
        for (; fromi < local_zicinfo->timecnt; ++fromi)
        {
            if (toi != 0 &&
                ((local_zicinfo->attypes[fromi].at + local_zicinfo->utoffs[local_zicinfo->attypes[toi - 1].type]) <=
                 (local_zicinfo->attypes[toi - 1].at +
                  local_zicinfo->utoffs[toi == 1 ? 0 : local_zicinfo->attypes[toi - 2].type])))
            {
                local_zicinfo->attypes[toi - 1].type = local_zicinfo->attypes[fromi].type;
                continue;
            }
            if (toi == 0 || local_zicinfo->attypes[fromi].dontmerge ||
                (local_zicinfo->utoffs[local_zicinfo->attypes[toi - 1].type] !=
                 local_zicinfo->utoffs[local_zicinfo->attypes[fromi].type]) ||
                (local_zicinfo->isdsts[local_zicinfo->attypes[toi - 1].type] !=
                 local_zicinfo->isdsts[local_zicinfo->attypes[fromi].type]) ||
                (local_zicinfo->desigidx[local_zicinfo->attypes[toi - 1].type] !=
                 local_zicinfo->desigidx[local_zicinfo->attypes[fromi].type]))
            {
                local_zicinfo->attypes[toi++] = local_zicinfo->attypes[fromi];
            }
        }
        local_zicinfo->timecnt = toi;
    }

    /*
     * Transfer.
     */
    for (i = 0; i < local_zicinfo->timecnt; ++i)
    {
        ats[i] = local_zicinfo->attypes[i].at;
        types[i] = local_zicinfo->attypes[i].type;
    }

    /*
     * Correct for leap seconds.
     */
    for (i = 0; i < local_zicinfo->timecnt; ++i)
    {
        j = local_zicinfo->leapcnt;
        while (--j >= 0)
        {
            if (ats[i] > local_zicinfo->trans[j] - local_zicinfo->corr[j])
            {
                ats[i] = tadd(ats[i], local_zicinfo->corr[j]);
                break;
            }
        }
    }

    /*
     * Work around QTBUG-53071 for timestamps less than y2038_boundary - 1, by
     * inserting a no-op transition at time y2038_boundary - 1. This works
     * only for timestamps before the boundary, which should be good enough in
     * practice as QTBUG-53071 should be long-dead by 2038.  Do this after
     * correcting for leap seconds, as the idea is to insert a transition just
     * before 32-bit pg_time_t rolls around, and this occurs at a slightly
     * different moment if transitions are leap-second corrected.
     */
    if (WORK_AROUND_QTBUG_53071 && local_zicinfo->timecnt != 0 && want_bloat(local_zicinfo) &&
        ats[local_zicinfo->timecnt - 1] < y2038_boundary - 1 && strchr(string, '<'))
    {
        ats[local_zicinfo->timecnt] = y2038_boundary - 1;
        types[local_zicinfo->timecnt] = types[local_zicinfo->timecnt - 1];
        local_zicinfo->timecnt++;
    }

    rangeall.defaulttype = defaulttype;
    rangeall.base = rangeall.leapbase = 0;
    rangeall.count = local_zicinfo->timecnt;
    rangeall.leapcount = local_zicinfo->leapcnt;
    range64 = limitrange(rangeall, lo_time, hi_time, ats, types, local_zicinfo);
    range32 = limitrange(range64, TIME_INT32_MIN, TIME_INT32_MAX, ats, types, local_zicinfo);

    for (pass = 1; pass <= 2; ++pass)
    {
        ptrdiff_t thistimei, thistimecnt, thistimelim;
        int32_t   thisleapi, thisleapcnt, thisleaplim;
        int32_t   currenttype, thisdefaulttype;
        bool      locut, hicut;
        zic_t     lo;
        int32_t   old0;
        char      omittype[TIME_TZ_MAX_TYPES];
        int32_t   typemap[TIME_TZ_MAX_TYPES];
        int32_t   thistypecnt, stdcnt, utcnt;
        char      thischars[TIME_TZ_MAX_CHARS];
        int32_t   thischarcnt;
        bool      toomanytimes;
        int32_t   indmap[TIME_TZ_MAX_CHARS];

        if (pass == 1)
        {
            /*
             * Arguably the default time type in the 32-bit data should be
             * range32.defaulttype, which is suited for timestamps just before
             * PG_INT32_MIN.  However, zic traditionally used the time type of
             * the indefinite past instead.  Internet RFC 8532 says readers
             * should ignore 32-bit data, so this discrepancy matters only to
             * obsolete readers where the traditional type might be more
             * appropriate even if it's "wrong".  So, use the historical zic
             * value, unless -r specifies a low cutoff that excludes some
             * 32-bit timestamps.
             */
            thisdefaulttype = (lo_time <= TIME_INT32_MIN ? range64.defaulttype : range32.defaulttype);

            thistimei = range32.base;
            thistimecnt = range32.count;
            toomanytimes = thistimecnt >> 31 >> 1 != 0;
            thisleapi = range32.leapbase;
            thisleapcnt = range32.leapcount;
            locut = TIME_INT32_MIN < lo_time;
            hicut = hi_time < TIME_INT32_MAX;
        }
        else
        {
            thisdefaulttype = range64.defaulttype;
            thistimei = range64.base;
            thistimecnt = range64.count;
            toomanytimes = thistimecnt >> 31 >> 31 >> 2 != 0;
            thisleapi = range64.leapbase;
            thisleapcnt = range64.leapcount;
            locut = min_time < lo_time;
            hicut = hi_time < max_time;
        }
        if (toomanytimes)
        {
            fprintf(stderr, "too many transition times");
        }

        /*
         * Keep the last too-low transition if no transition is exactly at LO.
         * The kept transition will be output as a LO "transition"; see
         * "Output a LO_TIME transition" below.  This is needed when the
         * output is truncated at the start, and is also useful when catering
         * to buggy 32-bit clients that do not use time type 0 for timestamps
         * before the first transition.
         */
        if (0 < thistimei && ats[thistimei] != lo_time)
        {
            thistimei--;
            thistimecnt++;
            locut = false;
        }

        thistimelim = thistimei + thistimecnt;
        thisleaplim = thisleapi + thisleapcnt;
        if (thistimecnt != 0)
        {
            if (ats[thistimei] == lo_time)
            {
                locut = false;
            }
            if (hi_time < ZIC_MAX && ats[thistimelim - 1] == hi_time + 1)
            {
                hicut = false;
            }
        }
        rmemset1(omittype, 0, true, local_zicinfo->typecnt);
        omittype[thisdefaulttype] = false;
        for (i = thistimei; i < thistimelim; i++)
        {
            omittype[types[i]] = false;
        }

        /*
         * Reorder types to make THISDEFAULTTYPE type 0. Use TYPEMAP to swap
         * OLD0 and THISDEFAULTTYPE so that THISDEFAULTTYPE appears as type 0
         * in the output instead of OLD0.  TYPEMAP also omits unused types.
         */
        old0 = strlen(omittype);

#ifndef LEAVE_SOME_PRE_2011_SYSTEMS_IN_THE_LURCH

        /*
         * For some pre-2011 systems: if the last-to-be-written standard (or
         * daylight) type has an offset different from the most recently used
         * offset, append an (unused) copy of the most recently used type (to
         * help get global "altzone" and "timezone" variables set correctly).
         */
        if (want_bloat(local_zicinfo))
        {
            int32_t mrudst, mrustd, hidst, histd, type;

            hidst = histd = mrudst = mrustd = -1;
            for (i = thistimei; i < thistimelim; ++i)
            {
                if (local_zicinfo->isdsts[types[i]])
                {
                    mrudst = types[i];
                }
                else
                {
                    mrustd = types[i];
                }
            }
            for (i = old0; i < local_zicinfo->typecnt; i++)
            {
                int32_t h = (i == old0 ? thisdefaulttype : i == thisdefaulttype ? old0 : i);

                if (!omittype[h])
                {
                    if (local_zicinfo->isdsts[h])
                    {
                        hidst = i;
                    }
                    else
                    {
                        histd = i;
                    }
                }
            }
            if (hidst >= 0 && mrudst >= 0 && hidst != mrudst &&
                local_zicinfo->utoffs[hidst] != local_zicinfo->utoffs[mrudst])
            {
                local_zicinfo->isdsts[mrudst] = -1;
                type = addtype(local_zicinfo->utoffs[mrudst],
                               &local_zicinfo->chars[local_zicinfo->desigidx[mrudst]],
                               true,
                               local_zicinfo->ttisstds[mrudst],
                               local_zicinfo->ttisuts[mrudst],
                               local_zicinfo);
                local_zicinfo->isdsts[mrudst] = 1;
                omittype[type] = false;
            }
            if (histd >= 0 && mrustd >= 0 && histd != mrustd &&
                local_zicinfo->utoffs[histd] != local_zicinfo->utoffs[mrustd])
            {
                local_zicinfo->isdsts[mrustd] = -1;
                type = addtype(local_zicinfo->utoffs[mrustd],
                               &local_zicinfo->chars[local_zicinfo->desigidx[mrustd]],
                               false,
                               local_zicinfo->ttisstds[mrustd],
                               local_zicinfo->ttisuts[mrustd],
                               local_zicinfo);
                local_zicinfo->isdsts[mrustd] = 0;
                omittype[type] = false;
            }
        }
#endif /* !defined \
        * LEAVE_SOME_PRE_2011_SYSTEMS_IN_THE_LURCH */
        thistypecnt = 0;
        for (i = old0; i < local_zicinfo->typecnt; i++)
        {
            if (!omittype[i])
            {
                typemap[i == old0 ? thisdefaulttype : i == thisdefaulttype ? old0 : i] = thistypecnt++;
            }
        }

        for (i = 0; ((uint32_t)i) < sizeof indmap / sizeof indmap[0]; ++i)
        {
            indmap[i] = -1;
        }
        thischarcnt = stdcnt = utcnt = 0;
        for (i = old0; i < local_zicinfo->typecnt; i++)
        {
            char* thisabbr;

            if (omittype[i])
            {
                continue;
            }
            if (local_zicinfo->ttisstds[i])
            {
                stdcnt = thistypecnt;
            }
            if (local_zicinfo->ttisuts[i])
            {
                utcnt = thistypecnt;
            }
            if (indmap[local_zicinfo->desigidx[i]] >= 0)
            {
                continue;
            }
            thisabbr = &local_zicinfo->chars[local_zicinfo->desigidx[i]];
            for (j = 0; j < thischarcnt; ++j)
            {
                if (strcmp(&thischars[j], thisabbr) == 0)
                {
                    break;
                }
            }
            if (j == thischarcnt)
            {
                strcpy(&thischars[thischarcnt], thisabbr);
                thischarcnt += strlen(thisabbr) + 1;
            }
            indmap[local_zicinfo->desigidx[i]] = j;
        }
        if (pass == 1 && !want_bloat(local_zicinfo))
        {
            utcnt = stdcnt = thisleapcnt = 0;
            thistimecnt = -(locut + hicut);
            thistypecnt = thischarcnt = 1;
            thistimelim = thistimei;
        }
        rmemset1(&tzh, 0, 0, sizeof(struct time_tzhead));
        rmemcpy1(tzh.tzh_magic, 0, TIME_TZ_MAGIC, sizeof tzh.tzh_magic);
        tzh.tzh_version[0] = version;
        convert(utcnt, tzh.tzh_ttisutcnt);
        convert(stdcnt, tzh.tzh_ttisstdcnt);
        convert(thisleapcnt, tzh.tzh_leapcnt);
        convert(locut + thistimecnt + hicut, tzh.tzh_timecnt);
        convert(thistypecnt, tzh.tzh_typecnt);
        convert(thischarcnt, tzh.tzh_charcnt);
        loop_append_char(sizeof tzh.tzh_magic, tzh.tzh_magic, local_tzdata);
        loop_append_char(sizeof tzh.tzh_version, tzh.tzh_version, local_tzdata);
        loop_append_char(sizeof tzh.tzh_reserved, tzh.tzh_reserved, local_tzdata);
        loop_append_char(sizeof tzh.tzh_ttisutcnt, tzh.tzh_ttisutcnt, local_tzdata);
        loop_append_char(sizeof tzh.tzh_ttisstdcnt, tzh.tzh_ttisstdcnt, local_tzdata);
        loop_append_char(sizeof tzh.tzh_leapcnt, tzh.tzh_leapcnt, local_tzdata);
        loop_append_char(sizeof tzh.tzh_timecnt, tzh.tzh_timecnt, local_tzdata);
        loop_append_char(sizeof tzh.tzh_typecnt, tzh.tzh_typecnt, local_tzdata);
        loop_append_char(sizeof tzh.tzh_charcnt, tzh.tzh_charcnt, local_tzdata);
        if (pass == 1 && !want_bloat(local_zicinfo))
        {
            /* Output a minimal data block with just one time type.  */
            puttzcode(0, local_tzdata);
            pg_parser_appendStringInfoChar(local_tzdata, 0);
            pg_parser_appendStringInfoChar(local_tzdata, 0);
            pg_parser_appendStringInfoChar(local_tzdata, 0);
            continue;
        }

        /*
         * Output a LO_TIME transition if needed; see limitrange. But do not
         * go below the minimum representable value for this pass.
         */
        lo = pass == 1 && lo_time < TIME_INT32_MIN ? TIME_INT32_MIN : lo_time;

        if (locut)
        {
            puttzcodepass(lo, pass, local_tzdata);
        }
        for (i = thistimei; i < thistimelim; ++i)
        {
            zic_t at = ats[i] < lo ? lo : ats[i];

            puttzcodepass(at, pass, local_tzdata);
        }
        if (hicut)
        {
            puttzcodepass(hi_time + 1, pass, local_tzdata);
        }
        currenttype = 0;
        if (locut)
        {
            pg_parser_appendStringInfoChar(local_tzdata, currenttype);
        }
        for (i = thistimei; i < thistimelim; ++i)
        {
            currenttype = typemap[types[i]];
            pg_parser_appendStringInfoChar(local_tzdata, currenttype);
        }
        if (hicut)
        {
            pg_parser_appendStringInfoChar(local_tzdata, currenttype);
        }

        for (i = old0; i < local_zicinfo->typecnt; i++)
        {
            int32_t h = (i == old0 ? thisdefaulttype : i == thisdefaulttype ? old0 : i);

            if (!omittype[h])
            {
                puttzcode(local_zicinfo->utoffs[h], local_tzdata);
                pg_parser_appendStringInfoChar(local_tzdata, local_zicinfo->isdsts[h]);
                pg_parser_appendStringInfoChar(local_tzdata, indmap[local_zicinfo->desigidx[h]]);
            }
        }
        if (thischarcnt != 0)
        {
            loop_append_char(thischarcnt, thischars, local_tzdata);
        }
        for (i = thisleapi; i < thisleaplim; ++i)
        {
            zic_t todo;

            if (local_zicinfo->roll[i])
            {
                if (local_zicinfo->timecnt == 0 || local_zicinfo->trans[i] < ats[0])
                {
                    j = 0;
                    while (local_zicinfo->isdsts[j])
                    {
                        if (++j >= local_zicinfo->typecnt)
                        {
                            j = 0;
                            break;
                        }
                    }
                }
                else
                {
                    j = 1;
                    while (j < local_zicinfo->timecnt && local_zicinfo->trans[i] >= ats[j])
                    {
                        ++j;
                    }
                    j = types[j - 1];
                }
                todo = tadd(local_zicinfo->trans[i], -local_zicinfo->utoffs[j]);
            }
            else
            {
                todo = local_zicinfo->trans[i];
            }
            puttzcodepass(todo, pass, local_tzdata);
            puttzcode(local_zicinfo->corr[i], local_tzdata);
        }
        if (stdcnt != 0)
        {
            for (i = old0; i < local_zicinfo->typecnt; i++)
            {
                if (!omittype[i])
                {
                    pg_parser_appendStringInfoChar(local_tzdata, local_zicinfo->ttisstds[i]);
                }
            }
        }
        if (utcnt != 0)
        {
            for (i = old0; i < local_zicinfo->typecnt; i++)
            {
                if (!omittype[i])
                {
                    pg_parser_appendStringInfoChar(local_tzdata, local_zicinfo->ttisuts[i]);
                }
            }
        }
    }
    pg_parser_appendStringInfo(local_tzdata, "\n%s\n", string);
    pg_parser_mcxt_free(ZIC_MCXT, (void*)ats);
}

#define swapcode(TYPE, parmi, parmj, n)    \
    do                                     \
    {                                      \
        size_t i = (n) / sizeof(TYPE);     \
        TYPE*  pi = (TYPE*)(void*)(parmi); \
        TYPE*  pj = (TYPE*)(void*)(parmj); \
        do                                 \
        {                                  \
            TYPE t = *pi;                  \
            *pi++ = *pj;                   \
            *pj++ = t;                     \
        } while (--i > 0);                 \
    } while (0)

static void swapfunc(char* a, char* b, size_t n, int swaptype)
{
    if (swaptype <= 1)
    {
        swapcode(long, a, b, n);
    }
    else
    {
        swapcode(char, a, b, n);
    }
}

#define swap(a, b)                               \
    if (swaptype == 0)                           \
    {                                            \
        long t = *(long*)(void*)(a);             \
        *(long*)(void*)(a) = *(long*)(void*)(b); \
        *(long*)(void*)(b) = t;                  \
    }                                            \
    else                                         \
        swapfunc(a, b, es, swaptype)

#define SWAPINIT(a, es) \
    swaptype = ((char*)(a) - (char*)0) % sizeof(long) || (es) % sizeof(long) ? 2 : (es) == sizeof(long) ? 0 : 1

static char* med3(char* a, char* b, char* c, int (*cmp)(const void*, const void*))
{
    return cmp(a, b) < 0 ? (cmp(b, c) < 0 ? b : (cmp(a, c) < 0 ? c : a))
                         : (cmp(b, c) > 0 ? b : (cmp(a, c) < 0 ? a : c));
}

#define Min(x, y) ((x) < (y) ? (x) : (y))
#define vecswap(a, b, n) \
    if ((n) > 0)         \
    swapfunc(a, b, n, swaptype)

static void local_qsort(void* a, size_t n, size_t es, int (*cmp)(const void*, const void*))
{
    char * pa, *pb, *pc, *pd, *pl, *pm, *pn;
    size_t d1, d2;
    int    r, swaptype, presorted;

loop:
    SWAPINIT(a, es);
    if (n < 7)
    {
        for (pm = (char*)a + es; pm < (char*)a + n * es; pm += es)
        {
            for (pl = pm; pl > (char*)a && cmp(pl - es, pl) > 0; pl -= es)
            {
                swap(pl, pl - es);
            }
        }
        return;
    }
    presorted = 1;
    for (pm = (char*)a + es; pm < (char*)a + n * es; pm += es)
    {
        if (cmp(pm - es, pm) > 0)
        {
            presorted = 0;
            break;
        }
    }
    if (presorted)
    {
        return;
    }
    pm = (char*)a + (n / 2) * es;
    if (n > 7)
    {
        pl = (char*)a;
        pn = (char*)a + (n - 1) * es;
        if (n > 40)
        {
            size_t d = (n / 8) * es;

            pl = med3(pl, pl + d, pl + 2 * d, cmp);
            pm = med3(pm - d, pm, pm + d, cmp);
            pn = med3(pn - 2 * d, pn - d, pn, cmp);
        }
        pm = med3(pl, pm, pn, cmp);
    }
    swap((char*)a, pm);
    pa = pb = (char*)a + es;
    pc = pd = (char*)a + (n - 1) * es;
    for (;;)
    {
        while (pb <= pc && (r = cmp(pb, a)) <= 0)
        {
            if (r == 0)
            {
                swap(pa, pb);
                pa += es;
            }
            pb += es;
        }
        while (pb <= pc && (r = cmp(pc, a)) >= 0)
        {
            if (r == 0)
            {
                swap(pc, pd);
                pd -= es;
            }
            pc -= es;
        }
        if (pb > pc)
        {
            break;
        }
        swap(pb, pc);
        pb += es;
        pc -= es;
    }
    pn = (char*)a + n * es;
    d1 = Min(pa - (char*)a, pb - pa);
    vecswap((char*)a, pb - d1, d1);
    d1 = Min((size_t)(pd - pc), (size_t)(pn - pd - es));
    vecswap(pb, pn - d1, d1);
    d1 = pb - pa;
    d2 = pd - pc;
    if (d1 <= d2)
    {
        /* Recurse on left partition, then iterate on right partition */
        if (d1 > es)
        {
            local_qsort(a, d1 / es, es, cmp);
        }
        if (d2 > es)
        {
            /* Iterate rather than recurse to save stack space */
            /* local_qsort(pn - d2, d2 / es, es, cmp); */
            a = pn - d2;
            n = d2 / es;
            goto loop;
        }
    }
    else
    {
        /* Recurse on right partition, then iterate on left partition */
        if (d2 > es)
        {
            local_qsort(pn - d2, d2 / es, es, cmp);
        }
        if (d1 > es)
        {
            /* Iterate rather than recurse to save stack space */
            /* local_qsort(a, d1 / es, es, cmp); */
            n = d1 / es;
            goto loop;
        }
    }
}

/* important function */
void pg_parser_zic_get_tzdata(const char* dbtz_name, pg_parser_StringInfo local_tzdata)
{
    char*           input_tzdata = NULL;
    int32_t         i, j;
    struct zicinfo* local_zicinfo = NULL;
    struct zone*    zones = NULL;
    ptrdiff_t       nzones;
    struct link*    links = NULL;
    ptrdiff_t       nlinks;

    if (!pg_parser_mcxt_malloc(ZIC_MCXT, (void**)(&local_zicinfo), sizeof(struct zicinfo)))
    {
        fprintf(stderr, "can't malloc in pg_parser_zic_get_tzdata");
    }
    local_zicinfo->max_abbrvar_len = PERCENT_Z_LEN_BOUND;
    /* Convert array in local_tzdata_info.h. */
    infile(dbtz_name, local_zicinfo);

    links = local_zicinfo->links;
    nlinks = local_zicinfo->nlinks;
    for (i = 0; i < nlinks; ++i)
    {
        if (strcmp(dbtz_name, links[i].l_linkname) == 0)
        {
            input_tzdata = (char*)links[i].l_target;
            break;
        }
    }
    if (!input_tzdata)
    {
        input_tzdata = (char*)dbtz_name;
    }

    associate(local_zicinfo);

    zones = local_zicinfo->zones;
    nzones = local_zicinfo->nzones;
    for (i = 0; i < nzones; ++i)
    {
        if (zones[i].z_name == NULL)
        {
            continue;
        }
        if (strcmp(input_tzdata, zones[i].z_name) != 0)
        {
            continue;
        }
        for (j = i + 1; j < nzones && zones[j].z_name == NULL; ++j)
        {
            continue;
        }
        outzone(&zones[i], j - i, local_tzdata, local_zicinfo);
    }

    free_data(local_zicinfo);
}
