#include "app_incl.h"
#include "utils/dttime/dttime.h"
#include "utils/dttime/dttimestamp.h"

TimestampTz dt_gettimestamp(void)
{
    TimestampTz    result;
    struct timeval tp;

    gettimeofday(&tp, NULL);

    result = (TimestampTz)tp.tv_sec -
             ((DTTIME_EPOCH_JDATE - DTTIME_UNIX_EPOCH_JDATE) * DTTIME_SECS_PER_DAY);
    result = (result * DTTIME_USECS_PER_SEC) + tp.tv_usec;
    return result;
}

void dt_timestamptz_to_string(TimestampTz t, char* buf)
{
    time_t    unix_sec;
    struct tm tm;
    int64_t   usec;

    /* Microsecond split */
    unix_sec = t / DTTIME_USECS_PER_SEC +
               (DTTIME_EPOCH_JDATE - DTTIME_UNIX_EPOCH_JDATE) * DTTIME_SECS_PER_DAY;
    usec = t % DTTIME_USECS_PER_SEC;

    /* Convert to local time (or gmtime) */
    localtime_r(&unix_sec, &tm);

    sprintf(buf,
            "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
            tm.tm_year + 1900,
            tm.tm_mon + 1,
            tm.tm_mday,
            tm.tm_hour,
            tm.tm_min,
            tm.tm_sec,
            usec);
    return;
}
