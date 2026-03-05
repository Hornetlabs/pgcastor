#ifndef _DTTIME_H_
#define _DTTIME_H_

#define DTTIME_SECS_PER_YEAR            (36525 * 864)
#define DTTIME_SECS_PER_DAY             86400
#define DTTIME_SECS_PER_HOUR            3600
#define DTTIME_SECS_PER_MINUTE          60
#define DTTIME_MINS_PER_HOUR            60

#define DTTIME_USECS_PER_DAY            INT64CONST(86400000000)
#define DTTIME_USECS_PER_HOUR           INT64CONST(3600000000)
#define DTTIME_USECS_PER_MINUTE         INT64CONST(60000000)
#define DTTIME_USECS_PER_SEC            INT64CONST(1000000)


#define DTTIME_UNIX_EPOCH_JDATE         2440588 /* == date2j(1970, 1, 1) */
#define DTTIME_EPOCH_JDATE              2451545 /* == date2j(2000, 1, 1) */

#endif
