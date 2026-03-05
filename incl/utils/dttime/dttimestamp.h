#ifndef _DTTIMESTAMP_H_
#define _DTTIMESTAMP_H_

TimestampTz dt_gettimestamp(void);

void dt_timestamptz_to_string(TimestampTz t, char *buf);

#endif
