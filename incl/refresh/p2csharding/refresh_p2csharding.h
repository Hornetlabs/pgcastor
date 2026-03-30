#ifndef _REFRESH_P2CSHARDING_H
#define _REFRESH_P2CSHARDING_H

typedef struct TASK_REFRESH_P2CSHARDING
{
    refresh_table_syncstats* syncstats;
    queue*                   taskqueue;
} task_refresh_p2csharding;

extern task_refresh_p2csharding* refresh_p2csharding_init(void);

void* refresh_p2csharding_main(void* args);

void refresh_p2csharding_free(void* args);

#endif
