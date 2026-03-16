#ifndef _RIPPLE_REFRESH_P2CSHARDING_H
#define _RIPPLE_REFRESH_P2CSHARDING_H

typedef struct RIPPLE_TASK_REFRESH_P2CSHARDING
{
    ripple_refresh_table_syncstats* syncstats;
    ripple_queue*                   taskqueue;
} ripple_task_refresh_p2csharding;

extern ripple_task_refresh_p2csharding *ripple_refresh_p2csharding_init(void);

void* ripple_refresh_p2csharding_main(void* args);

void ripple_refresh_p2csharding_free(void* args);

#endif
