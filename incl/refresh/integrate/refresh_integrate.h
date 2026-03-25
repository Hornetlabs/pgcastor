#ifndef _REFRESH_INTEGRATE_H
#define _REFRESH_INTEGRATE_H

typedef enum REFRESH_INTEGRATE_STAT
{
    REFRESH_INTEGRATE_STAT_NOP = 0x00,
    REFRESH_INTEGRATE_STAT_INIT,     /* Initialize monitor structure set to init */
    REFRESH_INTEGRATE_STAT_STARTING, /* Join list linked list status */
    REFRESH_INTEGRATE_STAT_WORK,     /* Set to working state when executing refresh */
    REFRESH_INTEGRATE_STAT_DONE      /* Set to done when refresh ends */
} refresh_integrate_stat;

typedef struct REFRESH_INTEGRATE
{
    int                      stat; /* Manage thread status */
    int                      parallelcnt;
    char*                    conn_info;    /* Save connection string */
    char*                    refresh_path; /* Refresh folder path */
    thrsubmgr*               thrsmgr;
    refresh_table_syncstats* sync_stats;
    queue*                   tqueue;
} refresh_integrate;

extern refresh_integrate* refresh_integrate_init(void);

extern bool refresh_integrate_write(refresh_integrate* rintegrate);

extern bool refresh_integrate_read(refresh_integrate** refresh);

extern void* refresh_integrate_main(void* args);

extern void refresh_integrate_free(void* args);

extern void refresh_integrate_listfree(void* args);

#endif
