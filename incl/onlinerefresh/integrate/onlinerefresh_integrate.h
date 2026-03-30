#ifndef _ONLINEREFRESH_INTEGRATE_H
#define _ONLINEREFRESH_INTEGRATE_H

typedef enum ONLINEREFRESH_INTEGRATE_STATE
{
    ONLINEREFRESH_INTEGRATE_NOP = 0x00,
    ONLINEREFRESH_INTEGRATE_INIT,
    ONLINEREFRESH_INTEGRATE_STARTING,
    ONLINEREFRESH_INTEGRATE_RUNNING,
    ONLINEREFRESH_INTEGRATE_REFRESHDONE,
    ONLINEREFRESH_INTEGRATE_DONE,
    ONLINEREFRESH_INTEGRATE_ABANDONED,
    ONLINEREFRESH_INTEGRATE_FREE
} onlinerefresh_integrate_state;

typedef struct ONLINEREFRESH_INTEGRATE
{
    int8                          increment;
    onlinerefresh_integrate_state stat;
    int                           parallelcnt; /* Stock worker threads, parallel count */
    FullTransactionId             txid;        /* Used to generate filter set on restart */
    recpos                        begin;
    uuid_t                        no;            /* onlinerefresh Number */
    char*                         conninfo;      /* Connection string */
    char                          data[MAXPATH]; /* Directory for storing data */
    char                          padding[CACHELINE_SIZE];
    refresh_table_syncstats*      tablesyncstats;
    char                          padding1[CACHELINE_SIZE];
    queue*                        tqueue;
    char                          padding2[CACHELINE_SIZE];
    queue*                        recordscache;
    char                          padding3[CACHELINE_SIZE];
    cache_txn*                    parser2rebuild;
    char                          padding4[CACHELINE_SIZE];
    cache_txn*                    rebuild2sync;
    char                          padding5[CACHELINE_SIZE];
    thrsubmgr*                    thrsmgr;
    char                          padding6[CACHELINE_SIZE];
} onlinerefresh_integrate;

onlinerefresh_integrate* onlinerefresh_integrate_init(bool increment);

void* onlinerefresh_integrate_manage(void* args);

/* Generate onlinerefresh node from persist */
bool onlinerefresh_integrate_persist2onlinerefreshmgr(onlinerefresh_persist* persist, void** onlinerefresh);

/* Check if onlinerefresh conflicts with started stock tables, return true if conflict */
bool onlinerefresh_integrate_isconflict(dlistnode* in_dlnode);

void onlinerefresh_integrate_free(void* in_onlinerefresh);

#endif
