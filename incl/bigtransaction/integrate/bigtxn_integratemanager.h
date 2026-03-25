#ifndef _BIGTXN_INTEGRATEMANAGER_H
#define _BIGTXN_INTEGRATEMANAGER_H

typedef enum BIGTXN_INTEGRATEMANAGER_STAT
{
    BIGTXN_INTEGRATEMANAGER_STAT_NOP = 0x00,
    BIGTXN_INTEGRATEMANAGER_STAT_INIT,
    BIGTXN_INTEGRATEMANAGER_STAT_INPROCESS,
    BIGTXN_INTEGRATEMANAGER_STAT_EXIT,    /* End */
    BIGTXN_INTEGRATEMANAGER_STAT_SIGTERM, /* Received exit signal */
    BIGTXN_INTEGRATEMANAGER_STAT_FREE     /* Release space */
} bigtxn_integratemanager_stat;

typedef struct BIGTXN_INTEGRATEMANAGER
{
    int                             stat; /* Flag status */
    FullTransactionId               xid;
    char                            padding[CACHELINE_SIZE];
    thrsubmgr*                      thrsmgr;
    char                            padding1[CACHELINE_SIZE];
    queue*                          recordscache;
    char                            padding2[CACHELINE_SIZE];
    cache_txn*                      parser2rebuild;
    char                            padding3[CACHELINE_SIZE];
    cache_txn*                      rebuild2sync;
    char                            padding4[CACHELINE_SIZE];
    onlinerefresh_integratedataset* onlinerefreshdataset;
    char                            padding5[CACHELINE_SIZE];
    HTAB*                           honlinerefreshfilterdataset;
    char                            padding6[CACHELINE_SIZE];
} bigtxn_integratemanager;

void bigtxn_integratemanager_stat_set(bigtxn_integratemanager* bigtxnmgr, int stat);

bigtxn_integratemanager* bigtxn_integratemanager_init(void);

void* bigtxn_integratemanager_main(void* args);

void bigtxn_integratemanager_free(void* args);

#endif
