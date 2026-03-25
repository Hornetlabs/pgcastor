#ifndef _INCREMENT_INTEGRATE_H
#define _INCREMENT_INTEGRATE_H

typedef struct INCREMENT_INTEGRATE
{
    uint64                          persistno;
    char                            padding[CACHELINE_SIZE];
    increment_integratesplittrail*  splittrailctx;
    char                            padding1[CACHELINE_SIZE];
    queue*                          recordscache;
    char                            padding2[CACHELINE_SIZE];
    increment_integrateparsertrail* decodingctx;
    char                            padding3[CACHELINE_SIZE];
    increment_integraterebuild*     rebuild;
    char                            padding4[CACHELINE_SIZE];
    cache_txn*                      parser2rebuild;
    char                            padding5[CACHELINE_SIZE];
    cache_txn*                      rebuild2sync;
    char                            padding6[CACHELINE_SIZE];
    increment_integratesyncstate*   syncworkstate;
    char                            padding7[CACHELINE_SIZE];
    pthread_mutex_t                 onlinerefreshlock;
    dlist*                          onlinerefresh; /* onlinerefresh_integrate */
    char                            padding8[CACHELINE_SIZE];
    pthread_mutex_t                 refreshlock;
    List*                           refresh;
    char                            padding9[CACHELINE_SIZE];
    pthread_mutex_t                 bigtxnlock;
    dlist*                          bigtxnmgr; /* bigtxn_integratemanager */
    char                            padding10[CACHELINE_SIZE];
    threads*                        threads; /* Thread management */
    char                            padding11[CACHELINE_SIZE];
    metric_integrate*               integratestate;
} increment_integrate;

increment_integrate* increment_integrate_init(void);

extern bool increment_integrate_startrefresh(increment_integrate* incintegrate);

extern bool increment_integrate_refreshload(increment_integrate* incintegrate);

extern void increment_integrate_refreshflush(increment_integrate* incintegrate);

extern bool increment_integrate_tryjoinonrefresh(increment_integrate* incintegrate);

extern bool increment_integrate_startonlinerefresh(increment_integrate* incintegrate);

extern bool increment_integrate_tryjoinononlinerefresh(increment_integrate* incintegrate);

extern bool increment_integrate_onlinerefreshload(increment_integrate* incintegrate);

extern bool increment_integrate_startbigtxn(increment_integrate* incintegrate);

extern bool increment_integrate_tryjoinonbigtxn(increment_integrate* incintegrate);

void increment_integrate_destroy(increment_integrate* integratestate);

#endif
