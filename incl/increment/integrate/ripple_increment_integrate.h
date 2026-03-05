#ifndef _RIPPLE_INCREMENT_INTEGRATE_H
#define _RIPPLE_INCREMENT_INTEGRATE_H

typedef struct RIPPLE_INCREMENT_INTEGRATE
{
    uint64                                  persistno;
    char                                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_increment_integratesplittrail*   splittrailctx;
    char                                    padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                           recordscache;
    char                                    padding2[RIPPLE_CACHELINE_SIZE];
    ripple_increment_integrateparsertrail*  decodingctx;
    char                                    padding3[RIPPLE_CACHELINE_SIZE];
    ripple_increment_integraterebuild*      rebuild;
    char                                    padding4[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                       parser2rebuild;
    char                                    padding5[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                       rebuild2sync;
    char                                    padding6[RIPPLE_CACHELINE_SIZE];
    ripple_increment_integratesyncstate*    syncworkstate;
    char                                    padding7[RIPPLE_CACHELINE_SIZE];
    pthread_mutex_t                         onlinerefreshlock;
    dlist*                                  onlinerefresh;                              /* ripple_onlinerefresh_integrate */
    char                                    padding8[RIPPLE_CACHELINE_SIZE];
    pthread_mutex_t                         refreshlock;
    List*                                   refresh;
    char                                    padding9[RIPPLE_CACHELINE_SIZE];
    pthread_mutex_t                         bigtxnlock;
    dlist*                                  bigtxnmgr;                                  /* ripple_bigtxn_integratemanager */
    char                                    padding10[RIPPLE_CACHELINE_SIZE];
    ripple_threads*                         threads;                                    /* 线程管理 */
    char                                    padding11[RIPPLE_CACHELINE_SIZE];
    ripple_metric_integrate*                integratestate;
} ripple_increment_integrate;

ripple_increment_integrate* ripple_increment_integrate_init(void);

extern bool ripple_increment_integrate_startrefresh(ripple_increment_integrate* incintegrate);

extern bool ripple_increment_integrate_refreshload(ripple_increment_integrate* incintegrate);

extern void ripple_increment_integrate_refreshflush(ripple_increment_integrate* incintegrate);

extern bool ripple_increment_integrate_tryjoinonrefresh(ripple_increment_integrate* incintegrate);

extern bool ripple_increment_integrate_startonlinerefresh(ripple_increment_integrate* incintegrate);

extern bool ripple_increment_integrate_tryjoinononlinerefresh(ripple_increment_integrate* incintegrate);

extern bool ripple_increment_integrate_onlinerefreshload(ripple_increment_integrate* incintegrate);

extern bool ripple_increment_integrate_startbigtxn(ripple_increment_integrate* incintegrate);

extern bool ripple_increment_integrate_tryjoinonbigtxn(ripple_increment_integrate* incintegrate);

void ripple_increment_integrate_destroy(ripple_increment_integrate* integratestate);

#endif
