#ifndef _RIPPLE_BIGTXN_INTEGRATEREBUILD_H
#define _RIPPLE_BIGTXN_INTEGRATEREBUILD_H


typedef struct RIPPLE_BIGTXN_INTEGRATEREBUILD
{
    ripple_rebuild                                          rebuild;
    bool                                                    mergetxn;
    bool                                                    burst;                              /* burst 模式 */
    int                                                     txbundlesize;                       /* 合并事务的阈值 */
    char                                                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_onlinerefresh_integratedataset*                  onlinerefreshdataset;
    char                                                    padding1[RIPPLE_CACHELINE_SIZE];
    HTAB*                                                   honlinerefreshfilterdataset;
    char                                                    padding2[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                                       parser2rebuild;
    char                                                    padding3[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                                       rebuild2sync;
    char                                                    padding4[RIPPLE_CACHELINE_SIZE];
    void*                                                   privdata;
    char                                                    padding5[RIPPLE_CACHELINE_SIZE];
} ripple_bigtxn_integraterebuild;

/* 初始化 */
ripple_bigtxn_integraterebuild* ripple_bigtxn_integraterebuild_init(void);

/* 工作 */
void *ripple_bigtxn_integraterebuild_main(void* args);

void ripple_bigtxn_integraterebuild_free(void *args);

#endif
