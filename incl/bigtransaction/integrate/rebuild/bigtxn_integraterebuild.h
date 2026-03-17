#ifndef _BIGTXN_INTEGRATEREBUILD_H
#define _BIGTXN_INTEGRATEREBUILD_H


typedef struct BIGTXN_INTEGRATEREBUILD
{
    rebuild                                          rebuild;
    bool                                                    mergetxn;
    bool                                                    burst;                              /* burst 模式 */
    int                                                     txbundlesize;                       /* 合并事务的阈值 */
    char                                                    padding[CACHELINE_SIZE];
    onlinerefresh_integratedataset*                  onlinerefreshdataset;
    char                                                    padding1[CACHELINE_SIZE];
    HTAB*                                                   honlinerefreshfilterdataset;
    char                                                    padding2[CACHELINE_SIZE];
    cache_txn*                                       parser2rebuild;
    char                                                    padding3[CACHELINE_SIZE];
    cache_txn*                                       rebuild2sync;
    char                                                    padding4[CACHELINE_SIZE];
    void*                                                   privdata;
    char                                                    padding5[CACHELINE_SIZE];
} bigtxn_integraterebuild;

/* 初始化 */
bigtxn_integraterebuild* bigtxn_integraterebuild_init(void);

/* 工作 */
void *bigtxn_integraterebuild_main(void* args);

void bigtxn_integraterebuild_free(void *args);

#endif
