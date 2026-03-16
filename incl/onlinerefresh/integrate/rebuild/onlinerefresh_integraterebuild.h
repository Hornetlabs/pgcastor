#ifndef _RIPPLE_ONLINEREFRESH_INTEGRATEREBUILD_H
#define _RIPPLE_ONLINEREFRESH_INTEGRATEREBUILD_H

typedef struct RIPPLE_ONLINEREFRESH_INTEGRATEREBUILD
{
    ripple_rebuild                                          rebuild;
    bool                                                    mergetxn;
    bool                                                    burst;                              /* burst 模式 */
    int                                                     txbundlesize;                       /* 合并事务的阈值 */
    char                                                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                                       parser2rebuild;
    char                                                    padding1[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                                       rebuild2sync;
    char                                                    padding2[RIPPLE_CACHELINE_SIZE];
    void*                                                   privdata;
    char                                                    padding3[RIPPLE_CACHELINE_SIZE];
} ripple_onlinerefresh_integraterebuild;

/* 初始化 */
ripple_onlinerefresh_integraterebuild* ripple_onlinerefresh_integraterebuild_init(void);

/* 工作 */
void *ripple_onlinerefresh_integraterebuild_main(void *args);

void ripple_onlinerefresh_integraterebuild_free(void *args);

#endif
