#ifndef _RIPPLE_XMANAGER_H_
#define _RIPPLE_XMANAGER_H_

typedef struct RIPPLE_XMANAGER
{
    uint64                              persistno;
    char*                               xsynchpath;
    /* 线程管理模块 */
    ripple_threads*                     threads;
    char                                padding[RIPPLE_CACHELINE_SIZE];
    ripple_xmanager_listen*             listens;
    char                                padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                       authqueue;
    char                                padding2[RIPPLE_CACHELINE_SIZE];
    ripple_xmanager_auth*               auth;
    char                                padding3[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                       metricqueue;
    char                                padding4[RIPPLE_CACHELINE_SIZE];
    ripple_xmanager_metric*             metric;
    char                                padding5[RIPPLE_CACHELINE_SIZE];
} ripple_xmanager;

ripple_xmanager* ripple_xmanager_init(void);


void ripple_xmanager_destroy(ripple_xmanager* xmgr);

#endif
