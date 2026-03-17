#ifndef _XMANAGER_H_
#define _XMANAGER_H_

typedef struct XMANAGER
{
    uint64                              persistno;
    char*                               xsynchpath;
    /* 线程管理模块 */
    threads*                     threads;
    char                                padding[CACHELINE_SIZE];
    xmanager_listen*             listens;
    char                                padding1[CACHELINE_SIZE];
    queue*                       authqueue;
    char                                padding2[CACHELINE_SIZE];
    xmanager_auth*               auth;
    char                                padding3[CACHELINE_SIZE];
    queue*                       metricqueue;
    char                                padding4[CACHELINE_SIZE];
    xmanager_metric*             metric;
    char                                padding5[CACHELINE_SIZE];
} xmanager;

xmanager* xmanager_init(void);


void xmanager_destroy(xmanager* xmgr);

#endif
