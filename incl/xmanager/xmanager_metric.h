#ifndef _RIPPLE_XMANAGER_METRIC_H_
#define _RIPPLE_XMANAGER_METRIC_H_

typedef struct RIPPLE_XMANAGER_METRIC
{
    /* 配置文件总目录 */
    char*                           xsynchpath;
    char*                           configpath;
    ripple_netpool*                 npool;
    dlist*                          metricnodes;
    dlist*                          fd2metricnodes;
    char                            padding[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                   metricqueue;
    char                            padding1[RIPPLE_CACHELINE_SIZE];
    void*                           privdata;
    void                            (*privdatadestroy)(void* args);
    char                            padding2[RIPPLE_CACHELINE_SIZE];
} ripple_xmanager_metric;

extern ripple_xmanager_metric* ripple_xmanager_metric_init(void);

/* 设置 configpath */
extern bool ripple_xmanager_metric_setxsynchpath(ripple_xmanager_metric* xmetric, char* xsynchpath);

extern void* ripple_xmanager_metric_main(void *args);

extern void ripple_xmanager_metric_destroy(ripple_xmanager_metric* xmetric);

#endif
