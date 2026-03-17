#ifndef _XMANAGER_METRIC_H_
#define _XMANAGER_METRIC_H_

typedef struct XMANAGER_METRIC
{
    /* 配置文件总目录 */
    char*                           xsynchpath;
    char*                           configpath;
    netpool*                 npool;
    dlist*                          metricnodes;
    dlist*                          fd2metricnodes;
    char                            padding[CACHELINE_SIZE];
    queue*                   metricqueue;
    char                            padding1[CACHELINE_SIZE];
    void*                           privdata;
    void                            (*privdatadestroy)(void* args);
    char                            padding2[CACHELINE_SIZE];
} xmanager_metric;

extern xmanager_metric* xmanager_metric_init(void);

/* 设置 configpath */
extern bool xmanager_metric_setxsynchpath(xmanager_metric* xmetric, char* xsynchpath);

extern void* xmanager_metric_main(void *args);

extern void xmanager_metric_destroy(xmanager_metric* xmetric);

#endif
