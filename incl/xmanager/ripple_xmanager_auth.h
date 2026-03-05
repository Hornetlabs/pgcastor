#ifndef _RIPPLE_XMANAGER_AUTH_H_
#define _RIPPLE_XMANAGER_AUTH_H_

#define RIPPLE_XMANAGER_AUTH_DEFAULTTIMEOUT         5

typedef struct RIPPLE_XMANAGER_AUTH
{
    int                         timeout;
    uint64                      no;
    ripple_netpool*             npool;
    char                        padding[RIPPLE_CACHELINE_SIZE];
    ripple_queue*               authqueue;
    char                        padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue*               metricqueue;
    char                        padding2[RIPPLE_CACHELINE_SIZE];
} ripple_xmanager_auth;

ripple_xmanager_auth* ripple_xmanager_auth_init(void);

/* 主流程 */
void* ripple_xmanager_auth_main(void *args);

void ripple_xmanager_auth_destroy(ripple_xmanager_auth* xauth);


#endif
