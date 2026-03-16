#ifndef _RIPPLE_XMANAGER_LISTEN_H_
#define _RIPPLE_XMANAGER_LISTEN_H_

typedef struct RIPPLE_XMANAGER_LISTEN
{
    ripple_netserver                            base;
    char                                        padding[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                               authqueue;
    char                                        padding1[RIPPLE_CACHELINE_SIZE];
} ripple_xmanager_listen;

/* 初始化 */
ripple_xmanager_listen* ripple_xmanager_listen_init(void);

/* 主流程 */
void* ripple_xmanager_listen_main(void *args);

/* 资源回收 */
void ripple_xmanager_listen_destroy(void* args);

#endif
