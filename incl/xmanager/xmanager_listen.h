#ifndef _XMANAGER_LISTEN_H_
#define _XMANAGER_LISTEN_H_

typedef struct XMANAGER_LISTEN
{
    netserver                            base;
    char                                        padding[CACHELINE_SIZE];
    queue*                               authqueue;
    char                                        padding1[CACHELINE_SIZE];
} xmanager_listen;

/* 初始化 */
xmanager_listen* xmanager_listen_init(void);

/* 主流程 */
void* xmanager_listen_main(void *args);

/* 资源回收 */
void xmanager_listen_destroy(void* args);

#endif
