#ifndef _XMANAGER_LISTEN_H_
#define _XMANAGER_LISTEN_H_

typedef struct XMANAGER_LISTEN
{
    netserver base;
    char      padding[CACHELINE_SIZE];
    queue*    authqueue;
    char      padding1[CACHELINE_SIZE];
} xmanager_listen;

/* Initialize */
xmanager_listen* xmanager_listen_init(void);

/* Main flow */
void* xmanager_listen_main(void* args);

/* Resource cleanup */
void xmanager_listen_destroy(void* args);

#endif
