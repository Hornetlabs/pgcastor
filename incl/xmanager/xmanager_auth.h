#ifndef _XMANAGER_AUTH_H_
#define _XMANAGER_AUTH_H_

#define XMANAGER_AUTH_DEFAULTTIMEOUT 5

typedef struct XMANAGER_AUTH
{
    int      timeout;
    uint64   no;
    netpool* npool;
    char     padding[CACHELINE_SIZE];
    queue*   authqueue;
    char     padding1[CACHELINE_SIZE];
    queue*   metricqueue;
    char     padding2[CACHELINE_SIZE];
} xmanager_auth;

xmanager_auth* xmanager_auth_init(void);

/* Main flow */
void* xmanager_auth_main(void* args);

void xmanager_auth_destroy(xmanager_auth* xauth);

#endif
