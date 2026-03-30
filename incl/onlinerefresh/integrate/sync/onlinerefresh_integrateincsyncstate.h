#ifndef _ONLINEREFRESH_INTEGRATEINCSYNC_H
#define _ONLINEREFRESH_INTEGRATEINCSYNC_H

typedef struct ONLINEREFRESH_INTEGRATEINCSYNC
{
    syncstate  base;
    cache_txn* rebuild2sync;
} onlinerefresh_integrateincsync;

onlinerefresh_integrateincsync* onlinerefresh_integrateincsync_init(void);

void* onlinerefresh_integrateincsync_main(void* args);

void onlinerefresh_integrateincsync_free(void* args);

#endif
