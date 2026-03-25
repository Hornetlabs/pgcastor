#ifndef _BIGTXN_INTEGRATEINCSYNCSTATE_H
#define _BIGTXN_INTEGRATEINCSYNCSTATE_H

typedef struct BIGTXN_INTEGRATEINCSYNC
{
    syncstate  base;
    cache_txn* rebuild2sync;
} bigtxn_integrateincsync;

bigtxn_integrateincsync* bigtxn_integrateincsync_init(void);

void* bigtxn_integrateincsync_main(void* args);

void bigtxn_integrateincsync_free(void* args);

#endif
