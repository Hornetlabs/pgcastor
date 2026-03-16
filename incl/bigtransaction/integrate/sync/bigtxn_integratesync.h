#ifndef _RIPPLE_BIGTXN_INTEGRATEINCSYNCSTATE_H
#define _RIPPLE_BIGTXN_INTEGRATEINCSYNCSTATE_H

typedef struct RIPPLE_BIGTXN_INTEGRATEINCSYNC
{
    ripple_syncstate                base;
    ripple_cache_txn*               rebuild2sync;
}ripple_bigtxn_integrateincsync;


ripple_bigtxn_integrateincsync* ripple_bigtxn_integrateincsync_init(void);

void *ripple_bigtxn_integrateincsync_main(void *args);

void ripple_bigtxn_integrateincsync_free(void *args);

#endif
