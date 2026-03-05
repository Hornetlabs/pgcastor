#ifndef _RIPPLE_ONLINEREFRESH_INTEGRATEINCSYNC_H
#define _RIPPLE_ONLINEREFRESH_INTEGRATEINCSYNC_H

typedef struct RIPPLE_ONLINEREFRESH_INTEGRATEINCSYNC
{
    ripple_syncstate                base;
    ripple_cache_txn*               rebuild2sync;
}ripple_onlinerefresh_integrateincsync;

ripple_onlinerefresh_integrateincsync* ripple_onlinerefresh_integrateincsync_init(void);

void *ripple_onlinerefresh_integrateincsync_main(void *args);

void ripple_onlinerefresh_integrateincsync_free(void *args);


#endif
