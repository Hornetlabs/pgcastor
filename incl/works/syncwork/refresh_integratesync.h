#ifndef _RIPPLE_REFRESH_INTEGRATESYNC_H
#define _RIPPLE_REFRESH_INTEGRATESYNC_H

typedef struct ripple_refresh_integratesyncstate
{
    ripple_syncstate                    base;
    ripple_queue*                       queue;
    ripple_refresh_table_syncstats*     tablesyncstats;
} ripple_refresh_integratesyncstate;

ripple_refresh_integratesyncstate* ripple_refresh_integratesyncstate_init(void);

void ripple_refresh_integratesync_destroy(ripple_refresh_integratesyncstate* syncworkstate);

#endif
