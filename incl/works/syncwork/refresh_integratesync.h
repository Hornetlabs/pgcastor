#ifndef _REFRESH_INTEGRATESYNC_H
#define _REFRESH_INTEGRATESYNC_H

typedef struct refresh_integratesyncstate
{
    syncstate                base;
    queue*                   queue;
    refresh_table_syncstats* tablesyncstats;
} refresh_integratesyncstate;

refresh_integratesyncstate* refresh_integratesyncstate_init(void);

void refresh_integratesync_destroy(refresh_integratesyncstate* syncworkstate);

#endif
