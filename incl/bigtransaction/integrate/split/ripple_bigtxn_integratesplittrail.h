#ifndef _RIPPLE_BIGTXN_INTEGRATESPLITTRAIL_H
#define _RIPPLE_BIGTXN_INTEGRATESPLITTRAIL_H

typedef struct RIPPLE_BIGTXN_INTEGRATESPLITTRAIL
{
    ripple_increment_integratesplittrail*   splittrailctx;
}ripple_bigtxn_integratesplittrail;

ripple_bigtxn_integratesplittrail* ripple_bigtxn_integratesplittrail_init(void);

void *ripple_bigtxn_integratesplittrail_main(void* args);

void ripple_bigtxn_integratesplittrail_free(void* args);

#endif

