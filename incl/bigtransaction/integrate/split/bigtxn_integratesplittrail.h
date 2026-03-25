#ifndef _BIGTXN_INTEGRATESPLITTRAIL_H
#define _BIGTXN_INTEGRATESPLITTRAIL_H

typedef struct BIGTXN_INTEGRATESPLITTRAIL
{
    increment_integratesplittrail* splittrailctx;
} bigtxn_integratesplittrail;

bigtxn_integratesplittrail* bigtxn_integratesplittrail_init(void);

void* bigtxn_integratesplittrail_main(void* args);

void bigtxn_integratesplittrail_free(void* args);

#endif
