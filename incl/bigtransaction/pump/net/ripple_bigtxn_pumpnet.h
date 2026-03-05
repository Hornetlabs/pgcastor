#ifndef _RIPPLE_BIGTXN_PUMPNET_H
#define _RIPPLE_BIGTXN_PUMPNET_H

typedef struct RIPPLE_BIGTXN_PUMPNET
{
    FullTransactionId                   xid;
    ripple_increment_pumpnetstate      *clientstate;
}ripple_bigtxn_pumpnet;

extern ripple_bigtxn_pumpnet* ripple_bigtxn_pumpnet_init(void);

extern void *ripple_bigtxn_pumpnet_main(void *args);

extern void ripple_bigtxn_pumpnet_free(void *args);

#endif
