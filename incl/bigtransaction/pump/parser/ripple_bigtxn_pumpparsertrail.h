#ifndef _RIPPLE_BIGTXN_PUMPPARSERTRAIL_H
#define _RIPPLE_BIGTXN_PUMPPARSERTRAIL_H

typedef struct RIPPLE_BIGTXNIN_PUMPPARSERTRAIL
{
    ripple_increment_pumpparsertrail   *pumpparsertrail;
}ripple_bigtxn_pumpparsertrail;

extern ripple_bigtxn_pumpparsertrail* ripple_bigtxn_pumpparsertrail_init(void);

extern void *ripple_bigtxn_pumpparsertrail_main(void *args);

extern void ripple_bigtxn_pumpparsertrail_free(void *args);

#endif
