#ifndef _RIPPLE_BIGTXN_PUMPSERIAL_H
#define _RIPPLE_BIGTXN_PUMPSERIAL_H

typedef struct RIPPLE_BIGTXN_PUMPSERIAL
{
    ripple_increment_pumpserialstate   *serialstate;
}ripple_bigtxn_pumpserial;

extern ripple_bigtxn_pumpserial* ripple_bigtxn_pumpserial_init(void);

extern void *ripple_bigtxn_pumpserial_main(void *args);

extern void ripple_bigtxn_pumpserial_free(void *args);

#endif
