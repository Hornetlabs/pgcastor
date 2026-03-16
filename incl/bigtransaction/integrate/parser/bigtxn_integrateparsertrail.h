#ifndef _RIPPLE_BIGTXN_INTEGRATEPARSERTRAIL_H
#define _RIPPLE_BIGTXN_INTEGRATEPARSERTRAIL_H

typedef struct RIPPLE_BIGTXNIN_TEGRATEPARSERTRAIL
{
    uint64                                      integrateparser_buffer;
    ripple_increment_integrateparsertrail*      decodingctx;
}ripple_bigtxn_integrateparsertrail;

ripple_bigtxn_integrateparsertrail* ripple_bigtxn_integrateparsertrail_init(void);

void *ripple_bigtxn_integrateparsertrail_main(void *args);

void ripple_bigtxn_integrateparsertrail_free(void *args);

#endif
