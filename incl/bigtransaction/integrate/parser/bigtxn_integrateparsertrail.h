#ifndef _BIGTXN_INTEGRATEPARSERTRAIL_H
#define _BIGTXN_INTEGRATEPARSERTRAIL_H

typedef struct BIGTXNIN_TEGRATEPARSERTRAIL
{
    uint64                                      integrateparser_buffer;
    increment_integrateparsertrail*      decodingctx;
}bigtxn_integrateparsertrail;

bigtxn_integrateparsertrail* bigtxn_integrateparsertrail_init(void);

void *bigtxn_integrateparsertrail_main(void *args);

void bigtxn_integrateparsertrail_free(void *args);

#endif
