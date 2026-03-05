#ifndef _RIPPLE_ONLINEREFRESH_INTEGRATEPARSERTRAIL_H
#define _RIPPLE_ONLINEREFRESH_INTEGRATEPARSERTRAIL_H

typedef struct RIPPLE_ONLINEREFRESH_INTEGRATEPARSERTRAIL
{
    ripple_increment_integrateparsertrail*      decodingctx;
}ripple_onlinerefresh_integrateparsertrail;

ripple_onlinerefresh_integrateparsertrail* ripple_onlinerefresh_integrateparsertrail_init(void);

void *ripple_onlinerefresh_integrateparsertrail_main(void* args);

void ripple_onlinerefresh_integrateparsertrail_free(void* args);

#endif
