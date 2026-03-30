#ifndef _ONLINEREFRESH_INTEGRATEPARSERTRAIL_H
#define _ONLINEREFRESH_INTEGRATEPARSERTRAIL_H

typedef struct ONLINEREFRESH_INTEGRATEPARSERTRAIL
{
    increment_integrateparsertrail* decodingctx;
} onlinerefresh_integrateparsertrail;

onlinerefresh_integrateparsertrail* onlinerefresh_integrateparsertrail_init(void);

void* onlinerefresh_integrateparsertrail_main(void* args);

void onlinerefresh_integrateparsertrail_free(void* args);

#endif
