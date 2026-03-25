#ifndef _ONLINEREFRESH_INTEGRATESPLITTRAIL_H
#define _ONLINEREFRESH_INTEGRATESPLITTRAIL_H

typedef struct ONLINEREFRESH_INTEGRATESPLITTRAIL
{
    increment_integratesplittrail* splittrailctx;
} onlinerefresh_integratesplittrail;

onlinerefresh_integratesplittrail* onlinerefresh_integratesplittrail_init(void);

void* onlinerefresh_integratesplittrail_main(void* args);

void onlinerefresh_integratesplittrail_free(void* args);

#endif
