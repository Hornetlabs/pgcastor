#ifndef _RIPPLE_ONLINEREFRESH_PUMPPARSERTRAIL_H
#define _RIPPLE_ONLINEREFRESH_PUMPPARSERTRAIL_H

typedef struct RIPPLE_TASK_ONLINEREFRESHPUMPPARSERTRAIL
{
    ripple_increment_pumpparsertrail*       decodingctx;
}ripple_task_onlinerefreshpumpparsertrail;

ripple_task_onlinerefreshpumpparsertrail* ripple_onlinerefresh_pumpparsertrail_init(void);

void* ripple_onlinerefresh_pumpparsertrail_main(void* args);

void ripple_onlinerefresh_pumpparsertrail_free(void* args);

#endif
