#ifndef _RIPPLE_ONLINEREFRESH_PUMPSPLITTRAIL_H
#define _RIPPLE_ONLINEREFRESH_PUMPSPLITTRAIL_H

typedef struct RIPPLE_TASK_ONLINEREFRESHPUMPSPLITTRAIL
{
    ripple_uuid_t                       onlinerefreshno;
    ripple_increment_pumpsplittrail*    splittrailctx;
}ripple_task_onlinerefreshpumpsplittrail;

ripple_task_onlinerefreshpumpsplittrail* ripple_onlinerefresh_pumpsplittrail_init(void);

void* ripple_onlinerefresh_pumpsplittrail_main(void* args);

void ripple_onlinerefresh_pumpsplittrail_free(void* args);

#endif