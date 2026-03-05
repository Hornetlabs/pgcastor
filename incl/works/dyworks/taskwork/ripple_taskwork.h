#ifndef _RIPPLE_TASKWORK_H
#define _RIPPLE_TASKWORK_H

extern void* ripple_taskwork_main(void* args);

extern void ripple_taskwork_free(void* privdata);

extern void ripple_taskwork_slot_free(ripple_task_slot *slot);

#endif
