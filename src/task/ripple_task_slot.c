#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_tables.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "storage/ripple_file_buffer.h"
#include "works/dyworks/ripple_dyworks.h"
#include "refresh/sharding2file/ripple_refresh_sharding2file.h"

void ripple_taskslot_stat_setidle(ripple_task_slot* slot)
{
    slot->stat = RIPPLE_TASKSLOT_IDLE;
}

void ripple_taskslot_stat_setwork(ripple_task_slot* slot)
{
    slot->stat = RIPPLE_TASKSLOT_WORK;
}

void ripple_taskslot_stat_setterm(ripple_task_slot* slot)
{
    slot->stat = RIPPLE_TASKSLOT_TERM;
}

void ripple_taskslot_stat_setexit(ripple_task_slot* slot)
{
    slot->stat = RIPPLE_TASKSLOT_EXIT;
}

int ripple_taskslot_stat_get(ripple_task_slot* slot)
{
    return slot->stat;
}

ripple_task_slots* ripple_taskslots_init(void)
{
    ripple_task_slots* slots = NULL;

    slots = (ripple_task_slots*)rmalloc0(sizeof(ripple_task_slots));
    if(NULL == slots)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(slots, 0, 0, sizeof(ripple_task_slots));

    return slots;
}

ripple_task_slot* ripple_taskslot_init(int cnt)
{
    ripple_task_slot* slot = NULL;

    if (0 == cnt)
    {
        return NULL;
    }

    slot = (ripple_task_slot*)rmalloc0(sizeof(ripple_task_slot) * cnt);
    if(NULL == slot)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(slot, 0, 0, sizeof(ripple_task_slot) * cnt);

    return slot;
}
