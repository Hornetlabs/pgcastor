#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_tables.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "storage/file_buffer.h"
#include "refresh/sharding2file/refresh_sharding2file.h"

void taskslot_stat_setidle(task_slot* slot)
{
    slot->stat = TASKSLOT_IDLE;
}

void taskslot_stat_setwork(task_slot* slot)
{
    slot->stat = TASKSLOT_WORK;
}

void taskslot_stat_setterm(task_slot* slot)
{
    slot->stat = TASKSLOT_TERM;
}

void taskslot_stat_setexit(task_slot* slot)
{
    slot->stat = TASKSLOT_EXIT;
}

int taskslot_stat_get(task_slot* slot)
{
    return slot->stat;
}

task_slots* taskslots_init(void)
{
    task_slots* slots = NULL;

    slots = (task_slots*)rmalloc0(sizeof(task_slots));
    if(NULL == slots)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(slots, 0, 0, sizeof(task_slots));

    return slots;
}

task_slot* taskslot_init(int cnt)
{
    task_slot* slot = NULL;

    if (0 == cnt)
    {
        return NULL;
    }

    slot = (task_slot*)rmalloc0(sizeof(task_slot) * cnt);
    if(NULL == slot)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(slot, 0, 0, sizeof(task_slot) * cnt);

    return slot;
}
