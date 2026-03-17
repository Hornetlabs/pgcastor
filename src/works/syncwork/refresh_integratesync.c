#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "queue/queue.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "sync/sync.h"
#include "works/syncwork/refresh_integratesync.h"
#include "task/task_slot.h"
#include "refresh/p2csharding/refresh_p2csharding.h"

/* 增量应用结构初始化 */
refresh_integratesyncstate* refresh_integratesyncstate_init(void)
{
    refresh_integratesyncstate* refreshsyncstate = NULL;

    refreshsyncstate = (refresh_integratesyncstate*)rmalloc0(sizeof(refresh_integratesyncstate));
    if(NULL == refreshsyncstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(refreshsyncstate, 0, '\0', sizeof(refresh_integratesyncstate));

    syncstate_reset((syncstate*) refreshsyncstate);
    refreshsyncstate->queue = NULL;
    refreshsyncstate->tablesyncstats = NULL;

    return refreshsyncstate;
}

/* 增量应用结构资源释放 */
void refresh_integratesync_destroy(refresh_integratesyncstate* syncworkstate)
{
    if (NULL == syncworkstate)
    {
        return;
    }

    syncstate_destroy((syncstate*) syncworkstate);

    rfree(syncworkstate);

}