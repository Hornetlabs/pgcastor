#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "queue/ripple_queue.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "sync/ripple_sync.h"
#include "works/syncwork/ripple_refresh_integratesync.h"
#include "task/ripple_task_slot.h"
#include "works/dyworks/ripple_dyworks.h"
#include "refresh/p2csharding/ripple_refresh_p2csharding.h"


/* 增量应用结构初始化 */
ripple_refresh_integratesyncstate* ripple_refresh_integratesyncstate_init(void)
{
    ripple_refresh_integratesyncstate* refreshsyncstate = NULL;

    refreshsyncstate = (ripple_refresh_integratesyncstate*)rmalloc0(sizeof(ripple_refresh_integratesyncstate));
    if(NULL == refreshsyncstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(refreshsyncstate, 0, '\0', sizeof(ripple_refresh_integratesyncstate));

    ripple_syncstate_reset((ripple_syncstate*) refreshsyncstate);
    refreshsyncstate->queue = NULL;
    refreshsyncstate->tablesyncstats = NULL;

    return refreshsyncstate;
}

/* 增量应用结构资源释放 */
void ripple_refresh_integratesync_destroy(ripple_refresh_integratesyncstate* syncworkstate)
{
    if (NULL == syncworkstate)
    {
        return;
    }

    ripple_syncstate_destroy((ripple_syncstate*) syncworkstate);

    rfree(syncworkstate);

}