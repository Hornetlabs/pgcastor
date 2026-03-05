#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/dlist/dlist.h"
#include "utils/mpage/mpage.h"
#include "utils/string/stringinfo.h"
#include "sync/ripple_sync.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "serial/ripple_serial.h"
#include "rebuild/ripple_rebuild.h"
#include "works/dyworks/ripple_dyworks.h"
#include "refresh/ripple_refresh_tables.h"
#include "works/dyworks/taskwork/ripple_taskwork.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "works/syncwork/ripple_refresh_integratesync.h"
#include "refresh/p2csharding/ripple_refresh_p2csharding.h"
#include "refresh/sharding2db/ripple_refresh_sharding2db.h"
#include "refresh/sharding2file/ripple_refresh_sharding2file.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "parser/trail/ripple_parsertrail.h"
#include "increment/pump/split/ripple_increment_pumpsplittrail.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"
#include "onlinerefresh/pump/netsharding/ripple_onlinerefresh_shardingnet.h"
#include "onlinerefresh/pump/splittrail/ripple_onlinerefresh_pumpsplittrail.h"
#include "onlinerefresh/pump/parsertrail/ripple_onlinerefresh_pumpparsertrail.h"
#include "onlinerefresh/pump/serial/ripple_onlinerefresh_pumpserial.h"
#include "onlinerefresh/pump/netincrement/ripple_onlinerefresh_pumpnet.h"


typedef void (*taskworkmain)(ripple_dyworks_node *dyworknode);
typedef void (*taskworkfree)(ripple_task_slot *slot);

typedef struct RIPPLE_TASKWORK_START
{
    ripple_task_type        type;
    taskworkmain            work;
    taskworkfree            free;
}ripple_taskwork_start;

static ripple_taskwork_start m_taskwork[] =
{
    {
        RIPPLE_TASKTYPE_NOP,
        NULL,
        NULL
    },
    {
        RIPPLE_TASKTYPE_REFRESHSHARDING,
        NULL,
        NULL
    },
    {
        RIPPLE_TASKTYPE_REFRESHP2CSHARDING,
        NULL, 
        NULL
    },
    {
        RIPPLE_TASKTYPE_REFRESHSHARDING2DB,
        NULL,
        NULL
    },
    {
        RIPPLE_TASKTYPE_ONLINEREFRESH_CAPTURE_PARSERWAL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASKTYPE_ONLINEREFRESH_CAPTURE_SPLITWAL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASKTYPE_ONLINEREFRESH_CAPTURE_SERIAL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASKTYPE_ONLINEREFRESH_CAPTURE_WRITE,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_INTEGRATE_ONLINEREFRESH_SPLITTRAIL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_INTEGRATE_ONLINEREFRESH_PARSERTRAIL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_INTEGRATE_ONLINEREFRESH_REBUILD,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_INTEGRATE_ONLINEREFRESH_SYNC,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_SPLITTRAIL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_PARSERTRAIL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_SERIAL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_CLIENT,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_PUMP_ONLINEREFRESH_P2CSHARDING,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_INTEGRATE_BIGTXN_SPLITTRAIL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_INTEGRATE_BIGTXN_PARSERTRAIL,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_INTEGRATE_BIGTXN_SYNC,
        NULL,
        NULL
    },
    {
        RIPPLE_TASK_TYPE_INTEGRATE_BIGTXN_REBUILD,
        NULL,
        NULL
    }
};

static int m_taskworksize = (sizeof(m_taskwork)/sizeof(ripple_taskwork_start));

void* ripple_taskwork_main(void* args)
{
    ripple_dyworks_node* dyworknode = (ripple_dyworks_node *)args;
    ripple_task_slot *slot = (ripple_task_slot *)dyworknode->data;

    /* 设置为启动状态 */
    dyworknode->status = RIPPLE_WORK_STATUS_WORK;

    if (slot->task->type > m_taskworksize)
    {
        elog(RLOG_ERROR, "wrong task type:%d", slot->task->type);
    }

    if (NULL == m_taskwork[slot->task->type].work)
    {
        elog(RLOG_ERROR, " taskwork unsupported type:%d", slot->task->type);
    }

    m_taskwork[slot->task->type].work(dyworknode);

    /* make compiler happy */
    return NULL;
}

/* 释放 */
void ripple_taskwork_slot_free(ripple_task_slot *slot)
{
    if (!slot)
    {
        return;
    }

    if (!slot->task)
    {
        return;
    }

    if (slot->task->type > m_taskworksize)
    {
        elog(RLOG_ERROR, "wrong task type:%d", slot->task->type);
    }

    if (NULL == m_taskwork[slot->task->type].free)
    {
        elog(RLOG_ERROR, " taskwork unsupported type:%d", slot->task->type);
    }

    m_taskwork[slot->task->type].free(slot);
}

/* 释放 */
void ripple_taskwork_free(void* privdata)
{
 /* do nothing, already free in mgr free */
}
