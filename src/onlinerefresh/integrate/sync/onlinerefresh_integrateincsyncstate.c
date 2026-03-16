#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "sync/ripple_sync.h"
#include "onlinerefresh/integrate/sync/ripple_onlinerefresh_integrateincsyncstate.h"

ripple_onlinerefresh_integrateincsync* ripple_onlinerefresh_integrateincsync_init(void)
{
    ripple_onlinerefresh_integrateincsync* syncworkstate = NULL;

    syncworkstate = (ripple_onlinerefresh_integrateincsync*)rmalloc0(sizeof(ripple_onlinerefresh_integrateincsync));
    if(NULL == syncworkstate)
    {
        elog(RLOG_WARNING, "onlinerefresh integrateincsync malloc out of memory");
        return NULL;
    }
    rmemset0(syncworkstate, 0, '\0', sizeof(ripple_onlinerefresh_integrateincsync));
    ripple_syncstate_reset((ripple_syncstate*) syncworkstate);

    return syncworkstate;
}

/* 删除状态表中增量数据 */
static bool ripple_onlinerefresh_integrateincsync_delinc(ripple_onlinerefresh_integrateincsync* syncwork)
{
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    char sql_exec[1024]     = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "DELETE FROM \"%s\".\"%s\" WHERE \"name\" = \'%s\';",
                      guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                      RIPPLE_SYNC_STATUSTABLE_NAME,
                      syncwork->base.name);
    res = PQexec(syncwork->base.conn, sql_exec);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,"Failed to update status table in: %s", PQerrorMessage(syncwork->base.conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    PQfinish(conn);

    return true;
}

/* 增量应用 */
void *ripple_onlinerefresh_integrateincsync_main(void* args)
{
    int timeout                                                 = 0;
    ripple_txn* entry                                           = NULL;
    ripple_thrnode* thrnode                                     = NULL;
    ripple_syncstate* syncstate                                 = NULL;
    ripple_onlinerefresh_integrateincsync* syncwork             = NULL;

    thrnode = (ripple_thrnode*)args;

    syncwork = (ripple_onlinerefresh_integrateincsync*)thrnode->data;

    syncstate = (ripple_syncstate*)syncwork;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate spliittrail stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while (ripple_syncstate_conn(syncstate, thrnode))
    {
        usleep(50000);
        if(false == ripple_syncstate_update_statustb(syncstate, NULL, false))
        {
            continue;
        }
        break;
    }

    while(1)
    {
        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        entry = NULL;
        /* 获取数据 */
        entry = ripple_cache_txn_get(syncwork->rebuild2sync, &timeout);
        if(NULL == entry)
        {
            /* 超时继续执行 */
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            elog(RLOG_WARNING, "onlinerefresh integrate sync get txn error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 提前处理无法避免增量结束的lsn和xid无效，并且增量结束位置信息不需要更新 */
        if (RIPPLE_TXN_TYPE_ONLINEREFRESH_INC_END == entry->type)
        {
            ripple_onlinerefresh_integrateincsync_delinc(syncwork);
            ripple_txn_free(entry);
            rfree(entry);
            entry = NULL;
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 同步数据 */
        while(false == ripple_syncstate_applytxn(syncstate, thrnode, (void*)entry, true))
        {
            sleep(1);
            ripple_syncstate_reset(syncstate);
            while (ripple_syncstate_conn(syncstate, thrnode))
            {
                usleep(50000);
                if(false == ripple_syncstate_update_statustb(syncstate, NULL, false))
                {
                    continue;
                }
                break;
            }

            if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
            {
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                break;
            }
        }

        /* TODO entry 释放 */
        ripple_txn_free(entry);
        rfree(entry);
        entry = NULL;
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_integrateincsync_free(void *args)
{
    ripple_onlinerefresh_integrateincsync* syncwork = NULL;

    syncwork = (ripple_onlinerefresh_integrateincsync*)args;

    if (NULL == syncwork)
    {
        return;
    }
    ripple_syncstate_destroy((ripple_syncstate*) syncwork);

    rfree(syncwork);

    return;

}