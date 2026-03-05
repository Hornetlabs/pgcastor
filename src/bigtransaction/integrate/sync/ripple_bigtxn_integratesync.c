#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "threads/ripple_threads.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "stmts/ripple_txnstmt.h"
#include "sync/ripple_sync.h"
#include "bigtransaction/integrate/sync/ripple_bigtxn_integratesync.h"

/* 错误处理，重新执行所有stmt */
static bool ripple_bigtxn_integrateincsync_restart_applytxn(ripple_syncstate* syncstate, ripple_thrnode* thrnode, ripple_txn* cur_txn)
{
    if (0 == cur_txn->stmts->length)
    {
        return true;
    }

    return ripple_syncstate_bigtxn_applytxn(syncstate, thrnode, (void*)cur_txn);
}

#if 0
/* 删除状态表中增量数据 */
static bool ripple_bigtxn_integrateincsync_delinc(ripple_bigtxn_integrateincsync* syncwork)
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
#endif

/* 更新状态表refresh任务的状态 */
static bool ripple_bigtxn_integrateincsync_updatasyncstatus(ripple_bigtxn_integrateincsync* syncwork, int16 stat)
{
    PGresult  *res          = NULL;
    char sql_exec[1024]     = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "UPDATE %s.%s SET \"stat\" = %hd WHERE \"name\" = \'%s\' ",
                      guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                      RIPPLE_SYNC_STATUSTABLE_NAME,
                      stat,
                      syncwork->base.name);
    res = PQexec(syncwork->base.conn, sql_exec);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,"Failed to update status table in: %s", PQerrorMessage(syncwork->base.conn));
        PQclear(res);
        return false;
    }
    PQclear(res);

    return true;
}

ripple_bigtxn_integrateincsync* ripple_bigtxn_integrateincsync_init(void)
{
    ripple_bigtxn_integrateincsync* syncworkstate = NULL;

    syncworkstate = (ripple_bigtxn_integrateincsync*)rmalloc0(sizeof(ripple_bigtxn_integrateincsync));
    if(NULL == syncworkstate)
    {
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }
    rmemset0(syncworkstate, 0, '\0', sizeof(ripple_bigtxn_integrateincsync));
    ripple_syncstate_reset((ripple_syncstate*) syncworkstate);

    return syncworkstate;
}

/* 增量应用 */
void *ripple_bigtxn_integrateincsync_main(void* args)
{
    int timeout                                         = 0;
    ripple_txn* entry                                   = NULL;
    ripple_txn* cur_txn                                 = NULL;
    ripple_thrnode* thrnode                             = NULL;
    ripple_syncstate* syncstate                         = NULL;
    ripple_bigtxn_integrateincsync* syncwork            = NULL;

    thrnode = (ripple_thrnode*)args;

    syncwork = (ripple_bigtxn_integrateincsync*)thrnode->data;

    syncstate = (ripple_syncstate*)syncwork;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn sync stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while (ripple_syncstate_conn(syncstate, thrnode))
    {
        usleep(50000);
        if(false == ripple_sync_txnbegin(syncstate))
        {
            continue;
        }

        if(false == ripple_syncstate_update_statustb(syncstate, NULL, false))
        {
            continue;
        }

        /* 更新状态表状态，可以优先解析到并过滤 */
        if (false == ripple_bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
        {
            continue;
        }

        break;
    }

    cur_txn = ripple_txn_init(InvalidFullTransactionId, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "integrate bigtxn sync cur_txn out of memory ");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    while(1)
    {
        entry = NULL;

        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        entry = ripple_cache_txn_get(syncwork->rebuild2sync, &timeout);
        if(NULL == entry)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }

             /* 出错了 */
            elog(RLOG_WARNING, "cache_txn_get error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(NULL != entry->stmts)
        {
            while(false == ripple_syncstate_bigtxn_applytxn(syncstate, thrnode, (void*)entry))
            {
                sleep(1);
                ripple_syncstate_reset(syncstate);
                while (ripple_syncstate_conn(syncstate, thrnode))
                {
                    usleep(50000);
                    if(false == ripple_sync_txnbegin(syncstate))
                    {
                        continue;
                    }

                    if(false == ripple_syncstate_update_statustb(syncstate, NULL, false))
                    {
                        continue;
                    }

                    /* 更新状态表状态，capture可以优先解析到并过滤 */
                    if (false == ripple_bigtxn_integrateincsync_updatasyncstatus(syncwork, 1))
                    {
                        continue;
                    }

                    if(false == ripple_bigtxn_integrateincsync_restart_applytxn(syncstate, thrnode, cur_txn))
                    {
                        continue;
                    }
                    break;
                }

                if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
                {
                    thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                    goto ripple_bigtxn_integrateincsync_main_exit;
                }
            }
        }

        if (true == entry->commit)
        {
            while (false == ripple_bigtxn_integrateincsync_updatasyncstatus(syncwork, 1))
            {
                sleep(1);
                ripple_syncstate_reset(syncstate);
                while (ripple_syncstate_conn(syncstate, thrnode))
                {
                    if(false == ripple_sync_txnbegin(syncstate))
                    {
                        continue;
                    }

                    if(false == ripple_syncstate_update_statustb(syncstate, NULL, false))
                    {
                        continue;
                    }

                    /* 更新状态表状态，capture可以优先解析到并过滤 */
                    if (false == ripple_bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
                    {
                        continue;
                    }

                    if(false == ripple_bigtxn_integrateincsync_restart_applytxn(syncstate, thrnode, cur_txn))
                    {
                        continue;
                    }

                    break;
                }

                if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
                {
                    thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                    goto ripple_bigtxn_integrateincsync_main_exit;
                }
            }

            while(false == ripple_sync_txncommit(syncstate, (void*)entry))
            {
                sleep(1);
                ripple_syncstate_reset(syncstate);
                while (ripple_syncstate_conn(syncstate, thrnode))
                {
                    if(false == ripple_sync_txnbegin(syncstate))
                    {
                        continue;
                    }

                    if(false == ripple_syncstate_update_statustb(syncstate, NULL, false))
                    {
                        continue;
                    }

                    /* 更新状态表状态，capture可以优先解析到并过滤 */
                    if (false == ripple_bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
                    {
                        continue;
                    }

                    if(false == ripple_bigtxn_integrateincsync_restart_applytxn(syncstate, thrnode, cur_txn))
                    {
                        continue;
                    }

                    break;
                }

                if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
                {
                    thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                    goto ripple_bigtxn_integrateincsync_main_exit;
                }
            }
            // ripple_bigtxn_integrateincsync_delinc(syncwork);
            elog(RLOG_INFO, "bigtxn commit %lu", entry->xid);

            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            goto ripple_bigtxn_integrateincsync_main_exit;
        }

        /* TODO entry 释放 */
        cur_txn->stmts = list_concat(cur_txn->stmts, entry->stmts);
        if (cur_txn->stmts != entry->stmts)
        {
            rfree(entry->stmts);
        }
        entry->stmts = NULL;
        ripple_txn_free(entry);
        rfree(entry);
    }
/* 退出 */
ripple_bigtxn_integrateincsync_main_exit:

    ripple_txn_free(entry);
    rfree(entry);
    ripple_txn_free(cur_txn);
    rfree(cur_txn);

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_bigtxn_integrateincsync_free(void *args)
{
    ripple_bigtxn_integrateincsync* syncwork = NULL;

    syncwork = (ripple_bigtxn_integrateincsync*)args;

    ripple_syncstate_destroy((ripple_syncstate*) syncwork);

    rfree(syncwork);

}
