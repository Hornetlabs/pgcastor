#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "threads/threads.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "sync/sync.h"
#include "bigtransaction/integrate/sync/bigtxn_integratesync.h"

/* 错误处理，重新执行所有stmt */
static bool bigtxn_integrateincsync_restart_applytxn(syncstate* syncstate, thrnode* thrnode, txn* cur_txn)
{
    if (0 == cur_txn->stmts->length)
    {
        return true;
    }

    return syncstate_bigtxn_applytxn(syncstate, thrnode, (void*)cur_txn);
}

#if 0
/* 删除状态表中增量数据 */
static bool bigtxn_integrateincsync_delinc(bigtxn_integrateincsync* syncwork)
{
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    char sql_exec[1024]     = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "DELETE FROM \"%s\".\"%s\" WHERE \"name\" = \'%s\';",
                      guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
                      SYNC_STATUSTABLE_NAME,
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
static bool bigtxn_integrateincsync_updatasyncstatus(bigtxn_integrateincsync* syncwork, int16 stat)
{
    PGresult  *res          = NULL;
    char sql_exec[1024]     = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "UPDATE %s.%s SET \"stat\" = %hd WHERE \"name\" = \'%s\' ",
                      guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
                      SYNC_STATUSTABLE_NAME,
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

bigtxn_integrateincsync* bigtxn_integrateincsync_init(void)
{
    bigtxn_integrateincsync* syncworkstate = NULL;

    syncworkstate = (bigtxn_integrateincsync*)rmalloc0(sizeof(bigtxn_integrateincsync));
    if(NULL == syncworkstate)
    {
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }
    rmemset0(syncworkstate, 0, '\0', sizeof(bigtxn_integrateincsync));
    syncstate_reset((syncstate*) syncworkstate);

    return syncworkstate;
}

/* 增量应用 */
void *bigtxn_integrateincsync_main(void* args)
{
    int timeout                                         = 0;
    txn* entry                                   = NULL;
    txn* cur_txn                                 = NULL;
    thrnode* thr_node                             = NULL;
    syncstate* sync_state                         = NULL;
    bigtxn_integrateincsync* syncwork            = NULL;

    thr_node = (thrnode*)args;

    syncwork = (bigtxn_integrateincsync*)thr_node->data;

    sync_state = (syncstate*)syncwork;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn sync stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    while (syncstate_conn(sync_state, thr_node))
    {
        usleep(50000);
        if(false == sync_txnbegin(sync_state))
        {
            continue;
        }

        if(false == syncstate_update_statustb(sync_state, NULL, false))
        {
            continue;
        }

        /* 更新状态表状态，可以优先解析到并过滤 */
        if (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
        {
            continue;
        }

        break;
    }

    cur_txn = txn_init(InvalidFullTransactionId, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if(NULL == cur_txn)
    {
        elog(RLOG_WARNING, "integrate bigtxn sync cur_txn out of memory ");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    while(1)
    {
        entry = NULL;

        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            /* 序列化/落盘 */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据 */
        entry = cache_txn_get(syncwork->rebuild2sync, &timeout);
        if(NULL == entry)
        {
            if(ERROR_TIMEOUT == timeout)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }

             /* 出错了 */
            elog(RLOG_WARNING, "cache_txn_get error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if(NULL != entry->stmts)
        {
            while(false == syncstate_bigtxn_applytxn(sync_state, thr_node, (void*)entry))
            {
                sleep(1);
                syncstate_reset(sync_state);
                while (syncstate_conn(sync_state, thr_node))
                {
                    usleep(50000);
                    if(false == sync_txnbegin(sync_state))
                    {
                        continue;
                    }

                    if(false == syncstate_update_statustb(sync_state, NULL, false))
                    {
                        continue;
                    }

                    /* 更新状态表状态，capture可以优先解析到并过滤 */
                    if (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 1))
                    {
                        continue;
                    }

                    if(false == bigtxn_integrateincsync_restart_applytxn(sync_state, thr_node, cur_txn))
                    {
                        continue;
                    }
                    break;
                }

                if(THRNODE_STAT_TERM == thr_node->stat)
                {
                    thr_node->stat = THRNODE_STAT_EXIT;
                    goto bigtxn_integrateincsync_main_exit;
                }
            }
        }

        if (true == entry->commit)
        {
            while (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 1))
            {
                sleep(1);
                syncstate_reset(sync_state);
                while (syncstate_conn(sync_state, thr_node))
                {
                    if(false == sync_txnbegin(sync_state))
                    {
                        continue;
                    }

                    if(false == syncstate_update_statustb(sync_state, NULL, false))
                    {
                        continue;
                    }

                    /* 更新状态表状态，capture可以优先解析到并过滤 */
                    if (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
                    {
                        continue;
                    }

                    if(false == bigtxn_integrateincsync_restart_applytxn(sync_state, thr_node, cur_txn))
                    {
                        continue;
                    }

                    break;
                }

                if(THRNODE_STAT_TERM == thr_node->stat)
                {
                    thr_node->stat = THRNODE_STAT_EXIT;
                    goto bigtxn_integrateincsync_main_exit;
                }
            }

            while(false == sync_txncommit(sync_state, (void*)entry))
            {
                sleep(1);
                syncstate_reset(sync_state);
                while (syncstate_conn(sync_state, thr_node))
                {
                    if(false == sync_txnbegin(sync_state))
                    {
                        continue;
                    }

                    if(false == syncstate_update_statustb(sync_state, NULL, false))
                    {
                        continue;
                    }

                    /* 更新状态表状态，capture可以优先解析到并过滤 */
                    if (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
                    {
                        continue;
                    }

                    if(false == bigtxn_integrateincsync_restart_applytxn(sync_state, thr_node, cur_txn))
                    {
                        continue;
                    }

                    break;
                }

                if(THRNODE_STAT_TERM == thr_node->stat)
                {
                    thr_node->stat = THRNODE_STAT_EXIT;
                    goto bigtxn_integrateincsync_main_exit;
                }
            }
            // bigtxn_integrateincsync_delinc(syncwork);
            elog(RLOG_INFO, "bigtxn commit %lu", entry->xid);

            thr_node->stat = THRNODE_STAT_EXIT;
            goto bigtxn_integrateincsync_main_exit;
        }

        /* TODO entry 释放 */
        cur_txn->stmts = list_concat(cur_txn->stmts, entry->stmts);
        if (cur_txn->stmts != entry->stmts)
        {
            rfree(entry->stmts);
        }
        entry->stmts = NULL;
        txn_free(entry);
        rfree(entry);
    }
/* 退出 */
bigtxn_integrateincsync_main_exit:

    txn_free(entry);
    rfree(entry);
    txn_free(cur_txn);
    rfree(cur_txn);

    pthread_exit(NULL);
    return NULL;
}

void bigtxn_integrateincsync_free(void *args)
{
    bigtxn_integrateincsync* syncwork = NULL;

    syncwork = (bigtxn_integrateincsync*)args;

    syncstate_destroy((syncstate*) syncwork);

    rfree(syncwork);

}
