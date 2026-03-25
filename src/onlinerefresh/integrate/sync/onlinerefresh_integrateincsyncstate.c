#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "sync/sync.h"
#include "onlinerefresh/integrate/sync/onlinerefresh_integrateincsyncstate.h"

onlinerefresh_integrateincsync* onlinerefresh_integrateincsync_init(void)
{
    onlinerefresh_integrateincsync* syncworkstate = NULL;

    syncworkstate =
        (onlinerefresh_integrateincsync*)rmalloc0(sizeof(onlinerefresh_integrateincsync));
    if (NULL == syncworkstate)
    {
        elog(RLOG_WARNING, "onlinerefresh integrateincsync malloc out of memory");
        return NULL;
    }
    rmemset0(syncworkstate, 0, '\0', sizeof(onlinerefresh_integrateincsync));
    syncstate_reset((syncstate*)syncworkstate);

    return syncworkstate;
}

/* Delete incremental data in status table */
static bool onlinerefresh_integrateincsync_delinc(onlinerefresh_integrateincsync* syncwork)
{
    PGconn*   conn = NULL;
    PGresult* res = NULL;
    char      sql_exec[1024] = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "DELETE FROM \"%s\".\"%s\" WHERE \"name\" = \'%s\';",
            guc_getConfigOption(CFG_KEY_CATALOGSCHEMA), SYNC_STATUSTABLE_NAME, syncwork->base.name);
    res = PQexec(syncwork->base.conn, sql_exec);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "Failed to update status table in: %s",
             PQerrorMessage(syncwork->base.conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    PQfinish(conn);

    return true;
}

/* Incremental apply */
void* onlinerefresh_integrateincsync_main(void* args)
{
    int                             timeout = 0;
    txn*                            entry = NULL;
    thrnode*                        thr_node = NULL;
    syncstate*                      sync_state = NULL;
    onlinerefresh_integrateincsync* syncwork = NULL;

    thr_node = (thrnode*)args;

    syncwork = (onlinerefresh_integrateincsync*)thr_node->data;

    sync_state = (syncstate*)syncwork;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "onlinerefresh integrate spliittrail stat exception, expected state is "
             "THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (syncstate_conn(sync_state, thr_node))
    {
        usleep(50000);
        if (false == syncstate_update_statustb(sync_state, NULL, false))
        {
            continue;
        }
        break;
    }

    while (1)
    {
        /* First check if exit signal is received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        entry = NULL;
        /* Get data */
        entry = cache_txn_get(syncwork->rebuild2sync, &timeout);
        if (NULL == entry)
        {
            /* Continue on timeout */
            if (ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            elog(RLOG_WARNING, "onlinerefresh integrate sync get txn error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Process early lsn and xid invalid that cannot be avoided for incremental end, and
         * incremental end position info does not need update */
        if (TXN_TYPE_ONLINEREFRESH_INC_END == entry->type)
        {
            onlinerefresh_integrateincsync_delinc(syncwork);
            txn_free(entry);
            rfree(entry);
            entry = NULL;
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Sync data */
        while (false == syncstate_applytxn(sync_state, thr_node, (void*)entry, true))
        {
            sleep(1);
            syncstate_reset(sync_state);
            while (syncstate_conn(sync_state, thr_node))
            {
                usleep(50000);
                if (false == syncstate_update_statustb(sync_state, NULL, false))
                {
                    continue;
                }
                break;
            }

            if (THRNODE_STAT_TERM == thr_node->stat)
            {
                thr_node->stat = THRNODE_STAT_EXIT;
                break;
            }
        }

        /* TODO entry release */
        txn_free(entry);
        rfree(entry);
        entry = NULL;
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_integrateincsync_free(void* args)
{
    onlinerefresh_integrateincsync* syncwork = NULL;

    syncwork = (onlinerefresh_integrateincsync*)args;

    if (NULL == syncwork)
    {
        return;
    }
    syncstate_destroy((syncstate*)syncwork);

    rfree(syncwork);

    return;
}