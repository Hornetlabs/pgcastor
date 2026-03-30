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

/* Error handling, re-execute all stmt */
static bool bigtxn_integrateincsync_restart_applytxn(syncstate* syncstate, thrnode* thrnode, txn* cur_txn)
{
    if (0 == cur_txn->stmts->length)
    {
        return true;
    }

    return syncstate_bigtxn_applytxn(syncstate, thrnode, (void*)cur_txn);
}

/* Update refresh task status in status table */
static bool bigtxn_integrateincsync_updatasyncstatus(bigtxn_integrateincsync* syncwork, int16 stat)
{
    PGresult* res = NULL;
    char      sql_exec[1024] = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,
            "UPDATE %s.%s SET \"stat\" = %hd WHERE \"name\" = \'%s\' ",
            guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
            SYNC_STATUSTABLE_NAME,
            stat,
            syncwork->base.name);
    res = PQexec(syncwork->base.conn, sql_exec);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "Failed to update status table in: %s", PQerrorMessage(syncwork->base.conn));
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
    if (NULL == syncworkstate)
    {
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }
    rmemset0(syncworkstate, 0, '\0', sizeof(bigtxn_integrateincsync));
    syncstate_reset((syncstate*)syncworkstate);

    return syncworkstate;
}

/* Incremental apply */
void* bigtxn_integrateincsync_main(void* args)
{
    int                      timeout = 0;
    txn*                     entry = NULL;
    txn*                     cur_txn = NULL;
    thrnode*                 thr_node = NULL;
    syncstate*               sync_state = NULL;
    bigtxn_integrateincsync* syncwork = NULL;

    thr_node = (thrnode*)args;

    syncwork = (bigtxn_integrateincsync*)thr_node->data;

    sync_state = (syncstate*)syncwork;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn sync stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (syncstate_conn(sync_state, thr_node))
    {
        usleep(50000);
        if (false == sync_txnbegin(sync_state))
        {
            continue;
        }

        if (false == syncstate_update_statustb(sync_state, NULL, false))
        {
            continue;
        }

        /* Update status table status, can be parsed and filtered first */
        if (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
        {
            continue;
        }

        break;
    }

    cur_txn = txn_init(InvalidFullTransactionId, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == cur_txn)
    {
        elog(RLOG_WARNING, "integrate bigtxn sync cur_txn out of memory ");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    while (1)
    {
        entry = NULL;

        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialization/flush to disk */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data */
        entry = cache_txn_get(syncwork->rebuild2sync, &timeout);
        if (NULL == entry)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                /* Sleep 10 milliseconds */
                usleep(10000);
                continue;
            }

            /* Error occurred */
            elog(RLOG_WARNING, "cache_txn_get error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if (NULL != entry->stmts)
        {
            while (false == syncstate_bigtxn_applytxn(sync_state, thr_node, (void*)entry))
            {
                sleep(1);
                syncstate_reset(sync_state);
                while (syncstate_conn(sync_state, thr_node))
                {
                    usleep(50000);
                    if (false == sync_txnbegin(sync_state))
                    {
                        continue;
                    }

                    if (false == syncstate_update_statustb(sync_state, NULL, false))
                    {
                        continue;
                    }

                    /* Update status table status, capture can be parsed and filtered first */
                    if (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 1))
                    {
                        continue;
                    }

                    if (false == bigtxn_integrateincsync_restart_applytxn(sync_state, thr_node, cur_txn))
                    {
                        continue;
                    }
                    break;
                }

                if (THRNODE_STAT_TERM == thr_node->stat)
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
                    if (false == sync_txnbegin(sync_state))
                    {
                        continue;
                    }

                    if (false == syncstate_update_statustb(sync_state, NULL, false))
                    {
                        continue;
                    }

                    /* Update status table status, capture can be parsed and filtered first */
                    if (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
                    {
                        continue;
                    }

                    if (false == bigtxn_integrateincsync_restart_applytxn(sync_state, thr_node, cur_txn))
                    {
                        continue;
                    }

                    break;
                }

                if (THRNODE_STAT_TERM == thr_node->stat)
                {
                    thr_node->stat = THRNODE_STAT_EXIT;
                    goto bigtxn_integrateincsync_main_exit;
                }
            }

            while (false == sync_txncommit(sync_state, (void*)entry))
            {
                sleep(1);
                syncstate_reset(sync_state);
                while (syncstate_conn(sync_state, thr_node))
                {
                    if (false == sync_txnbegin(sync_state))
                    {
                        continue;
                    }

                    if (false == syncstate_update_statustb(sync_state, NULL, false))
                    {
                        continue;
                    }

                    /* Update status table status, capture can be parsed and filtered first */
                    if (false == bigtxn_integrateincsync_updatasyncstatus(syncwork, 0))
                    {
                        continue;
                    }

                    if (false == bigtxn_integrateincsync_restart_applytxn(sync_state, thr_node, cur_txn))
                    {
                        continue;
                    }

                    break;
                }

                if (THRNODE_STAT_TERM == thr_node->stat)
                {
                    thr_node->stat = THRNODE_STAT_EXIT;
                    goto bigtxn_integrateincsync_main_exit;
                }
            }
            elog(RLOG_INFO, "bigtxn commit %lu", entry->xid);

            thr_node->stat = THRNODE_STAT_EXIT;
            goto bigtxn_integrateincsync_main_exit;
        }

        /* TODO release entry */
        cur_txn->stmts = list_concat(cur_txn->stmts, entry->stmts);
        if (cur_txn->stmts != entry->stmts)
        {
            rfree(entry->stmts);
        }
        entry->stmts = NULL;
        txn_free(entry);
        rfree(entry);
    }
/* Exit */
bigtxn_integrateincsync_main_exit:

    txn_free(entry);
    rfree(entry);
    txn_free(cur_txn);
    rfree(cur_txn);

    pthread_exit(NULL);
    return NULL;
}

void bigtxn_integrateincsync_free(void* args)
{
    bigtxn_integrateincsync* syncwork = NULL;

    syncwork = (bigtxn_integrateincsync*)args;

    syncstate_destroy((syncstate*)syncwork);

    rfree(syncwork);
}
