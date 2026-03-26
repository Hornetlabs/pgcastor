#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "sync/sync.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratefilterdataset.h"
#include "increment/integrate/sync/increment_integratesync.h"

/* Get the applied fileid and lsn from the target-end status table */
static bool increment_integratesync_trailnoandlsn_get(increment_integratesyncstate* syncworkstate)
{
    PGresult*  res;
    uint64     emitoffset = 0;
    syncstate* sync_state;
    char       sql_exec[1024] = {'\0'};

    sync_state = (syncstate*)syncworkstate;

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,
            "select rewind_fileid, rewind_offset, lsn from %s.sync_status where name = '%s';",
            guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
            syncworkstate->base.name);
    res = PQexec(sync_state->conn, sql_exec);
    if (PGRES_TUPLES_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "Failed to select sync_status: %s", PQerrorMessage(sync_state->conn));
        PQclear(res);
        PQfinish(sync_state->conn);
        sync_state->conn = NULL;
        return false;
    }
    /* Set the obtained fileid and offset as read information */
    if (PQntuples(res) != 0)
    {
        syncworkstate->trailno = strtoul(PQgetvalue(res, 0, 0), NULL, 10);

        emitoffset = strtoul(PQgetvalue(res, 0, 1), NULL, 10);

        /* LSN information */
        syncworkstate->lsn = strtoul(PQgetvalue(res, 0, 2), NULL, 10);
        elog(RLOG_DEBUG,
             "get record sync_status, trailno:%lu, emitoffset:%lu, lsn:%lu",
             syncworkstate->trailno,
             emitoffset,
             syncworkstate->lsn);
        syncworkstate->callback.setmetricsynctrailno(syncworkstate->privdata,
                                                     syncworkstate->trailno);
        syncworkstate->callback.setmetricsynclsn(syncworkstate->privdata, syncworkstate->lsn);
        syncworkstate->callback.setmetricsynctrailstart(syncworkstate->privdata, emitoffset);
        /* Set splittrail fileid and emitoffset */
        syncworkstate->callback.splittrail_fileid_emitoffse_set(
            syncworkstate->privdata, syncworkstate->trailno, emitoffset);
        syncworkstate->callback.integratestate_rebuildfilter_set(syncworkstate->privdata,
                                                                 syncworkstate->lsn);
        PQclear(res);
    }
    else
    {
        PQclear(res);
        rmemset1(sql_exec, 0, '\0', 1024);
        sprintf(sql_exec,
                "INSERT INTO %s.sync_status (name, type, stat, emit_fileid, emit_offset, "
                "rewind_fileid, rewind_offset, lsn, xid) VALUES ('%s', 0, 0, 0, 0, 0, 0, 0, 0);",
                guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
                syncworkstate->base.name);
        res = PQexec(sync_state->conn, sql_exec);
        if (PGRES_COMMAND_OK != PQresultStatus(res))
        {
            elog(RLOG_WARNING,
                 "Failed to INSERT INTO sync_status: %s",
                 PQerrorMessage(sync_state->conn));
            PQclear(res);
            PQfinish(sync_state->conn);
            sync_state->conn = NULL;
            return false;
        }
        PQclear(res);

        syncworkstate->trailno = 0;
        syncworkstate->lsn = InvalidXLogRecPtr;
        syncworkstate->callback.splittrail_fileid_emitoffse_set(syncworkstate->privdata, 0, 0);
        syncworkstate->callback.integratestate_rebuildfilter_set(syncworkstate->privdata,
                                                                 syncworkstate->lsn);
        syncworkstate->callback.setmetricsynclsn(syncworkstate->privdata, syncworkstate->lsn);
        syncworkstate->callback.setmetricsynctrailno(syncworkstate->privdata,
                                                     syncworkstate->trailno);
        syncworkstate->callback.setmetricsynctrailstart(syncworkstate->privdata, 0);
        syncworkstate->callback.setmetricsynctimestamp(syncworkstate->privdata, (TimestampTz)0);
    }

    return true;
}

static void increment_integratesync_state_set(increment_integratesyncstate* syncworkstate,
                                              int                           state)
{
    syncworkstate->state = state;
}
/* Incremental apply */
void* increment_integratesync_main(void* args)
{
    int                           extra = 0;
    txn*                          entry = NULL;
    thrnode*                      thr_node = NULL;
    syncstate*                    sync_state = NULL;
    increment_integratesyncstate* syncworkstate = NULL;

    thr_node = (thrnode*)args;

    syncworkstate = (increment_integratesyncstate*)thr_node->data;
    sync_state = (syncstate*)syncworkstate;

    /* Check state */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "increment integrate sync exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        osal_thread_exit(NULL);
    }

    /* Set to work state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (syncstate_conn(sync_state, thr_node))
    {
        usleep(50000);

        /* Get fileid and lsn from status table */
        if (false == increment_integratesync_trailnoandlsn_get(syncworkstate))
        {
            continue;
        }

        syncstate_update_statustb(sync_state, NULL, false);
        break;
    }

    if (THRNODE_STAT_TERM == thr_node->stat)
    {
        /* Exit */
        thr_node->stat = THRNODE_STAT_EXIT;
        osal_thread_exit(NULL);
    }

    while (1)
    {
        entry = NULL;
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialize/checkpoint */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data */
        entry = cache_txn_get(syncworkstate->rebuild2sync, &extra);
        if (NULL == entry)
        {
            /* Set to idle state */
            increment_integratesync_state_set(syncworkstate, INCREMENT_INTEGRATESYNC_STATE_IDLE);

            /* Need to exit, wait for thr_node->stat to become TERM then exit*/
            if (THRNODE_STAT_TERM != thr_node->stat)
            {
                /* Sleep 10 milliseconds */
                usleep(10000);
                continue;
            }
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Got entry, set to working state */
        increment_integratesync_state_set(syncworkstate, INCREMENT_INTEGRATESYNC_STATE_WORK);

        /* Sync data */
        if (NULL != entry->stmts)
        {
            while (false == syncstate_applytxn(sync_state, thr_node, (void*)entry, true))
            {
                sleep(1);
                syncstate_reset(sync_state);
                while (syncstate_conn(sync_state, thr_node))
                {
                    usleep(50000);

                    /* Get fileid and lsn from status table */
                    if (false == increment_integratesync_trailnoandlsn_get(syncworkstate))
                    {
                        continue;
                    }
                    syncstate_update_statustb(sync_state, NULL, false);
                    break;
                }

                if (THRNODE_STAT_TERM == thr_node->stat)
                {
                    /* Memory release */
                    txn_free(entry);
                    rfree(entry);
                    thr_node->stat = THRNODE_STAT_EXIT;
                    osal_thread_exit(NULL);
                    break;
                }
            }

            if (entry->confirm.wal.lsn != InvalidXLogRecPtr)
            {
                syncworkstate->lsn = entry->confirm.wal.lsn;
                syncworkstate->callback.setmetricsynclsn(syncworkstate->privdata,
                                                         entry->confirm.wal.lsn);
            }

            syncworkstate->trailno = entry->segno;
            syncworkstate->callback.setmetricsynctrailno(syncworkstate->privdata, entry->segno);
            syncworkstate->callback.setmetricsynctimestamp(syncworkstate->privdata,
                                                           entry->endtimestamp);
            syncworkstate->callback.setmetricsynctrailstart(syncworkstate->privdata,
                                                            entry->end.trail.offset);
        }

        /* TODO entry release */
        txn_free(entry);
        rfree(entry);
    }

    osal_thread_exit(NULL);
    return NULL;
}

/* Incremental apply structure initialization */
increment_integratesyncstate* increment_integratesync_init(void)
{
    increment_integratesyncstate* syncworkstate = NULL;

    syncworkstate = (increment_integratesyncstate*)rmalloc0(sizeof(increment_integratesyncstate));
    if (NULL == syncworkstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(syncworkstate, 0, '\0', sizeof(increment_integratesyncstate));
    syncstate_reset((syncstate*)syncworkstate);

    /* Set syncstate->name */
    syncworkstate->base.name = (char*)rmalloc0(NAMEDATALEN);
    if (NULL == syncworkstate->base.name)
    {
        elog(RLOG_WARNING, "malloc syncname out of memory");
        increment_integratesync_destroy(syncworkstate);
        return NULL;
    }
    rmemset0(syncworkstate->base.name, 0, '\0', NAMEDATALEN);
    sprintf(syncworkstate->base.name, "%s", REFRESH_INCREMENT);
    increment_integratesync_state_set(syncworkstate, INCREMENT_INTEGRATESYNC_STATE_NOP);
    return syncworkstate;
}

/* Incremental apply structure resource release */
void increment_integratesync_destroy(increment_integratesyncstate* syncworkstate)
{
    if (NULL == syncworkstate)
    {
        return;
    }

    syncstate_destroy((syncstate*)syncworkstate);

    syncworkstate->privdata = NULL;
    syncworkstate->rebuild2sync = NULL;

    rfree(syncworkstate);
}
