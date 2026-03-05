#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "sync/ripple_sync.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "refresh/ripple_refresh_tables.h"
#include "stmts/ripple_txnstmt.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratefilterdataset.h"
#include "increment/integrate/sync/ripple_increment_integratesync.h"

/* 获取目的端状体表中已应用到的fileid 和 lsn */
static bool ripple_increment_integratesync_trailnoandlsn_get(ripple_increment_integratesyncstate* syncworkstate)
{
    PGresult*    res;
    uint64 emitoffset = 0;
    ripple_syncstate* syncstate;
    char        sql_exec[1024] = {'\0'};

    syncstate = (ripple_syncstate*)syncworkstate;

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,"select rewind_fileid, rewind_offset, lsn from %s.ripple_sync_status where name = '%s';",
                        guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                        syncworkstate->base.name);
    res = PQexec(syncstate->conn,sql_exec);
    if (PGRES_TUPLES_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,"Failed to select ripple_sync_status: %s", PQerrorMessage(syncstate->conn));
        PQclear(res);
        PQfinish(syncstate->conn);
        syncstate->conn = NULL;
        return false;
    }
    /* 将获取的 fileid 和 offset 设置为 read 中的信息 */
    if (PQntuples(res) != 0 )
    {
        syncworkstate->trailno = strtoul(PQgetvalue(res, 0, 0), NULL, 10);

        emitoffset = strtoul(PQgetvalue(res, 0, 1), NULL, 10);

        /*lsn信息*/
        syncworkstate->lsn = strtoul(PQgetvalue(res, 0, 2), NULL, 10);
        elog(RLOG_DEBUG, "get record ripple_sync_status, trailno:%lu, emitoffset:%lu, lsn:%lu",
                                                         syncworkstate->trailno,
                                                         emitoffset,
                                                         syncworkstate->lsn);
        syncworkstate->callback.setmetricsynctrailno(syncworkstate->privdata, syncworkstate->trailno);
        syncworkstate->callback.setmetricsynclsn(syncworkstate->privdata, syncworkstate->lsn);
        syncworkstate->callback.setmetricsynctrailstart(syncworkstate->privdata, emitoffset);
        /* 设置 splittrail的fileid和emitoffset */
        syncworkstate->callback.splittrail_fileid_emitoffse_set(syncworkstate->privdata, syncworkstate->trailno, emitoffset);
        syncworkstate->callback.integratestate_rebuildfilter_set(syncworkstate->privdata, syncworkstate->lsn);
        PQclear(res);
    }
    else
    {
        PQclear(res);
        rmemset1(sql_exec, 0, '\0', 1024);
        sprintf(sql_exec, "INSERT INTO %s.ripple_sync_status (name, type, stat, emit_fileid, emit_offset, rewind_fileid, rewind_offset, lsn, xid) VALUES ('%s', 0, 0, 0, 0, 0, 0, 0, 0);",
                          guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                          syncworkstate->base.name);
        res = PQexec(syncstate->conn,sql_exec);
        if (PGRES_COMMAND_OK != PQresultStatus(res))
        {
            elog(RLOG_WARNING,"Failed to INSERT INTO ripple_sync_status: %s", PQerrorMessage(syncstate->conn));
            PQclear(res);
            PQfinish(syncstate->conn);
            syncstate->conn = NULL;
            return false;
        }
        PQclear(res);

        syncworkstate->trailno = 0;
        syncworkstate->lsn = InvalidXLogRecPtr;
        syncworkstate->callback.splittrail_fileid_emitoffse_set(syncworkstate->privdata, 0, 0);
        syncworkstate->callback.integratestate_rebuildfilter_set(syncworkstate->privdata, syncworkstate->lsn);
        syncworkstate->callback.setmetricsynclsn(syncworkstate->privdata, syncworkstate->lsn);
        syncworkstate->callback.setmetricsynctrailno(syncworkstate->privdata, syncworkstate->trailno);
        syncworkstate->callback.setmetricsynctrailstart(syncworkstate->privdata, 0);
        syncworkstate->callback.setmetricsynctimestamp(syncworkstate->privdata, (TimestampTz)0);
    }

    return true;
}

static void ripple_increment_integratesync_state_set(ripple_increment_integratesyncstate* syncworkstate, int state)
{
    syncworkstate->state = state;
}
/* 增量应用 */
void* ripple_increment_integratesync_main(void *args)
{
    int extra                                               = 0;
    ripple_txn* entry                                       = NULL;
    ripple_thrnode* thrnode                                 = NULL;
    ripple_syncstate* syncstate                             = NULL;
    ripple_increment_integratesyncstate* syncworkstate      = NULL;

    thrnode = (ripple_thrnode*)args;

    syncworkstate = (ripple_increment_integratesyncstate* )thrnode->data;
    syncstate = (ripple_syncstate*) syncworkstate;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment integrate sync exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while (ripple_syncstate_conn(syncstate, thrnode))
    {
            usleep(50000);

        /* 获取状态表中的fileid和lsn */
        if (false == ripple_increment_integratesync_trailnoandlsn_get(syncworkstate))
        {
            continue;
        }

        ripple_syncstate_update_statustb(syncstate, NULL, false);
        break;
    }
    
    if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
    {
        /* 退出 */
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
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
        entry = ripple_cache_txn_get(syncworkstate->rebuild2sync, &extra);
        if(NULL == entry)
        {
            /* 设置为空闲状态 */
            ripple_increment_integratesync_state_set(syncworkstate, RIPPLE_INCREMENT_INTEGRATESYNC_STATE_IDLE);

            /* 需要退出，等待 thrnode->stat 变为 TERM 后退出*/
            if(RIPPLE_THRNODE_STAT_TERM != thrnode->stat)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 获取到entry，设置为工作状态 */
        ripple_increment_integratesync_state_set(syncworkstate, RIPPLE_INCREMENT_INTEGRATESYNC_STATE_WORK);

        /* 同步数据 */
        // elog(RLOG_DEBUG, "[DEBUG] xid:%lu, %d", entry->xid, entry->stmts->length);
        if(NULL != entry->stmts)
        {
            while(false == ripple_syncstate_applytxn(syncstate, thrnode, (void*)entry, true))
            {
                sleep(1);
                ripple_syncstate_reset(syncstate);
                while (ripple_syncstate_conn(syncstate, thrnode))
                {
                    usleep(50000);

                    /* 获取状态表中的fileid和lsn */
                    if (false == ripple_increment_integratesync_trailnoandlsn_get(syncworkstate))
                    {
                        continue;
                    }
                    ripple_syncstate_update_statustb(syncstate, NULL, false);
                    break;
                }

                if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
                {
                    /* 内存释放 */
                    ripple_txn_free(entry);
                    rfree(entry);
                    thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                    pthread_exit(NULL);
                    break;
                }
            }

            if (entry->confirm.wal.lsn != InvalidXLogRecPtr)
            {
                syncworkstate->lsn = entry->confirm.wal.lsn;
                syncworkstate->callback.setmetricsynclsn(syncworkstate->privdata, entry->confirm.wal.lsn);
            }

            syncworkstate->trailno = entry->segno;
            syncworkstate->callback.setmetricsynctrailno(syncworkstate->privdata, entry->segno);
            syncworkstate->callback.setmetricsynctimestamp(syncworkstate->privdata, entry->endtimestamp);
            syncworkstate->callback.setmetricsynctrailstart(syncworkstate->privdata, entry->end.trail.offset);

        }

        /* TODO entry 释放 */
        ripple_txn_free(entry);
        rfree(entry);
    }

    pthread_exit(NULL);
    return NULL;
}

/* 增量应用结构初始化 */
ripple_increment_integratesyncstate* ripple_increment_integratesync_init(void)
{
    ripple_increment_integratesyncstate* syncworkstate = NULL;

    syncworkstate = (ripple_increment_integratesyncstate*)rmalloc0(sizeof(ripple_increment_integratesyncstate));
    if(NULL == syncworkstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(syncworkstate, 0, '\0', sizeof(ripple_increment_integratesyncstate));
    ripple_syncstate_reset((ripple_syncstate*) syncworkstate);

    /* 设置ripple_syncstate->name */
    syncworkstate->base.name = (char*)rmalloc0(RIPPLE_NAMEDATALEN);
    if(NULL == syncworkstate->base.name)
    {
        elog(RLOG_WARNING, "malloc syncname out of memory");
        ripple_increment_integratesync_destroy(syncworkstate);
        return NULL;
    }
    rmemset0(syncworkstate->base.name, 0, '\0', RIPPLE_NAMEDATALEN);
    sprintf(syncworkstate->base.name, "%s", RIPPLE_REFRESH_INCREMENT);
    ripple_increment_integratesync_state_set(syncworkstate, RIPPLE_INCREMENT_INTEGRATESYNC_STATE_NOP);
    return syncworkstate;
}

/* 增量应用结构资源释放 */
void ripple_increment_integratesync_destroy(ripple_increment_integratesyncstate* syncworkstate)
{
    if (NULL == syncworkstate)
    {
        return;
    }

    ripple_syncstate_destroy((ripple_syncstate*) syncworkstate);

    syncworkstate->privdata = NULL;
    syncworkstate->rebuild2sync = NULL;

    rfree(syncworkstate);

}
