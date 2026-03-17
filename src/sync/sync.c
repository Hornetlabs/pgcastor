#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/conn/conn.h"
#include "utils/list/list_func.h"
#include "utils/uuid/uuid.h"
#include "utils/string/stringinfo.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "threads/threads.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "stmts/txnstmt_burst.h"
#include "stmts/txnstmt.h"
#include "sync/sync.h"
#include "refresh/refresh_tables.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"

static bool syncstate_prepare(syncstate* syncstate, const char *stmtName,
                                                        const char *query, int nParams)
{
    PGresult *res = NULL;
    res = PQprepare(syncstate->conn, stmtName, query, nParams, NULL);

    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "prepare failed: %s", PQerrorMessage(syncstate->conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

static bool syncstate_execprepare(syncstate* syncstate,
                                 const char *stmtName,
                                 int nParams,
                                 const char *const *paramValues)
{
    PGresult *res = NULL;
    res = PQexecPrepared(syncstate->conn, stmtName, nParams, paramValues, NULL, NULL, 0);

    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "execprepare failed: %s", PQerrorMessage(syncstate->conn));
        PQclear(res);
        return false;
    }
    
    PQclear(res);
    return true;
}

/* 初始化syncstate->hatables2prepare哈希表 */
HTAB* syncstate_hpreparedno_init(void)
{
    HASHCTL        hash_ctl;
    HTAB* stmthtab;

    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint64);
    hash_ctl.entrysize = sizeof(syncstate_prepared);
    stmthtab = hash_create("sync_hpreparedno", 2048, &hash_ctl,
                                                    HASH_ELEM | HASH_BLOBS);
    return stmthtab;
}

/* 释放syncstate->hatables2prepare哈希表 */
void syncstate_hpreparedno_free(syncstate* syncstate)
{
    if (NULL == syncstate->hpreparedno)
    {
        return;
    }
    hash_destroy(syncstate->hpreparedno);

    return;
}

/* 重置syncstate */
void syncstate_reset(syncstate* syncstate)
{
    if (NULL == syncstate)
    {
        return;
    }
    syncstate->conninfo = NULL;
    if (syncstate->conn)
    {
        PQfinish(syncstate->conn);
    }
    syncstate->conn = NULL;
    if(syncstate->hpreparedno)
    {
        syncstate_hpreparedno_free(syncstate);
    }
    syncstate->hpreparedno = NULL;
    syncstate->hpreparedno = syncstate_hpreparedno_init();
}

/* 设置连接信息 */
void syncstate_conninfo_set(syncstate* syncstate, char* conn)
{
    syncstate->conninfo = conn;
}

/* 连接目的端 */
bool syncstate_conn(syncstate* sync_state, void* thr_node_ptr)
{
    thrnode* thr_node = NULL;
    if (NULL == sync_state)
    {
        return false;
    }

    thr_node = (thrnode*)thr_node_ptr;

    syncstate_conninfo_set(sync_state, guc_getConfigOption(CFG_KEY_URL));
    
    while (1)
    {
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            return false;
        }
        if (NULL != sync_state->conn)
        {
            PQfinish(sync_state->conn);
            sync_state->conn = NULL;
        }
        sync_state->conn = conn_get(sync_state->conninfo);
        if (PQstatus(sync_state->conn) == CONNECTION_OK)
        {
            break;
        }
        else
        {
            sleep(1);
        }
    }
    return true;
}

/* 更新状态表信息 */
bool syncstate_update_statustb(syncstate* sync_state, void* txn_ptr , bool exec)
{
    char xid[65] = {'\0'};
    char fileid[65] = {'\0'};
    char lsn[65] = {'\0'};
    char offset[65] = {'\0'};
    PGresult *res = NULL;
    txn* txn_entry = NULL;
    const char  *paramValues[4];

    char* stmtname = "update_sync_statustb";
    txn_entry = (txn*)txn_ptr;

    if (exec)
    {
        rmemset1(xid, 0, '\0', 65);
        rmemset1(fileid, 0, '\0', 65);
        rmemset1(lsn, 0, '\0', 65);
        rmemset1(offset, 0, '\0', 65);

        sprintf(xid, "%lu", txn_entry->xid);
        sprintf(fileid, "%lu", txn_entry->segno);
        sprintf(lsn, "%lu", txn_entry->confirm.wal.lsn);
        sprintf(offset, "%lu", txn_entry->end.trail.offset);

        paramValues[0] = fileid;
        paramValues[1] = lsn;
        paramValues[2] = xid;
        paramValues[3] = offset;

        res = PQexecPrepared(sync_state->conn, stmtname, 4, paramValues, NULL, NULL, 0);

        if (PGRES_COMMAND_OK != PQresultStatus(res))
        {
            PQclear(res);
            elog(RLOG_WARNING, "update_sync_statustb failde");
            return false;
        }

    }
    else
    {
        char sql_exec[1024] = {'\0'};
        rmemset1(sql_exec, 0, '\0', 1024);
        sprintf(sql_exec,"UPDATE %s.sync_status SET emit_fileid = $1, lsn = $2, xid = $3, emit_offset = $4 where name = '%s';",
                            guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
                            sync_state->name);

        res = PQprepare(sync_state->conn, stmtname, sql_exec, 4, NULL);

        if (PGRES_COMMAND_OK != PQresultStatus(res))
        {
            PQclear(res);
            elog(RLOG_WARNING, "failde to prepare update_sync_statustb");
            return false;
        }
    }
    PQclear(res);

    return true;
}

/* 更新状态表commitlsn信息 */
bool syncstate_update_statustb_commitlsn(syncstate* syncstate, XLogRecPtr commitlsn)
{
    char lsn[65] = {'\0'};
    char sql_exec[1024] = {'\0'};
    PGresult *res = NULL;

    rmemset1(lsn, 0, '\0', 65);

    sprintf(lsn, "%lu", commitlsn);


    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,"UPDATE %s.sync_status SET lsn = '%s' where name = '%s';",
                            guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
                            lsn,
                            syncstate->name);

    res = conn_exec(syncstate->conn, sql_exec);

    if (NULL == res)
    {
        elog(RLOG_WARNING, "failde to update_sync_statustb:%s", PQerrorMessage(syncstate->conn));
        return false;
    }
    PQclear(res);

    return true;
}


/* 更新状态表rewind信息 */
bool syncstate_update_rewind(syncstate* syncstate, recpos rewind)
{
    char sql_exec[1024] = {'\0'};
    char rewind_fileid[65] = {'\0'};
    char rewind_offset[65] = {'\0'};
    PGresult *res = NULL;

    rmemset1(sql_exec, 0, '\0', 1024);
    rmemset1(rewind_fileid, 0, '\0', 65);
    rmemset1(rewind_offset, 0, '\0', 65);

    sprintf(rewind_fileid, "%lu", rewind.trail.fileid);
    sprintf(rewind_offset, "%lu", rewind.trail.offset);
    sprintf(sql_exec,"UPDATE %s.sync_status SET rewind_fileid = %s, rewind_offset = %s where name = '%s';",
                            guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
                            rewind_fileid,
                            rewind_offset,
                            syncstate->name);

    res = conn_exec(syncstate->conn, sql_exec);
    if (NULL == res)
    {
        syncstate->conn = NULL;
        PQclear(res);
        elog(RLOG_WARNING, "failde to prepare update_sync_statustb");
        return false;
    }
    PQclear(res);

    return true;
}

bool syncstate_applytxn(syncstate* sync_state, void* thr_node_ptr, void* txn_ptr, bool update)
{
    bool find = false;
    uint64 debugno = 0;
    PGresult *res = NULL;
    ListCell* lcdebug = NULL;
    txn* txn_entry = NULL;
    txnstmt* stmtnode = NULL;
    thrnode* thr_node = NULL;
    syncstate_prepared* prepared_entry = NULL;

    if (NULL == sync_state)
    {
        return false;
    }

    thr_node = (thrnode*)thr_node_ptr;
    txn_entry = (txn*)txn_ptr;

    //elog(RLOG_DEBUG, "begin, %d", txn_entry->stmts->length);
stmts_write_retry:

    if (THRNODE_STAT_TERM == thr_node->stat)
    {
        return false;
    }

    /* 开启事务 */
    res = PQexec(sync_state->conn,"begin;");
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "begin failed: %s", PQerrorMessage(sync_state->conn));
        PQclear(res);
        return false;
    }
    PQclear(res);


    /* 执行事务中的语句 */
    foreach(lcdebug, txn_entry->stmts)
    {
        stmtnode = (txnstmt*)lfirst(lcdebug);

        /* 组装preparestmt和paramvalues */
        if (TXNSTMT_TYPE_UPDATESYNCTABLE == stmtnode->type)
        {
            if (false == syncstate_update_statustb(sync_state, txn_entry, true))
            {
                return false;
            }
            update = false;
        }
        else if (TXNSTMT_TYPE_UPDATEREWIND == stmtnode->type)
        {
            txnstmt_updaterewind* updaterewind = (txnstmt_updaterewind*)stmtnode->stmt;
            if (false == syncstate_update_rewind(sync_state, updaterewind->rewind))
            {
                return false;
            }
        }
        else if (TXNSTMT_TYPE_PREPARED == stmtnode->type)
        {
            txnstmt_prepared* preparedstmt = NULL;
            preparedstmt = (txnstmt_prepared*)stmtnode->stmt;
            prepared_entry = hash_search(sync_state->hpreparedno, &preparedstmt->number, HASH_ENTER, &find);
            if (false == find)
            {
                if(!syncstate_prepare(sync_state,
                                             preparedstmt->preparedname,
                                             preparedstmt->preparedsql,
                                             preparedstmt->valuecnt))
                {
                    if (CONNECTION_OK == PQstatus(sync_state->conn))
                    {
                        goto stmts_write_retry;
                    }
                    return false;
                }
                prepared_entry->number = preparedstmt->number;
                rmemset1(prepared_entry->preparename, 0, '\0', 64);
                rmemcpy1(prepared_entry->preparename, 0 , preparedstmt->preparedname, strlen(preparedstmt->preparedname));

            }

            if(!syncstate_execprepare(sync_state,
                                             preparedstmt->preparedname,
                                             preparedstmt->valuecnt,
                                             (const char**)preparedstmt->values))
            {
                if (CONNECTION_OK == PQstatus(sync_state->conn))
                {
                    txn_entry->stmts = list_delete(txn_entry->stmts,stmtnode);
                    txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
        }
        else if(TXNSTMT_TYPE_DDL == stmtnode->type)
        {
            txnstmt_ddl* ddlstmt = NULL;
            ddlstmt = (txnstmt_ddl*)stmtnode->stmt;
            res = PQexec(sync_state->conn, ddlstmt->ddlstmt);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING, "ddl failed: %s", PQerrorMessage(sync_state->conn));
                PQclear(res);
                if (CONNECTION_OK == PQstatus(sync_state->conn))
                {
                    txn_entry->stmts = list_delete(txn_entry->stmts, stmtnode);
                    txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
            PQclear(res);
        }
        else if(TXNSTMT_TYPE_BURST == stmtnode->type)
        {
            txnstmt_burst* burststmt = NULL;
            burststmt = (txnstmt_burst*)stmtnode->stmt;

            res = PQexec(sync_state->conn, (char*)burststmt->batchcmd);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING, "burst failed: %s", PQerrorMessage(sync_state->conn));
                PQclear(res);
                if (CONNECTION_OK == PQstatus(sync_state->conn))
                {
                    txn_entry->stmts = list_delete(txn_entry->stmts, stmtnode);
                    txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
            PQclear(res);
        }
        else
        {
            continue;
        }

        debugno++;
    }

    /* 更新同步表 */
    if (update)
    {
        if (false == syncstate_update_statustb(sync_state, txn_entry, true))
        {
            return false;
        }
    }

    /* 提交事务 */
    res = PQexec(sync_state->conn,"commit;");
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "commit failed: %s", PQerrorMessage(sync_state->conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    elog(RLOG_DEBUG, "SYNCWORK,debugno:%lu, xid:%lu", debugno, txn_entry->xid);
    return true;
}

bool sync_txnbegin(syncstate* syncstate)
{
    PGresult *res = NULL;
    /* 开启事务 */

    res = PQexec(syncstate->conn,"begin;");
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "bigtxn begin failed: %s", PQerrorMessage(syncstate->conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

bool sync_txncommit(syncstate* sync_state, void* txn_ptr)
{
    txn* txn_entry = NULL;
    PGresult *res = NULL;
    /* 开启事务 */

    txn_entry = (txn*)txn_ptr;

    if(!syncstate_update_statustb(sync_state, txn_entry, true))
    {
        return false;
    }
    
    /* 开启事务 */
    res = PQexec(sync_state->conn,"commit;");
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "bigtxn commit failed: %s", PQerrorMessage(sync_state->conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    
    elog(RLOG_INFO, "bigtxn commit lsn: %lu", txn_entry->confirm.wal.lsn);
    return true;
}

/* 大事务应用不提交 */
bool syncstate_bigtxn_applytxn(syncstate* sync_state, void* thr_node_ptr, void* txn_ptr)
{
    bool find = false;
    uint64 debugno = 0;
    PGresult *res = NULL;
    ListCell* lcdebug = NULL;
    txn* txn_entry = NULL;
    txnstmt* stmtnode = NULL;
    thrnode* thr_node = NULL;
    syncstate_prepared* prepared_entry = NULL;

    if (NULL == sync_state)
    {
        return false;
    }

    thr_node = (thrnode*)thr_node_ptr;
    txn_entry = (txn*)txn_ptr;

    //elog(RLOG_DEBUG, "begin, %d", txn_entry->stmts->length);
stmts_write_retry:
    if (THRNODE_STAT_TERM == thr_node->stat)
    {
        return false;
    }

    /* 执行事务中的语句 */
    foreach(lcdebug, txn_entry->stmts)
    {
        stmtnode = (txnstmt*)lfirst(lcdebug);

        if (TXNSTMT_TYPE_PREPARED == stmtnode->type)
        {
            txnstmt_prepared* preparedstmt = NULL;
            preparedstmt = (txnstmt_prepared*)stmtnode->stmt;
            prepared_entry = hash_search(sync_state->hpreparedno, &preparedstmt->number, HASH_ENTER, &find);
            if (false == find)
            {
                if(!syncstate_prepare(sync_state,
                                             preparedstmt->preparedname,
                                             preparedstmt->preparedsql,
                                             preparedstmt->valuecnt))
                {
                    if (CONNECTION_OK == PQstatus(sync_state->conn))
                    {
                        goto stmts_write_retry;
                    }
                    return false;
                }
                prepared_entry->number = preparedstmt->number;
                rmemset1(prepared_entry->preparename, 0, '\0', 64);
                rmemcpy1(prepared_entry->preparename, 0 , preparedstmt->preparedname, strlen(preparedstmt->preparedname));
            }

            if(!syncstate_execprepare(sync_state,
                                             preparedstmt->preparedname,
                                             preparedstmt->valuecnt,
                                             (const char**)preparedstmt->values))
            {
                if (CONNECTION_OK == PQstatus(sync_state->conn))
                {
                    txn_entry->stmts = list_delete(txn_entry->stmts,stmtnode);
                    txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
        }
        else if(TXNSTMT_TYPE_DDL == stmtnode->type)
        {
            txnstmt_ddl* ddlstmt = NULL;
            ddlstmt = (txnstmt_ddl*)stmtnode->stmt;
            res = PQexec(sync_state->conn, ddlstmt->ddlstmt);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING, "ddl failed: %s", PQerrorMessage(sync_state->conn));
                PQclear(res);
                if (CONNECTION_OK == PQstatus(sync_state->conn))
                {
                    txn_entry->stmts = list_delete(txn_entry->stmts, stmtnode);
                    txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
            PQclear(res);
        }
        else if(TXNSTMT_TYPE_BURST == stmtnode->type)
        {
            txnstmt_burst* burststmt = NULL;
            burststmt = (txnstmt_burst*)stmtnode->stmt;

            res = PQexec(sync_state->conn, (char*)burststmt->batchcmd);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING, "burst failed: %s", PQerrorMessage(sync_state->conn));
                PQclear(res);
                if (CONNECTION_OK == PQstatus(sync_state->conn))
                {
                    txn_entry->stmts = list_delete(txn_entry->stmts, stmtnode);
                    txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
            PQclear(res);
        }
        else
        {
            elog(RLOG_DEBUG, "stmtnode: %d", stmtnode->type);
            continue;
        }

        debugno++;
    }

    elog(RLOG_DEBUG, "bigtxn syncwork,debugno:%lu, xid:%lu", debugno, txn_entry->xid);
    return true;
}

/* syncstate资源回收 */
void syncstate_destroy(syncstate* syncstate)
{
    if(NULL == syncstate)
    {
        return;
    }

    if (syncstate->conn)
    {
        PQfinish(syncstate->conn);
        syncstate->conn = NULL;
    }

    if (syncstate->name)
    {
        rfree(syncstate->name);
    }

    if(syncstate->hpreparedno)
    {
        syncstate_hpreparedno_free(syncstate);
        syncstate->hpreparedno = NULL;
    }

    syncstate->conninfo = NULL;

    return;
}
