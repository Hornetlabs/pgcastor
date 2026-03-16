#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/conn/ripple_conn.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/string/stringinfo.h"
#include "utils/hash/hash_search.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "threads/ripple_threads.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "stmts/ripple_txnstmt_burst.h"
#include "stmts/ripple_txnstmt.h"
#include "sync/ripple_sync.h"
#include "refresh/ripple_refresh_tables.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratedataset.h"

static bool ripple_syncstate_prepare(ripple_syncstate* syncstate, const char *stmtName,
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

static bool ripple_syncstate_execprepare(ripple_syncstate* syncstate,
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
HTAB* ripple_syncstate_hpreparedno_init(void)
{
    HASHCTL        hash_ctl;
    HTAB* stmthtab;

    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(uint64);
    hash_ctl.entrysize = sizeof(ripple_syncstate_prepared);
    stmthtab = hash_create("sync_hpreparedno", 2048, &hash_ctl,
                                                    HASH_ELEM | HASH_BLOBS);
    return stmthtab;
}

/* 释放syncstate->hatables2prepare哈希表 */
void ripple_syncstate_hpreparedno_free(ripple_syncstate* syncstate)
{
    if (NULL == syncstate->hpreparedno)
    {
        return;
    }
    hash_destroy(syncstate->hpreparedno);

    return;
}

/* 重置syncstate */
void ripple_syncstate_reset(ripple_syncstate* syncstate)
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
        ripple_syncstate_hpreparedno_free(syncstate);
    }
    syncstate->hpreparedno = NULL;
    syncstate->hpreparedno = ripple_syncstate_hpreparedno_init();
}

/* 设置连接信息 */
void ripple_syncstate_conninfo_set(ripple_syncstate* syncstate, char* conn)
{
    syncstate->conninfo = conn;
}

/* 连接目的端 */
bool ripple_syncstate_conn(ripple_syncstate* syncstate, void* thrnode)
{
    ripple_thrnode* thread = NULL;
    if (NULL == syncstate)
    {
        return false;
    }

    thread = (ripple_thrnode*)thrnode;

    ripple_syncstate_conninfo_set(syncstate, guc_getConfigOption(RIPPLE_CFG_KEY_URL));
    
    while (1)
    {
        if (RIPPLE_THRNODE_STAT_TERM == thread->stat)
        {
            return false;
        }
        if (NULL != syncstate->conn)
        {
            PQfinish(syncstate->conn);
            syncstate->conn = NULL;
        }
        syncstate->conn = ripple_conn_get(syncstate->conninfo);
        if (PQstatus(syncstate->conn) == CONNECTION_OK)
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
bool ripple_syncstate_update_statustb(ripple_syncstate* syncstate, void* txn , bool exec)
{
    char xid[65] = {'\0'};
    char fileid[65] = {'\0'};
    char lsn[65] = {'\0'};
    char offset[65] = {'\0'};
    PGresult *res = NULL;
    ripple_txn* entry = NULL;
    const char  *paramValues[4];

    char* stmtname = "update_sync_statustb";
    entry = (ripple_txn*)txn;

    if (exec)
    {
        rmemset1(xid, 0, '\0', 65);
        rmemset1(fileid, 0, '\0', 65);
        rmemset1(lsn, 0, '\0', 65);
        rmemset1(offset, 0, '\0', 65);

        sprintf(xid, "%lu", entry->xid);
        sprintf(fileid, "%lu", entry->segno);
        sprintf(lsn, "%lu", entry->confirm.wal.lsn);
        sprintf(offset, "%lu", entry->end.trail.offset);

        paramValues[0] = fileid;
        paramValues[1] = lsn;
        paramValues[2] = xid;
        paramValues[3] = offset;

        res = PQexecPrepared(syncstate->conn, stmtname, 4, paramValues, NULL, NULL, 0);

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
        sprintf(sql_exec,"UPDATE %s.ripple_sync_status SET emit_fileid = $1, lsn = $2, xid = $3, emit_offset = $4 where name = '%s';",
                            guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                            syncstate->name);

        res = PQprepare(syncstate->conn, stmtname, sql_exec, 4, NULL);

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
bool ripple_syncstate_update_statustb_commitlsn(ripple_syncstate* syncstate, XLogRecPtr commitlsn)
{
    char lsn[65] = {'\0'};
    char sql_exec[1024] = {'\0'};
    PGresult *res = NULL;

    rmemset1(lsn, 0, '\0', 65);

    sprintf(lsn, "%lu", commitlsn);


    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,"UPDATE %s.ripple_sync_status SET lsn = '%s' where name = '%s';",
                            guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                            lsn,
                            syncstate->name);

    res = ripple_conn_exec(syncstate->conn, sql_exec);

    if (NULL == res)
    {
        elog(RLOG_WARNING, "failde to update_sync_statustb:%s", PQerrorMessage(syncstate->conn));
        return false;
    }
    PQclear(res);

    return true;
}


/* 更新状态表rewind信息 */
bool ripple_syncstate_update_rewind(ripple_syncstate* syncstate, ripple_recpos rewind)
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
    sprintf(sql_exec,"UPDATE %s.ripple_sync_status SET rewind_fileid = %s, rewind_offset = %s where name = '%s';",
                            guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                            rewind_fileid,
                            rewind_offset,
                            syncstate->name);

    res = ripple_conn_exec(syncstate->conn, sql_exec);
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

bool ripple_syncstate_applytxn(ripple_syncstate* syncstate, void* thrnode, void* txn, bool update)
{
    bool find = false;
    uint64 debugno = 0;
    PGresult *res = NULL;
    ListCell* lcdebug = NULL;
    ripple_txn* entry = NULL;
    ripple_txnstmt* stmtnode = NULL;
    ripple_thrnode* thread = NULL;
    ripple_syncstate_prepared* prepared_entry = NULL;

    if (NULL == syncstate)
    {
        return false;
    }

    thread = (ripple_thrnode*)thrnode;
    entry = (ripple_txn*)txn;

    //elog(RLOG_DEBUG, "begin, %d", entry->stmts->length);
stmts_write_retry:

    if (RIPPLE_THRNODE_STAT_TERM == thread->stat)
    {
        return false;
    }

    /* 开启事务 */
    res = PQexec(syncstate->conn,"begin;");
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "begin failed: %s", PQerrorMessage(syncstate->conn));
        PQclear(res);
        return false;
    }
    PQclear(res);


    /* 执行事务中的语句 */
    foreach(lcdebug, entry->stmts)
    {
        stmtnode = (ripple_txnstmt*)lfirst(lcdebug);

        /* 组装preparestmt和paramvalues */
        if (RIPPLE_TXNSTMT_TYPE_UPDATESYNCTABLE == stmtnode->type)
        {
            if (false == ripple_syncstate_update_statustb(syncstate, entry, true))
            {
                return false;
            }
            update = false;
        }
        else if (RIPPLE_TXNSTMT_TYPE_UPDATEREWIND == stmtnode->type)
        {
            ripple_txnstmt_updaterewind* updaterewind = (ripple_txnstmt_updaterewind*)stmtnode->stmt;
            if (false == ripple_syncstate_update_rewind(syncstate, updaterewind->rewind))
            {
                return false;
            }
        }
        else if (RIPPLE_TXNSTMT_TYPE_PREPARED == stmtnode->type)
        {
            ripple_txnstmt_prepared* preparedstmt = NULL;
            preparedstmt = (ripple_txnstmt_prepared*)stmtnode->stmt;
            prepared_entry = hash_search(syncstate->hpreparedno, &preparedstmt->number, HASH_ENTER, &find);
            if (false == find)
            {
                if(!ripple_syncstate_prepare(syncstate,
                                             preparedstmt->preparedname,
                                             preparedstmt->preparedsql,
                                             preparedstmt->valuecnt))
                {
                    if (CONNECTION_OK == PQstatus(syncstate->conn))
                    {
                        goto stmts_write_retry;
                    }
                    return false;
                }
                prepared_entry->number = preparedstmt->number;
                rmemset1(prepared_entry->preparename, 0, '\0', 64);
                rmemcpy1(prepared_entry->preparename, 0 , preparedstmt->preparedname, strlen(preparedstmt->preparedname));

            }

            if(!ripple_syncstate_execprepare(syncstate,
                                             preparedstmt->preparedname,
                                             preparedstmt->valuecnt,
                                             (const char**)preparedstmt->values))
            {
                if (CONNECTION_OK == PQstatus(syncstate->conn))
                {
                    entry->stmts = list_delete(entry->stmts,stmtnode);
                    ripple_txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
        }
        else if(RIPPLE_TXNSTMT_TYPE_DDL == stmtnode->type)
        {
            ripple_txnstmt_ddl* ddlstmt = NULL;
            ddlstmt = (ripple_txnstmt_ddl*)stmtnode->stmt;
            res = PQexec(syncstate->conn, ddlstmt->ddlstmt);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING, "ddl failed: %s", PQerrorMessage(syncstate->conn));
                PQclear(res);
                if (CONNECTION_OK == PQstatus(syncstate->conn))
                {
                    entry->stmts = list_delete(entry->stmts, stmtnode);
                    ripple_txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
            PQclear(res);
        }
        else if(RIPPLE_TXNSTMT_TYPE_BURST == stmtnode->type)
        {
            ripple_txnstmt_burst* burststmt = NULL;
            burststmt = (ripple_txnstmt_burst*)stmtnode->stmt;

            res = PQexec(syncstate->conn, (char*)burststmt->batchcmd);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING, "burst failed: %s", PQerrorMessage(syncstate->conn));
                PQclear(res);
                if (CONNECTION_OK == PQstatus(syncstate->conn))
                {
                    entry->stmts = list_delete(entry->stmts, stmtnode);
                    ripple_txnstmt_free(stmtnode);
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
        if (false == ripple_syncstate_update_statustb(syncstate, entry, true))
        {
            return false;
        }
    }

    /* 提交事务 */
    res = PQexec(syncstate->conn,"commit;");
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "commit failed: %s", PQerrorMessage(syncstate->conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    elog(RLOG_DEBUG, "SYNCWORK,debugno:%lu, xid:%lu", debugno, entry->xid);
    return true;
}

bool ripple_sync_txnbegin(ripple_syncstate* syncstate)
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

bool ripple_sync_txncommit(ripple_syncstate* syncstate, void* txn)
{
    ripple_txn* entry = NULL;
    PGresult *res = NULL;
    /* 开启事务 */

    entry = (ripple_txn*)txn;

    if(!ripple_syncstate_update_statustb(syncstate, entry, true))
    {
        return false;
    }
    
    /* 开启事务 */
    res = PQexec(syncstate->conn,"commit;");
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "bigtxn commit failed: %s", PQerrorMessage(syncstate->conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    
    elog(RLOG_INFO, "bigtxn commit lsn: %lu", entry->confirm.wal.lsn);
    return true;
}

/* 大事务应用不提交 */
bool ripple_syncstate_bigtxn_applytxn(ripple_syncstate* syncstate, void* thrnode, void* txn)
{
    bool find = false;
    uint64 debugno = 0;
    PGresult *res = NULL;
    ListCell* lcdebug = NULL;
    ripple_txn* entry = NULL;
    ripple_txnstmt* stmtnode = NULL;
    ripple_thrnode* thread = NULL;
    ripple_syncstate_prepared* prepared_entry = NULL;

    if (NULL == syncstate)
    {
        return false;
    }

    thread = (ripple_thrnode*)thrnode;
    entry = (ripple_txn*)txn;

    //elog(RLOG_DEBUG, "begin, %d", entry->stmts->length);
stmts_write_retry:
    if (RIPPLE_THRNODE_STAT_TERM == thread->stat)
    {
        return false;
    }

    /* 执行事务中的语句 */
    foreach(lcdebug, entry->stmts)
    {
        stmtnode = (ripple_txnstmt*)lfirst(lcdebug);

        if (RIPPLE_TXNSTMT_TYPE_PREPARED == stmtnode->type)
        {
            ripple_txnstmt_prepared* preparedstmt = NULL;
            preparedstmt = (ripple_txnstmt_prepared*)stmtnode->stmt;
            prepared_entry = hash_search(syncstate->hpreparedno, &preparedstmt->number, HASH_ENTER, &find);
            if (false == find)
            {
                if(!ripple_syncstate_prepare(syncstate,
                                             preparedstmt->preparedname,
                                             preparedstmt->preparedsql,
                                             preparedstmt->valuecnt))
                {
                    if (CONNECTION_OK == PQstatus(syncstate->conn))
                    {
                        goto stmts_write_retry;
                    }
                    return false;
                }
                prepared_entry->number = preparedstmt->number;
                rmemset1(prepared_entry->preparename, 0, '\0', 64);
                rmemcpy1(prepared_entry->preparename, 0 , preparedstmt->preparedname, strlen(preparedstmt->preparedname));
            }

            if(!ripple_syncstate_execprepare(syncstate,
                                             preparedstmt->preparedname,
                                             preparedstmt->valuecnt,
                                             (const char**)preparedstmt->values))
            {
                if (CONNECTION_OK == PQstatus(syncstate->conn))
                {
                    entry->stmts = list_delete(entry->stmts,stmtnode);
                    ripple_txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
        }
        else if(RIPPLE_TXNSTMT_TYPE_DDL == stmtnode->type)
        {
            ripple_txnstmt_ddl* ddlstmt = NULL;
            ddlstmt = (ripple_txnstmt_ddl*)stmtnode->stmt;
            res = PQexec(syncstate->conn, ddlstmt->ddlstmt);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING, "ddl failed: %s", PQerrorMessage(syncstate->conn));
                PQclear(res);
                if (CONNECTION_OK == PQstatus(syncstate->conn))
                {
                    entry->stmts = list_delete(entry->stmts, stmtnode);
                    ripple_txnstmt_free(stmtnode);
                    goto stmts_write_retry;
                }
                return false;
            }
            PQclear(res);
        }
        else if(RIPPLE_TXNSTMT_TYPE_BURST == stmtnode->type)
        {
            ripple_txnstmt_burst* burststmt = NULL;
            burststmt = (ripple_txnstmt_burst*)stmtnode->stmt;

            res = PQexec(syncstate->conn, (char*)burststmt->batchcmd);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING, "burst failed: %s", PQerrorMessage(syncstate->conn));
                PQclear(res);
                if (CONNECTION_OK == PQstatus(syncstate->conn))
                {
                    entry->stmts = list_delete(entry->stmts, stmtnode);
                    ripple_txnstmt_free(stmtnode);
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

    elog(RLOG_DEBUG, "bigtxn syncwork,debugno:%lu, xid:%lu", debugno, entry->xid);
    return true;
}

/* ripple_syncstate资源回收 */
void ripple_syncstate_destroy(ripple_syncstate* syncstate)
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
        ripple_syncstate_hpreparedno_free(syncstate);
        syncstate->hpreparedno = NULL;
    }

    syncstate->conninfo = NULL;

    return;
}
