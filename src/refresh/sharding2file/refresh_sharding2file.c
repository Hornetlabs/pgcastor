#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/conn/conn.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "refresh/sharding2file/refresh_sharding2file.h"

task_refresh_sharding2file* refresh_sharding2file_init(void)
{
    task_refresh_sharding2file* shard = NULL;

    shard = (task_refresh_sharding2file*)rmalloc0(sizeof(task_refresh_sharding2file));
    if (NULL == shard)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(shard, 0, 0, sizeof(task_refresh_sharding2file));
    return shard;
}

static void refresh_sharding2file_connectdb(task_refresh_sharding2file* shard)
{
    PGconn* conn = NULL;

    while (!conn)
    {
        conn = conn_get(shard->conn_info);
        if (NULL != conn)
        {
            shard->conn = conn;
            return;
        }
    }
}

void* refresh_sharding2file_work(void* args)
{
    int                         timeout = 0;
    char*                       compress = NULL;
    void*                       qitem_data = NULL;
    StringInfo                  str = NULL;
    StringInfo                  file_name = NULL;
    StringInfo                  file_path_partial = NULL;
    StringInfo                  file_path_complete = NULL;
    PGresult*                   res = NULL;
    thrnode*                    thr_node = NULL;
    refresh_table_condition*    cond = NULL;
    task_refresh_sharding2file* shard = NULL;

    compress = guc_getConfigOption(CFG_KEY_COMPRESS_ALGORITHM);

    thr_node = (thrnode*)args;
    shard = (task_refresh_sharding2file*)thr_node->data;

    /* check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "refresh capture job exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }
    thr_node->stat = THRNODE_STAT_WORK;

    str = makeStringInfo();
    file_name = makeStringInfo();
    file_path_partial = makeStringInfo();
    file_path_complete = makeStringInfo();

    /* create connection if not exists */
    if (!shard->conn)
    {
        refresh_sharding2file_connectdb(shard);
    }

    /* begin transaction with snapshot */
    appendStringInfo(str, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
    appendStringInfo(str, "SET TRANSACTION SNAPSHOT '%s';", shard->snap_shot_name);

    res = PQexec(shard->conn, str->data);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "Failed to set transaction snapshot: %s", PQerrorMessage(shard->conn));
        PQclear(res);
        PQfinish(shard->conn);
        shard->conn = NULL;
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    PQclear(res);
    resetStringInfo(str);

    while (true)
    {
        refresh_table_sharding* table_shard = NULL;
        qitem_data = NULL;

        /* first check if exit signal received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* get task from cache */
        qitem_data = queue_get(shard->tqueue, &timeout);
        if (NULL == qitem_data)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                if (THRNODE_STAT_TERM == thr_node->stat)
                {
                    continue;
                }
                thr_node->stat = THRNODE_STAT_IDLE;
                continue;
            }

            /* check status value */
            elog(RLOG_WARNING, "capture refresh get sharding from queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        thr_node->stat = THRNODE_STAT_WORK;
        table_shard = (refresh_table_sharding*)qitem_data;
        cond = table_shard->sharding_condition;

        /* assemble file name */
        appendStringInfo(file_name, "%s_%s_%d_%d", table_shard->schema, table_shard->table,
                         table_shard->shardings, table_shard->sharding_no);

        appendStringInfo(file_path_partial, "%s/%s_%s/%s/%s", shard->refresh_path,
                         table_shard->schema, table_shard->table, REFRESH_PARTIAL, file_name->data);

        appendStringInfo(file_path_complete, "%s/%s_%s/%s/%s", shard->refresh_path,
                         table_shard->schema, table_shard->table, REFRESH_COMPLETE,
                         file_name->data);

        if (compress)
        {
            appendStringInfo(str,
                             "COPY (SELECT * FROM \"%s\".\"%s\" WHERE CTID >= '(%u, 0)' "
                             "AND CTID < '(%u, 0)') TO PROGRAM '%s > %s' BINARY;",
                             table_shard->schema, table_shard->table,
                             cond ? cond->left_condition : 0, cond ? cond->right_condition : 0,
                             compress, file_path_partial->data);
        }
        else
        {
            appendStringInfo(str,
                             "COPY (SELECT * FROM \"%s\".\"%s\" WHERE CTID >= '(%u, 0)' "
                             "AND CTID < '(%u, 0)') TO '%s' BINARY;",
                             table_shard->schema, table_shard->table,
                             cond ? cond->left_condition : 0, cond ? cond->right_condition : 0,
                             file_path_partial->data);
        }

        elog(RLOG_DEBUG, "capture refresh worker, queue copy ready: %s.%s %4d %4d",
             table_shard->schema, table_shard->table, table_shard->shardings,
             table_shard->sharding_no);
        res = PQexec(shard->conn, str->data);
        if (PGRES_COMMAND_OK != PQresultStatus(res))
        {
            elog(RLOG_ERROR, "Failed to copy data out: %s", PQerrorMessage(shard->conn));
            PQclear(res);
            PQfinish(shard->conn);
            shard->conn = NULL;
        }
        PQclear(res);

        elog(RLOG_DEBUG, "capture refresh worker, queue copy done: %s.%s %4d %4d",
             table_shard->schema, table_shard->table, table_shard->shardings,
             table_shard->sharding_no);

        /* move to complete */
        if (osal_durable_rename(file_path_partial->data, file_path_complete->data, RLOG_WARNING) !=
            0)
        {
            elog(RLOG_WARNING, "Error renaming file %s 2 %s", file_path_partial->data,
                 file_path_complete->data);
        }

        /* reset */
        resetStringInfo(file_name);
        resetStringInfo(file_path_partial);
        resetStringInfo(file_path_complete);
        resetStringInfo(str);
        refresh_table_sharding_free(table_shard);
        table_shard = NULL;
    }

    deleteStringInfo(file_name);
    deleteStringInfo(file_path_partial);
    deleteStringInfo(file_path_complete);
    deleteStringInfo(str);
    pthread_exit(NULL);
    return NULL;
}

void refresh_sharding2file_free(void* args)
{
    task_refresh_sharding2file* sharding2file = NULL;

    sharding2file = (task_refresh_sharding2file*)args;
    if (sharding2file->conn)
    {
        PQfinish(sharding2file->conn);
        sharding2file->conn = NULL;
    }

    if (sharding2file->refresh_path)
    {
        rfree(sharding2file->refresh_path);
    }

    rfree(sharding2file);
    sharding2file = NULL;
}
