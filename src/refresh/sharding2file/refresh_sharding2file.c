#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/conn/ripple_conn.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/sharding2file/ripple_refresh_sharding2file.h"

ripple_task_refresh_sharding2file * ripple_refresh_sharding2file_init(void)
{
    ripple_task_refresh_sharding2file *shard = NULL;

    shard = (ripple_task_refresh_sharding2file *)rmalloc0(sizeof(ripple_task_refresh_sharding2file));
    if(NULL == shard)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(shard, 0, 0, sizeof(ripple_task_refresh_sharding2file));
    return shard;
}

static void ripple_refresh_sharding2file_connectdb( ripple_task_refresh_sharding2file *shard)
{
    PGconn *conn = NULL;

    while(!conn)
    {
        conn = ripple_conn_get(shard->conn_info);
        if (NULL != conn)
        {
            shard->conn = conn;
            return;
        }
    }
}

void* ripple_refresh_sharding2file_work(void* args)
{
    int timeout                                 = 0;
    char* compress                              = NULL;
    void* qitem_data                            = NULL;
    StringInfo str                              = NULL;
    StringInfo file_name                        = NULL;
    StringInfo file_path_partial                = NULL;
    StringInfo file_path_complete               = NULL;
    PGresult  *res                              = NULL;
    ripple_thrnode* thrnode                     = NULL;
    ripple_refresh_table_condition *cond        = NULL;
    ripple_task_refresh_sharding2file *shard    = NULL;

    compress = guc_getConfigOption(RIPPLE_CFG_KEY_COMPRESS_ALGORITHM);

    thrnode = (ripple_thrnode *)args;
    shard = ( ripple_task_refresh_sharding2file *)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "refresh capture job exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    str = makeStringInfo();
    file_name = makeStringInfo();
    file_path_partial = makeStringInfo();
    file_path_complete = makeStringInfo();

    /* 不存在连接时先创建连接 */
    if (!shard->conn)
    {
        ripple_refresh_sharding2file_connectdb(shard);
    }

    /* 开启事务, 使用快照 */
    appendStringInfo(str, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
    appendStringInfo(str, "SET TRANSACTION SNAPSHOT '%s';", shard->snap_shot_name);

    res = PQexec(shard->conn, str->data);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,"Failed to set transaction snapshot: %s", PQerrorMessage(shard->conn));
        PQclear(res);
        PQfinish(shard->conn);
        shard->conn = NULL;
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    PQclear(res);
    resetStringInfo(str);

    while (true)
    {
        ripple_refresh_table_sharding *table_shard = NULL;
        qitem_data = NULL;

        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 从缓存中获取任务 */
        qitem_data = ripple_queue_get(shard->tqueue, &timeout);
        if (NULL == qitem_data)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
                {
                    continue;
                }
                thrnode->stat = RIPPLE_THRNODE_STAT_IDLE;
                continue;
            }

            /* 检查状态值 */
            elog(RLOG_WARNING, "capture refresh get sharding from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        thrnode->stat = RIPPLE_THRNODE_STAT_WORK;
        table_shard = (ripple_refresh_table_sharding *)qitem_data;
        cond = table_shard->sharding_condition;

        /* 拼装文件名称 */
        appendStringInfo(file_name, "%s_%s_%d_%d", table_shard->schema,
                                                   table_shard->table,
                                                   table_shard->shardings,
                                                   table_shard->sharding_no);

        appendStringInfo(file_path_partial, "%s/%s_%s/%s/%s", shard->refresh_path,
                                                                      table_shard->schema,
                                                                      table_shard->table,
                                                                      RIPPLE_REFRESH_PARTIAL,
                                                                      file_name->data);

        appendStringInfo(file_path_complete, "%s/%s_%s/%s/%s", shard->refresh_path,
                                                                       table_shard->schema,
                                                                       table_shard->table,
                                                                       RIPPLE_REFRESH_COMPLETE,
                                                                       file_name->data);

        if (compress)
        {
            appendStringInfo(str, "COPY (SELECT * FROM \"%s\".\"%s\" WHERE CTID >= '(%u, 0)' "
                                "AND CTID < '(%u, 0)') TO PROGRAM '%s > %s' BINARY;", table_shard->schema,
                                                                        table_shard->table,
                                                                        cond ? cond->left_condition : 0,
                                                                        cond ? cond->right_condition : 0,
                                                                        compress,
                                                                        file_path_partial->data);
        }
        else
        {
            appendStringInfo(str, "COPY (SELECT * FROM \"%s\".\"%s\" WHERE CTID >= '(%u, 0)' "
                                "AND CTID < '(%u, 0)') TO '%s' BINARY;", table_shard->schema,
                                                                        table_shard->table,
                                                                        cond ? cond->left_condition : 0,
                                                                        cond ? cond->right_condition : 0,
                                                                        file_path_partial->data);
        }

        elog(RLOG_DEBUG, "capture refresh worker, queue copy ready: %s.%s %4d %4d",
                                                                      table_shard->schema,
                                                                      table_shard->table,
                                                                      table_shard->shardings,
                                                                      table_shard->sharding_no);
        res = PQexec(shard->conn, str->data);
        if (PGRES_COMMAND_OK != PQresultStatus(res))
        {
            elog(RLOG_ERROR,"Failed to copy data out: %s", PQerrorMessage(shard->conn));
            PQclear(res);
            PQfinish(shard->conn);
            shard->conn = NULL;
        }
        PQclear(res);

        elog(RLOG_DEBUG, "capture refresh worker, queue copy done: %s.%s %4d %4d",
                                                                      table_shard->schema,
                                                                      table_shard->table,
                                                                      table_shard->shardings,
                                                                      table_shard->sharding_no);

        /* 移动到complete */
        if (durable_rename(file_path_partial->data, file_path_complete->data, RLOG_WARNING) != 0) 
        {
            elog(RLOG_WARNING, "Error renaming file %s 2 %s", file_path_partial->data, file_path_complete->data);
        }

        /* 重置 */
        resetStringInfo(file_name);
        resetStringInfo(file_path_partial);
        resetStringInfo(file_path_complete);
        resetStringInfo(str);
        ripple_refresh_table_sharding_free(table_shard);
        table_shard = NULL;
    }

    deleteStringInfo(file_name);
    deleteStringInfo(file_path_partial);
    deleteStringInfo(file_path_complete);
    deleteStringInfo(str);
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_refresh_sharding2file_free(void* args)
{
    ripple_task_refresh_sharding2file* sharding2file = NULL;

    sharding2file = (ripple_task_refresh_sharding2file*)args;
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
