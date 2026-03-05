#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/ripple_conn.h"
#include "threads/ripple_threads.h"
#include "utils/conn/ripple_conn.h"
#include "utils/string/stringinfo.h"
#include "sync/ripple_sync.h"
#include "queue/ripple_queue.h"
#include "storage/ripple_file_buffer.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "works/syncwork/ripple_refresh_integratesync.h"
#include "refresh/sharding2db/ripple_refresh_sharding2db.h"
 

static void ripple_refresh_sharding2db_connectdb(ripple_refresh_sharding2db* sharding2db)
{
    PGconn *conn = NULL;

    while(!conn)
    {
        conn = ripple_conn_get(sharding2db->syncstats->base.conninfo);

        if (NULL != conn)
        {
            sharding2db->syncstats->base.conn = conn;
            return;
        }
    }

}

/* 执行sql失败后检测连接是否正常
 * 连接正常回滚事务，将任务重新加入队列
 * 连接出错重新连接，将任务重新加入队列
 */
static void ripple_refresh_sharding2db_checkconn(ripple_refresh_sharding2db* sharding2db,
                                                 ripple_refresh_table_sharding *table_shard)
{
    if (PQstatus(sharding2db->syncstats->base.conn) != CONNECTION_OK)
    {
        PQfinish(sharding2db->syncstats->base.conn);
        sharding2db->syncstats->base.conn = NULL;
        ripple_refresh_sharding2db_connectdb(sharding2db);
    }
    else
    {
        if (false == ripple_conn_rollback(sharding2db->syncstats->base.conn))
        {
            PQfinish(sharding2db->syncstats->base.conn);
            sharding2db->syncstats->base.conn = NULL;
            ripple_refresh_sharding2db_connectdb(sharding2db);
        }
    }
    sleep(1);
    ripple_queue_put(sharding2db->syncstats->queue, (void *)table_shard);
    return;

}

/* 更新状态表refresh任务的状态 */
static bool ripple_refresh_sharding2db_updatasyncstatustbstat(ripple_refresh_sharding2db* sharding2db, int16 stat)
{
    PGresult  *res          = NULL;
    char sql_exec[1024]     = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "UPDATE \"%s\".\"%s\" SET \"stat\" = %hd WHERE \"name\" = \'%s\' ",
                      guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                      RIPPLE_SYNC_STATUSTABLE_NAME,
                      stat,
                      sharding2db->name);
    res = PQexec(sharding2db->syncstats->base.conn, sql_exec);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,"Failed to update status table in: %s", PQerrorMessage(sharding2db->syncstats->base.conn));
        PQclear(res);
        return false;
    }
    PQclear(res);

    return true;
}

/* 删除状态表中存量数据 */
static bool ripple_refresh_sharding2db_delrefresh(ripple_refresh_sharding2db* sharding2db)
{
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    char sql_exec[1024]     = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec, "DELETE FROM \"%s\".\"%s\" WHERE \"name\" = \'%s\';",
                      guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA),
                      RIPPLE_SYNC_STATUSTABLE_NAME,
                      sharding2db->name);
    res = PQexec(sharding2db->syncstats->base.conn, sql_exec);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,"Failed to update status table in: %s", PQerrorMessage(sharding2db->syncstats->base.conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    PQfinish(conn);

    return true;
}

void* ripple_refresh_sharding2db_work(void* args)
{
    struct stat st;
    int timeout                                         = 0;
    void* qitem_data                                    = NULL;
    PGresult  *res                                      = NULL;
    StringInfo sql                                      = NULL;
    StringInfo file_name                                = NULL;
    StringInfo file_path_complete                       = NULL;
    ripple_thrnode* thrnode                             = NULL;
    ripple_refresh_sharding2db* sharding2db  = NULL;
    char *compress = guc_getConfigOption(RIPPLE_CFG_KEY_COMPRESS_ALGORITHM);

    thrnode = (ripple_thrnode *)args;
    sharding2db = (ripple_refresh_sharding2db*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "refresh integrate job exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    sql = makeStringInfo();
    file_name = makeStringInfo();
    file_path_complete = makeStringInfo();

    /* 不存在连接时先创建连接 */
    if (!sharding2db->syncstats->base.conn)
    {
        ripple_refresh_sharding2db_connectdb(sharding2db);
    }

    while (true)
    {
        ripple_refresh_table_sharding *table_shard = NULL;
        qitem_data = NULL;
        
        /* 重置 */
        resetStringInfo(file_name);
        resetStringInfo(file_path_complete);
        resetStringInfo(sql);

        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 从缓存中获取任务 */
        qitem_data = ripple_queue_get(sharding2db->syncstats->queue, &timeout);
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
            elog(RLOG_WARNING, "integrate refresh get sharding from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        table_shard = (ripple_refresh_table_sharding *)qitem_data;
        if (false == ripple_conn_begin(sharding2db->syncstats->base.conn))
        {
            ripple_refresh_sharding2db_checkconn(sharding2db, table_shard);
            continue;
        }

        /* 设置状态为end */
        if (false == ripple_refresh_sharding2db_updatasyncstatustbstat(sharding2db, 1))
        {
            ripple_refresh_sharding2db_checkconn(sharding2db, table_shard);
            continue;
        }

        if (0 != table_shard->shardings)
        {
            /* 拼装文件名称 */
            appendStringInfo(file_name, "%s_%s_%d_%d", table_shard->schema,
                                                       table_shard->table,
                                                       table_shard->shardings,
                                                       table_shard->sharding_no);

            appendStringInfo(file_path_complete, "%s/%s/%s_%s/%s/%s", sharding2db->refresh_path,
                                                                      RIPPLE_REFRESH_REFRESH,
                                                                      table_shard->schema,
                                                                      table_shard->table,
                                                                      RIPPLE_REFRESH_COMPLETE,
                                                                      file_name->data);
            if (compress)
            {
                appendStringInfo(sql, "COPY \"%s\".\"%s\" FROM PROGRAM '%s < %s' WITH BINARY;", table_shard->schema,
                                                                       table_shard->table,
                                                                       compress,
                                                                       file_path_complete->data);
            }
            else
            {
                appendStringInfo(sql, "COPY \"%s\".\"%s\" FROM '%s' WITH BINARY;", table_shard->schema,
                                                                       table_shard->table,
                                                                       file_path_complete->data);
            }


            res = PQexec(sharding2db->syncstats->base.conn, sql->data);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING,"Failed to copy from data in: %s", PQerrorMessage(sharding2db->syncstats->base.conn));
                ripple_refresh_sharding2db_checkconn(sharding2db, table_shard);
                continue;
            }
            PQclear(res);
        }

        /* 设置状态为init */
        if (false == ripple_refresh_sharding2db_updatasyncstatustbstat(sharding2db, 0))
        {
            ripple_refresh_sharding2db_checkconn(sharding2db, table_shard);
            continue;
        }

        /* 提交事务 */
        if (false == ripple_conn_commit(sharding2db->syncstats->base.conn))
        {
            ripple_refresh_sharding2db_checkconn(sharding2db, table_shard);
            continue;
        }

        ripple_refreshtablesyncstats_markstatdone(table_shard, sharding2db->syncstats->tablesyncstats, sharding2db->refresh_path);

        /* 接收到end先删除文件 */
        if(0 == stat(file_path_complete->data, &st))
        {
            elog(RLOG_DEBUG,"refresh complete sharding: %s", file_name->data);
            durable_unlink(file_path_complete->data, RLOG_DEBUG);
        }

        ripple_refresh_table_sharding_free(table_shard);
    }

    deleteStringInfo(file_name);
    deleteStringInfo(file_path_complete);
    deleteStringInfo(sql);
    ripple_refresh_sharding2db_delrefresh(sharding2db);
    ripple_pthread_exit(NULL);
    return NULL;
}

ripple_refresh_sharding2db* ripple_refresh_sharding2db_init(void)
{
    ripple_refresh_sharding2db* sharding2db = NULL;

    sharding2db = (ripple_refresh_sharding2db*)rmalloc0(sizeof(ripple_refresh_sharding2db));
    if(NULL == sharding2db)
    {
        elog(RLOG_WARNING, "refreshsharding2db out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(sharding2db, 0, 0, sizeof(ripple_refresh_sharding2db));
    sharding2db->syncstats = ripple_refresh_integratesyncstate_init();

    return sharding2db;
}

void ripple_refresh_sharding2db_free(void* args)
{
    ripple_refresh_sharding2db* sharding2db = NULL;

    sharding2db = (ripple_refresh_sharding2db*)args;

    if(sharding2db->syncstats->base.conn)
    {
        PQfinish(sharding2db->syncstats->base.conn);
        sharding2db->syncstats->base.conn = NULL;
    }

    if (sharding2db->name)
    {
        rfree(sharding2db->name);
    }

    if (sharding2db->syncstats)
    {
        ripple_refresh_integratesync_destroy(sharding2db->syncstats);
    }
    rfree(sharding2db);
}
