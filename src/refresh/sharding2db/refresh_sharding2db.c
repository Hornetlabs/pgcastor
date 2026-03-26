#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/conn.h"
#include "threads/threads.h"
#include "utils/conn/conn.h"
#include "utils/string/stringinfo.h"
#include "sync/sync.h"
#include "queue/queue.h"
#include "storage/file_buffer.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "works/syncwork/refresh_integratesync.h"
#include "refresh/sharding2db/refresh_sharding2db.h"

static void refresh_sharding2db_connectdb(refresh_sharding2db* sharding2db)
{
    PGconn* conn = NULL;

    while (!conn)
    {
        conn = conn_get(sharding2db->syncstats->base.conninfo);

        if (NULL != conn)
        {
            sharding2db->syncstats->base.conn = conn;
            return;
        }
    }
}

/* after SQL execution failure, check connection status
 * if connection is OK, rollback transaction and re-add task to queue
 * if connection error, reconnect and re-add task to queue
 */
static void refresh_sharding2db_checkconn(refresh_sharding2db*    sharding2db,
                                          refresh_table_sharding* table_shard)
{
    if (PQstatus(sharding2db->syncstats->base.conn) != CONNECTION_OK)
    {
        PQfinish(sharding2db->syncstats->base.conn);
        sharding2db->syncstats->base.conn = NULL;
        refresh_sharding2db_connectdb(sharding2db);
    }
    else
    {
        if (false == conn_rollback(sharding2db->syncstats->base.conn))
        {
            PQfinish(sharding2db->syncstats->base.conn);
            sharding2db->syncstats->base.conn = NULL;
            refresh_sharding2db_connectdb(sharding2db);
        }
    }
    sleep(1);
    queue_put(sharding2db->syncstats->queue, (void*)table_shard);
    return;
}

/* update refresh task status in status table */
static bool refresh_sharding2db_updatasyncstatustbstat(refresh_sharding2db* sharding2db, int16 stat)
{
    PGresult* res = NULL;
    char      sql_exec[1024] = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,
            "UPDATE \"%s\".\"%s\" SET \"stat\" = %hd WHERE \"name\" = \'%s\' ",
            guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
            SYNC_STATUSTABLE_NAME,
            stat,
            sharding2db->name);
    res = PQexec(sharding2db->syncstats->base.conn, sql_exec);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,
             "Failed to update status table in: %s",
             PQerrorMessage(sharding2db->syncstats->base.conn));
        PQclear(res);
        return false;
    }
    PQclear(res);

    return true;
}

/* delete existing data from status table */
static bool refresh_sharding2db_delrefresh(refresh_sharding2db* sharding2db)
{
    PGconn*   conn = NULL;
    PGresult* res = NULL;
    char      sql_exec[1024] = {'\0'};

    rmemset1(sql_exec, 0, '\0', 1024);
    sprintf(sql_exec,
            "DELETE FROM \"%s\".\"%s\" WHERE \"name\" = \'%s\';",
            guc_getConfigOption(CFG_KEY_CATALOGSCHEMA),
            SYNC_STATUSTABLE_NAME,
            sharding2db->name);
    res = PQexec(sharding2db->syncstats->base.conn, sql_exec);
    if (PGRES_COMMAND_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,
             "Failed to update status table in: %s",
             PQerrorMessage(sharding2db->syncstats->base.conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    PQfinish(conn);

    return true;
}

void* refresh_sharding2db_work(void* args)
{
    struct stat          st;
    int                  timeout = 0;
    void*                qitem_data = NULL;
    PGresult*            res = NULL;
    StringInfo           sql = NULL;
    StringInfo           file_name = NULL;
    StringInfo           file_path_complete = NULL;
    thrnode*             thr_node = NULL;
    refresh_sharding2db* sharding2db = NULL;
    char*                compress = guc_getConfigOption(CFG_KEY_COMPRESS_ALGORITHM);

    thr_node = (thrnode*)args;
    sharding2db = (refresh_sharding2db*)thr_node->data;

    /* check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "refresh integrate job exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }
    thr_node->stat = THRNODE_STAT_WORK;

    sql = makeStringInfo();
    file_name = makeStringInfo();
    file_path_complete = makeStringInfo();

    /* create connection if none exists */
    if (!sharding2db->syncstats->base.conn)
    {
        refresh_sharding2db_connectdb(sharding2db);
    }

    while (true)
    {
        refresh_table_sharding* table_shard = NULL;
        qitem_data = NULL;

        /* reset */
        resetStringInfo(file_name);
        resetStringInfo(file_path_complete);
        resetStringInfo(sql);

        /* first check if exit signal received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* serialization/flush */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* get task from cache */
        qitem_data = queue_get(sharding2db->syncstats->queue, &timeout);
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
            elog(RLOG_WARNING, "integrate refresh get sharding from queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        table_shard = (refresh_table_sharding*)qitem_data;
        if (false == conn_begin(sharding2db->syncstats->base.conn))
        {
            refresh_sharding2db_checkconn(sharding2db, table_shard);
            continue;
        }

        /* set status to end */
        if (false == refresh_sharding2db_updatasyncstatustbstat(sharding2db, 1))
        {
            refresh_sharding2db_checkconn(sharding2db, table_shard);
            continue;
        }

        if (0 != table_shard->shardings)
        {
            /* assemble file name */
            appendStringInfo(file_name,
                             "%s_%s_%d_%d",
                             table_shard->schema,
                             table_shard->table,
                             table_shard->shardings,
                             table_shard->sharding_no);

            appendStringInfo(file_path_complete,
                             "%s/%s/%s_%s/%s/%s",
                             sharding2db->refresh_path,
                             REFRESH_REFRESH,
                             table_shard->schema,
                             table_shard->table,
                             REFRESH_COMPLETE,
                             file_name->data);
            if (compress)
            {
                appendStringInfo(sql,
                                 "COPY \"%s\".\"%s\" FROM PROGRAM '%s < %s' WITH BINARY;",
                                 table_shard->schema,
                                 table_shard->table,
                                 compress,
                                 file_path_complete->data);
            }
            else
            {
                appendStringInfo(sql,
                                 "COPY \"%s\".\"%s\" FROM '%s' WITH BINARY;",
                                 table_shard->schema,
                                 table_shard->table,
                                 file_path_complete->data);
            }

            res = PQexec(sharding2db->syncstats->base.conn, sql->data);
            if (PGRES_COMMAND_OK != PQresultStatus(res))
            {
                elog(RLOG_WARNING,
                     "Failed to copy from data in: %s %s",
                     PQerrorMessage(sharding2db->syncstats->base.conn),
                     sql->data);
                refresh_sharding2db_checkconn(sharding2db, table_shard);
                continue;
            }
            PQclear(res);
        }

        /* set status to init */
        if (false == refresh_sharding2db_updatasyncstatustbstat(sharding2db, 0))
        {
            refresh_sharding2db_checkconn(sharding2db, table_shard);
            continue;
        }

        /* commit transaction */
        if (false == conn_commit(sharding2db->syncstats->base.conn))
        {
            refresh_sharding2db_checkconn(sharding2db, table_shard);
            continue;
        }

        refreshtablesyncstats_markstatdone(
            table_shard, sharding2db->syncstats->tablesyncstats, sharding2db->refresh_path);

        /* received end, delete file first */
        if (0 == stat(file_path_complete->data, &st))
        {
            elog(RLOG_DEBUG, "refresh complete sharding: %s", file_name->data);
            osal_durable_unlink(file_path_complete->data, RLOG_DEBUG);
        }

        refresh_table_sharding_free(table_shard);
    }

    deleteStringInfo(file_name);
    deleteStringInfo(file_path_complete);
    deleteStringInfo(sql);
    refresh_sharding2db_delrefresh(sharding2db);
    pthread_exit(NULL);
    return NULL;
}

refresh_sharding2db* refresh_sharding2db_init(void)
{
    refresh_sharding2db* sharding2db = NULL;

    sharding2db = (refresh_sharding2db*)rmalloc0(sizeof(refresh_sharding2db));
    if (NULL == sharding2db)
    {
        elog(RLOG_WARNING, "refreshsharding2db out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(sharding2db, 0, 0, sizeof(refresh_sharding2db));
    sharding2db->syncstats = refresh_integratesyncstate_init();

    return sharding2db;
}

void refresh_sharding2db_free(void* args)
{
    refresh_sharding2db* sharding2db = NULL;

    sharding2db = (refresh_sharding2db*)args;

    if (sharding2db->syncstats->base.conn)
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
        refresh_integratesync_destroy(sharding2db->syncstats);
    }
    rfree(sharding2db);
}
