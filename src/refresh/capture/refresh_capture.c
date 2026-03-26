#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/conn/conn.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "storage/file_buffer.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/capture/refresh_capture.h"
#include "refresh/sharding2file/refresh_sharding2file.h"

typedef enum REFRESH_CAPTURE_STAT
{
    REFRESH_CAPTURE_STAT_JOBNOP = 0x00,
    REFRESH_CAPTURE_STAT_JOBSTARTING,   /* worker thread starting */
    REFRESH_CAPTURE_STAT_JOBWORKING,    /* worker thread working */
    REFRESH_CAPTURE_STAT_JOBWAITINGDONE /* waiting for worker thread to complete */
} refresh_capture_stat;

/* initialize */
refresh_capture* refresh_capture_init(void)
{
    refresh_capture* rcapture = NULL;

    /* initialize main structure */
    rcapture = rmalloc0(sizeof(refresh_capture));
    if (NULL == rcapture)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rcapture, 0, 0, sizeof(refresh_capture));
    rcapture->thrsmgr = NULL;
    rcapture->parallelcnt = guc_getConfigOptionInt(CFG_KEY_MAX_WORK_PER_REFRESH);
    if (0 == rcapture->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }
    rcapture->conn_info = guc_getConfigOption(CFG_KEY_URL);
    rcapture->refresh_path = rmalloc0(MAXPGPATH);
    if (NULL == rcapture->refresh_path)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rcapture->refresh_path, 0, 0, MAXPGPATH);
    sprintf(rcapture->refresh_path, "%s/%s", guc_getConfigOption(CFG_KEY_DATA), REFRESH_REFRESH);

    /* initialize task queue */
    rcapture->tqueue = queue_init();
    if (NULL == rcapture->tqueue)
    {
        elog(RLOG_WARNING, "task queue init error");
        return NULL;
    }

    return rcapture;
}

/* traverse tables to be refreshed, generate queue */
static bool refresh_capture_tables2shardings(refresh_capture* rcapture)
{
    int            max_shard_num = 0;
    uint32         ctid_blkid_max = 0;
    StringInfo     str = NULL;
    PGresult*      res = NULL;
    PGconn*        conn = rcapture->conn;
    refresh_table* table = NULL;

    str = makeStringInfo();
    max_shard_num = guc_getConfigOptionInt(CFG_KEY_MAX_PAGE_PER_REFRESHSHARDING);
    elog(RLOG_DEBUG, "refresh capture table 2 sharding, gen queue begin");

    /* first check if connection exists, open connection if not */
    while (!conn)
    {
        conn = conn_get(rcapture->conn_info);
        if (NULL != conn)
        {
            rcapture->conn = conn;
            break;
        }
    }

    /* traverse sync table list, query max ctid estimate count, shard and generate tasks */
    for (table = rcapture->tables->tables; table != NULL; table = table->next)
    {
        uint32 left = 0;
        uint32 right = 0;
        uint32 remain = 0;
        int    shard_no = 1;

        appendStringInfo(str,
                         "select pg_relation_size('\"%s\".\"%s\"')/%d;",
                         table->schema,
                         table->table,
                         g_blocksize);
        res = PQexec(conn, str->data);
        if (PGRES_TUPLES_OK != PQresultStatus(res))
        {
            elog(RLOG_WARNING, "Failed execute in compute max ctid: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            rcapture->conn = NULL;
            deleteStringInfo(str);
            return false;
        }

        if (PQntuples(res) != 0)
        {
            ctid_blkid_max = (uint32)atoi(PQgetvalue(res, 0, 0));
        }
        else
        {
            elog(RLOG_WARNING, "Failed execute in compute max ctid: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            rcapture->conn = NULL;
            deleteStringInfo(str);
            return false;
        }

        /* reset */
        resetStringInfo(str);
        PQclear(res);

        /* if no data, directly set sharding to 0 */
        if (ctid_blkid_max == 0)
        {
            refresh_table_sharding* table_shard = NULL;

            table_shard = refresh_table_sharding_init();

            /* sharding */
            refresh_table_sharding_set_schema(table_shard, table->schema);
            refresh_table_sharding_set_table(table_shard, table->table);
            refresh_table_sharding_set_shardings(table_shard, 0);
            refresh_table_sharding_set_shardno(table_shard, 0);
            refresh_table_sharding_set_condition(table_shard, NULL);

            elog(RLOG_DEBUG,
                 "capture refresh queue: %s.%s %4d %4d",
                 table_shard->schema,
                 table_shard->table,
                 table_shard->shardings,
                 table_shard->sharding_no);
            /* add to cache */
            queue_put(rcapture->tqueue, (void*)table_shard);
            continue;
        }

        /* first calculate sharding value */
        right = ctid_blkid_max < max_shard_num ? ctid_blkid_max : max_shard_num;
        remain = ctid_blkid_max;

        /* generate queue */
        do
        {
            refresh_table_sharding*  table_shard = NULL;
            refresh_table_condition* cond = NULL;

            table_shard = refresh_table_sharding_init();
            cond = refresh_table_sharding_condition_init();

            /* sharding */
            refresh_table_sharding_set_schema(table_shard, table->schema);
            refresh_table_sharding_set_table(table_shard, table->table);
            refresh_table_sharding_set_shardings(table_shard,
                                                 ((ctid_blkid_max - 1) / max_shard_num) + 1);
            refresh_table_sharding_set_shardno(table_shard, shard_no++);
            refresh_table_sharding_set_condition(table_shard, cond);

            cond->left_condition = left;
            cond->right_condition = right;

            elog(RLOG_DEBUG,
                 "capture refresh queue: %s.%s %4d %4d",
                 table_shard->schema,
                 table_shard->table,
                 table_shard->shardings,
                 table_shard->sharding_no);

            /* add to cache */
            queue_put(rcapture->tqueue, (void*)table_shard);

            remain = ctid_blkid_max - right;
            left = right;
            right += remain > max_shard_num ? max_shard_num : remain;
        } while (remain);
    }

    /* cleanup */
    deleteStringInfo(str);

    return true;
}

/* create directories */
static void refresh_capture_trymkdir(refresh_tables* tables)
{
    char*          data_path = NULL;
    StringInfo     path = NULL;
    StringInfo     path_partial = NULL;
    StringInfo     path_complete = NULL;
    refresh_table* table = NULL;

    path = makeStringInfo();
    path_partial = makeStringInfo();
    path_complete = makeStringInfo();

    data_path = guc_getConfigOption(CFG_KEY_DATA);

    for (table = tables->tables; table != NULL; table = table->next)
    {
        resetStringInfo(path);
        appendStringInfo(
            path, "%s/%s/%s_%s", data_path, REFRESH_REFRESH, table->schema, table->table);

        elog(RLOG_DEBUG, "path:%s", path->data);
        if (!osal_dir_exist(path->data))
        {
            resetStringInfo(path_partial);
            resetStringInfo(path_complete);

            if (0 != osal_make_dir(path->data))
            {
                elog(RLOG_ERROR, "could not create directory:%s", path);
            }
            appendStringInfo(path_partial, "%s/%s", path->data, REFRESH_PARTIAL);
            appendStringInfo(path_complete, "%s/%s", path->data, REFRESH_COMPLETE);

            if (0 != osal_make_dir(path_partial->data))
            {
                elog(RLOG_ERROR, "could not create directory:%s", path_partial->data);
            }

            if (0 != osal_make_dir(path_complete->data))
            {
                elog(RLOG_ERROR, "could not create directory:%s", path_complete->data);
            }
        }
    }

    deleteStringInfo(path);
    deleteStringInfo(path_complete);
    deleteStringInfo(path_partial);
}

/* start worker threads */
static bool refresh_capture_startjobs(refresh_capture* rcapture)
{
    int                         index = 0;
    task_refresh_sharding2file* sharding2file = NULL;
    elog(RLOG_DEBUG, "capture refresh, work thread num: %d", rcapture->parallelcnt);

    /* allocate space for each thread */
    for (index = 0; index < rcapture->parallelcnt; index++)
    {
        /* allocate space and initialize */
        sharding2file = refresh_sharding2file_init();
        sharding2file->conn = NULL;
        sharding2file->conn_info = rcapture->conn_info;
        sharding2file->refresh_path = rstrdup(rcapture->refresh_path);
        sharding2file->snap_shot_name = rcapture->snap_shot_name;
        sharding2file->tqueue = rcapture->tqueue;

        /* register worker thread */
        if (false == threads_addjobthread(rcapture->thrsmgr->parents,
                                          THRNODE_IDENTITY_CAPTURE_REFRESH_JOB,
                                          rcapture->thrsmgr->submgrref.no,
                                          (void*)sharding2file,
                                          refresh_sharding2file_free,
                                          NULL,
                                          refresh_sharding2file_work))
        {
            elog(RLOG_WARNING, "refresh capture start job error");
            return false;
        }
    }

    return true;
}

/* set snapshot name */
void refresh_capture_setsnapshotname(refresh_capture* rcapture, char* snapname)
{
    rcapture->snap_shot_name = rstrdup(snapname);
}

void refresh_capture_setrefreshtables(refresh_tables* tables, refresh_capture* rcapture)
{
    rcapture->tables = tables;
}

void refresh_capture_setconn(PGconn* conn, refresh_capture* rcapture)
{
    rcapture->conn = conn;
}

static bool refresh_capture_keep_alive(PGconn* conn)
{
    PGresult* res = NULL;
    res = PQexec(conn, "SELECT 1;");
    if (PGRES_TUPLES_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING, "Failed in snapshot keep alive: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

void* refresh_capture_main(void* args)
{
    int                  jobcnt = 0;
    uint32               delay = 200;
    refresh_capture_stat jobstat = REFRESH_CAPTURE_STAT_JOBNOP;
    thrnode*             thr_node = NULL;
    refresh_capture*     rcapture = NULL;

    thr_node = (thrnode*)args;
    rcapture = (refresh_capture*)thr_node->data;

    /* check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "refresh capture stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    /* can exit if there are no tables to sync */
    if (NULL == rcapture->tables || 0 == rcapture->tables->cnt)
    {
        elog(RLOG_INFO, "There are no tables to be synchronized, so no refresh is needed");
        thr_node->stat = THRNODE_STAT_EXIT;
        pthread_exit(NULL);
        return NULL;
    }

    elog(RLOG_DEBUG, "capture refresh start");

    /* create directories first, only creates if directory doesn't exist */
    refresh_capture_trymkdir(rcapture->tables);

    /* get configured number of existing worker threads and initialize management structure */
    if (false == refresh_capture_startjobs(rcapture))
    {
        elog(RLOG_INFO, "capture refresh start job threads error");
        thr_node->stat = THRNODE_STAT_EXIT;
        pthread_exit(NULL);
        return NULL;
    }

    jobstat = REFRESH_CAPTURE_STAT_JOBSTARTING;
    while (true)
    {
        /*
         * first check if exit signal received
         *  for sub-manager thread, there are two scenarios for receiving TERM signal:
         *  1. parent persistent thread of sub-manager thread exits
         *  2. exit flag received
         *
         * in both scenarios, sub-manager thread does not need to set worker threads to FREE state
         */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        usleep(50000);

        if (delay >= 200)
        {
            refresh_capture_keep_alive(rcapture->conn);
            delay = 0;
        }

        /* wait for all worker threads to start successfully */
        if (REFRESH_CAPTURE_STAT_JOBSTARTING == jobstat)
        {
            /* check if already started successfully */
            jobcnt = 0;
            delay++;
            if (false == threads_countsubmgrjobthredsabovework(
                             rcapture->thrsmgr->parents, rcapture->thrsmgr->childthrrefs, &jobcnt))
            {
                elog(RLOG_WARNING, "capture refresh count job thread above work stat error");
                thr_node->stat = THRNODE_STAT_ABORT;
                goto refresh_capture_main_done;
            }

            if (jobcnt != rcapture->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = REFRESH_CAPTURE_STAT_JOBWORKING;
            continue;
        }
        else if (REFRESH_CAPTURE_STAT_JOBWORKING == jobstat)
        {
            /* worker threads started, add work tasks to queue */
            if (false == refresh_capture_tables2shardings(rcapture))
            {
                /* failed to add tasks to queue, manager thread exits, main thread handles
                 * sub-thread cleanup */
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            jobstat = REFRESH_CAPTURE_STAT_JOBWAITINGDONE;
            delay++;
            continue;
        }
        else if (REFRESH_CAPTURE_STAT_JOBWAITINGDONE == jobstat)
        {
            /*
             * waiting for task completion has two parts
             *  1. task queue is empty
             *  2. sub-threads have fully exited
             */
            if (false == queue_isnull(rcapture->tqueue))
            {
                delay++;
                continue;
            }

            /* set idle threads to exit and count exited threads */
            jobcnt = rcapture->thrsmgr->childthrrefs->length;
            if (false ==
                threads_setsubmgrjobthredstermandcountexit(
                    rcapture->thrsmgr->parents, rcapture->thrsmgr->childthrrefs, 0, &jobcnt))
            {
                elog(RLOG_WARNING, "capture refresh set job threads term in idle error");
                thr_node->stat = THRNODE_STAT_ABORT;
                goto refresh_capture_main_done;
            }

            /* not fully exited, continue waiting */
            if (jobcnt != rcapture->thrsmgr->childthrrefs->length)
            {
                continue;
            }

            /* all threads have exited, set sub-thread state to FREE */
            threads_setsubmgrjobthredsfree(rcapture->thrsmgr->parents,
                                           rcapture->thrsmgr->childthrrefs,
                                           0,
                                           rcapture->parallelcnt);

            /* set this thread to exit */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
    }

refresh_capture_main_done:
    pthread_exit(NULL);
    return NULL;
}

/* free */
void refresh_capture_free(void* privdata)
{
    refresh_capture* rcapture = NULL;

    rcapture = (refresh_capture*)privdata;

    if (!rcapture)
    {
        return;
    }

    /* cleanup conn connection handle */
    if (rcapture->conn)
    {
        PQfinish(rcapture->conn);
        rcapture->conn = NULL;
    }

    /* cleanup snap_shot_name */
    if (rcapture->snap_shot_name)
    {
        rfree(rcapture->snap_shot_name);
    }

    if (rcapture->refresh_path)
    {
        rfree(rcapture->refresh_path);
    }

    if (rcapture->tqueue)
    {
        queue_destroy(rcapture->tqueue, NULL);
    }

    /* cleanup refresh tables */
    if (rcapture->tables)
    {
        refresh_freetables(rcapture->tables);
    }

    rfree(rcapture);
}
