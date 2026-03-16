#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/conn/ripple_conn.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "storage/ripple_file_buffer.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/capture/ripple_refresh_capture.h"
#include "refresh/sharding2file/ripple_refresh_sharding2file.h"

typedef enum RIPPLE_REFRESH_CAPTURE_STAT
{
    RIPPLE_REFRESH_CAPTURE_STAT_JOBNOP             = 0x00,
    RIPPLE_REFRESH_CAPTURE_STAT_JOBSTARTING     ,               /* 工作线程启动中 */
    RIPPLE_REFRESH_CAPTURE_STAT_JOBWORKING      ,               /* 工作线程工作状态 */
    RIPPLE_REFRESH_CAPTURE_STAT_JOBWAITINGDONE                  /* 等待工作线程工作完成 */
} ripple_refresh_capture_stat;

/* 初始化 */
ripple_refresh_capture *ripple_refresh_capture_init(void)
{
    ripple_refresh_capture *rcapture = NULL;

    /* 初始化主结构 */
    rcapture = rmalloc0(sizeof(ripple_refresh_capture));
    if (NULL == rcapture)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rcapture, 0, 0, sizeof(ripple_refresh_capture));
    rcapture->thrsmgr = NULL;
    rcapture->parallelcnt = guc_getConfigOptionInt(RIPPLE_CFG_KEY_MAX_WORK_PER_REFRESH);
    if(0 == rcapture->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }
    rcapture->conn_info = guc_getConfigOption(RIPPLE_CFG_KEY_URL);
    rcapture->refresh_path = rmalloc0(MAXPGPATH);
    if (NULL == rcapture->refresh_path)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rcapture->refresh_path, 0, 0, MAXPGPATH);
    sprintf(rcapture->refresh_path, "%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA), RIPPLE_REFRESH_REFRESH);

    /* 初始化任务队列 */
    rcapture->tqueue = ripple_queue_init();
    if(NULL == rcapture->tqueue)
    {
        elog(RLOG_WARNING, "task queue init error");
        return NULL;
    }

    return rcapture;
}

/* 遍历待refresh表, 生成queue */
static bool ripple_refresh_capture_tables2shardings(ripple_refresh_capture* rcapture)
{
    int max_shard_num       = 0;
    uint32 ctid_blkid_max   = 0;
    StringInfo str          = NULL;
    PGresult *res           = NULL;
    PGconn* conn            = rcapture->conn;
    ripple_refresh_table *table = NULL;

    str = makeStringInfo();
    max_shard_num = guc_getConfigOptionInt(RIPPLE_CFG_KEY_MAX_PAGE_PER_REFRESHSHARDING);
    elog(RLOG_DEBUG, "refresh capture table 2 sharding, gen queue begin");

    /* 先判断连接是否存在, 不存在打开连接 */
    while(!conn)
    {
        conn = ripple_conn_get(rcapture->conn_info);
        if (NULL != conn)
        {
            rcapture->conn = conn;
            break;
        }
    }

    /* 遍历待同步表链表, 查询最大ctid估值数, 进行分片后生成任务 */
    for (table = rcapture->tables->tables; table != NULL; table = table->next)
    {
        uint32 left = 0;
        uint32 right = 0;
        uint32 remain = 0;
        int    shard_no = 1;

        appendStringInfo(str, "select pg_relation_size('\"%s\".\"%s\"')/%d;", table->schema, table->table, g_blocksize);
        res = PQexec(conn, str->data);
        if (PGRES_TUPLES_OK != PQresultStatus(res))
        {
            elog(RLOG_WARNING,"Failed execute in compute max ctid: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            rcapture->conn = NULL;
            deleteStringInfo(str);
            return false;
        }

        if (PQntuples(res) != 0 )
        {
            ctid_blkid_max = (uint32) atoi(PQgetvalue(res, 0, 0));
        }
        else
        {
            elog(RLOG_WARNING,"Failed execute in compute max ctid: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            rcapture->conn = NULL;
            deleteStringInfo(str);
            return false;
        }

        /* 重置 */
        resetStringInfo(str);
        PQclear(res);

        /* 没有数据的情况下直接设置分片为0 */
        if (ctid_blkid_max == 0)
        {
            ripple_refresh_table_sharding *table_shard = NULL;

            table_shard = ripple_refresh_table_sharding_init();

            /* 分片 */
            ripple_refresh_table_sharding_set_schema(table_shard, table->schema);
            ripple_refresh_table_sharding_set_table(table_shard, table->table);
            ripple_refresh_table_sharding_set_shardings(table_shard, 0);
            ripple_refresh_table_sharding_set_shardno(table_shard, 0);
            ripple_refresh_table_sharding_set_condition(table_shard, NULL);

            elog(RLOG_DEBUG, "capture refresh queue: %s.%s %4d %4d",
                                table_shard->schema,
                                table_shard->table,
                                table_shard->shardings,
                                table_shard->sharding_no);
            /* 添加到缓存中 */
            ripple_queue_put(rcapture->tqueue, (void *)table_shard);
            continue;
        }

        /* 第一次计算分片值 */
        right = ctid_blkid_max < max_shard_num ? ctid_blkid_max : max_shard_num;
        remain = ctid_blkid_max;

        /* 生成queue */
        do
        {
            ripple_refresh_table_sharding *table_shard = NULL;
            ripple_refresh_table_condition *cond = NULL;

            table_shard = ripple_refresh_table_sharding_init();
            cond = ripple_refresh_table_sharding_condition_init();

            /* 分片 */
            ripple_refresh_table_sharding_set_schema(table_shard, table->schema);
            ripple_refresh_table_sharding_set_table(table_shard, table->table);
            ripple_refresh_table_sharding_set_shardings(table_shard, ((ctid_blkid_max - 1) / max_shard_num) + 1);
            ripple_refresh_table_sharding_set_shardno(table_shard, shard_no++);
            ripple_refresh_table_sharding_set_condition(table_shard, cond);

            cond->left_condition = left;
            cond->right_condition = right;

            elog(RLOG_DEBUG, "capture refresh queue: %s.%s %4d %4d",
                                table_shard->schema,
                                table_shard->table,
                                table_shard->shardings,
                                table_shard->sharding_no);

            /* 添加到缓存中 */
            ripple_queue_put(rcapture->tqueue, (void *)table_shard);

            remain = ctid_blkid_max - right;
            left = right;
            right += remain > max_shard_num ? max_shard_num : remain;
        } while (remain);
    }

    /* 清理工作 */
    deleteStringInfo(str);

    return true;
}

/* 创建目录 */
static void ripple_refresh_capture_trymkdir(ripple_refresh_tables *tables)
{
    char* data_path = NULL;
    StringInfo path = NULL;
    StringInfo path_partial = NULL;
    StringInfo path_complete = NULL;
    ripple_refresh_table *table = NULL;

    path = makeStringInfo();
    path_partial = makeStringInfo();
    path_complete = makeStringInfo();

    data_path = guc_getConfigOption(RIPPLE_CFG_KEY_DATA);

    for (table = tables->tables; table != NULL; table = table->next)
    {
        resetStringInfo(path);
        appendStringInfo(path, "%s/%s/%s_%s",
                                     data_path,
                                     RIPPLE_REFRESH_REFRESH,
                                     table->schema,
                                     table->table);

        elog(RLOG_DEBUG, "path:%s", path->data);
        if (!DirExist(path->data))
        {
            resetStringInfo(path_partial);
            resetStringInfo(path_complete);

            if(0 != MakeDir(path->data))
            {
                elog(RLOG_ERROR, "could not create directory:%s", path);
            }
            appendStringInfo(path_partial, "%s/%s",
                                            path->data,
                                            RIPPLE_REFRESH_PARTIAL);
            appendStringInfo(path_complete, "%s/%s",
                                            path->data,
                                            RIPPLE_REFRESH_COMPLETE);

            if(0 != MakeDir(path_partial->data))
            {
                elog(RLOG_ERROR, "could not create directory:%s", path_partial->data);
            }

            if(0 != MakeDir(path_complete->data))
            {
                elog(RLOG_ERROR, "could not create directory:%s", path_complete->data);
            }
        }
    }

    deleteStringInfo(path);
    deleteStringInfo(path_complete);
    deleteStringInfo(path_partial);
}

/* 启动工作线程 */
static bool ripple_refresh_capture_startjobs(ripple_refresh_capture *rcapture)
{
    int index                                           = 0;
    ripple_task_refresh_sharding2file* sharding2file    = NULL;
    elog(RLOG_DEBUG, "capture refresh, work thread num: %d", rcapture->parallelcnt);

    /* 为每一个线程分配空间 */
    for (index = 0; index < rcapture->parallelcnt; index++)
    {
        /* 分配空间和初始化 */
        sharding2file = ripple_refresh_sharding2file_init();
        sharding2file->conn = NULL;
        sharding2file->conn_info = rcapture->conn_info;
        sharding2file->refresh_path = rstrdup(rcapture->refresh_path);
        sharding2file->snap_shot_name = rcapture->snap_shot_name;
        sharding2file->tqueue = rcapture->tqueue;

        /* 注册工作线程 */
        if(false == ripple_threads_addjobthread(rcapture->thrsmgr->parents,
                                                RIPPLE_THRNODE_IDENTITY_CAPTURE_REFRESH_JOB,
                                                rcapture->thrsmgr->submgrref.no,
                                                (void*)sharding2file,
                                                ripple_refresh_sharding2file_free,
                                                NULL,
                                                ripple_refresh_sharding2file_work))
        {
            elog(RLOG_WARNING, "refresh capture start job error");
            return false;
        }
    }
    
    return true;
}

/* 设置快照名称 */
void ripple_refresh_capture_setsnapshotname(ripple_refresh_capture *rcapture, char *snapname)
{
    rcapture->snap_shot_name = rstrdup(snapname);
}

void ripple_refresh_capture_setrefreshtables(ripple_refresh_tables* tables, ripple_refresh_capture *rcapture)
{
    rcapture->tables = tables;
}

void ripple_refresh_capture_setconn(PGconn* conn, ripple_refresh_capture *rcapture)
{
    rcapture->conn = conn;
}

static bool ripple_refresh_capture_keep_alive(PGconn* conn)
{
    PGresult  *res = NULL;
    res = PQexec(conn, "SELECT 1;");
    if (PGRES_TUPLES_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,"Failed in snapshot keep alive: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

void *ripple_refresh_capture_main(void* args)
{
    int jobcnt                          = 0;
    uint32 delay                        = 200;
    ripple_refresh_capture_stat jobstat = RIPPLE_REFRESH_CAPTURE_STAT_JOBNOP;
    ripple_thrnode* thrnode             = NULL;
    ripple_refresh_capture *rcapture    = NULL;

    thrnode = (ripple_thrnode *)args;
    rcapture = (ripple_refresh_capture *)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "refresh capture stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 没有待同步的表的情况下可以退出 */
    if (NULL == rcapture->tables || 0 == rcapture->tables->cnt)
    {
        elog(RLOG_INFO, "There are no tables to be synchronized, so no refresh is needed");
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    elog(RLOG_DEBUG, "capture refresh start");

    /* 先创建目录, 只有不存在目录时才会创建 */
    ripple_refresh_capture_trymkdir(rcapture->tables);

    /* 获取配置的存量工作线程数量并初始化管理结构 */
    if(false == ripple_refresh_capture_startjobs(rcapture))
    {
        elog(RLOG_INFO, "capture refresh start job threads error");
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    jobstat = RIPPLE_REFRESH_CAPTURE_STAT_JOBSTARTING;
    while (true)
    {
        /* 
         * 首先判断是否接收到退出信号
         *  对于子管理线程，收到 TERM 信号有两种场景:
         *  1、子管理线程的上级常驻线程退出
         *  2、接收到了退出标识
         * 
         * 上述两种场景, 都不需要子管理线程设置工作线程为 FREE 状态
         */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        usleep(50000);

        if (delay >= 200)
        {
            ripple_refresh_capture_keep_alive(rcapture->conn);
            delay = 0;
        }

        /* 等待所有工作线程启动成功 */
        if(RIPPLE_REFRESH_CAPTURE_STAT_JOBSTARTING == jobstat)
        {
            /* 查看是否已经启动成功 */
            jobcnt = 0;
            delay++;
            if(false == ripple_threads_countsubmgrjobthredsabovework(rcapture->thrsmgr->parents,
                                                                    rcapture->thrsmgr->childthrrefs,
                                                                    &jobcnt))
            {
                elog(RLOG_WARNING, "capture refresh count job thread above work stat error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_refresh_capture_main_done;
            }

            if(jobcnt != rcapture->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = RIPPLE_REFRESH_CAPTURE_STAT_JOBWORKING;
            continue;
        }
        else if(RIPPLE_REFRESH_CAPTURE_STAT_JOBWORKING == jobstat)
        {
            /* 工作线程已经启动, 那么向队列中加入工作任务 */
            if(false == ripple_refresh_capture_tables2shardings(rcapture))
            {
                /* 向队列中加入任务失败, 那么管理线程退出, 子线程的回收由主线程处理 */
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
            jobstat = RIPPLE_REFRESH_CAPTURE_STAT_JOBWAITINGDONE;
            delay++;
            continue;
        }
        else if(RIPPLE_REFRESH_CAPTURE_STAT_JOBWAITINGDONE == jobstat)
        {
            /* 
             * 等待任务的完成分为两个部分
             *  1、任务队列为空
             *  2、子线程已完全退出
             */
            if(false == ripple_queue_isnull(rcapture->tqueue))
            {
                delay++;
                continue;
            }

            /* 设置空闲的线程退出并统计退出的线程个数 */
            jobcnt = rcapture->thrsmgr->childthrrefs->length;
            if(false == ripple_threads_setsubmgrjobthredstermandcountexit(rcapture->thrsmgr->parents,
                                                                        rcapture->thrsmgr->childthrrefs,
                                                                        0,
                                                                        &jobcnt))
            {
                elog(RLOG_WARNING, "capture refresh set job threads term in idle error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_refresh_capture_main_done;
            }

            /* 没有完全退出, 那么继续等待 */
            if(jobcnt != rcapture->thrsmgr->childthrrefs->length)
            {
                continue;
            }

            /* 所有线程已经退出, 那么设置子线程状态为 FREE */
            ripple_threads_setsubmgrjobthredsfree(rcapture->thrsmgr->parents,
                                                rcapture->thrsmgr->childthrrefs,
                                                0,
                                                rcapture->parallelcnt);

            /* 设置本线程退出 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
    }

ripple_refresh_capture_main_done:
    ripple_pthread_exit(NULL);
    return NULL;
}

/* 释放 */
void ripple_refresh_capture_free(void* privdata)
{
    ripple_refresh_capture *rcapture = NULL;

    rcapture = (ripple_refresh_capture *)privdata;

    if (!rcapture)
    {
        return;
    }
    

    /* 清理conn连接句柄 */
    if (rcapture->conn)
    {
        PQfinish(rcapture->conn);
        rcapture->conn = NULL;
    }

    /* 清理snap_shot_name */
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
        ripple_queue_destroy(rcapture->tqueue, NULL);
    }

    /* 清理refresh tables */
    if (rcapture->tables)
    {
        ripple_refresh_freetables(rcapture->tables);
    }

    rfree(rcapture);
}
