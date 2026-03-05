#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/p2csharding/ripple_refresh_p2csharding.h"
#include "refresh/pump/ripple_refresh_pump.h"

typedef enum RIPPLE_REFRESH_PUMPNODE_STAT
{
    RIPPLE_REFRESH_PUMPNODE_STAT_JOBNOP             = 0x00,
    RIPPLE_REFRESH_PUMPNODE_STAT_JOBSTARTING        ,               /* 工作线程启动中 */
    RIPPLE_REFRESH_PUMPNODE_STAT_JOBWORKING         ,               /* 工作线程工作状态 */
    RIPPLE_REFRESH_PUMPNODE_STAT_JOBWAITINGDONE                     /* 等待工作线程工作完成 */
} ripple_refresh_pumpnode_stat;

ripple_refresh_pump *ripple_refresh_pump_init(void)
{
    ripple_refresh_pump *rpump = NULL;

    rpump = (ripple_refresh_pump *)rmalloc0(sizeof(ripple_refresh_pump));
    if (NULL == rpump)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rpump, 0, 0, sizeof(ripple_refresh_pump));

    rpump->refresh_path = (char*)rmalloc0(MAXPGPATH);
    if (NULL == rpump->refresh_path)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rpump->refresh_path, 0, 0, MAXPGPATH);

    rpump->parallelcnt = guc_getConfigOptionInt(RIPPLE_CFG_KEY_MAX_WORK_PER_REFRESH);
    if(0 == rpump->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }
    snprintf(rpump->refresh_path, MAXPGPATH, "%s", guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR));

    rpump->sync_stats = NULL;
    rpump->stat = RIPPLE_REFRESH_PUMP_STAT_NOP;
    rpump->tqueue = ripple_queue_init();
    if(NULL == rpump->tqueue)
    {
        elog(RLOG_WARNING, "pump refresh init task queue error");
        return NULL;
    }
    return rpump;
}

/* 设置状态为 init */
void ripple_refresh_pump_setstatinit(ripple_refresh_pump* rpump)
{
    rpump->stat = RIPPLE_REFRESH_PUMP_STAT_INIT;
}

/* 设置需要刷新的 refresh 表 */
void ripple_refresh_pump_setsynctablestat(ripple_refresh_pump* rpump, ripple_refresh_table_syncstats* tbsyncstat)
{
    rpump->sync_stats = tbsyncstat;
}

/* 填充monitor结构 */
static bool ripple_refresh_pump_startjobs(ripple_refresh_pump *rpump)
{
    int index = 0;

    elog(RLOG_DEBUG, "pump refresh, work thread num: %d", rpump->parallelcnt);

    /* 初始化工作线程基础信息 */
    for (index = 0; index < rpump->parallelcnt; index++)
    {
        ripple_task_refresh_p2csharding *p2csharding = NULL;

        p2csharding = ripple_refresh_p2csharding_init();
        p2csharding->taskqueue = rpump->tqueue;
        p2csharding->syncstats = rpump->sync_stats;

        /* 注册工作线程 */
        if(false == ripple_threads_addjobthread(rpump->thrsmgr->parents,
                                                RIPPLE_THRNODE_IDENTITY_PUMP_REFRESH_JOB,
                                                rpump->thrsmgr->submgrref.no,
                                                (void*)p2csharding,
                                                ripple_refresh_p2csharding_free,
                                                NULL,
                                                ripple_refresh_p2csharding_main))
        {
            elog(RLOG_WARNING, "refresh capture start job error");
            return false;
        }
    }
    return true;
}

/* 线程处理入口 */
void *ripple_refresh_pump_main(void* args)
{
    int jobcnt = 0;
    ripple_refresh_pumpnode_stat jobstat = RIPPLE_REFRESH_PUMPNODE_STAT_JOBNOP;
    ripple_thrnode* thrnode = NULL;
    ripple_refresh_pump *rpump = NULL;

    thrnode = (ripple_thrnode*)args;
    rpump = (ripple_refresh_pump *)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    if (!rpump->sync_stats->tablesyncall || !rpump->sync_stats->tablesyncing)
    {
        elog(RLOG_INFO, "don't do refresh monitor sync_stats is null");
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        ripple_pthread_exit(NULL);
    }

    rpump->stat = RIPPLE_REFRESH_PUMP_STAT_WORK;

    /* 初始化必要数据 */
    if(false == ripple_refresh_pump_startjobs(rpump))
    {
        elog(RLOG_WARNING, "pump refresh start job threads error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 主循环 */
    jobstat = RIPPLE_REFRESH_PUMPNODE_STAT_JOBSTARTING;
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

        /* 等待所有工作线程启动成功 */
        if(RIPPLE_REFRESH_PUMPNODE_STAT_JOBSTARTING == jobstat)
        {
            /* 查看是否已经启动成功 */
            jobcnt = 0;
            if(false == ripple_threads_countsubmgrjobthredsabovework(rpump->thrsmgr->parents,
                                                                    rpump->thrsmgr->childthrrefs,
                                                                    &jobcnt))
            {
                elog(RLOG_WARNING, "pump refresh count job thread above work stat error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_pump_refresh_main_done;
            }

            if(jobcnt != rpump->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = RIPPLE_REFRESH_PUMPNODE_STAT_JOBWORKING;
            continue;
        }
        else if(RIPPLE_REFRESH_PUMPNODE_STAT_JOBWORKING == jobstat)
        {
            /* 遍历状态表和文件, 生成queue */
            if(false == ripple_refresh_table_syncstat_genqueue(rpump->sync_stats, (void*)rpump->tqueue, rpump->refresh_path))
            {
                elog(RLOG_WARNING, "pump refresh refresh tables 2 queue error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_pump_refresh_main_done;
            }

            /* 判断是否存在任务 保证任务全部加入到队列中 */
            if (NULL == rpump->sync_stats->tablesyncing)
            {
                jobstat = RIPPLE_REFRESH_PUMPNODE_STAT_JOBWAITINGDONE;
            }
            continue;
        }
        else if(RIPPLE_REFRESH_PUMPNODE_STAT_JOBWAITINGDONE == jobstat)
        {
            /* 
             * 等待任务的完成分为两个部分
             *  1、任务队列为空
             *  2、子线程已完全退出
             */
            if(false == ripple_queue_isnull(rpump->tqueue))
            {
                continue;
            }

            /* 设置空闲的线程退出并统计退出的线程个数 */
            jobcnt = rpump->thrsmgr->childthrrefs->length;
            if(false == ripple_threads_setsubmgrjobthredstermandcountexit(rpump->thrsmgr->parents,
                                                                        rpump->thrsmgr->childthrrefs,
                                                                        0,
                                                                        &jobcnt))
            {
                elog(RLOG_WARNING, "pump refresh set job threads term in idle error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_pump_refresh_main_done;
            }

            /* 没有完全退出, 那么继续等待 */
            if(jobcnt != rpump->thrsmgr->childthrrefs->length)
            {
                continue;
            }

            /* 所有线程已经退出, 那么设置子线程状态为 FREE */
            ripple_threads_setsubmgrjobthredsfree(rpump->thrsmgr->parents,
                                                rpump->thrsmgr->childthrrefs,
                                                0,
                                                rpump->parallelcnt);
            
            /* 设置本线程退出 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
    }

ripple_pump_refresh_main_done:
    rpump->stat = RIPPLE_REFRESH_PUMP_STAT_DONE;
    ripple_pthread_exit(NULL);
    return NULL;
}


/* 真实释放 */
void ripple_refresh_pump_free(ripple_refresh_pump *rpump)
{
    if (!rpump)
    {
        return;
    }

    if(NULL != rpump->refresh_path)
    {
        rfree(rpump->refresh_path);
        rpump->refresh_path = NULL;
    }

    if (rpump->sync_stats)
    {
        ripple_refresh_table_syncstats_free(rpump->sync_stats);
    }

    if (rpump->tqueue)
    {
        ripple_queue_destroy(rpump->tqueue, ripple_refresh_table_sharding_queuefree);
    }

    rfree(rpump);
}

