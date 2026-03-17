#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/mpage/mpage.h"
#include "utils/uuid/uuid.h"
#include "utils/daemon/process.h"
#include "utils/init/databaserecv.h"
#include "misc/misc_lockfiles.h"
#include "signal/app_signal.h"
#include "queue/queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "task/task_slot.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "threads/threads.h"
#include "parser/trail/parsertrail.h"
#include "rebuild/rebuild.h"
#include "sync/sync.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "onlinerefresh/onlinerefresh_persist.h"
#include "onlinerefresh/integrate/onlinerefresh_integrate.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "metric/integrate/metric_integrate.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/integrate/bigtxn_integratemanager.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"
#include "increment/integrate/split/increment_integratesplittrail.h"
#include "increment/integrate/sync/increment_integratesync.h"
#include "increment/integrate/rebuild/increment_integraterebuild.h"
#include "increment/integrate/increment_integrate.h"
#include "command/cmd_startintegrate.h"

/* 启动常驻线程 */
static bool cmd_startintegratethreads(increment_integrate* incintegrate)
{
    thrnode* thrnode                 = NULL;

    /*-------------------------------启动常驻工作线程 begin---------------------------------*/
    /* 启动的顺序为退出的逆序, 即先启动的后退出 */
    /* 启动应用线程 */
    if(false == threads_addpersistthread(incintegrate->threads,
                                                &thrnode,
                                                THRNODE_IDENTITY_INC_INTEGRATE_SYNC,
                                                incintegrate->persistno,
                                                (void*)incintegrate->syncworkstate,
                                                NULL,
                                                NULL,
                                                increment_integratesync_main))
    {
        elog(RLOG_WARNING, "add integrate increment bigtxn sync persist to threads error");
        return false;
    }

    /* 启动重组线程 */
    if(false == threads_addpersistthread(incintegrate->threads,
                                                &thrnode,
                                                THRNODE_IDENTITY_INC_INTEGRATE_REBUILD,
                                                incintegrate->persistno,
                                                (void*)incintegrate->rebuild,
                                                NULL,
                                                NULL,
                                                increment_integraterebuild_main))
    {
        elog(RLOG_WARNING, "add integrate increment rebuild persist to threads error");
        return false;
    }

    /* 启动解析器线程 */
    if(false == threads_addpersistthread(incintegrate->threads,
                                                &thrnode,
                                                THRNODE_IDENTITY_INC_INTEGRATE_PARSER,
                                                incintegrate->persistno,
                                                (void*)incintegrate->decodingctx,
                                                NULL,
                                                NULL,
                                                increment_integrateparsertrail_main))
    {
        elog(RLOG_WARNING, "add integrate increment parser persist to threads error");
        return false;
    }

    /* 启动spli trail线程 */
    if(false == threads_addpersistthread(incintegrate->threads,
                                                &thrnode,
                                                THRNODE_IDENTITY_INC_INTEGRATE_LOADRECORDS,
                                                incintegrate->persistno,
                                                (void*)incintegrate->splittrailctx,
                                                NULL,
                                                NULL,
                                                increment_integratesplitrail_main))
    {
        elog(RLOG_WARNING, "add integrate increment splittrail persist to threads error");
        return false;
    }

    /* 启动 状态 线程 */
    if(false == threads_addpersistthread(incintegrate->threads,
                                                &thrnode,
                                                THRNODE_IDENTITY_INTEGRATE_METRIC,
                                                incintegrate->persistno,
                                                (void*)incintegrate->integratestate,
                                                NULL,
                                                NULL,
                                                metric_integrate_main))
    {
        elog(RLOG_WARNING, "add integrate increment metric persist to threads error");
        return false;
    }

    /*-------------------------------启动常驻工作线程   end---------------------------------*/
    return true;
}

/* integrate 启动 */
bool cmd_startintegrate(void)
{
    /*
     * 1、切换工作目录
     * 2、创建锁文件
     * 3、初始化 log 信息
     */
    bool bret                                   = true;
    int gctime                                  = 0;
    int forcefree                               = 0;
    char* wdata                                 = NULL;
    increment_integrate* incintegrate    = NULL;

    /* 获取工作目录 */
    wdata = guc_getdata();

    /* 检测 data 目录是否存在 */
    if(false == osal_dir_exist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        bret = false;
        goto cmd_startintegrate_done;
    }

    /* 切换工作目录 */
    chdir(wdata);

    /* 设置为后台运行 */
    makedaemon();

    /* 获取主线程号 */
    g_mainthrid = pthread_self();

    /* 在 wdata 查看锁文件是否存在,不存在则创建,存在则检测进程是否启动 */
    misc_lockfiles_create(LOCK_FILE);

    /* log 初始化 */
    log_init();

    /* 获取内存回收时间 */
    gctime = guc_getConfigOptionInt(CFG_KEY_GCTIME);

    /* incintegrate */
    incintegrate = increment_integrate_init();

    /* 设置信号处理函数 */
    signal_init();

    /* 创建同步表 */
    if(false == databaserecv_integrate_dbinit())
    {
        bret = false;
        goto cmd_startintegrate_done;
    }

    if (false == increment_integrate_refreshload(incintegrate))
    {
        elog(RLOG_WARNING, "read refresh file error");
        bret = false;
        goto cmd_startintegrate_done;
    }

    /* 加载onlinerefresh状态文件 生成 onlinerefresh节点 */
    if (false == increment_integrate_onlinerefreshload(incintegrate))
    {
        elog(RLOG_WARNING, "load onlinerefresh error");
        bret = false;
        goto cmd_startintegrate_done;
    }

    /*
     * 添加主常驻线程
     */
    if(false == threads_addpersist(incintegrate->threads, &incintegrate->persistno, "INTEGRATE INCREMENT"))
    {
        elog(RLOG_WARNING, "add integrate increment persist to threads error");
        bret = false;
        goto cmd_startintegrate_done;
    }

    /* 启动常驻工作线程 */
    if(false == cmd_startintegratethreads(incintegrate))
    {
        bret = false;
        elog(RLOG_WARNING, "start integrate increment persist job threads error");
        goto cmd_startintegrate_done;
    }

    /* 解除信号屏蔽 */
    singal_setmask();

    elog(RLOG_INFO, "xsynch integrate start, pid:%d", getpid());

    log_destroyerrorstack();

    /* 关闭标准输入/输出/错误 */
    closestd();

    while(1)
    {
        /* 日志信息打印 */
        if(true == g_gotsigterm)
        {
            /* 捕获到 sigterm 信号, 设置线程退出 */
            threads_exit(incintegrate->threads);
            break;
        }

        /*
         * refresh 处理
         *  1、启动 refresh 节点
         *  2、回收完成的 refresh 节点
         */
        /* 启动 refresh 节点 */
        if(false == increment_integrate_startrefresh(incintegrate))
        {
            elog(RLOG_WARNING, "start refresh error");
            break;
        }

        /* 尝试回收 refresh 节点 */
        if(false == increment_integrate_tryjoinonrefresh(incintegrate))
        {
            elog(RLOG_WARNING, "try join on refresh error");
            break;
        }

        /* 
         * onlinerefresh 处理
         *  1、启动 onlinerefesh 节点
         *  2、回收完成的 onlinerefresh 节点
         */
        /* 启动 onlinerefesh 节点 */
        if(false == increment_integrate_startonlinerefresh(incintegrate))
        {
            elog(RLOG_WARNING, "start onlinerefresh error");
            break;
        }

        /* 尝试回收 onlinerefresh 节点 */
        if(false == increment_integrate_tryjoinononlinerefresh(incintegrate))
        {
            elog(RLOG_WARNING, "try join on onlinerefresh error");
            break;
        }

        /* 
         * bigtxn 处理
         *  1、启动 bigtxn 节点
         *  2、回收完成的 bigtxn 节点
         */
        /* 启动 bigtxn 节点 */
        if(false == increment_integrate_startbigtxn(incintegrate))
        {
            elog(RLOG_WARNING, "start bigtxn error");
            break;
        }

        /* 尝试回收 bigtxn 节点 */
        if(false == increment_integrate_tryjoinonbigtxn(incintegrate))
        {
            elog(RLOG_WARNING, "try join on bigtxn error");
            break;
        }

        /* 启动线程 */
        threads_startthread(incintegrate->threads);

        /* 尝试捕获异常线程 */
        threads_tryjoin(incintegrate->threads);

        /* 回收 FREE 节点 */
        threads_thrnoderecycle(incintegrate->threads);

        if(false == threads_hasthrnode(incintegrate->threads))
        {
            /* 所有的线程退出, 主线程退出 */
            break;
        }

        if(0 == gctime)
        {
            ;
        }
        else if(gctime > forcefree)
        {
            forcefree++;
        }
        else
        {
            /* 回收内存 */
            malloc_trim(0);
            forcefree = 0;
        }
        sleep(1);
        continue;
    }

    /* 所有的线程都退出了, 那么主线程也退出 */
cmd_startintegrate_done:

    /* 落盘refresh信息 */
    if (NULL != incintegrate)
    {
        increment_integrate_refreshflush(incintegrate);

        /* 落盘onlinerefresh信息 */
        if (NULL != incintegrate->rebuild)
        {
            onlinerefresh_persist_write(incintegrate->rebuild->olpersist);
        }

        /* incintegrate 资源回收*/
        increment_integrate_destroy(incintegrate);
    }

    /* 锁文件释放 */
    misc_lockfiles_unlink(0, NULL);

    /* 泄露内存打印 */
    mem_print(MEMPRINT_ALL);
    return bret;
}
