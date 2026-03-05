#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/license/license.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/daemon/ripple_process.h"
#include "misc/ripple_misc_stat.h"
#include "misc/ripple_misc_lockfiles.h"
#include "signal/ripple_signal.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/ripple_netserver.h"
#include "storage/ripple_file_buffer.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "filetransfer/collector/ripple_filetransfer_collector.h"
#include "metric/collector/ripple_metric_collector.h"
#include "increment/collector/net/ripple_increment_collectornetsvr.h"
#include "increment/collector/flush/ripple_increment_collectorflush.h"
#include "increment/collector/ripple_increment_collector.h"
#include "command/ripple_cmd_startcollector.h"

/* 启动落盘线程 */
static bool ripple_cmd_startcollector_flushthread(ripple_increment_collector* inccollector)
{
    int iret                                            = 0;
    ListCell* lc                                        = NULL;
    ripple_thrnode* thrnode                             = NULL;
    ripple_increment_collectorflushnode* flushnode      = NULL;

    /* 启动中转写线程 */
    iret = ripple_thread_lock(&inccollector->flushlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }
    foreach(lc, inccollector->flushthreads)
    {
        flushnode = (ripple_increment_collectorflushnode*)lfirst(lc);
        if (RIPPLE_INCREMENT_COLLECTORFLUSHNODE_STAT_INIT == flushnode->stat)
        {
            /* 启动落盘线程 */
            if(false == ripple_threads_addpersistthread(inccollector->threads,
                                                        &thrnode,
                                                        RIPPLE_THRNODE_IDENTITY_INC_COLLECTOR_FLUSH,
                                                        inccollector->persistno,
                                                        (void*)flushnode->flush,
                                                        NULL,
                                                        NULL,
                                                        ripple_increment_collectorflush_main))
            {
                elog(RLOG_WARNING, "add collector increment flush persist to threads error");
                ripple_thread_unlock(&inccollector->flushlock);
                return false;
            }
            flushnode->stat = RIPPLE_INCREMENT_COLLECTORFLUSHNODE_STAT_WORK;
        }
    }
    ripple_thread_unlock(&inccollector->flushlock);

    return true;
}

/* 启动常驻线程 */
static bool ripple_cmd_startcollectorthreads(ripple_increment_collector* inccollector)
{
    ripple_thrnode* thrnode                 = NULL;

    /*-------------------------------启动常驻工作线程 begin---------------------------------*/
    /* 启动的顺序为退出的逆序, 即先启动的后退出 */
    /* 启动落盘线程 */
    if(false == ripple_threads_addpersistthread(inccollector->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_COLLECTOR_SVR,
                                                inccollector->persistno,
                                                (void*)inccollector->netsvr,
                                                NULL,
                                                NULL,
                                                ripple_increment_collectornetsvr_main))
    {
        elog(RLOG_WARNING, "add collector increment net service persist to threads error");
        return false;
    }

    /* 启动大事务序列化线程 */
    if(false == ripple_threads_addpersistthread(inccollector->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_COLLECTOR_FTP,
                                                inccollector->persistno,
                                                (void*)inccollector->ftptransfer,
                                                NULL,
                                                NULL,
                                                ripple_filetransfer_collector_main))
    {
        elog(RLOG_WARNING, "add collector filetransfer persist to threads error");
        return false;
    }

    /* 启动增量落盘线程 */
    if(false == ripple_threads_addpersistthread(inccollector->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_COLLECTOR_METRIC,
                                                inccollector->persistno,
                                                (void*)inccollector->collectorstate,
                                                NULL,
                                                NULL,
                                                ripple_metric_collector_main))
    {
        elog(RLOG_WARNING, "add collector increment flush persist to threads error");
        return false;
    }
    /*-------------------------------启动常驻工作线程   end---------------------------------*/
    return true;
}

/* collector */
bool ripple_cmd_startcollector(void)
{
    /*
     * 1、license check
     * 2、切换工作目录
     * 3、创建锁文件
     * 4、初始化 log 信息
     */
    bool bret                                           = true;
    int gctime                                          = 0;
    int forcefree                                       = 0;
    char* wdata                                         = NULL;
    ripple_increment_collector* inccollector            = NULL;

    /* 获取工作目录 */
    wdata = guc_getdata();

    /* 校验license */
    if (false == ripple_license_check(g_cfgpath))
    {
        elog(RLOG_WARNING, "license expired");
        bret = false;
        goto ripple_cmd_startcollector_done;
    }

    /* 检测 data 目录是否存在 */
    if(false == DirExist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        bret = false;
        goto ripple_cmd_startcollector_done;
    }

    /* 切换工作目录 */
    chdir(wdata);

    /* 设置为后台运行 */
    ripple_makedaemon();

    /* 在 wdata 查看锁文件是否存在,不存在则创建,存在则检测进程是否启动 */
    ripple_misc_lockfiles_create(RIPPLE_LOCK_FILE);

    /* log 初始化 */
    ripple_log_init();

    /* 设置信号处理函数 */
    ripple_signal_init();

    /* 获取内存回收时间 */
    gctime = guc_getConfigOptionInt(RIPPLE_CFG_KEY_GCTIME);

    /* 初始化collectorstate */
    inccollector = ripple_increment_collector_init();
    if (NULL == inccollector)
    {
        elog(RLOG_WARNING, "collector init error");
        bret = false;
        goto ripple_cmd_startcollector_done;
    }

    /*
     * 添加主常驻线程
     */
    if(false == ripple_threads_addpersist(inccollector->threads, &inccollector->persistno, "COLLECTOR INCREMENT"))
    {
        elog(RLOG_WARNING, "add collector increment persist to threads error");
        bret = false;
        goto ripple_cmd_startcollector_done;
    }

    /* 启动常驻工作线程 */
    if(false == ripple_cmd_startcollectorthreads(inccollector))
    {
        elog(RLOG_WARNING, "start collector increment persist job threads error");
        bret = false;
        goto ripple_cmd_startcollector_done;
    }

    /* 解除信号屏蔽 */
    ripple_singal_setmask();

    elog(RLOG_INFO, "xsynch collector start, pid:%d", getpid());

    ripple_log_destroyerrorstack();

    /* 关闭标准输入/输出/错误 */
    ripple_closestd();

    while(1)
    {
        /* 日志信息打印 */
        if(true == g_gotsigterm)
        {
            /* 捕获到 sigterm 信号, 设置线程退出 */
            ripple_threads_exit(inccollector->threads);
            break;
        }

        /* 启动线程 */
        ripple_threads_startthread(inccollector->threads);

        /* 启动落盘线程 */
        if (false == ripple_cmd_startcollector_flushthread(inccollector))
        {
            break;
        }

        /* 尝试捕获异常线程 */
        ripple_threads_tryjoin(inccollector->threads);

        /* 回收 FREE 节点 */
        ripple_threads_thrnoderecycle(inccollector->threads);

        if(false == ripple_threads_hasthrnode(inccollector->threads))
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


        break;
    }


ripple_cmd_startcollector_done:
    /* 资源回收 */
    ripple_increment_collector_destroy(inccollector);

    ripple_misc_lockfiles_unlink(0, NULL);

    elog(RLOG_INFO, "collector exit");
    /* 泄露内存打印 */
    ripple_mem_print(RIPPLE_MEMPRINT_ALL);
    return bret;
}
