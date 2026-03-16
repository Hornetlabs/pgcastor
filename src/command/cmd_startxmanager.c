#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/path/ripple_path.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/daemon/ripple_process.h"
#include "misc/ripple_misc_lockfiles.h"
#include "signal/ripple_signal.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/ripple_netserver.h"
#include "net/ripple_netpool.h"
#include "xmanager/ripple_xmanager_listen.h"
#include "xmanager/ripple_xmanager_auth.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager.h"
#include "command/ripple_cmd_startxmanager.h"


/* 启动常驻线程 */
static bool ripple_cmd_startxmanagerthreads(ripple_xmanager* xmgr)
{
    ripple_thrnode* thrnode                 = NULL;

    /*-------------------------------启动常驻工作线程 begin---------------------------------*/
    /* 启动的顺序为退出的逆序, 即先启动的后退出 */

    /* 启动metric线程 */
    if (false == ripple_threads_addpersistthread(xmgr->threads,
                                                 &thrnode,
                                                 RIPPLE_THRNODE_IDENTITY_XMANAGER_METRIC,
                                                 xmgr->persistno,
                                                 (void*)xmgr->metric,
                                                 NULL,
                                                 NULL,
                                                 ripple_xmanager_metric_main))
    {
        elog(RLOG_WARNING, "add xmanager metric module persist to threads error");
        return false;
    }

    /* 启动auth线程 */
    if (false == ripple_threads_addpersistthread(xmgr->threads,
                                                 &thrnode,
                                                 RIPPLE_THRNODE_IDENTITY_XMANAGER_AUTH,
                                                 xmgr->persistno,
                                                 (void*)xmgr->auth,
                                                 NULL,
                                                 NULL,
                                                 ripple_xmanager_auth_main))
    {
        elog(RLOG_WARNING, "add xmanager auth module persist to threads error");
        return false;
    }

    /* 启动监听线程 */
    if (false == ripple_threads_addpersistthread(xmgr->threads,
                                                 &thrnode,
                                                 RIPPLE_THRNODE_IDENTITY_XMANAGER_LISTEN,
                                                 xmgr->persistno,
                                                 (void*)xmgr->listens,
                                                 NULL,
                                                 NULL,
                                                 ripple_xmanager_listen_main))
    {
        elog(RLOG_WARNING, "add xmanager listen module persist to threads error");
        return false;
    }

    /*-------------------------------启动常驻工作线程   end---------------------------------*/
    return true;
}

/* xmanager 启动 */
bool ripple_cmd_startxmanager(void)
{
    char* wdata                 = NULL;
    ripple_xmanager* xmgr       = NULL;

    /*
     * 1、获取工作目录
     * 2、校验是否存在
     * 3、切换工作目录
     */
    wdata = guc_getdata();
    if (false == DirExist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    /* 设置为后台运行 */
    ripple_makedaemon();

    /* 获取主线程号 */
    g_mainthrid = pthread_self();

    /* 在 wdata 查看锁文件是否存在,不存在则创建,存在则检测进程是否启动 */
    ripple_misc_lockfiles_create(RIPPLE_LOCK_FILE);

    /* log 初始化 */
    ripple_log_init();

    xmgr = ripple_xmanager_init();
    if (NULL == xmgr)
    {
        elog(RLOG_WARNING, "xmanager init error");
        return false;
    }

    /* 
     * 启动工作线程
     */
    /* 设置信号处理函数 */
    ripple_signal_init();

    /*
     * 添加主常驻线程
     */
    if (false == ripple_threads_addpersist(xmgr->threads, &xmgr->persistno, "XMANAGER"))
    {
        elog(RLOG_WARNING, "add xmanager persist to threads error");
        return false;
    }

    /* 启动常驻工作线程 */
    if (false == ripple_cmd_startxmanagerthreads(xmgr))
    {
        elog(RLOG_WARNING, "start capture increment persist job threads error");
        return false;
    }

    /* 解除信号屏蔽 */
    ripple_singal_setmask();

    elog(RLOG_INFO, "xmanager start, pid:%d", getpid());

    ripple_log_destroyerrorstack();

    /* 关闭标准输入/输出/错误 */
    ripple_closestd();

    while(1)
    {
        /* 日志信息打印 */
        if (true == g_gotsigterm)
        {
            /* 捕获到 sigterm 信号, 设置线程退出 */
            ripple_threads_exit(xmgr->threads);
            break;
        }

        /* 启动线程 */
        ripple_threads_startthread(xmgr->threads);

        /* 尝试捕获异常线程 */
        ripple_threads_tryjoin(xmgr->threads);

        /* 回收 FREE 节点 */
        ripple_threads_thrnoderecycle(xmgr->threads);

        if (false == ripple_threads_hasthrnode(xmgr->threads))
        {
            /* 所有的线程退出, 主线程退出 */
            /* TODO 内容文件落盘 */

            break;
        }

        usleep(100000);
    }

    ripple_xmanager_destroy(xmgr);

    ripple_misc_lockfiles_unlink(0, NULL);

    /* 泄露内存打印 */
    ripple_mem_print(RIPPLE_MEMPRINT_ALL);
    return true;
}