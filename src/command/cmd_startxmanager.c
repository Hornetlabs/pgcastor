#include "app_incl.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/path/path.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/daemon/process.h"
#include "misc/misc_lockfiles.h"
#include "signal/app_signal.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netserver.h"
#include "net/netpool.h"
#include "xmanager/xmanager_listen.h"
#include "xmanager/xmanager_auth.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager.h"
#include "command/cmd_startxmanager.h"

/* Start resident threads */
static bool cmd_startxmanagerthreads(xmanager* xmgr)
{
    thrnode* thrnode = NULL;

    /*-------------------------------Start resident worker threads
     * begin---------------------------------*/
    /* Threads are started in reverse order of exit, i.e., first started exits last */

    /* Start metric thread */
    if (false == threads_addpersistthread(xmgr->threads,
                                          &thrnode,
                                          THRNODE_IDENTITY_XMANAGER_METRIC,
                                          xmgr->persistno,
                                          (void*)xmgr->metric,
                                          NULL,
                                          NULL,
                                          xmanager_metric_main))
    {
        elog(RLOG_WARNING, "add xmanager metric module persist to threads error");
        return false;
    }

    /* Start auth thread */
    if (false == threads_addpersistthread(xmgr->threads,
                                          &thrnode,
                                          THRNODE_IDENTITY_XMANAGER_AUTH,
                                          xmgr->persistno,
                                          (void*)xmgr->auth,
                                          NULL,
                                          NULL,
                                          xmanager_auth_main))
    {
        elog(RLOG_WARNING, "add xmanager auth module persist to threads error");
        return false;
    }

    /* Start listen thread */
    if (false == threads_addpersistthread(xmgr->threads,
                                          &thrnode,
                                          THRNODE_IDENTITY_XMANAGER_LISTEN,
                                          xmgr->persistno,
                                          (void*)xmgr->listens,
                                          NULL,
                                          NULL,
                                          xmanager_listen_main))
    {
        elog(RLOG_WARNING, "add xmanager listen module persist to threads error");
        return false;
    }

    /*-------------------------------Start resident worker threads
     * end---------------------------------*/
    return true;
}

/* xmanager startup */
bool cmd_startxmanager(void)
{
    char*     wdata = NULL;
    xmanager* xmgr = NULL;

    /*
     * 1. Get working directory
     * 2. Check if it exists
     * 3. Change working directory
     */
    wdata = guc_getdata();
    if (false == osal_dir_exist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        return false;
    }

    /* Change working directory */
    chdir(wdata);

    /* Set to daemon mode */
    makedaemon();

    /* Get main thread ID */
    g_mainthrid = pthread_self();

    /* Check if lock file exists in wdata, create if not, check if process is running if exists */
    misc_lockfiles_create(LOCK_FILE);

    /* Initialize log */
    log_init();

    xmgr = xmanager_init();
    if (NULL == xmgr)
    {
        elog(RLOG_WARNING, "xmanager init error");
        return false;
    }

    /*
     * Start worker threads
     */
    /* Set signal handler */
    signal_init();

    /*
     * Add main resident thread
     */
    if (false == threads_addpersist(xmgr->threads, &xmgr->persistno, "XMANAGER"))
    {
        elog(RLOG_WARNING, "add xmanager persist to threads error");
        return false;
    }

    /* Start resident worker threads */
    if (false == cmd_startxmanagerthreads(xmgr))
    {
        elog(RLOG_WARNING, "start capture increment persist job threads error");
        return false;
    }

    /* Unblock signals */
    singal_setmask();

    elog(RLOG_INFO, "xmanager start, pid:%d", getpid());

    log_destroyerrorstack();

    /* Close stdin/stdout/stderr */
    closestd();

    while (1)
    {
        /* Print log information */
        if (true == g_gotsigterm)
        {
            /* Caught sigterm signal, set thread to exit */
            threads_exit(xmgr->threads);
            break;
        }

        /* Start threads */
        threads_startthread(xmgr->threads);

        /* Try to capture abnormal threads */
        threads_tryjoin(xmgr->threads);

        /* Recycle FREE nodes */
        threads_thrnoderecycle(xmgr->threads);

        if (false == threads_hasthrnode(xmgr->threads))
        {
            /* All threads exit, main thread exits */
            /* TODO: Persist content files to disk */

            break;
        }

        usleep(100000);
    }

    xmanager_destroy(xmgr);

    misc_lockfiles_unlink(0, NULL);

    /* Print leaked memory */
    mem_print(MEMPRINT_ALL);
    return true;
}