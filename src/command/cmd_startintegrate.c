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
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
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

/* Start resident threads */
static bool cmd_startintegratethreads(increment_integrate* incintegrate)
{
    thrnode* thrnode = NULL;

    /*-------------------------------Start resident worker threads
     * begin---------------------------------*/
    /* Threads are started in reverse order of exit, i.e., first started exits last */
    /* Start apply thread */
    if (false == threads_addpersistthread(incintegrate->threads,
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

    /* Start rebuild thread */
    if (false == threads_addpersistthread(incintegrate->threads,
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

    /* Start parser thread */
    if (false == threads_addpersistthread(incintegrate->threads,
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

    /* Start split trail thread */
    if (false == threads_addpersistthread(incintegrate->threads,
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

    /* Start metric thread */
    if (false == threads_addpersistthread(incintegrate->threads,
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

    /*-------------------------------Start resident worker threads
     * end---------------------------------*/
    return true;
}

/* Integrate startup */
bool cmd_startintegrate(void)
{
    /*
     * 1. Change working directory
     * 2. Create lock file
     * 3. Initialize log
     */
    bool                 bret = true;
    int                  gctime = 0;
    int                  forcefree = 0;
    char*                wdata = NULL;
    increment_integrate* incintegrate = NULL;

    /* Get working directory */
    wdata = guc_getdata();

    /* Check if data directory exists */
    if (false == osal_dir_exist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        bret = false;
        goto cmd_startintegrate_done;
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

    /* Get memory reclaim time */
    gctime = guc_getConfigOptionInt(CFG_KEY_GCTIME);

    /* incintegrate */
    incintegrate = increment_integrate_init();

    /* Set signal handler */
    signal_init();

    /* Create sync tables */
    if (false == databaserecv_integrate_dbinit())
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

    /* Load onlinerefresh state file and generate onlinerefresh node */
    if (false == increment_integrate_onlinerefreshload(incintegrate))
    {
        elog(RLOG_WARNING, "load onlinerefresh error");
        bret = false;
        goto cmd_startintegrate_done;
    }

    /*
     * Add main resident thread
     */
    if (false ==
        threads_addpersist(incintegrate->threads, &incintegrate->persistno, "INTEGRATE INCREMENT"))
    {
        elog(RLOG_WARNING, "add integrate increment persist to threads error");
        bret = false;
        goto cmd_startintegrate_done;
    }

    /* Start resident worker threads */
    if (false == cmd_startintegratethreads(incintegrate))
    {
        bret = false;
        elog(RLOG_WARNING, "start integrate increment persist job threads error");
        goto cmd_startintegrate_done;
    }

    /* Unblock signals */
    singal_setmask();

    elog(RLOG_INFO, "xsynch integrate start, pid:%d", getpid());

    log_destroyerrorstack();

    /* Close stdin/stdout/stderr */
    closestd();

    while (1)
    {
        /* Print log information */
        if (true == g_gotsigterm)
        {
            /* Caught sigterm signal, set thread to exit */
            threads_exit(incintegrate->threads);
            break;
        }

        /*
         * refresh processing
         *  1. Start refresh node
         *  2. Recycle completed refresh nodes
         */
        /* Start refresh node */
        if (false == increment_integrate_startrefresh(incintegrate))
        {
            elog(RLOG_WARNING, "start refresh error");
            break;
        }

        /* Try to recycle refresh node */
        if (false == increment_integrate_tryjoinonrefresh(incintegrate))
        {
            elog(RLOG_WARNING, "try join on refresh error");
            break;
        }

        /*
         * onlinerefresh processing
         *  1. Start onlinerefresh node
         *  2. Recycle completed onlinerefresh nodes
         */
        /* Start onlinerefresh node */
        if (false == increment_integrate_startonlinerefresh(incintegrate))
        {
            elog(RLOG_WARNING, "start onlinerefresh error");
            break;
        }

        /* Try to recycle onlinerefresh node */
        if (false == increment_integrate_tryjoinononlinerefresh(incintegrate))
        {
            elog(RLOG_WARNING, "try join on onlinerefresh error");
            break;
        }

        /*
         * bigtxn processing
         *  1. Start bigtxn node
         *  2. Recycle completed bigtxn nodes
         */
        /* Start bigtxn node */
        if (false == increment_integrate_startbigtxn(incintegrate))
        {
            elog(RLOG_WARNING, "start bigtxn error");
            break;
        }

        /* Try to recycle bigtxn node */
        if (false == increment_integrate_tryjoinonbigtxn(incintegrate))
        {
            elog(RLOG_WARNING, "try join on bigtxn error");
            break;
        }

        /* Start threads */
        threads_startthread(incintegrate->threads);

        /* Try to capture abnormal threads */
        threads_tryjoin(incintegrate->threads);

        /* Recycle FREE nodes */
        threads_thrnoderecycle(incintegrate->threads);

        if (false == threads_hasthrnode(incintegrate->threads))
        {
            /* All threads exit, main thread exits */
            break;
        }

        if (0 == gctime)
        {
            ;
        }
        else if (gctime > forcefree)
        {
            forcefree++;
        }
        else
        {
            /* Reclaim memory */
            malloc_trim(0);
            forcefree = 0;
        }
        sleep(1);
        continue;
    }

    /* All threads have exited, main thread also exits */
cmd_startintegrate_done:

    /* Persist refresh information */
    if (NULL != incintegrate)
    {
        increment_integrate_refreshflush(incintegrate);

        /* Persist onlinerefresh information */
        if (NULL != incintegrate->rebuild)
        {
            onlinerefresh_persist_write(incintegrate->rebuild->olpersist);
        }

        /* incintegrate resource cleanup*/
        increment_integrate_destroy(incintegrate);
    }

    /* Lock file release */
    misc_lockfiles_unlink(0, NULL);

    /* Print leaked memory */
    mem_print(MEMPRINT_ALL);
    return bret;
}
