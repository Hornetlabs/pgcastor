#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/threads.h"

typedef struct THRIDENTITY2NAME
{
    thrnode_identity identity;
    char*            name;
} thridentityname;

static thridentityname m_threadname[] = {
    {THRNODE_IDENTITY_NOP, "NOP"},

    /*------------capture thread types begin---------------------*/
    {THRNODE_IDENTITY_INC_CAPTURE_FLUSH, "IncFlush"},
    {THRNODE_IDENTITY_INC_CAPTURE_SERIAL, "IncSerial"},
    {THRNODE_IDENTITY_INC_CAPTURE_PARSER, "IncParser"},
    {THRNODE_IDENTITY_INC_CAPTURE_LOADRECORD, "IncLoadRec"},
    {THRNODE_IDENTITY_CAPTURE_METRIC, "Metric"},
    {THRNODE_IDENTITY_INC_CAPTURE_BIGTXNMGR, "BigTxnMgr"},
    {THRNODE_IDENTITY_INC_CAPTURE_BIGTXNFLUSH, "BigTxnFlush"},
    {THRNODE_IDENTITY_INC_CAPTURE_BIGTXNSERIAL, "BigTxnSerial"},
    {THRNODE_IDENTITY_CAPTURE_REFRESH_MGR, "RefreshManger"},
    {THRNODE_IDENTITY_CAPTURE_REFRESH_JOB, "RefreshJob"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_MGR, "OLRManger"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_JOB, "OLRJob"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_FLUSH, "OLRIncFlush"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_SERIAL, "OLRIncSerial"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_PARSER, "OLRIncParser"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_LOADRECORDS, "OLRIncLoadRec"},

    /*------------capture thread types   end---------------------*/

    /*------------integrate thread types begin-------------------*/
    {THRNODE_IDENTITY_INC_INTEGRATE_SYNC, "IncSync"},
    {THRNODE_IDENTITY_INC_INTEGRATE_REBUILD, "IncRebuild"},
    {THRNODE_IDENTITY_INC_INTEGRATE_PARSER, "IncParser"},
    {THRNODE_IDENTITY_INC_INTEGRATE_LOADRECORDS, "IncLoadRec"},
    {THRNODE_IDENTITY_INTEGRATE_METRIC, "Metric"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNMGR, "BigTxnMgr"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNSYNC, "BigTxnSync"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNREBUILD, "BigTxnRebuild"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNPARSER, "BigTxnParser"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNLOADRECORDS, "BigTxnLoadRec"},
    {THRNODE_IDENTITY_INTEGRATE_REFRESH_MGR, "RefreshManger"},
    {THRNODE_IDENTITY_INTEGRATE_REFRESH_JOB, "RefreshJob"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_MGR, "OLRManger"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_JOB, "OLRJob"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_SYNC, "OLRIncSync"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_REBUILD, "OLRIncRebuild"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_PARSER, "OLRIncParser"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_LOADRECORDS, "OLRIncLoadRec"},
    /*------------integrate thread types   end-------------------*/

    /*------------xmanager thread types begin---------------------*/
    {THRNODE_IDENTITY_XMANAGER_LISTEN, "Listens"},
    {THRNODE_IDENTITY_XMANAGER_AUTH, "Auth"},
    {THRNODE_IDENTITY_XMANAGER_METRIC, "Metric"},
    /*------------xmanager thread types   end---------------------*/

    /* add before this line */
    {THRNODE_IDENTITY_MAX, "ThreadMax"}};

/* thread pool initialization */
threads* threads_init(void)
{
    threads* thrs = NULL;

    thrs = (threads*)rmalloc0(sizeof(threads));
    if (NULL == thrs)
    {
        elog(RLOG_WARNING, "threas init out of memory");
        return NULL;
    }
    rmemset0(thrs, 0, '\0', sizeof(threads));
    thrs->no = THRNODE_NO_NORMAL;
    thrs->persistno = 0;
    thrs->thrnodes = NULL;
    thrs->thrpersist = NULL;
    thrs->thrsubmgrs = NULL;

    osal_thread_mutex_init(&thrs->lock, NULL);
    return thrs;
}

/* thread node initialization */
static thrnode* thr_node_init(void)
{
    thrnode* thr_node = NULL;

    thr_node = rmalloc0(sizeof(thrnode));
    if (NULL == thr_node)
    {
        elog(RLOG_WARNING, "thrnode init error");
        return NULL;
    }
    rmemset0(thr_node, 0, '\0', sizeof(thrnode));
    thr_node->data = NULL;
    thr_node->free = NULL;
    thr_node->exitcondition = NULL;
    thr_node->identity = THRNODE_IDENTITY_NOP;
    thr_node->no = 0;
    thr_node->stat = THRNODE_STAT_NOP;
    thr_node->thrid = INVALIDTHRID;
    thr_node->type = THRNODE_TYPE_NOP;
    return thr_node;
}

/* persistent thread initialization */
static thrpersist* thr_persist_init(void)
{
    thrpersist* thr_persist = NULL;

    thr_persist = rmalloc0(sizeof(thrpersist));
    if (NULL == thr_persist)
    {
        elog(RLOG_WARNING, "thrpersist_init error");
        return NULL;
    }
    rmemset0(thr_persist, 0, '\0', sizeof(thrpersist));
    thr_persist->thrrefs = NULL;
    return thr_persist;
}

/* persistent node free */
static void thrpersist_free(void* args)
{
    thrpersist* thr_persist = NULL;
    if (NULL == args)
    {
        return;
    }

    thr_persist = (thrpersist*)args;
    list_free_deep(thr_persist->thrrefs);
    rfree(thr_persist);
}

/* initialize */
static thrref* thr_ref_init(void)
{
    thrref* thr_ref = NULL;

    thr_ref = rmalloc0(sizeof(thrref));
    if (NULL == thr_ref)
    {
        elog(RLOG_WARNING, "thrref init error");
        return NULL;
    }
    rmemset0(thr_ref, 0, '\0', sizeof(thrref));
    thr_ref->no = 0;
    return thr_ref;
}

/* manager thread node initialization */
static thrsubmgr* thr_submgr_init(void)
{
    thrsubmgr* thr_submgr = NULL;

    thr_submgr = rmalloc0(sizeof(thrsubmgr));
    if (NULL == thr_submgr)
    {
        elog(RLOG_WARNING, "thrsubmgr_init out of memory");
        return NULL;
    }
    rmemset0(thr_submgr, 0, '\0', sizeof(thrsubmgr));
    thr_submgr->childthrrefs = NULL;
    thr_submgr->parents = NULL;
    thr_submgr->persistref.no = 0;
    thr_submgr->submgrref.no = 0;

    return thr_submgr;
}

/* manager node free */
static void thrsubmgr_free(void* args)
{
    thrsubmgr* thr_submgr = NULL;

    if (NULL == args)
    {
        return;
    }
    thr_submgr = (thrsubmgr*)args;
    list_free_deep(thr_submgr->childthrrefs);
    rfree(thr_submgr);
}

/* thread node free */
static void thrnode_free(void* args)
{
    thrnode* thr_node = NULL;
    if (NULL == args)
    {
        return;
    }
    thr_node = (thrnode*)args;

    if (NULL != thr_node->free && NULL != thr_node->data)
    {
        thr_node->free(thr_node->data);
    }

    rfree(thr_node);
    return;
}

/* get persistent thread node by no */
static int threads_thrpersistcmp(void* argv1, void* argv2)
{
    uint64      no = 0;
    thrpersist* thr_persist = NULL;

    no = *((uint64*)argv1);
    thr_persist = (thrpersist*)argv2;

    if (no != thr_persist->no)
    {
        return 1;
    }
    return 0;
}

/* get thread node by no */
static int threads_thrnodecmp(void* argv1, void* argv2)
{
    uint64   no = 0;
    thrnode* thr_node = NULL;

    no = *((uint64*)argv1);
    thr_node = (thrnode*)argv2;

    if (no != thr_node->no)
    {
        return 1;
    }
    return 0;
}

/* manager node compare */
static int threads_thrsubmgrcmp(void* argv1, void* argv2)
{
    uint64     no = 0;
    thrsubmgr* thr_submgr = NULL;

    no = *((uint64*)argv1);
    thr_submgr = (thrsubmgr*)argv2;

    if (no != thr_submgr->submgrref.no)
    {
        return 1;
    }

    /* 0 means equal */
    return 0;
}

/*----------------------------------external use: query or set begin---------------------------*/
/* get node by thread id */
thrnode* threads_getthrnodebyno(threads* thrs, uint64 no)
{
    thrnode* thr_node = NULL;
    if (NULL == thrs)
    {
        return NULL;
    }
    osal_thread_lock(&thrs->lock);
    thr_node = dlist_get(thrs->thrnodes, &no, threads_thrnodecmp);
    osal_thread_unlock(&thrs->lock);

    return thr_node;
}

/*
 * set sub-threads to term state
 */
void threads_setsubmgrjobthreadterm(threads* thrs, List* jobthreads)
{
    ListCell* lc = NULL;
    thrref*   thr_ref = NULL;
    thrnode*  thr_node = NULL;

    foreach (lc, jobthreads)
    {
        thr_ref = (thrref*)lfirst(lc);
        thr_node = threads_getthrnodebyno(thrs, thr_ref->no);
        if (THRNODE_STAT_TERM > thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_TERM;
        }
    }
}

/*
 * count sub-manager's worker threads with state > WORK
 */
bool threads_countsubmgrjobthredsabovework(threads* thrs, List* jobthreads, int* scnt)
{
    ListCell* lc = NULL;
    thrref*   thr_ref = NULL;
    thrnode*  thr_node = NULL;

    foreach (lc, jobthreads)
    {
        thr_ref = (thrref*)lfirst(lc);
        thr_node = threads_getthrnodebyno(thrs, thr_ref->no);
        if (NULL == thr_node)
        {
            elog(RLOG_WARNING, "can not get thrnode by no:%lu", thr_ref->no);
            return false;
        }

        if (THRNODE_STAT_STARTING >= thr_node->stat)
        {
            continue;
        }
        (*scnt)++;
    }
    return true;
}

/*
 * set sub-manager's sub-threads to TERM state
 *  1. set to term when in IDLE state
 *  2. count the number of already-exited threads
 *
 *  skipcnt     number of threads to skip
 *  scnt        input: count to check
 *              output: number of exited threads
 *
 *  return true:  no logic errors encountered during execution
 *  return false: logic error encountered during execution
 */
bool threads_setsubmgrjobthredstermandcountexit(threads* thrs,
                                                List*    jobthreads,
                                                int      skipcnt,
                                                int*     scnt)
{
    int       jobcnt = 0;
    ListCell* lc = NULL;
    thrref*   thr_ref = NULL;
    thrnode*  thr_node = NULL;

    foreach (lc, jobthreads)
    {
        if (0 != skipcnt)
        {
            skipcnt--;
            continue;
        }

        if (0 == *scnt)
        {
            break;
        }
        (*scnt)--;

        thr_ref = (thrref*)lfirst(lc);
        thr_node = threads_getthrnodebyno(thrs, thr_ref->no);
        if (NULL == thr_node)
        {
            elog(RLOG_WARNING, "can not get thrnode by no:%lu", thr_ref->no);
            return false;
        }

        /* sub-thread is working, skip */
        if (THRNODE_STAT_IDLE > thr_node->stat)
        {
            continue;
        }

        if (THRNODE_STAT_IDLE == thr_node->stat)
        {
            /*
             * thread is idle, set to term
             *  sub-thread only sets to IDLE when no task is obtained from queue
             *  when sub-thread enters idle state and immediately gets a task from queue,
             *  it will immediately set the task state to WORK
             */
            thr_node->stat = THRNODE_STAT_TERM;
            continue;
        }

        /* sub-thread has exited */
        if (THRNODE_STAT_EXITED != thr_node->stat)
        {
            /* sub-thread has not exited, wait for it to fully exit */
            continue;
        }

        jobcnt++;
    }

    *scnt = jobcnt;
    return true;
}

/* set sub-thread state to FREE */
void threads_setsubmgrjobthredsfree(threads* thrs, List* jobthreads, int skipcnt, int scnt)
{
    ListCell* lc = NULL;
    thrref*   thr_ref = NULL;
    thrnode*  thr_node = NULL;

    foreach (lc, jobthreads)
    {
        if (0 != skipcnt)
        {
            skipcnt--;
            continue;
        }

        if (0 == scnt)
        {
            break;
        }
        scnt--;
        thr_ref = (thrref*)lfirst(lc);
        thr_node = threads_getthrnodebyno(thrs, thr_ref->no);
        thr_node->stat = THRNODE_STAT_FREE;
    }
}

/*----------------------------------external use: query or set   end---------------------------*/

/*----------------------------------add node begin-----------------------------------------*/
/* add a persistent thread node */
bool threads_addpersist(threads* thrs, uint64* pno, char* name)
{
    thrpersist* thr_persist = NULL;

    thr_persist = thr_persist_init();
    if (NULL == thr_persist)
    {
        elog(RLOG_WARNING, "add persist thread error");
        return false;
    }
    rmemcpy1(thr_persist->name, 0, name, strlen(name));

    /* add to linked list */
    osal_thread_lock(&thrs->lock);
    thr_persist->no = ++thrs->persistno;

    /* add to queue */
    thrs->thrpersist = dlist_put(thrs->thrpersist, (void*)thr_persist);
    if (NULL == thrs->thrpersist)
    {
        elog(RLOG_WARNING, "add persist thread error");
        osal_thread_unlock(&thrs->lock);
        return false;
    }
    osal_thread_unlock(&thrs->lock);
    *pno = thr_persist->no;
    return true;
}

/*
 * add persistent thread
 *  identity                thread identity
 *  data                    thread main structure
 *
 *  pthrnode                return value
 */
bool threads_addpersistthread(threads*         thrs,
                              thrnode**        pthrnode,
                              thrnode_identity identity,
                              uint64           persistno,
                              void*            data,
                              thrdatafree      free,
                              threxitcondition exitcondition,
                              thrmain          tmain)
{
    thrref*     thr_ref = NULL;
    thrnode*    thr_node = NULL;
    thrpersist* thr_persist = NULL;
    if (NULL == thrs)
    {
        return true;
    }

    thr_ref = thr_ref_init();
    if (NULL == thr_ref)
    {
        elog(RLOG_WARNING, "add persist thread error");
        return false;
    }

    thr_node = thr_node_init();
    if (NULL == thr_node)
    {
        elog(RLOG_WARNING, "add persist thread error");
        return false;
    }
    *pthrnode = thr_node;

    thr_node->data = data;
    thr_node->free = free;
    thr_node->exitcondition = exitcondition;
    thr_node->tmain = tmain;
    thr_node->identity = identity;
    thr_node->stat = THRNODE_STAT_INIT;
    thr_node->thrid = 0;
    thr_node->type = THRNODE_TYPE_PERSIST;
    *pthrnode = thr_node;

    /* add to linked list */
    osal_thread_lock(&thrs->lock);
    thr_node->no = ++thrs->no;
    thr_ref->no = thr_node->no;

    /* find persistent node */
    thr_persist = dlist_get(thrs->thrpersist, &persistno, threads_thrpersistcmp);
    if (NULL == thr_persist)
    {
        elog(RLOG_WARNING, "add persist thread error, can not get persist node by %lu", persistno);
        osal_thread_unlock(&thrs->lock);
        return false;
    }

    /* add persistent thread to persistent thread node */
    thr_persist->thrrefs = lcons(thr_ref, thr_persist->thrrefs);

    /* add to thread management */
    thrs->thrnodes = dlist_put(thrs->thrnodes, thr_node);
    if (NULL == thrs->thrnodes)
    {
        elog(RLOG_WARNING, "add thread error");
        osal_thread_unlock(&thrs->lock);
        return false;
    }

    osal_thread_unlock(&thrs->lock);
    return true;
}

/* add manager thread */
bool threads_addsubmanger(threads*         thrs,
                          thrnode_identity identity,
                          uint64           persistno,
                          thrsubmgr**      pthrsubmgr,
                          void*            data,
                          thrdatafree      free,
                          threxitcondition exitcondition,
                          thrmain          tmain)
{
    thrnode*   thr_node = NULL;
    thrsubmgr* thr_submgr = NULL;

    thr_submgr = thr_submgr_init();
    if (NULL == thr_submgr)
    {
        elog(RLOG_WARNING, "addsubmanger error");
        return false;
    }
    thr_submgr->parents = thrs;
    thr_submgr->persistref.no = persistno;
    *pthrsubmgr = thr_submgr;

    thr_node = thr_node_init();
    if (NULL == thr_node)
    {
        elog(RLOG_WARNING, "add sub manger thread error");
        return false;
    }

    thr_node->data = data;
    thr_node->free = free;
    thr_node->exitcondition = exitcondition;
    thr_node->tmain = tmain;
    thr_node->identity = identity;
    thr_node->stat = THRNODE_STAT_INIT;
    thr_node->thrid = 0;
    thr_node->type = THRNODE_TYPE_TMGR;
    thr_node->thrsubmgr = thr_submgr;

    osal_thread_lock(&thrs->lock);
    thr_node->no = ++thrs->no;
    thr_submgr->submgrref.no = thr_node->no;

    /* add to linked list */
    thrs->thrsubmgrs = dlist_put(thrs->thrsubmgrs, thr_submgr);
    if (NULL == thrs->thrsubmgrs)
    {
        elog(RLOG_WARNING, "add sub manger thread error");
        return false;
    }

    /* add node */
    thrs->thrnodes = dlist_put(thrs->thrnodes, thr_node);
    if (NULL == thrs->thrnodes)
    {
        elog(RLOG_WARNING, "add thread error");
        osal_thread_unlock(&thrs->lock);
        return false;
    }
    osal_thread_unlock(&thrs->lock);
    return true;
}

/* add worker thread */
bool threads_addjobthread(threads*         thrs,
                          thrnode_identity identity,
                          uint64           submgrno,
                          void*            data,
                          thrdatafree      free,
                          threxitcondition exitcondition,
                          thrmain          tmain)
{
    thrref*    thr_ref = NULL;
    thrnode*   thr_node = NULL;
    thrsubmgr* thr_submgr = NULL;

    thr_ref = thr_ref_init();
    if (NULL == thr_ref)
    {
        elog(RLOG_WARNING, "add job thread error");
        return false;
    }

    thr_node = thr_node_init();
    if (NULL == thr_node)
    {
        elog(RLOG_WARNING, "add job thread error");
        return false;
    }
    thr_node->data = data;
    thr_node->free = free;
    thr_node->exitcondition = exitcondition;
    thr_node->tmain = tmain;
    thr_node->identity = identity;
    thr_node->stat = THRNODE_STAT_INIT;
    thr_node->thrid = 0;
    thr_node->type = THRNODE_TYPE_TJOB;

    osal_thread_lock(&thrs->lock);
    thr_node->no = ++thrs->no;
    thr_ref->no = thr_node->no;

    /* add to thread nodes */
    thrs->thrnodes = dlist_put(thrs->thrnodes, (void*)thr_node);

    /* find manager node */
    thr_submgr = dlist_get(thrs->thrsubmgrs, &submgrno, threads_thrsubmgrcmp);
    if (NULL == thr_submgr)
    {
        elog(RLOG_WARNING, "add job thread error, can not get submgr node by %lu", submgrno);
        osal_thread_unlock(&thrs->lock);
        return false;
    }

    thr_submgr->childthrrefs = lcons(thr_ref, thr_submgr->childthrrefs);
    osal_thread_unlock(&thrs->lock);
    return true;
}

/*----------------------------------add node   end-----------------------------------------*/

/*----------------------------------exit handling begin-----------------------------------------*/
/* single thread exit */
static void threads_thrnodeexit(thrnode* thr_node)
{
    int iret = 0;
    if (NULL == thr_node)
    {
        return;
    }

    /* handle based on thread state */
    if (THRNODE_STAT_EXITED == thr_node->stat || THRNODE_STAT_FREE == thr_node->stat)
    {
        /* already exited or marked for reclamation, skip */
        elog(RLOG_INFO, "%s thread already exit", m_threadname[thr_node->identity].name);
        return;
    }
    else if (THRNODE_STAT_ABORT == thr_node->stat || THRNODE_STAT_EXIT == thr_node->stat)
    {
        /* abnormal/normal exit, reclaim thread */
        osal_thread_join(thr_node->thrid, NULL);

        /* intra-thread resource reclamation */
        goto threads_thrnodeexit_done;
    }

    /* thread has not been started, no need to start it */
    if (INVALIDTHRID == thr_node->thrid)
    {
        /* thread no longer needs to start, intra-thread resource reclamation */
        goto threads_thrnodeexit_done;
    }

    /* check if thread is running, reclaim if not */
    iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
    if (EBUSY != iret && EINTR != iret)
    {
        /* thread has exited abnormally, intra-thread resource reclamation */
        goto threads_thrnodeexit_done;
    }

    /* wait for thread to exit */
    while (1)
    {
        if (NULL == thr_node->exitcondition)
        {
            /* no exit condition required, set to TERM */
            if (THRNODE_STAT_TERM > thr_node->stat)
            {
                thr_node->stat = THRNODE_STAT_TERM;
            }

            iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
            if (EBUSY == iret || EINTR == iret)
            {
                /* still running, continue waiting */
                usleep(10000);
                continue;
            }

            if (THRNODE_STAT_TERM >= thr_node->stat)
            {
                continue;
            }
            break;
        }

        /* check if exit condition is met */
        while (1)
        {
            if (true == thr_node->exitcondition(thr_node->data))
            {
                /* set to NULL, break out of this loop */
                thr_node->exitcondition = NULL;
                break;
            }

            /* check if exited abnormally */
            iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
            if (EBUSY == iret || EINTR == iret)
            {
                /* still running, continue waiting */
                usleep(10000);
                continue;
            }
            goto threads_thrnodeexit_done;
        }
    }

threads_thrnodeexit_done:
    /* thread has exited, set to exited */
    elog(RLOG_INFO, "%lu::%s thread exit", thr_node->thrid, m_threadname[thr_node->identity].name);
    thr_node->stat = THRNODE_STAT_EXITED;
    return;
}

/*
 * exit a thread group
 *  set threads to term state in order and wait for them to exit
 */
static void threads_workinggroupexit(threads* thrs, List* thrwg)
{
    ListCell* lc = NULL;
    thrref*   thr_ref = NULL;
    thrnode*  thr_node = NULL;
    foreach (lc, thrwg)
    {
        thr_ref = (thrref*)lfirst(lc);

        /* get thrnode */
        thr_node = dlist_get(thrs->thrnodes, &thr_ref->no, threads_thrnodecmp);
        if (NULL == thr_node)
        {
            continue;
        }
        threads_thrnodeexit(thr_node);
    }
    return;
}

/*
 * set thread group state to FREE
 */
static void threads_setworkinggroupfree(threads* thrs, List* thrwg)
{
    ListCell* lc = NULL;
    thrref*   thr_ref = NULL;
    thrnode*  thr_node = NULL;
    foreach (lc, thrwg)
    {
        thr_ref = (thrref*)lfirst(lc);

        /* get thrnode */
        thr_node = dlist_get(thrs->thrnodes, &thr_ref->no, threads_thrnodecmp);
        if (NULL == thr_node)
        {
            continue;
        }
        thr_node->stat = THRNODE_STAT_FREE;
    }

    return;
}

/* manager node abnormal exit */
static void threads_tmgrabort(threads* thrs, thrsubmgr* thr_submgr)
{
    /* manager thread node */
    thrnode* thr_node = NULL;

    /* get manager thread thrnode */
    thr_node = dlist_get(thrs->thrnodes, &thr_submgr->submgrref.no, threads_thrnodecmp);

    /* sub-threads exit */
    threads_workinggroupexit(thrs, thr_submgr->childthrrefs);

    /* manager thread exit */
    threads_thrnodeexit(thr_node);

    /* set sub-threads to FREE, set manager node to FREE */
    threads_setworkinggroupfree(thrs, thr_submgr->childthrrefs);
    thr_node->stat = THRNODE_STAT_FREE;
    return;
}

/* manager node exit */
static void threads_tmgrexit(threads* thrs, thrsubmgr* thr_submgr, bool holdlock)
{
    int      iret = 0;
    /* manager thread node */
    thrnode* thr_node = NULL;

    /* get manager thread thrnode */
    thr_node = dlist_get(thrs->thrnodes, &thr_submgr->submgrref.no, threads_thrnodecmp);

    /* added to linked list but not started */
    if (THRNODE_STAT_INIT == thr_node->stat)
    {
        thr_node->stat = THRNODE_STAT_FREE;
    }

    if (THRNODE_STAT_FREE == thr_node->stat)
    {
        return;
    }

    /*
     * if not in WORK state, manager thread is starting up
     * manager does the following between start---->work:
     * 1. initialize resources
     * 2. add worker threads to startup queue
     * when starting sub-threads, lock acquisition is needed, so release lock here first,
     * then acquire lock after manager thread reaches work state
     */
    if (THRNODE_STAT_WORK > thr_node->stat)
    {
        /* unlock first */
        if (true == holdlock)
        {
            osal_thread_unlock(&thrs->lock);
        }
        while (1)
        {
            /* wait until >= WORK */
            if (THRNODE_STAT_WORK <= thr_node->stat)
            {
                /* manager thread started successfully or exited abnormally */
                break;
            }

            /* check if thread has error */
            /* check if thread is running, reclaim if not */
            iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
            if (EBUSY != iret && EINTR != iret)
            {
                /* thread has exited abnormally */
                thr_node->stat = THRNODE_STAT_EXITED;
                break;
            }
            usleep(10000);
            continue;
        }

        /* re-acquire lock */
        if (true == holdlock)
        {
            osal_thread_lock(&thrs->lock);
        }
    }

    /* sub-threads exit first
     * consider that during stop process, if manager thread exits abnormally,
     * worker threads also need to be reclaimed
     */
    threads_workinggroupexit(thrs, thr_submgr->childthrrefs);

    /* set sub-threads state to FREE */
    threads_setworkinggroupfree(thrs, thr_submgr->childthrrefs);

    /* abort */
    if (THRNODE_STAT_ABORT == thr_node->stat)
    {
        /* abort means sub-thread exited without reclamation */
        threads_tmgrabort(thrs, thr_submgr);
        return;
    }
    else if (THRNODE_STAT_EXITED != thr_node->stat)
    {
        if (THRNODE_STAT_TERM > thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_TERM;
        }
        while (1)
        {
            /* set thread state to TERM */
            iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
            if (EBUSY == iret || EINTR == iret)
            {
                /* still running, continue waiting */
                usleep(10000);
                continue;
            }
            break;
        }
    }

    /* set manager thread state to FREE */
    elog(RLOG_INFO, "%s thread exit", m_threadname[thr_node->identity].name);
    thr_node->stat = THRNODE_STAT_FREE;
    return;
}

/* abnormal exit, thread set ABORT on exit */
static void threads_abort(threads* thrs, thrnode* thr_node)
{
    /*
     * persistent thread abort
     *  1. exit sub-manager threads
     *  2. exit all persistent threads
     *
     * sub-manager thread exit
     *  exit manager's worker threads and set state to FREE
     *
     * sub-manager's worker thread abort
     *  reclaim resources and set state to THRNODE_STAT_EXITED
     */
    dlistnode*  dlnode = NULL;
    thrsubmgr*  thr_submgr = NULL;
    thrpersist* thr_persist = NULL;
    if (NULL == thrs || NULL == thr_node)
    {
        return;
    }

    /* all threads must exit */
    if (THRNODE_TYPE_PERSIST == thr_node->type)
    {
        /* set all mgr threads to exit */
        if (false == dlist_isnull(thrs->thrsubmgrs))
        {
            for (dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
            {
                thr_submgr = (thrsubmgr*)dlnode->value;
                threads_tmgrexit(thrs, thr_submgr, true);
            }
        }

        /* set all persist threads to exit */
        for (dlnode = thrs->thrpersist->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thr_persist = (thrpersist*)dlnode->value;

            /* persistent thread exit */
            threads_workinggroupexit(thrs, thr_persist->thrrefs);
            threads_setworkinggroupfree(thrs, thr_persist->thrrefs);
        }
    }
    else if (THRNODE_TYPE_TMGR == thr_node->type)
    {
        /* set manager's worker threads to exit */
        thr_submgr = dlist_get(thrs->thrsubmgrs, &thr_node->no, threads_thrsubmgrcmp);
        threads_tmgrabort(thrs, thr_submgr);
    }
    else if (THRNODE_TYPE_TJOB == thr_node->type)
    {
        /* worker thread exit */
        threads_thrnodeexit(thr_node);
    }
    return;
}

/*
 * thread abnormal exit, possible scenarios:
 *  1. abort or exit was already set, but thread exit was captured first due to program execution
 * context
 *  2. logic error, forgot to set abort or exit, it's best to investigate logic in this case
 */
static void threads_exitimpolite(threads* thrs, thrnode* thr_node)
{
    if (NULL == thrs || NULL == thr_node)
    {
        return;
    }

    /* set as already reclaimed */
    thr_node->stat = THRNODE_STAT_EXITED;
    elog(RLOG_INFO, "%lu::%s thread exit", thr_node->thrid, m_threadname[thr_node->identity].name);
    threads_abort(thrs, thr_node);
}

/* thread exit, called when sigterm is received */
bool threads_exit(threads* thrs)
{
    /* find manager nodes */
    dlistnode*  dlnode = NULL;
    thrsubmgr*  thr_submgr = NULL;
    thrpersist* thr_persist = NULL;

    /* step1: traverse manager thread nodes, exit all child nodes */
    if (false == dlist_isnull(thrs->thrsubmgrs))
    {
        for (dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thr_submgr = (thrsubmgr*)dlnode->value;
            threads_tmgrexit(thrs, thr_submgr, false);
        }
    }

    /* step2: persistent thread exit */
    for (dlnode = thrs->thrpersist->head; NULL != dlnode; dlnode = dlnode->next)
    {
        thr_persist = (thrpersist*)dlnode->value;

        /* persistent thread exit */
        threads_workinggroupexit(thrs, thr_persist->thrrefs);
    }

    /* step3: traverse manager thread nodes, exit all child nodes */
    if (false == dlist_isnull(thrs->thrsubmgrs))
    {
        for (dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thr_submgr = (thrsubmgr*)dlnode->value;
            threads_tmgrexit(thrs, thr_submgr, false);
        }
    }

    return true;
}

/*----------------------------------exit handling   end-----------------------------------------*/

/*
 * attempt to reclaim threads
 *
 * persistent thread exit: set all threads to exit and wait for them
 * manager thread abnormal exit: manager and its sub-threads exit
 * worker thread under manager exits: only reclaim the worker thread
 */
bool threads_tryjoin(threads* thrs)
{
    int        iret = 0;
    dlistnode* dlnode = NULL;
    thrnode*   thr_node = NULL;
    osal_thread_lock(&thrs->lock);

    /* traverse sub-threads, check if any exited abnormally */
    for (dlnode = thrs->thrnodes->head; NULL != dlnode;)
    {
        thr_node = (thrnode*)dlnode->value;
        if (THRNODE_STAT_EXITED == thr_node->stat)
        {
            /* already reclaimed, skip */
            dlnode = dlnode->next;
            continue;
        }
        else if (THRNODE_STAT_FREE == thr_node->stat)
        {
            /* skip, wait for later reclamation */
            dlnode = dlnode->next;
            continue;
        }
        else if (THRNODE_STAT_ABORT == thr_node->stat)
        {
            /* exited abnormally, handle based on thread state */
            threads_abort(thrs, thr_node);
            elog(RLOG_INFO, "%s thread about exit", m_threadname[thr_node->identity].name);
            dlnode = dlnode->next;
            continue;
        }
        else if (THRNODE_STAT_EXIT == thr_node->stat)
        {
            /* reclaim */
            osal_thread_join(thr_node->thrid, NULL);
            thr_node->stat = THRNODE_STAT_EXITED;
            elog(RLOG_INFO, "%s thread exit", m_threadname[thr_node->identity].name);
            dlnode = dlnode->next;

            /* sub-manager thread cannot set itself to FREE, so check if thread type is sub-manager
             */
            if (THRNODE_TYPE_TMGR == thr_node->type)
            {
                thr_node->stat = THRNODE_STAT_FREE;
            }
            continue;
        }

        /* not started yet, skip */
        if (INVALIDTHRID == thr_node->thrid)
        {
            dlnode = dlnode->next;
            continue;
        }

        /* attempt to reclaim */
        iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
        if (EBUSY == iret || EINTR == iret)
        {
            /* running normally */
            dlnode = dlnode->next;
            continue;
        }

        /* execute different exit logic based on thread type */
        threads_exitimpolite(thrs, thr_node);
        dlnode = dlnode->next;
    }

    osal_thread_unlock(&thrs->lock);
    return true;
}

/*
 * start sub-threads
 */
void threads_startthread(threads* thrs)
{
    dlistnode* dlnode = NULL;
    thrnode*   thr_node = NULL;
    osal_thread_lock(&thrs->lock);
    if (true == dlist_isnull(thrs->thrnodes))
    {
        osal_thread_unlock(&thrs->lock);
        return;
    }

    /* traverse and start */
    for (dlnode = thrs->thrnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        thr_node = (thrnode*)dlnode->value;
        if (THRNODE_STAT_INIT != thr_node->stat)
        {
            continue;
        }

        thr_node->stat = THRNODE_STAT_STARTING;
        /* create worker thread */
        osal_thread_create(&thr_node->thrid, NULL, thr_node->tmain, thr_node);

        elog(RLOG_DEBUG,
             "start thrid:%lu, threadname:%s",
             thr_node->thrid,
             m_threadname[thr_node->identity].name);

        /* set thread name */
        osal_thread_setname_np(thr_node->thrid, m_threadname[thr_node->identity].name);
    }
    osal_thread_unlock(&thrs->lock);
}

/* reclaim nodes */
void threads_thrnoderecycle(threads* thrs)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodenext = NULL;
    thrnode*   thr_node = NULL;

    osal_thread_lock(&thrs->lock);
    if (true == dlist_isnull(thrs->thrnodes))
    {
        osal_thread_unlock(&thrs->lock);
        return;
    }

    for (dlnode = thrs->thrnodes->head; NULL != dlnode;)
    {
        thr_node = (thrnode*)dlnode->value;
        if (THRNODE_STAT_FREE != thr_node->stat)
        {
            dlnode = dlnode->next;
            continue;
        }

        /* sub-manager node, need to free the sub-manager node */
        if (THRNODE_TYPE_TMGR == thr_node->type)
        {
            /* clean up manager node */
            thrs->thrsubmgrs = dlist_deletebyvalue(
                thrs->thrsubmgrs, &thr_node->no, threads_thrsubmgrcmp, thrsubmgr_free);
        }

        /* reclaim node */
        dlnodenext = dlnode->next;
        thrs->thrnodes = dlist_delete(thrs->thrnodes, dlnode, thrnode_free);
        dlnode = dlnodenext;
    }

    osal_thread_unlock(&thrs->lock);
}

/*
 * check if there are sub-threads
 *  false       no sub-threads
 *  true        has sub-threads
 */
bool threads_hasthrnode(threads* thrs)
{
    osal_thread_lock(&thrs->lock);
    if (true == dlist_isnull(thrs->thrnodes))
    {
        osal_thread_unlock(&thrs->lock);
        return false;
    }
    osal_thread_unlock(&thrs->lock);
    return true;
}

/* memory reclamation */
void threads_free(threads* thrs)
{
    if (NULL == thrs)
    {
        return;
    }

    dlist_free(thrs->thrnodes, thrnode_free);

    dlist_free(thrs->thrpersist, thrpersist_free);

    dlist_free(thrs->thrsubmgrs, thrsubmgr_free);

    osal_thread_mutex_destroy(&thrs->lock);

    rfree(thrs);
}
