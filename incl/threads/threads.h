#ifndef _THREADS_H_
#define _THREADS_H_

/*
 * Thread types are divided into three categories: persistent threads, temporary manager threads,
 * and temporary worker threads
 *
 *  Persistent threads:
 *      Business threads that normally do not exit. On error, they may exit; if any persistent
 *      thread exits, the entire service will shut down.
 *      Persistent thread node
 *          Persistent thread 1
 *          Persistent thread 2
 *          Persistent thread 3
 *          No manager thread -> temporary manager thread
 *          No manager thread -> temporary manager thread
 *      Persistent thread node
 *          Persistent thread 1
 *          Persistent thread 2
 *          Persistent thread 3
 *
 *  Temporary manager threads:
 *      Started when needed, responsible for registering and starting/stopping sub-threads.
 *      Manages multiple sub-threads; when a sub-thread exits abnormally, the temporary manager
 *      thread follows the normal exit mechanism.
 *
 *  Temporary worker threads:
 *      Perform specific business tasks and exit after completion.
 *      When exiting, the temporary manager thread is responsible for cleanup; if the temporary
 *      manager thread exits abnormally, cleanup will be performed by the main thread.
 *
 */

/* invalid thread id */
#define INVALIDTHRID 0

/* temporary thread ids should be greater than this value */
#define THRNODE_NO_NORMAL 4096

typedef enum THRNODE_TYPE
{
    THRNODE_TYPE_NOP = 0x00,

    /* persistent worker thread */
    THRNODE_TYPE_PERSIST,

    /* temporary manager thread */
    THRNODE_TYPE_TMGR,

    /* temporary worker thread */
    THRNODE_TYPE_TJOB

} thrnode_type;

typedef enum THRNODE_IDENTITY
{
    THRNODE_IDENTITY_NOP = 0x00,

    /* flush thread (increment capture) */
    THRNODE_IDENTITY_INC_CAPTURE_FLUSH,

    /* serialization thread (increment capture) */
    THRNODE_IDENTITY_INC_CAPTURE_SERIAL,

    /* parser thread */
    THRNODE_IDENTITY_INC_CAPTURE_PARSER,

    /* log split thread */
    THRNODE_IDENTITY_INC_CAPTURE_LOADRECORD,

    /* metrics thread */
    THRNODE_IDENTITY_CAPTURE_METRIC,

    /* big transaction manager thread */
    THRNODE_IDENTITY_INC_CAPTURE_BIGTXNMGR,

    /* big transaction flush thread */
    THRNODE_IDENTITY_INC_CAPTURE_BIGTXNFLUSH,

    /* big transaction serialization thread */
    THRNODE_IDENTITY_INC_CAPTURE_BIGTXNSERIAL,

    /* refresh manager thread */
    THRNODE_IDENTITY_CAPTURE_REFRESH_MGR,

    /* refresh worker thread */
    THRNODE_IDENTITY_CAPTURE_REFRESH_JOB,

    /* online refresh manager thread */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_MGR,

    /* online refresh worker thread */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_JOB,

    /* online refresh increment flush thread */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_FLUSH,

    /* online refresh increment serial thread */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_SERIAL,

    /* online refresh increment parser thread */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_PARSER,

    /* online refresh increment loadrecords thread */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_LOADRECORDS,

    /*------------add capture thread types above---------------------*/

    /* separator thread */
    THRNODE_IDENTITY_GAP,

    /*------------add integrate thread types BEGIN --------------*/
    /* sync/apply thread */
    THRNODE_IDENTITY_INC_INTEGRATE_SYNC,

    /* transaction rebuild thread */
    THRNODE_IDENTITY_INC_INTEGRATE_REBUILD,

    /* parser thread */
    THRNODE_IDENTITY_INC_INTEGRATE_PARSER,

    /* trail split thread */
    THRNODE_IDENTITY_INC_INTEGRATE_LOADRECORDS,

    /* metrics thread */
    THRNODE_IDENTITY_INTEGRATE_METRIC,

    /* big transaction manager thread */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNMGR,

    /* big transaction apply thread */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNSYNC,

    /* big transaction rebuild thread */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNREBUILD,

    /* big transaction parser thread */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNPARSER,

    /* big transaction trail split thread */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNLOADRECORDS,

    /* refresh manager thread */
    THRNODE_IDENTITY_INTEGRATE_REFRESH_MGR,

    /* refresh worker thread */
    THRNODE_IDENTITY_INTEGRATE_REFRESH_JOB,

    /* online refresh manager thread */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_MGR,

    /* online refresh worker thread */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_JOB,

    /* online refresh increment apply thread */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_SYNC,

    /* online refresh increment rebuild thread */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_REBUILD,

    /* online refresh increment parser thread */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_PARSER,

    /* online refresh increment trail split thread */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_LOADRECORDS,
    /*------------integrate thread types END------------------------*/

    /*------------xmanager thread types begin-----------------------*/
    THRNODE_IDENTITY_XMANAGER_LISTEN,
    THRNODE_IDENTITY_XMANAGER_AUTH,
    THRNODE_IDENTITY_XMANAGER_METRIC,

    /*------------xmanager thread types   end-----------------------*/

    /* add new types before this line */
    THRNODE_IDENTITY_MAX
} thrnode_identity;

/*
 * state transition logic
 *  NOP------------> init -----------> starting -----------------> work ---------->
 * term---------->exit ------------> exited----->free (struct initialization)
 *             |--->added to startup list
 *                                 |---> main thread starts thread and sets state to starting
 *                                                              |------->sub-thread started
 * successfully, set to work
 *                                                                                 |--->
 * main/manager thread sets to term
 *                                                                                                 |--->
 * sub-thread exits, set to exit
 *                                                                                                                   |---->main
 * thread detects exit, set to exited
 *                                                                                                                              |---->main/manager
 * thread reclaim resources
 *
 */
typedef enum THRNODE_STAT
{
    THRNODE_STAT_NOP = 0x00, /* unused                            */
    THRNODE_STAT_INIT,       /* initial state when adding thread  */
    THRNODE_STAT_STARTING,   /* starting up                       */
    THRNODE_STAT_WORK,       /*
                              * sub-thread is running, set to WORK
                              * note: manager thread sets state to WORK after starting sub-threads
                              */
    THRNODE_STAT_IDLE,       /* sub-thread has no work */
    THRNODE_STAT_TERM,       /* main thread received SIGTERM, set thread state to exit */
    THRNODE_STAT_ABORT,      /*
                              * thread's active behavior: thread detected an error internally
                              * abnormal exit, exit logic varies by thread type:
                              *  persistent thread: execute exit logic (worker -> manager -> persistent)
                              *  manager thread: worker thread exits
                              *  worker thread exits: find the corresponding manager thread (worker ->
                              * manager)      manager thread sets the abnormally exited thread's state to
                              * EXIT
                              */
    THRNODE_STAT_EXIT,       /* sub-thread exits normally, set state to EXIT */
    THRNODE_STAT_EXITED,     /* main thread completed reclamation, set to exited */
    THRNODE_STAT_FREE,       /* marks the node as reclaimable    */
} thrnode_stat;

typedef void  (*thrdatafree)(void* args);
typedef bool  (*threxitcondition)(void* args);
typedef void* (*thrmain)(void* args);

typedef struct THREADS
{
    /* other thread ids, minimum value is THRNODE_NO_NORMAL */
    uint64          no;

    /* persistent thread ids, maximum value is THRNODE_NO_NORMAL */
    uint64          persistno;

    /* lock */
    pthread_mutex_t lock;

    /* persistent threads */
    dlist*          thrpersist;

    /* sub-manager threads */
    dlist*          thrsubmgrs;

    /* thread list */
    dlist*          thrnodes;
} threads;

/* threadnoderef */
typedef struct THRREF
{
    /* used to look up thread info in threads->thrnodes by no */
    uint64 no;
} thrref;

/* manager thread */
typedef struct THRSUBMGR
{
    /* owning persistent thread node */
    thrref   persistref;

    /* manager thread */
    thrref   submgrref;

    /* sub-threads */
    List*    childthrrefs;

    /* parent thread pool */
    threads* parents;
} thrsubmgr;

typedef struct THRNODE
{
    /* thread type flag, persistent/dynamic, see above */
    int              type;

    /* thread identity */
    int              identity;

    /* state, see above */
    int              stat;

    pthread_t        thrid;

    /* thread id, greater than THRNODE_NO_NORMAL */
    uint64           no;

    /*
     * when this is a manager thread, thrsubmgr is not NULL, points to itself
     */
    thrsubmgr*       thrsubmgr;
    void*            data;
    thrdatafree      free;
    threxitcondition exitcondition;
    thrmain          tmain;
} thrnode;

/* persistent thread */
typedef struct THRPERSIST
{
    /* thread id */
    uint64 no;

    /* name */
    char   name[NAMEDATALEN];

    /* persistent thread references */
    List*  thrrefs;
} thrpersist;

/* thread pool initialization */
threads* threads_init(void);

/* get node by thread id */
thrnode* threads_getthrnodebyno(threads* thrs, uint64 no);

/* set sub-thread state to FREE */
void threads_setsubmgrjobthredsfree(threads* thrs, List* jobthreads, int skipcnt, int scnt);

/*
 * set sub-threads to term state and exit
 */
void threads_setsubmgrjobthreadterm(threads* thrs, List* jobthreads);

/*
 * count the number of sub-manager's worker threads with state > WORK
 */
bool threads_countsubmgrjobthredsabovework(threads* thrs, List* jobthreads, int* scnt);

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
bool threads_setsubmgrjobthredstermandcountexit(threads* thrs, List* jobthreads, int skipcnt, int* scnt);

/* add a persistent thread node */
bool threads_addpersist(threads* thrs, uint64* pno, char* name);

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
                              thrmain          tmain);

/* add manager thread */
bool threads_addsubmanger(threads*         thrs,
                          thrnode_identity identity,
                          uint64           persistno,
                          thrsubmgr**      pthrsubmgr,
                          void*            data,
                          thrdatafree      free,
                          threxitcondition exitcondition,
                          thrmain          tmain);

/* add worker thread */
bool threads_addjobthread(threads*         thrs,
                          thrnode_identity identity,
                          uint64           submgrno,
                          void*            data,
                          thrdatafree      free,
                          threxitcondition exitcondition,
                          thrmain          tmain);

/* thread exit, called when SIGTERM is received */
bool threads_exit(threads* thrs);

/*
 * attempt to join threads
 *
 * persistent thread exits: set all threads to exit and wait for them
 * manager thread abnormal exit: manager and its sub-threads exit
 * worker thread under manager exits: only reclaim the worker thread
 */
bool threads_tryjoin(threads* thrs);

/*
 * start sub-threads
 */
void threads_startthread(threads* thrs);

/* reclaim node */
void threads_thrnoderecycle(threads* thrs);

/*
 * check if there are sub-threads
 *  false       no sub-threads
 *  true        has sub-threads
 */
bool threads_hasthrnode(threads* thrs);

/* memory reclamation */
void threads_free(threads* thrs);

#endif
