#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/threads.h"

typedef struct THRIDENTITY2NAME
{
    thrnode_identity      identity;
    char*                           name;
} thridentityname;

static thridentityname    m_threadname[]=
{
    {THRNODE_IDENTITY_NOP,                                       "NOP"},

    /*------------capture 线程类型 begin---------------------*/
    {THRNODE_IDENTITY_INC_CAPTURE_FLUSH,                         "IncFlush"},
    {THRNODE_IDENTITY_INC_CAPTURE_SERIAL,                        "IncSerial"},
    {THRNODE_IDENTITY_INC_CAPTURE_PARSER,                        "IncParser"},
    {THRNODE_IDENTITY_INC_CAPTURE_LOADRECORD,                    "IncLoadRec"},
    {THRNODE_IDENTITY_CAPTURE_METRIC,                            "Metric"},
    {THRNODE_IDENTITY_INC_CAPTURE_BIGTXNMGR,                     "BigTxnMgr"},
    {THRNODE_IDENTITY_INC_CAPTURE_BIGTXNFLUSH,                   "BigTxnFlush"},
    {THRNODE_IDENTITY_INC_CAPTURE_BIGTXNSERIAL,                  "BigTxnSerial"},
    {THRNODE_IDENTITY_CAPTURE_REFRESH_MGR,                       "RefreshManger"},
    {THRNODE_IDENTITY_CAPTURE_REFRESH_JOB,                       "RefreshJob"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_MGR,                  "OLRManger"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_JOB,                  "OLRJob"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_FLUSH,            "OLRIncFlush"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_SERIAL,           "OLRIncSerial"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_PARSER,           "OLRIncParser"},
    {THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_LOADRECORDS,      "OLRIncLoadRec"},

    /*------------capture 线程类型   end---------------------*/

    /*------------integrate 线程类型 begin-------------------*/
    {THRNODE_IDENTITY_INC_INTEGRATE_SYNC,                        "IncSync"},
    {THRNODE_IDENTITY_INC_INTEGRATE_REBUILD,                     "IncRebuild"},
    {THRNODE_IDENTITY_INC_INTEGRATE_PARSER,                      "IncParser"},
    {THRNODE_IDENTITY_INC_INTEGRATE_LOADRECORDS,                 "IncLoadRec"},
    {THRNODE_IDENTITY_INTEGRATE_METRIC,                          "Metric"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNMGR,                   "BigTxnMgr"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNSYNC,                  "BigTxnSync"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNREBUILD,               "BigTxnRebuild"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNPARSER,                "BigTxnParser"},
    {THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNLOADRECORDS,           "BigTxnLoadRec"},
    {THRNODE_IDENTITY_INTEGRATE_REFRESH_MGR,                     "RefreshManger"},
    {THRNODE_IDENTITY_INTEGRATE_REFRESH_JOB,                     "RefreshJob"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_MGR,                "OLRManger"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_JOB,                "OLRJob"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_SYNC,           "OLRIncSync"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_REBUILD,        "OLRIncRebuild"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_PARSER,         "OLRIncParser"},
    {THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_LOADRECORDS,    "OLRIncLoadRec"},
    /*------------integrate 线程类型   end-------------------*/

    /*------------xmanger 线程类型 begin---------------------*/
    {THRNODE_IDENTITY_XMANAGER_LISTEN,                           "Listens"},
    {THRNODE_IDENTITY_XMANAGER_AUTH,                             "Auth"},
    {THRNODE_IDENTITY_XMANAGER_METRIC,                           "Metric"},
    /*------------xmanger 线程类型   end---------------------*/

    /* 在此之前添加 */
    {THRNODE_IDENTITY_MAX,                    "ThreadMax"}
};

/* 线程池初始化 */
threads* threads_init(void)
{
    threads* thrs = NULL;

    thrs = (threads*)rmalloc0(sizeof(threads));
    if(NULL == thrs)
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

/* 线程节点初始化 */
static thrnode* thr_node_init(void)
{
    thrnode* thr_node = NULL;

    thr_node = rmalloc0(sizeof(thrnode));
    if(NULL == thr_node)
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

/* 常驻线程初始化 */
static thrpersist* thr_persist_init(void)
{
    thrpersist* thr_persist = NULL;

    thr_persist = rmalloc0(sizeof(thrpersist));
    if(NULL == thr_persist)
    {
        elog(RLOG_WARNING, "thrpersist_init error");
        return NULL;
    }
    rmemset0(thr_persist, 0, '\0', sizeof(thrpersist));
    thr_persist->thrrefs = NULL;
    return thr_persist;
}

/* 常驻节点释放 */
static void thrpersist_free(void* args)
{
    thrpersist* thr_persist = NULL;
    if(NULL == args)
    {
        return;
    }

    thr_persist = (thrpersist*)args;
    list_free_deep(thr_persist->thrrefs);
    rfree(thr_persist);
}

/* 初始化 */
static thrref* thr_ref_init(void)
{
    thrref* thr_ref = NULL;

    thr_ref = rmalloc0(sizeof(thrref));
    if(NULL == thr_ref)
    {
        elog(RLOG_WARNING, "thrref init error");
        return NULL;
    }
    rmemset0(thr_ref, 0, '\0', sizeof(thrref));
    thr_ref->no = 0;
    return thr_ref;
}

/* 管理线程节点初始化 */
static thrsubmgr* thr_submgr_init(void)
{
    thrsubmgr* thr_submgr = NULL;

    thr_submgr = rmalloc0(sizeof(thrsubmgr));
    if(NULL == thr_submgr)
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

/* 管理节点释放 */
static void thrsubmgr_free(void* args)
{
    thrsubmgr* thr_submgr = NULL;

    if(NULL == args)
    {
        return;
    }
    thr_submgr = (thrsubmgr*)args;
    list_free_deep(thr_submgr->childthrrefs);
    rfree(thr_submgr);
}

/* 线程节点释放 */
static void thrnode_free(void* args)
{
    thrnode* thr_node = NULL;
    if(NULL == args)
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

/* 根据 no 获取常驻线程节点 */
static int threads_thrpersistcmp(void* argv1, void *argv2)
{
    uint64 no = 0;
    thrpersist* thr_persist = NULL;

    no = *((uint64*)argv1);
    thr_persist = (thrpersist*)argv2;

    if(no != thr_persist->no)
    {
        return 1;
    }
    return 0;
}

/* 根据 no 获取线程节点 */
static int threads_thrnodecmp(void* argv1, void *argv2)
{
    uint64 no = 0;
    thrnode* thr_node = NULL;

    no = *((uint64*)argv1);
    thr_node = (thrnode*)argv2;

    if(no != thr_node->no)
    {
        return 1;
    }
    return 0;
}

/* 管理节点比较 */
static int threads_thrsubmgrcmp(void* argv1, void *argv2)
{
    uint64 no = 0;
    thrsubmgr* thr_submgr = NULL;

    no = *((uint64*)argv1);
    thr_submgr = (thrsubmgr*)argv2;

    if(no != thr_submgr->submgrref.no)
    {
        return 1;
    }

    /* 0 相等 */
    return 0;
}

/*----------------------------------供外部使用: 查询或设置 begin---------------------------*/
/* 根据编号获取 node 节点 */
thrnode* threads_getthrnodebyno(threads* thrs, uint64 no)
{
    thrnode* thr_node = NULL;
    if(NULL == thrs)
    {
        return NULL;
    }
    osal_thread_lock(&thrs->lock);
    thr_node = dlist_get(thrs->thrnodes, &no, threads_thrnodecmp);
    osal_thread_unlock(&thrs->lock);

    return thr_node;
}

/* 
 * 设置子线程为 term 状态
 */
void threads_setsubmgrjobthreadterm(threads* thrs, List* jobthreads)
{
    ListCell* lc                        = NULL;
    thrref* thr_ref               = NULL;
    thrnode* thr_node             = NULL;

    foreach(lc, jobthreads)
    {
        thr_ref = (thrref*)lfirst(lc);
        thr_node = threads_getthrnodebyno(thrs, thr_ref->no);
        if(THRNODE_STAT_TERM > thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_TERM;
        }
    }
}

/*
 * 统计子管理线程下的工作线程状态 > 工作状态下的线程个数
*/
bool threads_countsubmgrjobthredsabovework(threads* thrs, List* jobthreads, int* scnt)
{
    ListCell* lc                        = NULL;
    thrref* thr_ref               = NULL;
    thrnode* thr_node             = NULL;

    foreach(lc, jobthreads)
    {
        thr_ref = (thrref*)lfirst(lc);
        thr_node = threads_getthrnodebyno(thrs, thr_ref->no);
        if(NULL == thr_node)
        {
            elog(RLOG_WARNING, "can not get thrnode by no:%lu", thr_ref->no);
            return false;
        }

        if(THRNODE_STAT_STARTING >= thr_node->stat)
        {
            continue;
        }
        (*scnt)++;
    }
    return true;
}

/* 
 * 设置子管理线程的子线程为 TERM
 *  1、在 IDLE 状态下为term
 *  2、统计已经退出线程的个数
 * 
 *  skipcnt     跳过的个数
 *  scnt        入参时为统计的个数
 *              出参为已退出的个数
 * 
 *  true        函数执行过程中未遇到逻辑错误
 *  false       函数执行过程中遇到错误的逻辑
 */
bool threads_setsubmgrjobthredstermandcountexit(threads* thrs, List* jobthreads, int skipcnt, int* scnt)
{
    int jobcnt                          = 0;
    ListCell* lc                        = NULL;
    thrref* thr_ref               = NULL;
    thrnode* thr_node             = NULL;

    foreach(lc, jobthreads)
    {
        if(0 != skipcnt)
        {
            skipcnt--;
            continue;
        }

        if(0 == *scnt)
        {
            break;
        }
        (*scnt)--;

        thr_ref = (thrref*)lfirst(lc);
        thr_node = threads_getthrnodebyno(thrs, thr_ref->no);
        if(NULL == thr_node)
        {
            elog(RLOG_WARNING, "can not get thrnode by no:%lu", thr_ref->no);
            return false;
        }

        /* 子线程在工作, 不处理 */
        if(THRNODE_STAT_IDLE > thr_node->stat)
        {
            continue;
        }

        if(THRNODE_STAT_IDLE == thr_node->stat)
        {
            /* 
            * 线程处于空闲状态, 那么设置为 term
            *  子线程只有在队列中没有获取到任务时才会设置为 IDLE
            *  当子线程刚好处于 idle 状态进入到队列中且立即获取到了任务，那么子线程会立即将任务的状态设置为 WORK
            */
            thr_node->stat = THRNODE_STAT_TERM;
            continue;
        }

        /* 子线程退出 */
        if(THRNODE_STAT_EXITED != thr_node->stat)
        {
            /* 子线程没有退出, 那么等待子线程完全退出 */
            continue;
        }

        jobcnt++;
    }

    *scnt = jobcnt;
    return true;
}

/* 设置子线程状态为 FREE */
void threads_setsubmgrjobthredsfree(threads* thrs, List* jobthreads, int skipcnt, int scnt)
{
    ListCell* lc                        = NULL;
    thrref* thr_ref               = NULL;
    thrnode* thr_node             = NULL;

    foreach(lc, jobthreads)
    {
        if(0 != skipcnt)
        {
            skipcnt--;
            continue;
        }

        if(0 == scnt)
        {
            break;
        }
        scnt--;
        thr_ref = (thrref*)lfirst(lc);
        thr_node = threads_getthrnodebyno(thrs, thr_ref->no);
        thr_node->stat = THRNODE_STAT_FREE;
    }
}


/*----------------------------------供外部使用: 查询或设置   end---------------------------*/

/*----------------------------------添加节点 begin-----------------------------------------*/
/* 添加一个常驻线程的节点 */
bool threads_addpersist(threads* thrs, uint64* pno, char* name)
{
    thrpersist* thr_persist = NULL;

    thr_persist = thr_persist_init();
    if(NULL == thr_persist)
    {
        elog(RLOG_WARNING, "add persist thread error");
        return false;
    }
    rmemcpy1(thr_persist->name, 0, name, strlen(name));

    /* 加入到链表中 */
    osal_thread_lock(&thrs->lock);
    thr_persist->no = ++thrs->persistno;

    /* 加入到队列中 */
    thrs->thrpersist = dlist_put(thrs->thrpersist, (void*)thr_persist);
    if(NULL == thrs->thrpersist)
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
 * 增加常驻线程
 *  identity                线程标识
 *  data                    线程主体结构
 *  
 *  pthrnode                返回值
*/
bool threads_addpersistthread(threads* thrs,
                                        thrnode** pthrnode,
                                        thrnode_identity identity,
                                        uint64 persistno,
                                        void* data,
                                        thrdatafree free,
                                        threxitcondition exitcondition,
                                        thrmain tmain)
{
    thrref* thr_ref = NULL;
    thrnode* thr_node = NULL;
    thrpersist* thr_persist = NULL;
    if(NULL == thrs)
    {
        return true;
    }

    thr_ref = thr_ref_init();
    if(NULL == thr_ref)
    {
        elog(RLOG_WARNING, "add persist thread error");
        return false;
    }

    thr_node = thr_node_init();
    if(NULL == thr_node)
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

    /* 加入到链表中 */
    osal_thread_lock(&thrs->lock);
    thr_node->no = ++thrs->no;
    thr_ref->no = thr_node->no;

    /* 查找常驻节点 */
    thr_persist = dlist_get(thrs->thrpersist, &persistno, threads_thrpersistcmp);
    if(NULL == thr_persist)
    {
        elog(RLOG_WARNING, "add persist thread error, can not get persist node by %lu", persistno);
        osal_thread_unlock(&thrs->lock);
        return false;
    }

    /* 将常驻线程加入到常驻线程节点中 */
    thr_persist->thrrefs = lcons(thr_ref, thr_persist->thrrefs);

    /* 加入到线程管理中 */
    thrs->thrnodes =  dlist_put(thrs->thrnodes, thr_node);
    if(NULL == thrs->thrnodes)
    {
        elog(RLOG_WARNING, "add thread error");
        osal_thread_unlock(&thrs->lock);
        return false;
    }

    osal_thread_unlock(&thrs->lock);
    return true;
}

/* 增加管理线程 */
bool threads_addsubmanger(threads* thrs,
                                    thrnode_identity identity,
                                    uint64 persistno,
                                    thrsubmgr** pthrsubmgr,
                                    void* data,
                                    thrdatafree free,
                                    threxitcondition exitcondition,
                                    thrmain tmain)
{
    thrnode* thr_node = NULL;
    thrsubmgr* thr_submgr = NULL;

    thr_submgr = thr_submgr_init();
    if(NULL == thr_submgr)
    {
        elog(RLOG_WARNING, "addsubmanger error");
        return false;
    }
    thr_submgr->parents = thrs;
    thr_submgr->persistref.no = persistno;
    *pthrsubmgr = thr_submgr;

    thr_node = thr_node_init();
    if(NULL == thr_node)
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

    /* 加入到链表中 */
    thrs->thrsubmgrs = dlist_put(thrs->thrsubmgrs, thr_submgr);
    if(NULL == thrs->thrsubmgrs)
    {
        elog(RLOG_WARNING, "add sub manger thread error");
        return false;
    }

    /* 节点添加 */
    thrs->thrnodes =  dlist_put(thrs->thrnodes, thr_node);
    if(NULL == thrs->thrnodes)
    {
        elog(RLOG_WARNING, "add thread error");
        osal_thread_unlock(&thrs->lock);
        return false;
    }
    osal_thread_unlock(&thrs->lock);
    return true;
}


/* 添加工作线程 */
bool threads_addjobthread(threads* thrs,
                                    thrnode_identity identity,
                                    uint64 submgrno,
                                    void* data,
                                    thrdatafree free,
                                    threxitcondition exitcondition,
                                    thrmain tmain)
{
    thrref* thr_ref = NULL;
    thrnode* thr_node = NULL;
    thrsubmgr* thr_submgr = NULL;

    thr_ref = thr_ref_init();
    if(NULL == thr_ref)
    {
        elog(RLOG_WARNING, "add job thread error");
        return false;
    }

    thr_node = thr_node_init();
    if(NULL == thr_node)
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

    /* 加入到线程节点 */
    thrs->thrnodes = dlist_put(thrs->thrnodes, (void*)thr_node);

    /* 查找管理节点 */
    thr_submgr = dlist_get(thrs->thrsubmgrs, &submgrno, threads_thrsubmgrcmp);
    if(NULL == thr_submgr)
    {
        elog(RLOG_WARNING, "add job thread error, can not get submgr node by %lu", submgrno);
        osal_thread_unlock(&thrs->lock);
        return false;
    }

    thr_submgr->childthrrefs = lcons(thr_ref, thr_submgr->childthrrefs);
    osal_thread_unlock(&thrs->lock);
    return true;
}

/*----------------------------------添加节点   end-----------------------------------------*/

/*----------------------------------退出处理 begin-----------------------------------------*/
/* 一个线程退出 */
static void threads_thrnodeexit(thrnode* thr_node)
{
    int iret = 0;
    if(NULL == thr_node)
    {
        return;
    }

    /* 根据线程状态做不同的处理 */
    if(THRNODE_STAT_EXITED == thr_node->stat || THRNODE_STAT_FREE == thr_node->stat)
    {
        /* 已经退出或标识为待回收，那么不做处理 */
        elog(RLOG_INFO, "%s thread already exit", m_threadname[thr_node->identity].name);
        return;
    }
    else if(THRNODE_STAT_ABORT == thr_node->stat
            || THRNODE_STAT_EXIT == thr_node->stat)
    {
        /* 异常/正常退出, 那么做线程回收 */
        osal_thread_join(thr_node->thrid , NULL);

        /* 线程内资源回收 */
        goto threads_thrnodeexit_done;
    }

    /* 证明线程还未启动，那么就不需要启动了 */
    if(INVALIDTHRID == thr_node->thrid)
    {
        /* 线程不需要启动了, 线程内资源回收 */
        goto threads_thrnodeexit_done;
    }

    /* 查看线程是否在运行, 未运行那么回收 */
    iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
    if(EBUSY != iret && EINTR != iret)
    {
        /* 线程已经异常退出, 线程内资源回收 */
        goto threads_thrnodeexit_done;
    }

    /* 等待线程退出 */
    while(1)
    {
        if(NULL == thr_node->exitcondition)
        {
            /* 不需要退出的条件, 那么设置为 TERM */
            if(THRNODE_STAT_TERM > thr_node->stat)
            {
                thr_node->stat = THRNODE_STAT_TERM;
            }

            iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
            if(EBUSY == iret || EINTR == iret)
            {
                /* 依然在运行, 那么继续等待 */
                usleep(10000);
                continue;
            }

            if (THRNODE_STAT_TERM >= thr_node->stat)
            {
                continue;
            }
            break;
        }

        /* 查看是否满足退出条件 */
        while(1)
        {
            if(true == thr_node->exitcondition(thr_node->data))
            {
                /* 设置为空，跳出此层循环即可 */
                thr_node->exitcondition = NULL;
                break;
            }

            /* 查看是否异常退出 */
            iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
            if(EBUSY == iret || EINTR == iret)
            {
                /* 依然在运行, 那么继续等待 */
                usleep(10000);
                continue;
            }
            goto threads_thrnodeexit_done;
        }
    }

threads_thrnodeexit_done:
    /* 线程已经退出, 设置为 exited */
    elog(RLOG_INFO, "%lu::%s thread exit", thr_node->thrid, m_threadname[thr_node->identity].name);
    thr_node->stat = THRNODE_STAT_EXITED;
    return;
}

/* 
 * 一个线程组退出
 *  按照顺序设置线程的状态为 term, 并等待线程退出
*/
static void threads_workinggroupexit(threads* thrs, List* thrwg)
{
    ListCell* lc = NULL;
    thrref* thr_ref = NULL;
    thrnode* thr_node = NULL;
    foreach(lc, thrwg)
    {
        thr_ref = (thrref*)lfirst(lc);

        /* 获取 thrnode */
        thr_node = dlist_get(thrs->thrnodes, &thr_ref->no, threads_thrnodecmp);
        if(NULL == thr_node)
        {
            continue;
        }
        threads_thrnodeexit(thr_node);
    }
    return;
}

/*
 * 设置线程组的状态为 FREE
*/
static void threads_setworkinggroupfree(threads* thrs, List* thrwg)
{
    ListCell* lc = NULL;
    thrref* thr_ref = NULL;
    thrnode* thr_node = NULL;
    foreach(lc, thrwg)
    {
        thr_ref = (thrref*)lfirst(lc);

        /* 获取 thrnode */
        thr_node = dlist_get(thrs->thrnodes, &thr_ref->no, threads_thrnodecmp);
        if(NULL == thr_node)
        {
            continue;
        }
        thr_node->stat = THRNODE_STAT_FREE;
    }

    return;
}

/* 管理节点异常退出 */
static void threads_tmgrabort(threads* thrs, thrsubmgr* thr_submgr)
{
    /* 管理线程节点 */
    thrnode* thr_node = NULL;

    /* 获取管理线程 thrnode */
    thr_node = dlist_get(thrs->thrnodes, &thr_submgr->submgrref.no, threads_thrnodecmp);

    /* 子线程退出 */
    threads_workinggroupexit(thrs, thr_submgr->childthrrefs);

    /* 管理线程退出 */
    threads_thrnodeexit(thr_node);

    /* 设置子线程为 FREE, 设置管理节点为 FREE */
    threads_setworkinggroupfree(thrs, thr_submgr->childthrrefs);
    thr_node->stat = THRNODE_STAT_FREE;
    return;
}

/* 管理节点退出 */
static void threads_tmgrexit(threads* thrs, thrsubmgr* thr_submgr, bool holdlock)
{
    int iret = 0;
    /* 管理线程节点 */
    thrnode* thr_node = NULL;

    /* 获取管理线程 thrnode */
    thr_node = dlist_get(thrs->thrnodes, &thr_submgr->submgrref.no, threads_thrnodecmp);

    /* 加入到链表中未启动 */
    if (THRNODE_STAT_INIT == thr_node->stat)
    {
        thr_node->stat = THRNODE_STAT_FREE;
    }

    if(THRNODE_STAT_FREE == thr_node->stat)
    {
        return;
    }

    /*
     * 不为 work 状态,那么说明 manager 线程在启动中
     * manager 在 start---->work 之间做的工作为:
     * 1、初始化资源
     * 2、添加工作线程到待启动队列
     * 而在启动子线程时,此时需要获取锁,所以在此处先把锁释放,等管理线程的状态为work以后的状态,在获取锁
     */
    if(THRNODE_STAT_WORK > thr_node->stat)
    {
        /* 先解锁 */
        if (true == holdlock)
        {
            osal_thread_unlock(&thrs->lock);
        }
        while(1)
        {
            /* 等待大于等于 WORK */
            if(THRNODE_STAT_WORK <= thr_node->stat)
            {
                /* 管理线程启动成功或异常退出 */
                break;
            }

            /* 检测线程是否异常 */
            /* 查看线程是否在运行, 未运行那么回收 */
            iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
            if(EBUSY != iret && EINTR != iret)
            {
                /* 线程已经异常退出 */
                thr_node->stat = THRNODE_STAT_EXITED;
                break;
            }
            usleep(10000);
            continue;
        }

        /* 重新获取锁 */
        if (true == holdlock)
        {
            osal_thread_lock(&thrs->lock);
        }
    }

    /* 子线程先退出
     * 考虑到停止过程中， 管理线程异常退出，那么也需要将工作线程回收掉
     */
    threads_workinggroupexit(thrs, thr_submgr->childthrrefs);

    /* 设置子线程的状态为 FREE */
    threads_setworkinggroupfree(thrs, thr_submgr->childthrrefs);

    /* abort */
    if(THRNODE_STAT_ABORT == thr_node->stat)
    {
        /* 为 abort 说明子线程退出了且没有回收 */
        threads_tmgrabort(thrs, thr_submgr);
        return;
    }
    else if(THRNODE_STAT_EXITED != thr_node->stat)
    {
        if(THRNODE_STAT_TERM > thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_TERM;
        }
        while(1)
        {
            /* 设置线程状态为 TERM */
            iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
            if(EBUSY == iret || EINTR == iret)
            {
                /* 依然在运行, 那么继续等待 */
                usleep(10000);
                continue;
            }
            break;
        }
    }

    /* 设置管理线程状态为 FREE */
    elog(RLOG_INFO, "%s thread exit", m_threadname[thr_node->identity].name);
    thr_node->stat = THRNODE_STAT_FREE;
    return;
}

/* 异常退出, 线程退出时设置了 ABORT */
static void threads_abort(threads* thrs, thrnode* thr_node)
{
    /*
     * 常驻线程 abort
     *  1、将子管理线程退出
     *  2、所有的常驻线程退出
     * 
     * 子管理线程退出
     *  将管理线程下的工作线程退出并设置状态为 FREE
     * 
     * 子管理线程下的工作线程 abort
     *  回收资源并设置状态为 THRNODE_STAT_EXITED
     */
    dlistnode* dlnode = NULL;
    thrsubmgr* thr_submgr = NULL;
    thrpersist* thr_persist = NULL;
    if(NULL == thrs || NULL == thr_node)
    {
        return;
    }

    /* 所有的线程都得退出 */
    if(THRNODE_TYPE_PERSIST == thr_node->type)
    {
        /* 设置所有的mgr线程退出 */
        if(false == dlist_isnull(thrs->thrsubmgrs))
        {
            for(dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
            {
                thr_submgr = (thrsubmgr*)dlnode->value;
                threads_tmgrexit(thrs, thr_submgr, true);
            }
        }

        /* 设置所有的 persist 退出 */
        for(dlnode = thrs->thrpersist->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thr_persist = (thrpersist*)dlnode->value;

            /* 常驻线程退出 */
            threads_workinggroupexit(thrs, thr_persist->thrrefs);
            threads_setworkinggroupfree(thrs, thr_persist->thrrefs);
        }
    }
    else if(THRNODE_TYPE_TMGR == thr_node->type)
    {
        /* 设置管理下程下的工作线程退出 */
        thr_submgr = dlist_get(thrs->thrsubmgrs, &thr_node->no, threads_thrsubmgrcmp);
        threads_tmgrabort(thrs, thr_submgr);
    }
    else if(THRNODE_TYPE_TJOB == thr_node->type)
    {
        /* 工作线程退出 */
        threads_thrnodeexit(thr_node);
    }
    return;
}

/*
 * 线程异常退出,出现的情况可能如下:
 *  1、已经设置了 abort 或 exit, 但是因程序执行上下文的关系,导致先捕获到了线程的退出
 *  2、逻辑错误, 忘记设置 abort 或 exit, 出现此种情况最好排查一下逻辑
*/
static void threads_exitimpolite(threads* thrs, thrnode* thr_node)
{
    if(NULL == thrs || NULL == thr_node)
    {
        return;
    }

    /* 设置为已经回收 */
    thr_node->stat = THRNODE_STAT_EXITED;
    elog(RLOG_INFO, "%lu::%s thread exit", thr_node->thrid, m_threadname[thr_node->identity].name);
    threads_abort(thrs, thr_node);
}


/* 线程退出,接收到 sigterm 执行此函数 */
bool threads_exit(threads* thrs)
{
    /* 查找管理节点 */
    dlistnode* dlnode = NULL;
    thrsubmgr* thr_submgr = NULL;
    thrpersist* thr_persist = NULL;

    /* step1 遍历管理线程节点, 让所有的子节点退出 */
    if(false == dlist_isnull(thrs->thrsubmgrs))
    {
        for(dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thr_submgr = (thrsubmgr*)dlnode->value;
            threads_tmgrexit(thrs, thr_submgr, false);
        }
    }

    /* step2 常驻线程退出 */
    for(dlnode = thrs->thrpersist->head; NULL != dlnode; dlnode = dlnode->next)
    {
        thr_persist = (thrpersist*)dlnode->value;

        /* 常驻线程退出 */
        threads_workinggroupexit(thrs, thr_persist->thrrefs);
    }

    /* step3 遍历管理线程节点, 让所有的子节点退出 */
    if(false == dlist_isnull(thrs->thrsubmgrs))
    {
        for(dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thr_submgr = (thrsubmgr*)dlnode->value;
            threads_tmgrexit(thrs, thr_submgr, false);
        }
    }


    return true;
}

/*----------------------------------退出处理   end-----------------------------------------*/


/*
 * 尝试回收线程
 * 
 * 常驻线程退出, 那么设置所有的线程退出, 并等待线程退出
 * 管理线程异常退出, 管理线程和管理线程下的子线程退出
 * 管理线程下的工作线程退出, 只回收工作线程
*/
bool threads_tryjoin(threads* thrs)
{
    int iret = 0;
    dlistnode* dlnode = NULL;
    thrnode* thr_node = NULL;
    osal_thread_lock(&thrs->lock);

    /* 遍历子线程， 查看子线程是否异常退出 */
    for(dlnode = thrs->thrnodes->head; NULL != dlnode;)
    {
        thr_node = (thrnode*)dlnode->value;
        if(THRNODE_STAT_EXITED == thr_node->stat)
        {
            /* 已经被回收过,那么不做处理 */
            dlnode = dlnode->next;
            continue;
        }
        else if(THRNODE_STAT_FREE == thr_node->stat)
        {
            /* 不做处理, 等待后续流程回收掉该节点 */
            dlnode = dlnode->next;
            continue;
        }
        else if(THRNODE_STAT_ABORT == thr_node->stat)
        {
            /* 异常退出了,那么根据线程的状态做处理 */
            threads_abort(thrs, thr_node);
            elog(RLOG_INFO, "%s thread about exit", m_threadname[thr_node->identity].name);
            dlnode = dlnode->next;
            continue;
        }
        else if(THRNODE_STAT_EXIT == thr_node->stat)
        {
            /* 回收 */
            osal_thread_join(thr_node->thrid , NULL);
            thr_node->stat = THRNODE_STAT_EXITED;
            elog(RLOG_INFO, "%s thread exit", m_threadname[thr_node->identity].name);
            dlnode = dlnode->next;

            /* 子管理线程因为没有办法设置自己为 FREE, 所以在此处判断线程的类型是否为子管理线程 */
            if(THRNODE_TYPE_TMGR == thr_node->type)
            {
                thr_node->stat = THRNODE_STAT_FREE;
            }
            continue;
        }

        /* 证明还没有启动, 不做处理 */
        if(INVALIDTHRID == thr_node->thrid)
        {
            dlnode = dlnode->next;
            continue;
        }

        /* 尝试回收 */
        iret = osal_thread_tryjoin_np(thr_node->thrid, NULL);
        if(EBUSY == iret || EINTR == iret)
        {
            /* 正常运行 */
            dlnode = dlnode->next;
            continue;
        }

        /* 根据线程类型, 执行不同的退出逻辑 */
        threads_exitimpolite(thrs, thr_node);
        dlnode = dlnode->next;
    }

    osal_thread_unlock(&thrs->lock);
    return true;
}

/*
 * 启动子线程
*/
void threads_startthread(threads* thrs)
{
    dlistnode* dlnode = NULL;
    thrnode* thr_node = NULL;
    osal_thread_lock(&thrs->lock);
    if(true == dlist_isnull(thrs->thrnodes))
    {
        osal_thread_unlock(&thrs->lock);
        return;
    }

    /* 遍历启动 */
    for(dlnode = thrs->thrnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        thr_node = (thrnode*)dlnode->value;
        if(THRNODE_STAT_INIT != thr_node->stat)
        {
            continue;
        }

        thr_node->stat = THRNODE_STAT_STARTING;
        /* 创建工作线程 */
        osal_thread_create(&thr_node->thrid, NULL, thr_node->tmain, thr_node);

        elog(RLOG_DEBUG,"start thrid:%lu, threadname:%s",thr_node->thrid, m_threadname[thr_node->identity].name);

        /* 设置线程名称 */
        osal_thread_setname_np(thr_node->thrid, m_threadname[thr_node->identity].name);
    }
    osal_thread_unlock(&thrs->lock);
}

/* 回收节点 */
void threads_thrnoderecycle(threads* thrs)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodenext = NULL;
    thrnode* thr_node = NULL;

    osal_thread_lock(&thrs->lock);
    if(true == dlist_isnull(thrs->thrnodes))
    {
        osal_thread_unlock(&thrs->lock);
        return;
    }

    for(dlnode = thrs->thrnodes->head; NULL != dlnode; )
    {
        thr_node = (thrnode*)dlnode->value;
        if(THRNODE_STAT_FREE != thr_node->stat)
        {
            dlnode = dlnode->next;
            continue;
        }

        /* 子管理节点, 需要将子管理节点释放掉 */
        if(THRNODE_TYPE_TMGR == thr_node->type)
        {
            /* 清理掉管理节点 */
            thrs->thrsubmgrs = dlist_deletebyvalue(thrs->thrsubmgrs,
                                                    &thr_node->no,
                                                    threads_thrsubmgrcmp,
                                                    thrsubmgr_free);
        }

        /* 回收节点 */
        dlnodenext = dlnode->next;
        thrs->thrnodes = dlist_delete(thrs->thrnodes, dlnode, thrnode_free);
        dlnode = dlnodenext;
    }

    osal_thread_unlock(&thrs->lock);
}

/* 
 * 是否含有子线程
 *  false       不含有子线程
 *  true        含有子线程
*/
bool threads_hasthrnode(threads* thrs)
{
    osal_thread_lock(&thrs->lock);
    if(true == dlist_isnull(thrs->thrnodes))
    {
        osal_thread_unlock(&thrs->lock);
        return false;
    }
    osal_thread_unlock(&thrs->lock);
    return true;
}

/* 内存回收 */
void threads_free(threads* thrs)
{
    if(NULL == thrs)
    {
        return;
    }

    dlist_free(thrs->thrnodes, thrnode_free);

    dlist_free(thrs->thrpersist, thrpersist_free);

    dlist_free(thrs->thrsubmgrs, thrsubmgr_free);

    osal_thread_mutex_destroy(&thrs->lock);

    rfree(thrs);
}
