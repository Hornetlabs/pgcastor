#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/ripple_threads.h"

typedef struct RIPPLE_THRIDENTITY2NAME
{
    ripple_thrnode_identity      identity;
    char*                           name;
} ripple_thridentityname;

static ripple_thridentityname    m_threadname[]=
{
    {RIPPLE_THRNODE_IDENTITY_NOP,                                       "NOP"},

    /*------------capture 线程类型 begin---------------------*/
    {RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_FLUSH,                         "IncFlush"},
    {RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_SERIAL,                        "IncSerial"},
    {RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_PARSER,                        "IncParser"},
    {RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_LOADRECORD,                    "IncLoadRec"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_METRIC,                            "Metric"},
    {RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_BIGTXNMGR,                     "BigTxnMgr"},
    {RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_BIGTXNFLUSH,                   "BigTxnFlush"},
    {RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_BIGTXNSERIAL,                  "BigTxnSerial"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_REFRESH_MGR,                       "RefreshManger"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_REFRESH_JOB,                       "RefreshJob"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_MGR,                  "OLRManger"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_JOB,                  "OLRJob"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_FLUSH,            "OLRIncFlush"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_SERIAL,           "OLRIncSerial"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_PARSER,           "OLRIncParser"},
    {RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_LOADRECORDS,      "OLRIncLoadRec"},

    /*------------capture 线程类型   end---------------------*/

    /*------------pump 线程类型 begin------------------------*/
    {RIPPLE_THRNODE_IDENTITY_INC_PUMP_NETCLIENT,                        "IncNetClt"},
    {RIPPLE_THRNODE_IDENTITY_INC_PUMP_SERIAL,                           "IncSerial"},
    {RIPPLE_THRNODE_IDENTITY_INC_PUMP_PARSER,                           "IncParser"},
    {RIPPLE_THRNODE_IDENTITY_INC_PUMP_LOADRECORD,                       "IncLoadRec"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_METRIC,                               "Metric"},
    {RIPPLE_THRNODE_IDENTITY_GAP,                                       "Gap"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_REFRESH_MGR,                          "RefreshMgr"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_REFRESH_JOB,                          "RefreshJob"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_ONLINEREFRESH_INC_MGR,                "OLRMgr"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_ONLINEREFRESH_JOB,                    "OLRJob"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_OLINEREFRESH_INC_NET,                 "OLRIncNet"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_OLINEREFRESH_INC_SERIAL,              "OLRIncSerial"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_OLINEREFRESH_INC_PARSER,              "OLRIncParser"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_OLINEREFRESH_INC_LOADRECORDS,         "OLRIncLoadRec"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_MGR,                           "BigTxnMgr"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_NETCLIENT,                     "BigTxnNetClt"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_SERIAL,                        "BigTxnSerial"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_PARSER,                        "BigTxnParser"},
    {RIPPLE_THRNODE_IDENTITY_PUMP_BIGTXN_LOADRECORD,                    "BigTxnLoadRec"},

    /*------------pump 线程类型   end------------------------*/

    /*------------collector 线程类型 begin-------------------*/
    /* 网络服务线程 */
    {RIPPLE_THRNODE_IDENTITY_INC_COLLECTOR_SVR,                         "IncNetSvr"},
    {RIPPLE_THRNODE_IDENTITY_INC_COLLECTOR_FTP,                         "IncFTP"},
    {RIPPLE_THRNODE_IDENTITY_COLLECTOR_METRIC,                          "Metric"},
    {RIPPLE_THRNODE_IDENTITY_INC_COLLECTOR_FLUSH,                       "IncFlush"},
    {RIPPLE_THRNODE_IDENTITY_INC_COLLECTOR_NETCLINT,                    "IncNetClt"},
    /*------------collector 线程类型   end-------------------*/

    /*------------integrate 线程类型 begin-------------------*/
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_SYNC,                        "IncSync"},
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_REBUILD,                     "IncRebuild"},
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_PARSER,                      "IncParser"},
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_LOADRECORDS,                 "IncLoadRec"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_METRIC,                          "Metric"},
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNMGR,                   "BigTxnMgr"},
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNSYNC,                  "BigTxnSync"},
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNREBUILD,               "BigTxnRebuild"},
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNPARSER,                "BigTxnParser"},
    {RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNLOADRECORDS,           "BigTxnLoadRec"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_REFRESH_MGR,                     "RefreshManger"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_REFRESH_JOB,                     "RefreshJob"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_MGR,                "OLRManger"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_JOB,                "OLRJob"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_SYNC,           "OLRIncSync"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_REBUILD,        "OLRIncRebuild"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_PARSER,         "OLRIncParser"},
    {RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_LOADRECORDS,    "OLRIncLoadRec"},
    /*------------integrate 线程类型   end-------------------*/

    /*------------xmanger 线程类型 begin---------------------*/
    {RIPPLE_THRNODE_IDENTITY_XMANAGER_LISTEN,                           "Listens"},
    {RIPPLE_THRNODE_IDENTITY_XMANAGER_AUTH,                             "Auth"},
    {RIPPLE_THRNODE_IDENTITY_XMANAGER_METRIC,                           "Metric"},
    /*------------xmanger 线程类型   end---------------------*/

    /* 在此之前添加 */
    {RIPPLE_THRNODE_IDENTITY_MAX,                    "ThreadMax"}
};

/* 线程池初始化 */
ripple_threads* ripple_threads_init(void)
{
    ripple_threads* thrs = NULL;

    thrs = (ripple_threads*)rmalloc0(sizeof(ripple_threads));
    if(NULL == thrs)
    {
        elog(RLOG_WARNING, "threas init out of memory");
        return NULL;
    }
    rmemset0(thrs, 0, '\0', sizeof(ripple_threads));
    thrs->no = RIPPLE_THRNODE_NO_NORMAL;
    thrs->persistno = 0;
    thrs->thrnodes = NULL;
    thrs->thrpersist = NULL;
    thrs->thrsubmgrs = NULL;

    ripple_thread_mutex_init(&thrs->lock, NULL);
    return thrs;
}

/* 线程节点初始化 */
static ripple_thrnode* ripple_thrnode_init(void)
{
    ripple_thrnode* thrnode = NULL;

    thrnode = rmalloc0(sizeof(ripple_thrnode));
    if(NULL == thrnode)
    {
        elog(RLOG_WARNING, "thrnode init error");
        return NULL;
    }
    rmemset0(thrnode, 0, '\0', sizeof(ripple_thrnode));
    thrnode->data = NULL;
    thrnode->free = NULL;
    thrnode->exitcondition = NULL;
    thrnode->identity = RIPPLE_THRNODE_IDENTITY_NOP;
    thrnode->no = 0;
    thrnode->stat = RIPPLE_THRNODE_STAT_NOP;
    thrnode->thrid = RIPPLE_INVALIDTHRID;
    thrnode->type = RIPPLE_THRNODE_TYPE_NOP;
    return thrnode;
}

/* 常驻线程初始化 */
static ripple_thrpersist* ripple_thrpersist_init(void)
{
    ripple_thrpersist* thrpersist = NULL;

    thrpersist = rmalloc0(sizeof(ripple_thrpersist));
    if(NULL == thrpersist)
    {
        elog(RLOG_WARNING, "thrpersist_init error");
        return NULL;
    }
    rmemset0(thrpersist, 0, '\0', sizeof(ripple_thrpersist));
    thrpersist->thrrefs = NULL;
    return thrpersist;
}

/* 常驻节点释放 */
static void ripple_thrpersist_free(void* args)
{
    ripple_thrpersist* thrpersist = NULL;
    if(NULL == args)
    {
        return;
    }

    thrpersist = (ripple_thrpersist*)args;
    list_free_deep(thrpersist->thrrefs);
    rfree(thrpersist);
}

/* 初始化 */
static ripple_thrref* ripple_thrref_init(void)
{
    ripple_thrref* thrref = NULL;

    thrref = rmalloc0(sizeof(ripple_thrref));
    if(NULL == thrref)
    {
        elog(RLOG_WARNING, "thrref init error");
        return NULL;
    }
    rmemset0(thrref, 0, '\0', sizeof(ripple_thrref));
    thrref->no = 0;
    return thrref;
}

/* 管理线程节点初始化 */
static ripple_thrsubmgr* ripple_thrsubmgr_init(void)
{
    ripple_thrsubmgr* thrsubmgr = NULL;

    thrsubmgr = rmalloc0(sizeof(ripple_thrsubmgr));
    if(NULL == thrsubmgr)
    {
        elog(RLOG_WARNING, "thrsubmgr_init out of memory");
        return NULL;
    }
    rmemset0(thrsubmgr, 0, '\0', sizeof(ripple_thrsubmgr));
    thrsubmgr->childthrrefs = NULL;
    thrsubmgr->parents = NULL;
    thrsubmgr->persistref.no = 0;
    thrsubmgr->submgrref.no = 0;

    return thrsubmgr;
}

/* 管理节点释放 */
static void ripple_thrsubmgr_free(void* args)
{
    ripple_thrsubmgr* thrsubmgr = NULL;

    if(NULL == args)
    {
        return;
    }
    thrsubmgr = (ripple_thrsubmgr*)args;
    list_free_deep(thrsubmgr->childthrrefs);
    rfree(thrsubmgr);
}

/* 线程节点释放 */
static void ripple_thrnode_free(void* args)
{
    ripple_thrnode* thrnode = NULL;
    if(NULL == args)
    {
        return;
    }
    thrnode = (ripple_thrnode*)args;
    
    if (NULL != thrnode->free && NULL != thrnode->data)
    {
        thrnode->free(thrnode->data);
    }

    rfree(thrnode);
    return;
}

/* 根据 no 获取常驻线程节点 */
static int ripple_threads_thrpersistcmp(void* argv1, void *argv2)
{
    uint64 no = 0;
    ripple_thrpersist* thrpersist = NULL;

    no = *((uint64*)argv1);
    thrpersist = (ripple_thrpersist*)argv2;

    if(no != thrpersist->no)
    {
        return 1;
    }

    /* 0 相等 */
    return 0;
}

/* 根据 no 获取线程节点 */
static int ripple_threads_thrnodecmp(void* argv1, void *argv2)
{
    uint64 no = 0;
    ripple_thrnode* thrnode = NULL;

    no = *((uint64*)argv1);
    thrnode = (ripple_thrnode*)argv2;

    if(no != thrnode->no)
    {
        return 1;
    }

    /* 0 相等 */
    return 0;
}

/* 管理节点比较 */
static int ripple_threads_thrsubmgrcmp(void* argv1, void *argv2)
{
    uint64 no = 0;
    ripple_thrsubmgr* thrsubmgr = NULL;

    no = *((uint64*)argv1);
    thrsubmgr = (ripple_thrsubmgr*)argv2;

    if(no != thrsubmgr->submgrref.no)
    {
        return 1;
    }

    /* 0 相等 */
    return 0;
}

/*----------------------------------供外部使用: 查询或设置 begin---------------------------*/
/* 根据编号获取 node 节点 */
ripple_thrnode* ripple_threads_getthrnodebyno(ripple_threads* thrs, uint64 no)
{
    ripple_thrnode* thrnode = NULL;
    if(NULL == thrs)
    {
        return NULL;
    }
    ripple_thread_lock(&thrs->lock);
    thrnode = dlist_get(thrs->thrnodes, &no, ripple_threads_thrnodecmp);
    ripple_thread_unlock(&thrs->lock);

    return thrnode;
}

/* 
 * 设置子线程为 term 状态
 */
void ripple_threads_setsubmgrjobthreadterm(ripple_threads* thrs, List* jobthreads)
{
    ListCell* lc                        = NULL;
    ripple_thrref* thrref               = NULL;
    ripple_thrnode* thrnode             = NULL;

    foreach(lc, jobthreads)
    {
        thrref = (ripple_thrref*)lfirst(lc);
        thrnode = ripple_threads_getthrnodebyno(thrs, thrref->no);
        if(RIPPLE_THRNODE_STAT_TERM > thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_TERM;
        }
    }
}

/*
 * 统计子管理线程下的工作线程状态 > 工作状态下的线程个数
*/
bool ripple_threads_countsubmgrjobthredsabovework(ripple_threads* thrs, List* jobthreads, int* scnt)
{
    ListCell* lc                        = NULL;
    ripple_thrref* thrref               = NULL;
    ripple_thrnode* thrnode             = NULL;

    foreach(lc, jobthreads)
    {
        thrref = (ripple_thrref*)lfirst(lc);
        thrnode = ripple_threads_getthrnodebyno(thrs, thrref->no);
        if(NULL == thrnode)
        {
            elog(RLOG_WARNING, "can not get thrnode by no:%lu", thrref->no);
            return false;
        }

        if(RIPPLE_THRNODE_STAT_STARTING >= thrnode->stat)
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
bool ripple_threads_setsubmgrjobthredstermandcountexit(ripple_threads* thrs, List* jobthreads, int skipcnt, int* scnt)
{
    int jobcnt                          = 0;
    ListCell* lc                        = NULL;
    ripple_thrref* thrref               = NULL;
    ripple_thrnode* thrnode             = NULL;

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

        thrref = (ripple_thrref*)lfirst(lc);
        thrnode = ripple_threads_getthrnodebyno(thrs, thrref->no);
        if(NULL == thrnode)
        {
            elog(RLOG_WARNING, "can not get thrnode by no:%lu", thrref->no);
            return false;
        }

        /* 子线程在工作, 不处理 */
        if(RIPPLE_THRNODE_STAT_IDLE > thrnode->stat)
        {
            continue;
        }

        if(RIPPLE_THRNODE_STAT_IDLE == thrnode->stat)
        {
            /* 
            * 线程处于空闲状态, 那么设置为 term
            *  子线程只有在队列中没有获取到任务时才会设置为 IDLE
            *  当子线程刚好处于 idle 状态进入到队列中且立即获取到了任务，那么子线程会立即将任务的状态设置为 WORK
            */
            thrnode->stat = RIPPLE_THRNODE_STAT_TERM;
            continue;
        }

        /* 子线程退出 */
        if(RIPPLE_THRNODE_STAT_EXITED != thrnode->stat)
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
void ripple_threads_setsubmgrjobthredsfree(ripple_threads* thrs, List* jobthreads, int skipcnt, int scnt)
{
    ListCell* lc                        = NULL;
    ripple_thrref* thrref               = NULL;
    ripple_thrnode* thrnode             = NULL;

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
        thrref = (ripple_thrref*)lfirst(lc);
        thrnode = ripple_threads_getthrnodebyno(thrs, thrref->no);
        thrnode->stat = RIPPLE_THRNODE_STAT_FREE;
    }
}


/*----------------------------------供外部使用: 查询或设置   end---------------------------*/

/*----------------------------------添加节点 begin-----------------------------------------*/
/* 添加一个常驻线程的节点 */
bool ripple_threads_addpersist(ripple_threads* thrs, uint64* pno, char* name)
{
    ripple_thrpersist* thrpersist = NULL;

    thrpersist = ripple_thrpersist_init();
    if(NULL == thrpersist)
    {
        elog(RLOG_WARNING, "add persist thread error");
        return false;
    }
    rmemcpy1(thrpersist->name, 0, name, strlen(name));

    /* 加入到链表中 */
    ripple_thread_lock(&thrs->lock);
    thrpersist->no = ++thrs->persistno;

    /* 加入到队列中 */
    thrs->thrpersist = dlist_put(thrs->thrpersist, (void*)thrpersist);
    if(NULL == thrs->thrpersist)
    {
        elog(RLOG_WARNING, "add persist thread error");
        ripple_thread_unlock(&thrs->lock);
        return false;
    }
    ripple_thread_unlock(&thrs->lock);
    *pno = thrpersist->no;
    return true;
}

/* 
 * 增加常驻线程
 *  identity                线程标识
 *  data                    线程主体结构
 *  
 *  pthrnode                返回值
*/
bool ripple_threads_addpersistthread(ripple_threads* thrs,
                                        ripple_thrnode** pthrnode,
                                        ripple_thrnode_identity identity,
                                        uint64 persistno,
                                        void* data,
                                        thrdatafree free,
                                        threxitcondition exitcondition,
                                        thrmain tmain)
{
    ripple_thrref* thrref = NULL;
    ripple_thrnode* thrnode = NULL;
    ripple_thrpersist* thrpersist = NULL;
    if(NULL == thrs)
    {
        return true;
    }

    thrref = ripple_thrref_init();
    if(NULL == thrref)
    {
        elog(RLOG_WARNING, "add persist thread error");
        return false;
    }

    thrnode = ripple_thrnode_init();
    if(NULL == thrnode)
    {
        elog(RLOG_WARNING, "add persist thread error");
        return false;
    }
    *pthrnode = thrnode;

    thrnode->data = data;
    thrnode->free = free;
    thrnode->exitcondition = exitcondition;
    thrnode->tmain = tmain;
    thrnode->identity = identity;
    thrnode->stat = RIPPLE_THRNODE_STAT_INIT;
    thrnode->thrid = 0;
    thrnode->type = RIPPLE_THRNODE_TYPE_PERSIST;
    *pthrnode = thrnode;

    /* 加入到链表中 */
    ripple_thread_lock(&thrs->lock);
    thrnode->no = ++thrs->no;
    thrref->no = thrnode->no;

    /* 查找常驻节点 */
    thrpersist = dlist_get(thrs->thrpersist, &persistno, ripple_threads_thrpersistcmp);
    if(NULL == thrpersist)
    {
        elog(RLOG_WARNING, "add persist thread error, can not get persist node by %lu", persistno);
        ripple_thread_unlock(&thrs->lock);
        return false;
    }

    /* 将常驻线程加入到常驻线程节点中 */
    thrpersist->thrrefs = lcons(thrref, thrpersist->thrrefs);

    /* 加入到线程管理中 */
    thrs->thrnodes =  dlist_put(thrs->thrnodes, thrnode);
    if(NULL == thrs->thrnodes)
    {
        elog(RLOG_WARNING, "add thread error");
        ripple_thread_unlock(&thrs->lock);
        return false;
    }

    ripple_thread_unlock(&thrs->lock);
    return true;
}

/* 增加管理线程 */
bool ripple_threads_addsubmanger(ripple_threads* thrs,
                                    ripple_thrnode_identity identity,
                                    uint64 persistno,
                                    ripple_thrsubmgr** pthrsubmgr,
                                    void* data,
                                    thrdatafree free,
                                    threxitcondition exitcondition,
                                    thrmain tmain)
{
    ripple_thrnode* thrnode = NULL;
    ripple_thrsubmgr* thrsubmgr = NULL;

    thrsubmgr = ripple_thrsubmgr_init();
    if(NULL == thrsubmgr)
    {
        elog(RLOG_WARNING, "addsubmanger error");
        return false;
    }
    thrsubmgr->parents = thrs;
    thrsubmgr->persistref.no = persistno;
    *pthrsubmgr = thrsubmgr;

    thrnode = ripple_thrnode_init();
    if(NULL == thrnode)
    {
        elog(RLOG_WARNING, "add sub manger thread error");
        return false;
    }

    thrnode->data = data;
    thrnode->free = free;
    thrnode->exitcondition = exitcondition;
    thrnode->tmain = tmain;
    thrnode->identity = identity;
    thrnode->stat = RIPPLE_THRNODE_STAT_INIT;
    thrnode->thrid = 0;
    thrnode->type = RIPPLE_THRNODE_TYPE_TMGR;
    thrnode->thrsubmgr = thrsubmgr;

    ripple_thread_lock(&thrs->lock);
    thrnode->no = ++thrs->no;
    thrsubmgr->submgrref.no = thrnode->no;

    /* 加入到链表中 */
    thrs->thrsubmgrs = dlist_put(thrs->thrsubmgrs, thrsubmgr);
    if(NULL == thrs->thrsubmgrs)
    {
        elog(RLOG_WARNING, "add sub manger thread error");
        return false;
    }

    /* 节点添加 */
    thrs->thrnodes =  dlist_put(thrs->thrnodes, thrnode);
    if(NULL == thrs->thrnodes)
    {
        elog(RLOG_WARNING, "add thread error");
        ripple_thread_unlock(&thrs->lock);
        return false;
    }
    ripple_thread_unlock(&thrs->lock);
    return true;
}


/* 添加工作线程 */
bool ripple_threads_addjobthread(ripple_threads* thrs,
                                    ripple_thrnode_identity identity,
                                    uint64 submgrno,
                                    void* data,
                                    thrdatafree free,
                                    threxitcondition exitcondition,
                                    thrmain tmain)
{
    ripple_thrref* thrref = NULL;
    ripple_thrnode* thrnode = NULL;
    ripple_thrsubmgr* thrsubmgr = NULL;

    thrref = ripple_thrref_init();
    if(NULL == thrref)
    {
        elog(RLOG_WARNING, "add job thread error");
        return false;
    }

    thrnode = ripple_thrnode_init();
    if(NULL == thrnode)
    {
        elog(RLOG_WARNING, "add job thread error");
        return false;
    }
    thrnode->data = data;
    thrnode->free = free;
    thrnode->exitcondition = exitcondition;
    thrnode->tmain = tmain;
    thrnode->identity = identity;
    thrnode->stat = RIPPLE_THRNODE_STAT_INIT;
    thrnode->thrid = 0;
    thrnode->type = RIPPLE_THRNODE_TYPE_TJOB;

    ripple_thread_lock(&thrs->lock);
    thrnode->no = ++thrs->no;
    thrref->no = thrnode->no;

    /* 加入到线程节点 */
    thrs->thrnodes = dlist_put(thrs->thrnodes, (void*)thrnode);

    /* 查找管理节点 */
    thrsubmgr = dlist_get(thrs->thrsubmgrs, &submgrno, ripple_threads_thrsubmgrcmp);
    if(NULL == thrsubmgr)
    {
        elog(RLOG_WARNING, "add job thread error, can not get submgr node by %lu", submgrno);
        ripple_thread_unlock(&thrs->lock);
        return false;
    }

    thrsubmgr->childthrrefs = lcons(thrref, thrsubmgr->childthrrefs);
    ripple_thread_unlock(&thrs->lock);
    return true;
}

/*----------------------------------添加节点   end-----------------------------------------*/

/*----------------------------------退出处理 begin-----------------------------------------*/
/* 一个线程退出 */
static void ripple_threads_thrnodeexit(ripple_thrnode* thrnode)
{
    int iret = 0;
    if(NULL == thrnode)
    {
        return;
    }

    /* 根据线程状态做不同的处理 */
    if(RIPPLE_THRNODE_STAT_EXITED == thrnode->stat || RIPPLE_THRNODE_STAT_FREE == thrnode->stat)
    {
        /* 已经退出或标识为待回收，那么不做处理 */
        elog(RLOG_INFO, "%s thread already exit", m_threadname[thrnode->identity].name);
        return;
    }
    else if(RIPPLE_THRNODE_STAT_ABORT == thrnode->stat
            || RIPPLE_THRNODE_STAT_EXIT == thrnode->stat)
    {
        /* 异常/正常退出, 那么做线程回收 */
        ripple_thread_join(thrnode->thrid , NULL);

        /* 线程内资源回收 */
        goto ripple_threads_thrnodeexit_done;
    }

    /* 证明线程还未启动，那么就不需要启动了 */
    if(RIPPLE_INVALIDTHRID == thrnode->thrid)
    {
        /* 线程不需要启动了, 线程内资源回收 */
        goto ripple_threads_thrnodeexit_done;
    }

    /* 查看线程是否在运行, 未运行那么回收 */
    iret = ripple_thread_tryjoin_np(thrnode->thrid, NULL);
    if(EBUSY != iret && EINTR != iret)
    {
        /* 线程已经异常退出, 线程内资源回收 */
        goto ripple_threads_thrnodeexit_done;
    }

    /* 等待线程退出 */
    while(1)
    {
        if(NULL == thrnode->exitcondition)
        {
            /* 不需要退出的条件, 那么设置为 TERM */
            if(RIPPLE_THRNODE_STAT_TERM > thrnode->stat)
            {
                thrnode->stat = RIPPLE_THRNODE_STAT_TERM;
            }

            iret = ripple_thread_tryjoin_np(thrnode->thrid, NULL);
            if(EBUSY == iret || EINTR == iret)
            {
                /* 依然在运行, 那么继续等待 */
                usleep(10000);
                continue;
            }

            if (RIPPLE_THRNODE_STAT_TERM >= thrnode->stat)
            {
                continue;
            }
            break;
        }

        /* 查看是否满足退出条件 */
        while(1)
        {
            if(true == thrnode->exitcondition(thrnode->data))
            {
                /* 设置为空，跳出此层循环即可 */
                thrnode->exitcondition = NULL;
                break;
            }

            /* 查看是否异常退出 */
            iret = ripple_thread_tryjoin_np(thrnode->thrid, NULL);
            if(EBUSY == iret || EINTR == iret)
            {
                /* 依然在运行, 那么继续等待 */
                usleep(10000);
                continue;
            }
            goto ripple_threads_thrnodeexit_done;
        }
    }

ripple_threads_thrnodeexit_done:
    /* 线程已经退出, 设置为 exited */
    elog(RLOG_INFO, "%lu::%s thread exit", thrnode->thrid, m_threadname[thrnode->identity].name);
    thrnode->stat = RIPPLE_THRNODE_STAT_EXITED;
    return;
}

/* 
 * 一个线程组退出
 *  按照顺序设置线程的状态为 term, 并等待线程退出
*/
static void ripple_threads_workinggroupexit(ripple_threads* thrs, List* thrwg)
{
    ListCell* lc = NULL;
    ripple_thrref* thrref = NULL;
    ripple_thrnode* thrnode = NULL;
    foreach(lc, thrwg)
    {
        thrref = (ripple_thrref*)lfirst(lc);

        /* 获取 thrnode */
        thrnode = dlist_get(thrs->thrnodes, &thrref->no, ripple_threads_thrnodecmp);
        if(NULL == thrnode)
        {
            continue;
        }
        ripple_threads_thrnodeexit(thrnode);
    }
    return;
}

/*
 * 设置线程组的状态为 FREE
*/
static void ripple_threads_setworkinggroupfree(ripple_threads* thrs, List* thrwg)
{
    ListCell* lc = NULL;
    ripple_thrref* thrref = NULL;
    ripple_thrnode* thrnode = NULL;
    foreach(lc, thrwg)
    {
        thrref = (ripple_thrref*)lfirst(lc);

        /* 获取 thrnode */
        thrnode = dlist_get(thrs->thrnodes, &thrref->no, ripple_threads_thrnodecmp);
        if(NULL == thrnode)
        {
            continue;
        }
        thrnode->stat = RIPPLE_THRNODE_STAT_FREE;
    }

    return;
}

/* 管理节点异常退出 */
static void ripple_threads_tmgrabort(ripple_threads* thrs, ripple_thrsubmgr* thrsubmgr)
{
    /* 管理线程节点 */
    ripple_thrnode* thrnode = NULL;

    /* 获取管理线程 thrnode */
    thrnode = dlist_get(thrs->thrnodes, &thrsubmgr->submgrref.no, ripple_threads_thrnodecmp);

    /* 子线程退出 */
    ripple_threads_workinggroupexit(thrs, thrsubmgr->childthrrefs);

    /* 管理线程退出 */
    ripple_threads_thrnodeexit(thrnode);

    /* 设置子线程为 FREE, 设置管理节点为 FREE */
    ripple_threads_setworkinggroupfree(thrs, thrsubmgr->childthrrefs);
    thrnode->stat = RIPPLE_THRNODE_STAT_FREE;
    return;
}

/* 管理节点退出 */
static void ripple_threads_tmgrexit(ripple_threads* thrs, ripple_thrsubmgr* thrsubmgr, bool holdlock)
{
    int iret = 0;
    /* 管理线程节点 */
    ripple_thrnode* thrnode = NULL;

    /* 获取管理线程 thrnode */
    thrnode = dlist_get(thrs->thrnodes, &thrsubmgr->submgrref.no, ripple_threads_thrnodecmp);

    /* 加入到链表中未启动 */
    if (RIPPLE_THRNODE_STAT_INIT == thrnode->stat)
    {
        thrnode->stat = RIPPLE_THRNODE_STAT_FREE;
    }

    if(RIPPLE_THRNODE_STAT_FREE == thrnode->stat)
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
    if(RIPPLE_THRNODE_STAT_WORK > thrnode->stat)
    {
        /* 先解锁 */
        if (true == holdlock)
        {
            ripple_thread_unlock(&thrs->lock);
        }
        while(1)
        {
            /* 等待大于等于 WORK */
            if(RIPPLE_THRNODE_STAT_WORK <= thrnode->stat)
            {
                /* 管理线程启动成功或异常退出 */
                break;
            }

            /* 检测线程是否异常 */
            /* 查看线程是否在运行, 未运行那么回收 */
            iret = ripple_thread_tryjoin_np(thrnode->thrid, NULL);
            if(EBUSY != iret && EINTR != iret)
            {
                /* 线程已经异常退出 */
                thrnode->stat = RIPPLE_THRNODE_STAT_EXITED;
                break;
            }
            usleep(10000);
            continue;
        }

        /* 重新获取锁 */
        if (true == holdlock)
        {
            ripple_thread_lock(&thrs->lock);
        }
    }

    /* 子线程先退出
     * 考虑到停止过程中， 管理线程异常退出，那么也需要将工作线程回收掉
     */
    ripple_threads_workinggroupexit(thrs, thrsubmgr->childthrrefs);

    /* 设置子线程的状态为 FREE */
    ripple_threads_setworkinggroupfree(thrs, thrsubmgr->childthrrefs);

    /* abort */
    if(RIPPLE_THRNODE_STAT_ABORT == thrnode->stat)
    {
        /* 为 abort 说明子线程退出了且没有回收 */
        ripple_threads_tmgrabort(thrs, thrsubmgr);
        return;
    }
    else if(RIPPLE_THRNODE_STAT_EXITED != thrnode->stat)
    {
        if(RIPPLE_THRNODE_STAT_TERM > thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_TERM;
        }
        while(1)
        {
            /* 设置线程状态为 TERM */
            iret = ripple_thread_tryjoin_np(thrnode->thrid, NULL);
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
    elog(RLOG_INFO, "%s thread exit", m_threadname[thrnode->identity].name);
    thrnode->stat = RIPPLE_THRNODE_STAT_FREE;
    return;
}

/* 异常退出, 线程退出时设置了 ABORT */
static void ripple_threads_abort(ripple_threads* thrs, ripple_thrnode* thrnode)
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
     *  回收资源并设置状态为 RIPPLE_THRNODE_STAT_EXITED
     */
    dlistnode* dlnode = NULL;
    ripple_thrsubmgr* thrsubmgr = NULL;
    ripple_thrpersist* thrpersist = NULL;
    if(NULL == thrs || NULL == thrnode)
    {
        return;
    }

    /* 所有的线程都得退出 */
    if(RIPPLE_THRNODE_TYPE_PERSIST == thrnode->type)
    {
        /* 设置所有的mgr线程退出 */
        if(false == dlist_isnull(thrs->thrsubmgrs))
        {
            for(dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
            {
                thrsubmgr = (ripple_thrsubmgr*)dlnode->value;
                ripple_threads_tmgrexit(thrs, thrsubmgr, true);
            }
        }

        /* 设置所有的 persist 退出 */
        for(dlnode = thrs->thrpersist->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thrpersist = (ripple_thrpersist*)dlnode->value;

            /* 常驻线程退出 */
            ripple_threads_workinggroupexit(thrs, thrpersist->thrrefs);
            ripple_threads_setworkinggroupfree(thrs, thrpersist->thrrefs);
        }
    }
    else if(RIPPLE_THRNODE_TYPE_TMGR == thrnode->type)
    {
        /* 设置管理下程下的工作线程退出 */
        thrsubmgr = dlist_get(thrs->thrsubmgrs, &thrnode->no, ripple_threads_thrsubmgrcmp);
        ripple_threads_tmgrabort(thrs, thrsubmgr);
    }
    else if(RIPPLE_THRNODE_TYPE_TJOB == thrnode->type)
    {
        /* 工作线程退出 */
        ripple_threads_thrnodeexit(thrnode);
    }
    return;
}

/*
 * 线程异常退出,出现的情况可能如下:
 *  1、已经设置了 abort 或 exit, 但是因程序执行上下文的关系,导致先捕获到了线程的退出
 *  2、逻辑错误, 忘记设置 abort 或 exit, 出现此种情况最好排查一下逻辑
*/
static void ripple_threads_exitimpolite(ripple_threads* thrs, ripple_thrnode* thrnode)
{
    if(NULL == thrs || NULL == thrnode)
    {
        return;
    }

    /* 设置为已经回收 */
    thrnode->stat = RIPPLE_THRNODE_STAT_EXITED;
    elog(RLOG_INFO, "%lu::%s thread exit", thrnode->thrid, m_threadname[thrnode->identity].name);
    ripple_threads_abort(thrs, thrnode);
}


/* 线程退出,接收到 sigterm 执行此函数 */
bool ripple_threads_exit(ripple_threads* thrs)
{
    /* 查找管理节点 */
    dlistnode* dlnode = NULL;
    ripple_thrsubmgr* thrsubmgr = NULL;
    ripple_thrpersist* thrpersist = NULL;

    /* step1 遍历管理线程节点, 让所有的子节点退出 */
    if(false == dlist_isnull(thrs->thrsubmgrs))
    {
        for(dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thrsubmgr = (ripple_thrsubmgr*)dlnode->value;
            ripple_threads_tmgrexit(thrs, thrsubmgr, false);
        }
    }

    /* step2 常驻线程退出 */
    for(dlnode = thrs->thrpersist->head; NULL != dlnode; dlnode = dlnode->next)
    {
        thrpersist = (ripple_thrpersist*)dlnode->value;

        /* 常驻线程退出 */
        ripple_threads_workinggroupexit(thrs, thrpersist->thrrefs);
    }

    /* step3 遍历管理线程节点, 让所有的子节点退出 */
    if(false == dlist_isnull(thrs->thrsubmgrs))
    {
        for(dlnode = thrs->thrsubmgrs->head; NULL != dlnode; dlnode = dlnode->next)
        {
            thrsubmgr = (ripple_thrsubmgr*)dlnode->value;
            ripple_threads_tmgrexit(thrs, thrsubmgr, false);
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
bool ripple_threads_tryjoin(ripple_threads* thrs)
{
    int iret = 0;
    dlistnode* dlnode = NULL;
    ripple_thrnode* thrnode = NULL;
    ripple_thread_lock(&thrs->lock);

    /* 遍历子线程， 查看子线程是否异常退出 */
    for(dlnode = thrs->thrnodes->head; NULL != dlnode;)
    {
        thrnode = (ripple_thrnode*)dlnode->value;
        if(RIPPLE_THRNODE_STAT_EXITED == thrnode->stat)
        {
            /* 已经被回收过,那么不做处理 */
            dlnode = dlnode->next;
            continue;
        }
        else if(RIPPLE_THRNODE_STAT_FREE == thrnode->stat)
        {
            /* 不做处理, 等待后续流程回收掉该节点 */
            dlnode = dlnode->next;
            continue;
        }
        else if(RIPPLE_THRNODE_STAT_ABORT == thrnode->stat)
        {
            /* 异常退出了,那么根据线程的状态做处理 */
            ripple_threads_abort(thrs, thrnode);
            elog(RLOG_INFO, "%s thread about exit", m_threadname[thrnode->identity].name);
            dlnode = dlnode->next;
            continue;
        }
        else if(RIPPLE_THRNODE_STAT_EXIT == thrnode->stat)
        {
            /* 回收 */
            ripple_thread_join(thrnode->thrid , NULL);
            thrnode->stat = RIPPLE_THRNODE_STAT_EXITED;
            elog(RLOG_INFO, "%s thread exit", m_threadname[thrnode->identity].name);
            dlnode = dlnode->next;

            /* 子管理线程因为没有办法设置自己为 FREE, 所以在此处判断线程的类型是否为子管理线程 */
            if(RIPPLE_THRNODE_TYPE_TMGR == thrnode->type)
            {
                thrnode->stat = RIPPLE_THRNODE_STAT_FREE;
            }
            continue;
        }

        /* 证明还没有启动, 不做处理 */
        if(RIPPLE_INVALIDTHRID == thrnode->thrid)
        {
            dlnode = dlnode->next;
            continue;
        }

        /* 尝试回收 */
        iret = ripple_thread_tryjoin_np(thrnode->thrid, NULL);
        if(EBUSY == iret || EINTR == iret)
        {
            /* 正常运行 */
            dlnode = dlnode->next;
            continue;
        }

        /* 根据线程类型, 执行不同的退出逻辑 */
        ripple_threads_exitimpolite(thrs, thrnode);
        dlnode = dlnode->next;
    }

    ripple_thread_unlock(&thrs->lock);
    return true;
}

/*
 * 启动子线程
*/
void ripple_threads_startthread(ripple_threads* thrs)
{
    dlistnode* dlnode = NULL;
    ripple_thrnode* thrnode = NULL;
    ripple_thread_lock(&thrs->lock);
    if(true == dlist_isnull(thrs->thrnodes))
    {
        ripple_thread_unlock(&thrs->lock);
        return;
    }

    /* 遍历启动 */
    for(dlnode = thrs->thrnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        thrnode = (ripple_thrnode*)dlnode->value;
        if(RIPPLE_THRNODE_STAT_INIT != thrnode->stat)
        {
            continue;
        }

        thrnode->stat = RIPPLE_THRNODE_STAT_STARTING;
        /* 创建工作线程 */
        ripple_thread_create(&thrnode->thrid, NULL, thrnode->tmain, thrnode);

        elog(RLOG_DEBUG,"start thrid:%lu, threadname:%s",thrnode->thrid, m_threadname[thrnode->identity].name);

        /* 设置线程名称 */
        ripple_thread_setname_np(thrnode->thrid, m_threadname[thrnode->identity].name);
    }
    ripple_thread_unlock(&thrs->lock);
}

/* 回收节点 */
void ripple_threads_thrnoderecycle(ripple_threads* thrs)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodenext = NULL;
    ripple_thrnode* thrnode = NULL;

    ripple_thread_lock(&thrs->lock);
    if(true == dlist_isnull(thrs->thrnodes))
    {
        ripple_thread_unlock(&thrs->lock);
        return;
    }

    for(dlnode = thrs->thrnodes->head; NULL != dlnode; )
    {
        thrnode = (ripple_thrnode*)dlnode->value;
        if(RIPPLE_THRNODE_STAT_FREE != thrnode->stat)
        {
            dlnode = dlnode->next;
            continue;
        }

        /* 子管理节点, 需要将子管理节点释放掉 */
        if(RIPPLE_THRNODE_TYPE_TMGR == thrnode->type)
        {
            /* 清理掉管理节点 */
            thrs->thrsubmgrs = dlist_deletebyvalue(thrs->thrsubmgrs,
                                                    &thrnode->no,
                                                    ripple_threads_thrsubmgrcmp,
                                                    ripple_thrsubmgr_free);
        }

        /* 回收节点 */
        dlnodenext = dlnode->next;
        thrs->thrnodes = dlist_delete(thrs->thrnodes, dlnode, ripple_thrnode_free);
        dlnode = dlnodenext;
    }

    ripple_thread_unlock(&thrs->lock);
}

/* 
 * 是否含有子线程
 *  false       不含有子线程
 *  true        含有子线程
*/
bool ripple_threads_hasthrnode(ripple_threads* thrs)
{
    ripple_thread_lock(&thrs->lock);
    if(true == dlist_isnull(thrs->thrnodes))
    {
        ripple_thread_unlock(&thrs->lock);
        return false;
    }
    ripple_thread_unlock(&thrs->lock);
    return true;
}

/* 内存回收 */
void ripple_threads_free(ripple_threads* thrs)
{
    if(NULL == thrs)
    {
        return;
    }

    dlist_free(thrs->thrnodes, ripple_thrnode_free);

    dlist_free(thrs->thrpersist, ripple_thrpersist_free);

    dlist_free(thrs->thrsubmgrs, ripple_thrsubmgr_free);

    ripple_thread_mutex_destroy(&thrs->lock);

    rfree(thrs);
}
