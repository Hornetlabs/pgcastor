#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "threads/threads.h"
#include "rebuild/rebuild.h"
#include "sync/sync.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "parser/trail/parsertrail.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "refresh/integrate/refresh_integrate.h"
#include "metric/integrate/metric_integrate.h"
#include "onlinerefresh/onlinerefresh_persist.h"
#include "onlinerefresh/integrate/onlinerefresh_integrate.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratefilterdataset.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/integrate/bigtxn_integratemanager.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"
#include "increment/integrate/split/increment_integratesplittrail.h"
#include "increment/integrate/sync/increment_integratesync.h"
#include "increment/integrate/rebuild/increment_integraterebuild.h"
#include "increment/integrate/increment_integrate.h"

/* integrate端 解析线程添加refresh */
static void increment_integrate_addrefresh(void* privdata, void* refresh)
{
    int iret = 0;
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate refresh add exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    iret = osal_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    if (NULL == incintegrate->refresh
        || 0 == incintegrate->refresh->length)
    {
        incintegrate->refresh = lappend(incintegrate->refresh, refresh);
    }
    
    osal_thread_unlock(&incintegrate->refreshlock);

    return;
}

/* integrate端 refresh是否结束 sync继续执行 */
static bool increment_integrate_isrefreshdown(void* privdata)
{
    int iret = 0;
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate refresh isdown exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    iret = osal_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    if (NULL == incintegrate->refresh
        || 0 == incintegrate->refresh->length)
    {
        osal_thread_unlock(&incintegrate->refreshlock);
        return true;
    }

    osal_thread_unlock(&incintegrate->refreshlock);
    return false;
}

/* integrate端 设置metric重组后事务的 lsn */
static void increment_integrate_loadlsn_set(void* privdata, XLogRecPtr loadlsn)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric loadlsn set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric loadlsn set exception, incintegrate point is NULL");
    }

    if(InvalidXLogRecPtr != loadlsn && MAX_LSN > loadlsn)
    {
        incintegrate->integratestate->loadlsn = loadlsn;
    }

    return;
}

/* integrate端 设置metric加载 trail 文件编号 */
static void increment_integrate_loadtrailno_set(void* privdata, uint64 loadtrailno)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric loadtrailno set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric loadtrailno set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->loadtrailno = loadtrailno;

    return;
}

/* integrate端 设置metric加载 trail 文件内的偏移 */
static void increment_integrate_loadtrailstart_set(void* privdata, uint64 loadtrailstart)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric loadtrailstart set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric loadtrailstart set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->loadtrailstart = loadtrailstart;

    return;
}

/* integrate端 设置metric重组后事务 timestamp */
static void increment_integrate_loadtimestamp_set(void* privdata, TimestampTz loadtimestamp)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric loadtimestamp set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric loadtimestamp set exception, incintegrate point is NULL");
    }

    if (0 != loadtimestamp)
    {
        incintegrate->integratestate->loadtimestamp = loadtimestamp;
    }

    return;
}

/* integrate端 设置metric同步到库中的 lsn */
static void increment_integrate_synclsn_set(void* privdata, XLogRecPtr synclsn)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric synclsn set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric synclsn set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->synclsn = synclsn;

    return;
}

/* integrate端 设置metric已入库的 trail 文件编号 */
static void increment_integrate_synctrailno_set(void* privdata, uint64 synctrailno)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric synctrailno set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric synctrailno set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->synctrailno = synctrailno;

    return;
}

/* integrate端 设置metric已入库的 trail 文件内的偏移 */
static void increment_integrate_synctrailstart_set(void* privdata, uint64 synctrailstart)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric synctrailstart set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric synctrailstart set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->synctrailstart = synctrailstart;

    return;
}

/* integrate端 设置metric已入库的 timestamp */
static void increment_integrate_synctimestamp_set(void* privdata, TimestampTz synctimestamp)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric synctimestamp set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric synctrailstart set exception, incintegrate point is NULL");
    }

    if (0 != synctimestamp)
    {
        incintegrate->integratestate->synctimestamp = synctimestamp;
    }

    return;
}

/* integrate端 设置添加大事务节点 */
static void increment_integrate_addbigtxnnode(void* privdata, void* bigtxn)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate bigtxnnode add exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    osal_thread_lock(&incintegrate->bigtxnlock);

    incintegrate->bigtxnmgr = dlist_put(incintegrate->bigtxnmgr, bigtxn);

    osal_thread_unlock(&incintegrate->bigtxnlock);

    return;
}

/* integrate端 检查大事务是否结束，并设置节点为free */
static bool increment_integrate_isbigtxndone(void* privdata, FullTransactionId xid)
{
    int iret = 0;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    bigtxn_integratemanager* bigtxnmgr= NULL;
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate isbigtxndone exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->bigtxnmgr)
    {
        elog(RLOG_ERROR, "integrate isbigtxndone exception, incintegrate point is NULL");
    }

    iret = osal_thread_lock(&incintegrate->bigtxnlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    for(dlnode = incintegrate->bigtxnmgr->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;

        bigtxnmgr = (bigtxn_integratemanager*)dlnode->value;
        if (xid == bigtxnmgr->xid && BIGTXN_INTEGRATEMANAGER_STAT_EXIT == bigtxnmgr->stat)
        {
            bigtxn_integratemanager_stat_set(bigtxnmgr, BIGTXN_INTEGRATEMANAGER_STAT_FREE);
            osal_thread_unlock(&incintegrate->bigtxnlock);
            return true;
        }
    }
    osal_thread_unlock(&incintegrate->bigtxnlock);

    return false;
}


/* integrate端 检查大事务是否接收到退出信号 */
static bool increment_integrate_isbigtxnsigterm(void* privdata, FullTransactionId xid)
{
    int iret = 0;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    bigtxn_integratemanager* bigtxnmgr= NULL;
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate isbigtxnsigterm, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->bigtxnmgr)
    {
        elog(RLOG_ERROR, "integrate isbigtxnsigterm exception, incintegrate point is NULL");
    }

    iret = osal_thread_lock(&incintegrate->bigtxnlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    for(dlnode = incintegrate->bigtxnmgr->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;

        bigtxnmgr = (bigtxn_integratemanager*)dlnode->value;
        if (xid == bigtxnmgr->xid && BIGTXN_INTEGRATEMANAGER_STAT_SIGTERM == bigtxnmgr->stat)
        {
            osal_thread_unlock(&incintegrate->bigtxnlock);
            return true;
        }
    }
    osal_thread_unlock(&incintegrate->bigtxnlock);

    return false;
}

/* syncwork设置 splittrail的fileid和工作状态 */
static void increment_integrate_splittrail_fileid_emitoffset_set(void* privdata, uint64 fileid, uint64 emitoffset)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate splittrail fileid exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->splittrailctx)
    {
        elog(RLOG_ERROR, "integrate splittrail fileid exception, splittrailctx point is NULL");
    }

    if (INTEGRATE_STATUS_SPLIT_WAITSET != incintegrate->splittrailctx->state)
    {
        return;
    }

    /* 重置读取的起点和应用的起点 */
    increment_integratesplittrail_emit_set(incintegrate->splittrailctx, fileid, emitoffset);
    incintegrate->splittrailctx->filter = true;
    increment_integratesplittrail_state_set(incintegrate->splittrailctx, INTEGRATE_STATUS_SPLIT_WORKING);
    elog(RLOG_INFO, "splittrail_fileid_set, trailno:%lu, emitoffset:%lu",
                                                         fileid,
                                                         emitoffset);
    return;
}

/* 添加onlinerefresh节点 */
static void increment_integrate_addonlinerefresh(void* privdata, void* onlinerefresh)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate addonlinerefresh exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    osal_thread_lock(&incintegrate->onlinerefreshlock);

    incintegrate->onlinerefresh = dlist_put(incintegrate->onlinerefresh, onlinerefresh);

    osal_thread_unlock(&incintegrate->onlinerefreshlock);
}

/* 检查onlinerefresh是否退出 */
static bool increment_integrate_isonlinerefreshdone(void* privdata, void* no)
{
    bool result = false;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    increment_integrate* incintegrate = NULL;
    onlinerefresh_integrate* onlinerefresh = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate sync isonlinerefreshdone fileid exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    osal_thread_lock(&incintegrate->onlinerefreshlock);

    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        onlinerefresh = (onlinerefresh_integrate*)dlnode->value;
        if ((0 == memcmp(onlinerefresh->no.data, no, UUID_LEN)) 
            && (onlinerefresh->stat >= ONLINEREFRESH_INTEGRATE_DONE))
        {
           result = true;
           break;
        }
    }

    osal_thread_unlock(&incintegrate->onlinerefreshlock);

    return result;
}

/* 设置onlinerefresh为free状态 */
static void increment_integrate_setonlinerefreshfree(void* privdata, void* no)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    increment_integrate* incintegrate = NULL;
    onlinerefresh_integrate* onlinerefresh = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate sync setonlinerefreshfree exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    osal_thread_lock(&incintegrate->onlinerefreshlock);

    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        onlinerefresh = (onlinerefresh_integrate*)dlnode->value;
        if (0 == memcmp(onlinerefresh->no.data, no, UUID_LEN))
        {
            onlinerefresh->stat = ONLINEREFRESH_INTEGRATE_FREE;
            break;
        }
    }

    osal_thread_unlock(&incintegrate->onlinerefreshlock);
}

/* 检测onlinerefresh的refresh是否结束 */
static bool increment_integrate_isonlinerefresh_refreshdone(void* privdata, void* no)
{
    bool result = false;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    increment_integrate* incintegrate = NULL;
    onlinerefresh_integrate* onlinerefresh = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate sync isonlinerefreshdone fileid exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    osal_thread_lock(&incintegrate->onlinerefreshlock);

    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        onlinerefresh = (onlinerefresh_integrate*)dlnode->value;
        if ((0 == memcmp(onlinerefresh->no.data, no, UUID_LEN)) 
            && (onlinerefresh->stat > ONLINEREFRESH_INTEGRATE_RUNNING))
        {
           result = true;
           break;
        }
    }

    osal_thread_unlock(&incintegrate->onlinerefreshlock);

    return result;
}

/* integrate端 事务重组线程lsn及状态 */
static void integrate_rebuildfilter_set(void* privdata, XLogRecPtr lsn)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate rebuildfilter lsn set exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->rebuild)
    {
        elog(RLOG_ERROR, "integrate rebuildfilter lsn set exception, rebuild point is NULL");
    }

    incintegrate->rebuild->filterlsn = lsn;
    incintegrate->rebuild->stat = INCREMENT_INTEGRATEREBUILD_STAT_READY;
    return;
}

/* integrate端 获取sync线程状态 */
static bool integrate_issyncidle(void* privdata)
{
    increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate syncstate get exception, privdata point is NULL");
    }

    incintegrate = (increment_integrate*)privdata;

    if (NULL == incintegrate->syncworkstate)
    {
        elog(RLOG_ERROR, "integrate syncstate get exception, rebuild point is NULL");
    }

    if (INCREMENT_INTEGRATESYNC_STATE_IDLE == incintegrate->syncworkstate->state)
    {
        return true;
    }

    return false;
}

increment_integrate* increment_integrate_init(void)
{
    increment_integrate* incintegrate = NULL;
    incintegrate = (increment_integrate*)rmalloc1(sizeof(increment_integrate));
    if (NULL == incintegrate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(incintegrate, 0, '\0', sizeof(increment_integrate));

    incintegrate->splittrailctx = increment_integratesplittrail_init();
    incintegrate->recordscache = queue_init();
    incintegrate->decodingctx = increment_integrateparsertrail_init();
    incintegrate->parser2rebuild = cache_txn_init();
    incintegrate->rebuild2sync = cache_txn_init();
    incintegrate->syncworkstate = increment_integratesync_init();
    incintegrate->integratestate = metric_integrate_init();
    incintegrate->rebuild = increment_integraterebuild_init();
    incintegrate->threads = threads_init();
    osal_thread_mutex_init(&incintegrate->onlinerefreshlock, NULL);
    osal_thread_mutex_init(&incintegrate->bigtxnlock, NULL);
    osal_thread_mutex_init(&incintegrate->refreshlock, NULL);

    /*------------------loadrecords 初始化 begin-----------------------*/
    incintegrate->splittrailctx->privdata = (void*)incintegrate;
    incintegrate->splittrailctx->recordscache = incintegrate->recordscache;
    incintegrate->splittrailctx->callback.setmetricloadtrailno = increment_integrate_loadtrailno_set;
    incintegrate->splittrailctx->callback.setmetricloadtrailstart = increment_integrate_loadtrailstart_set;

    /*------------------loadrecords 初始化   end-----------------------*/

    /*------------------parser 初始化 begin----------------------------*/
    incintegrate->decodingctx->privdata = (void*)incintegrate;
    incintegrate->decodingctx->recordscache = incintegrate->recordscache;
    incintegrate->decodingctx->parsertrail.parser2txn = incintegrate->parser2rebuild;
    incintegrate->decodingctx->callback.integratestate_addrefresh = increment_integrate_addrefresh;
    incintegrate->decodingctx->callback.integratestate_isrefreshdown = increment_integrate_isrefreshdown;
    incintegrate->decodingctx->callback.setmetricloadlsn = increment_integrate_loadlsn_set;
    incintegrate->decodingctx->callback.setmetricloadtimestamp = increment_integrate_loadtimestamp_set;

    /*------------------parser 初始化   end----------------------------*/

    /*------------------rebuild 初始化 begin---------------------------*/
    incintegrate->rebuild->privdata = (void*)incintegrate;
    incintegrate->rebuild->parser2rebuild = incintegrate->parser2rebuild;
    incintegrate->rebuild->rebuild2sync = incintegrate->rebuild2sync;
    incintegrate->rebuild->callback.isonlinerefreshdone = increment_integrate_isonlinerefreshdone;
    incintegrate->rebuild->callback.isolrrefreshdone = increment_integrate_isonlinerefresh_refreshdone;
    incintegrate->rebuild->callback.setonlinerefreshfree = increment_integrate_setonlinerefreshfree;
    incintegrate->rebuild->callback.isrefreshdown = increment_integrate_isrefreshdown;
    incintegrate->rebuild->callback.isbigtxndown = increment_integrate_isbigtxndone;
    incintegrate->rebuild->callback.isbigtxnsigterm = increment_integrate_isbigtxnsigterm;
    incintegrate->rebuild->callback.addonlinerefresh = increment_integrate_addonlinerefresh;
    incintegrate->rebuild->callback.issyncidle = integrate_issyncidle;
    incintegrate->rebuild->callback.addbigtxn = increment_integrate_addbigtxnnode;

    /*------------------rebuild 初始化   end---------------------------*/

    /*------------------sync 初始化 begin------------------------------*/
    incintegrate->syncworkstate->privdata = (void*)incintegrate;
    incintegrate->syncworkstate->rebuild2sync = incintegrate->rebuild2sync;
    incintegrate->syncworkstate->callback.splittrail_fileid_emitoffse_set = increment_integrate_splittrail_fileid_emitoffset_set;
    incintegrate->syncworkstate->callback.setmetricsynclsn = increment_integrate_synclsn_set;
    incintegrate->syncworkstate->callback.setmetricsynctrailno = increment_integrate_synctrailno_set;
    incintegrate->syncworkstate->callback.setmetricsynctrailstart = increment_integrate_synctrailstart_set;
    incintegrate->syncworkstate->callback.setmetricsynctimestamp = increment_integrate_synctimestamp_set;
    incintegrate->syncworkstate->callback.integratestate_isrefreshdown = increment_integrate_isrefreshdown;
    incintegrate->syncworkstate->callback.integratestate_rebuildfilter_set = integrate_rebuildfilter_set;

    /*------------------sync 初始化   end------------------------------*/
    return incintegrate;

}

/*------------refresh 管理 begin-------------------------*/
/* 启动 refresh */
bool increment_integrate_startrefresh(increment_integrate* incintegrate)
{
    int iret = 0;
    ListCell* lc = NULL;
    refresh_integrate* rintegrate = NULL;

    iret = osal_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    /* 遍历 refresh 节点并启动 */
    foreach(lc, incintegrate->refresh)
    {
        rintegrate = (refresh_integrate*)lfirst(lc);
        if (REFRESH_INTEGRATE_STAT_INIT != rintegrate->stat)
        {
            continue;
        }

        /* 启动 refresh */
        /* 设置为启动中 */
        rintegrate->stat = REFRESH_INTEGRATE_STAT_STARTING;
        /* 注册 refresh 管理线程 */
        if(false == threads_addsubmanger(incintegrate->threads,
                                                THRNODE_IDENTITY_INTEGRATE_REFRESH_MGR,
                                                incintegrate->persistno,
                                                &rintegrate->thrsmgr,
                                                (void*)rintegrate,
                                                NULL,
                                                NULL,
                                                refresh_integrate_main))
        {
            elog(RLOG_WARNING, "integrate start refresh mgr failed");
            return false;
        }
    }

    osal_thread_unlock(&incintegrate->refreshlock);
    return true;
}

/* 回收 refresh 节点 */
bool increment_integrate_tryjoinonrefresh(increment_integrate* incintegrate)
{
    int iret = 0;
    List* nl = NULL;
    ListCell* lc = NULL;
    refresh_integrate* rintegrate = NULL;

    iret = osal_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    foreach(lc, incintegrate->refresh)
    {
        rintegrate = (refresh_integrate*)lfirst(lc);
        if (REFRESH_INTEGRATE_STAT_DONE != rintegrate->stat)
        {
            nl = lappend(nl, rintegrate);
            continue;
        }

        /* 
         * refresh 已经做完了
         *  1、资源释放
         */
        refresh_integrate_free(rintegrate);
    }

    list_free(incintegrate->refresh);
    incintegrate->refresh = nl;
    osal_thread_unlock(&incintegrate->refreshlock);
    return true;
}

/* 从状态文件加载 refresh 节点 */
bool increment_integrate_refreshload(increment_integrate* incintegrate)
{
    refresh_integrate *refresh = NULL;

    /* 读取状态文件，生成refresh任务 */
    if (false == refresh_integrate_read(&refresh))
    {
        return false;
    }
    
    /* 添加refresh任务 */
    if (NULL != refresh)
    {
        increment_integrate_addrefresh((void*)incintegrate, (void*)refresh);
    }

    return true;
}

/* 状态文件落盘 */
void increment_integrate_refreshflush(increment_integrate* incintegrate)
{
    int iret = 0;
    ListCell* lc = NULL;
    refresh_integrate* rintegrate = NULL;

    if (NULL == incintegrate)
    {
        return;
    }

    iret = osal_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return;
    }

    /* 遍历 refresh 节点并落盘*/
    foreach(lc, incintegrate->refresh)
    {
        rintegrate = (refresh_integrate*)lfirst(lc);
        refresh_integrate_write(rintegrate);
    }

    osal_thread_unlock(&incintegrate->refreshlock);

    return;
}

/*------------refresh 管理   end-------------------------*/

/*------------onlinerefresh 管理 begin-------------------*/
/* 启动 onlinerefresh 管理线程 */
bool increment_integrate_startonlinerefresh(increment_integrate* incintegrate)
{
    dlistnode* dlnode                               = NULL;
    onlinerefresh_integrate* olrintegrate    = NULL;

    if(true == dlist_isnull(incintegrate->onlinerefresh))
    {
        return true;
    }

    osal_thread_lock(&incintegrate->onlinerefreshlock);

    /* 遍历 onlinerefresh 节点并启动 */
    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnode->next)
    {
        olrintegrate = (onlinerefresh_integrate*)dlnode->value;

        if (ONLINEREFRESH_INTEGRATE_INIT != olrintegrate->stat)
        {
            continue;
        }

        if(true == onlinerefresh_integrate_isconflict(dlnode))
        {
            break;
        }

        olrintegrate->stat = ONLINEREFRESH_INTEGRATE_STARTING;

        /* 注册 onlinerefresh 管理线程 */
        if(false == threads_addsubmanger(incintegrate->threads,
                                                THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_MGR,
                                                incintegrate->persistno,
                                                &olrintegrate->thrsmgr,
                                                (void*)olrintegrate,
                                                NULL,
                                                NULL,
                                                onlinerefresh_integrate_manage))
        {
            elog(RLOG_WARNING, "start onlinerefresh mgr failed");
            osal_thread_unlock(&incintegrate->onlinerefreshlock);
            return false;
        }
    }

    osal_thread_unlock(&incintegrate->onlinerefreshlock);
    return true;
}

/* 回收 onlinerefresh 节点 */
bool increment_integrate_tryjoinononlinerefresh(increment_integrate* incintegrate)
{
    dlistnode* dlnode                               = NULL;
    dlistnode* dlnodenext                           = NULL;
    onlinerefresh_integrate* olrintegrate    = NULL;

    if(true == dlist_isnull(incintegrate->onlinerefresh))
    {
        return true;
    }

    osal_thread_lock(&incintegrate->onlinerefreshlock);
    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        olrintegrate = (onlinerefresh_integrate*)dlnode->value;

        if (ONLINEREFRESH_INTEGRATE_FREE != olrintegrate->stat)
        {
            continue;
        }
        incintegrate->onlinerefresh = dlist_delete(incintegrate->onlinerefresh, dlnode, onlinerefresh_integrate_free);
    }

    osal_thread_unlock(&incintegrate->onlinerefreshlock);
    return true;
}

/* 从状态文件加载 onlinerefrerefresh 节点 */
bool increment_integrate_onlinerefreshload(increment_integrate* incintegrate)
{
    dlistnode* dlnode                                       = NULL;
    dlistnode* dlnodenext                                   = NULL;
    refresh_tables* refreshtbs                       = NULL;
    onlinerefresh_integrate* olrintegrate            = NULL;
    onlinerefresh_integratedatasetnode* datasetnode  = NULL;

    /* 加载onlinerefresh状态文件 生成 persist */
    incintegrate->rebuild->olpersist = onlinerefresh_persist_read();
    if (NULL == incintegrate->rebuild->olpersist)
    {
        elog(RLOG_WARNING, "read onlinerefresh persist error");
        return false;
    }

    osal_thread_lock(&incintegrate->onlinerefreshlock);
    /* 根据persists构建onlinerefresh管理线程节点 */
    if (false == onlinerefresh_integrate_persist2onlinerefreshmgr(incintegrate->rebuild->olpersist, (void**)&incintegrate->onlinerefresh))
    {
        osal_thread_unlock(&incintegrate->onlinerefreshlock);
        return false;
    }

    if(true == dlist_isnull(incintegrate->onlinerefresh))
    {
        osal_thread_unlock(&incintegrate->onlinerefreshlock);
        return true;
    }

    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        olrintegrate = (onlinerefresh_integrate*)dlnode->value;

        refreshtbs = refresh_table_syncstats_tablesyncing2tables(olrintegrate->tablesyncstats);

        /* 创建onlinerefresh过滤数据集 */
        onlinerefresh_integratefilterdataset_add(incintegrate->rebuild->honlinerefreshfilterdataset, refreshtbs, olrintegrate->txid);
        datasetnode = onlinerefresh_integratedatasetnode_init();
        onlinerefresh_integratedatasetnode_no_set(datasetnode, olrintegrate->no.data);
        onlinerefresh_integratedatasetnode_txid_set(datasetnode, olrintegrate->txid);
        onlinerefresh_integratedatasetnode_refreshtables_set(datasetnode, refreshtbs);
        onlinerefresh_integratedataset_add(incintegrate->rebuild->onlinerefreshdataset, datasetnode);
    }

    osal_thread_unlock(&incintegrate->onlinerefreshlock);

    refresh_freetables(refreshtbs);

    return true;
}

/*------------onlinerefresh 管理   end-------------------*/

/*------------bigtxn 管理 begin-------------------*/
/* 启动 bigtxn 管理线程 */
bool increment_integrate_startbigtxn(increment_integrate* incintegrate)
{
    dlistnode* dlnode = NULL;
    bigtxn_integratemanager* bigtxnintegrate = NULL;

    if(true == dlist_isnull(incintegrate->bigtxnmgr))
    {
        return true;
    }

    osal_thread_lock(&incintegrate->bigtxnlock);

    /* 遍历 refresh 节点并启动 */
    for(dlnode = incintegrate->bigtxnmgr->head; NULL != dlnode; dlnode = dlnode->next)
    {
        bigtxnintegrate = (bigtxn_integratemanager*)dlnode->value;
        if (BIGTXN_INTEGRATEMANAGER_STAT_INIT != bigtxnintegrate->stat)
        {
            continue;
        }
        bigtxnintegrate->stat = BIGTXN_INTEGRATEMANAGER_STAT_INPROCESS;

        /* 启动 bigtxn manager 线程 */
        if(false == threads_addsubmanger(incintegrate->threads,
                                                THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNMGR,
                                                incintegrate->persistno,
                                                &bigtxnintegrate->thrsmgr,
                                                (void*)bigtxnintegrate,
                                                NULL,
                                                NULL,
                                                bigtxn_integratemanager_main))
        {
            elog(RLOG_WARNING, "integrate start bigtxn manager failed");
            osal_thread_unlock(&incintegrate->bigtxnlock);
            return false;
        }
    }

    osal_thread_unlock(&incintegrate->bigtxnlock);
    return true;
}

/* 回收 bigtxn 节点 */
bool increment_integrate_tryjoinonbigtxn(increment_integrate* incintegrate)
{
    char path[MAXPGPATH]                    = {'\0'};
    dlistnode* dlnode                       = NULL;
    dlistnode* dlnodenext                   = NULL;
    bigtxn_integratemanager* bigtxnintegrate   = NULL;

    if(true == dlist_isnull(incintegrate->bigtxnmgr))
    {
        return true;
    }

    osal_thread_lock(&incintegrate->bigtxnlock);
    for(dlnode = incintegrate->bigtxnmgr->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        bigtxnintegrate = (bigtxn_integratemanager*)dlnode->value;

        if (BIGTXN_INTEGRATEMANAGER_STAT_FREE != bigtxnintegrate->stat)
        {
            continue;
        }

        /* 删除xid对应大事务文件夹 */
        rmemset1(path, 0, '\0', MAXPGPATH);
        snprintf(path, MAXPGPATH, "%s/%s/%lu", guc_getConfigOption(CFG_KEY_TRAIL_DIR), 
                                               STORAGE_BIG_TRANSACTION_DIR, 
                                               bigtxnintegrate->xid);
        osal_remove_dir(path);

        incintegrate->bigtxnmgr = dlist_delete(incintegrate->bigtxnmgr, dlnode, bigtxn_integratemanager_free);
    }

    osal_thread_unlock(&incintegrate->bigtxnlock);
    return true;
}

/*------------bigtxn 管理   end-------------------*/

void increment_integrate_destroy(increment_integrate* incintegrate)
{
    if (NULL == incintegrate)
    {
        return;
    }

    increment_integratesplittrail_free(incintegrate->splittrailctx);

    increment_integrateparsertrail_free(incintegrate->decodingctx);

    increment_integratesync_destroy(incintegrate->syncworkstate);

    increment_integraterebuild_free(incintegrate->rebuild);

    queue_destroy(incintegrate->recordscache, dlist_freevoid);

    cache_txn_destroy(incintegrate->parser2rebuild);

    cache_txn_destroy(incintegrate->rebuild2sync);

    osal_thread_mutex_destroy(&incintegrate->onlinerefreshlock);
    
    osal_thread_mutex_destroy(&incintegrate->bigtxnlock);

    osal_thread_mutex_destroy(&incintegrate->refreshlock);

    refresh_integrate_listfree(incintegrate->refresh);

    metric_integrate_destroy(incintegrate->integratestate);

    threads_free(incintegrate->threads);

    dlist_free(incintegrate->onlinerefresh, onlinerefresh_integrate_free);

    dlist_free(incintegrate->bigtxnmgr, bigtxn_integratemanager_free);

    rfree(incintegrate);
    incintegrate = NULL;
}
