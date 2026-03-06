#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "threads/ripple_threads.h"
#include "rebuild/ripple_rebuild.h"
#include "sync/ripple_sync.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "parser/trail/ripple_parsertrail.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/integrate/ripple_refresh_integrate.h"
#include "metric/integrate/ripple_metric_integrate.h"
#include "onlinerefresh/ripple_onlinerefresh_persist.h"
#include "onlinerefresh/integrate/ripple_onlinerefresh_integrate.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratefilterdataset.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/integrate/ripple_bigtxn_integratemanager.h"
#include "increment/integrate/parser/ripple_increment_integrateparsertrail.h"
#include "increment/integrate/split/ripple_increment_integratesplittrail.h"
#include "increment/integrate/sync/ripple_increment_integratesync.h"
#include "increment/integrate/rebuild/ripple_increment_integraterebuild.h"
#include "increment/integrate/ripple_increment_integrate.h"

/* integrate端 解析线程添加refresh */
static void ripple_increment_integrate_addrefresh(void* privdata, void* refresh)
{
    int iret = 0;
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate refresh add exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    iret = ripple_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    incintegrate->refresh = lappend(incintegrate->refresh, refresh);

    ripple_thread_unlock(&incintegrate->refreshlock);

    return;
}

/* integrate端 refresh是否结束 sync继续执行 */
static bool ripple_increment_integrate_isrefreshdown(void* privdata)
{
    int iret = 0;
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate refresh isdown exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    iret = ripple_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    if (NULL == incintegrate->refresh
        || 0 == incintegrate->refresh->length)
    {
        ripple_thread_unlock(&incintegrate->refreshlock);
        return true;
    }

    ripple_thread_unlock(&incintegrate->refreshlock);
    return false;
}

/* integrate端 设置metric重组后事务的 lsn */
static void ripple_increment_integrate_loadlsn_set(void* privdata, XLogRecPtr loadlsn)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric loadlsn set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric loadlsn set exception, incintegrate point is NULL");
    }

    if(InvalidXLogRecPtr != loadlsn && RIPPLE_MAX_LSN > loadlsn)
    {
        incintegrate->integratestate->loadlsn = loadlsn;
    }

    return;
}

/* integrate端 设置metric加载 trail 文件编号 */
static void ripple_increment_integrate_loadtrailno_set(void* privdata, uint64 loadtrailno)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric loadtrailno set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric loadtrailno set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->loadtrailno = loadtrailno;

    return;
}

/* integrate端 设置metric加载 trail 文件内的偏移 */
static void ripple_increment_integrate_loadtrailstart_set(void* privdata, uint64 loadtrailstart)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric loadtrailstart set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric loadtrailstart set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->loadtrailstart = loadtrailstart;

    return;
}

/* integrate端 设置metric重组后事务 timestamp */
static void ripple_increment_integrate_loadtimestamp_set(void* privdata, TimestampTz loadtimestamp)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric loadtimestamp set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

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
static void ripple_increment_integrate_synclsn_set(void* privdata, XLogRecPtr synclsn)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric synclsn set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric synclsn set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->synclsn = synclsn;

    return;
}

/* integrate端 设置metric已入库的 trail 文件编号 */
static void ripple_increment_integrate_synctrailno_set(void* privdata, uint64 synctrailno)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric synctrailno set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric synctrailno set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->synctrailno = synctrailno;

    return;
}

/* integrate端 设置metric已入库的 trail 文件内的偏移 */
static void ripple_increment_integrate_synctrailstart_set(void* privdata, uint64 synctrailstart)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric synctrailstart set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->integratestate)
    {
        elog(RLOG_ERROR, "integrate metric synctrailstart set exception, incintegrate point is NULL");
    }

    incintegrate->integratestate->synctrailstart = synctrailstart;

    return;
}

/* integrate端 设置metric已入库的 timestamp */
static void ripple_increment_integrate_synctimestamp_set(void* privdata, TimestampTz synctimestamp)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate metric synctimestamp set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

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
static void ripple_increment_integrate_addbigtxnnode(void* privdata, void* bigtxn)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate bigtxnnode add exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    ripple_thread_lock(&incintegrate->bigtxnlock);

    incintegrate->bigtxnmgr = dlist_put(incintegrate->bigtxnmgr, bigtxn);

    ripple_thread_unlock(&incintegrate->bigtxnlock);

    return;
}

/* integrate端 检查大事务是否结束，并设置节点为free */
static bool ripple_increment_integrate_isbigtxndone(void* privdata, FullTransactionId xid)
{
    int iret = 0;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_bigtxn_integratemanager* bigtxnmgr= NULL;
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate isbigtxndone exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->bigtxnmgr)
    {
        elog(RLOG_ERROR, "integrate isbigtxndone exception, incintegrate point is NULL");
    }

    iret = ripple_thread_lock(&incintegrate->bigtxnlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    for(dlnode = incintegrate->bigtxnmgr->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;

        bigtxnmgr = (ripple_bigtxn_integratemanager*)dlnode->value;
        if (xid == bigtxnmgr->xid && RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_EXIT == bigtxnmgr->stat)
        {
            ripple_bigtxn_integratemanager_stat_set(bigtxnmgr, RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_FREE);
            ripple_thread_unlock(&incintegrate->bigtxnlock);
            return true;
        }
    }
    ripple_thread_unlock(&incintegrate->bigtxnlock);

    return false;
}


/* integrate端 检查大事务是否接收到退出信号 */
static bool ripple_increment_integrate_isbigtxnsigterm(void* privdata, FullTransactionId xid)
{
    int iret = 0;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_bigtxn_integratemanager* bigtxnmgr= NULL;
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate isbigtxnsigterm, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->bigtxnmgr)
    {
        elog(RLOG_ERROR, "integrate isbigtxnsigterm exception, incintegrate point is NULL");
    }

    iret = ripple_thread_lock(&incintegrate->bigtxnlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    for(dlnode = incintegrate->bigtxnmgr->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;

        bigtxnmgr = (ripple_bigtxn_integratemanager*)dlnode->value;
        if (xid == bigtxnmgr->xid && RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_SIGTERM == bigtxnmgr->stat)
        {
            ripple_thread_unlock(&incintegrate->bigtxnlock);
            return true;
        }
    }
    ripple_thread_unlock(&incintegrate->bigtxnlock);

    return false;
}

/* syncwork设置 splittrail的fileid和工作状态 */
static void ripple_increment_integrate_splittrail_fileid_emitoffset_set(void* privdata, uint64 fileid, uint64 emitoffset)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate splittrail fileid exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->splittrailctx)
    {
        elog(RLOG_ERROR, "integrate splittrail fileid exception, splittrailctx point is NULL");
    }

    if (RIPPLE_INTEGRATE_STATUS_SPLIT_WAITSET != incintegrate->splittrailctx->state)
    {
        return;
    }

    /* 重置读取的起点和应用的起点 */
    ripple_increment_integratesplittrail_emit_set(incintegrate->splittrailctx, fileid, emitoffset);
    incintegrate->splittrailctx->filter = true;
    ripple_increment_integratesplittrail_state_set(incintegrate->splittrailctx, RIPPLE_INTEGRATE_STATUS_SPLIT_WORKING);
    elog(RLOG_INFO, "splittrail_fileid_set, trailno:%lu, emitoffset:%lu",
                                                         fileid,
                                                         emitoffset);
    return;
}

/* 添加onlinerefresh节点 */
static void ripple_increment_integrate_addonlinerefresh(void* privdata, void* onlinerefresh)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate addonlinerefresh exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    ripple_thread_lock(&incintegrate->onlinerefreshlock);

    incintegrate->onlinerefresh = dlist_put(incintegrate->onlinerefresh, onlinerefresh);

    ripple_thread_unlock(&incintegrate->onlinerefreshlock);
}

/* 检查onlinerefresh是否退出 */
static bool ripple_increment_integrate_isonlinerefreshdone(void* privdata, void* no)
{
    bool result = false;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_increment_integrate* incintegrate = NULL;
    ripple_onlinerefresh_integrate* onlinerefresh = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate sync isonlinerefreshdone fileid exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    ripple_thread_lock(&incintegrate->onlinerefreshlock);

    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        onlinerefresh = (ripple_onlinerefresh_integrate*)dlnode->value;
        if ((0 == memcmp(onlinerefresh->no.data, no, RIPPLE_UUID_LEN)) 
            && (onlinerefresh->stat >= RIPPLE_ONLINEREFRESH_INTEGRATE_DONE))
        {
           result = true;
           break;
        }
    }

    ripple_thread_unlock(&incintegrate->onlinerefreshlock);

    return result;
}

/* 设置onlinerefresh为free状态 */
static void ripple_increment_integrate_setonlinerefreshfree(void* privdata, void* no)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_increment_integrate* incintegrate = NULL;
    ripple_onlinerefresh_integrate* onlinerefresh = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate sync setonlinerefreshfree exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    ripple_thread_lock(&incintegrate->onlinerefreshlock);

    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        onlinerefresh = (ripple_onlinerefresh_integrate*)dlnode->value;
        if (0 == memcmp(onlinerefresh->no.data, no, RIPPLE_UUID_LEN))
        {
            onlinerefresh->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_FREE;
            break;
        }
    }

    ripple_thread_unlock(&incintegrate->onlinerefreshlock);
}

/* 检测onlinerefresh的refresh是否结束 */
static bool ripple_increment_integrate_isonlinerefresh_refreshdone(void* privdata, void* no)
{
    bool result = false;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_increment_integrate* incintegrate = NULL;
    ripple_onlinerefresh_integrate* onlinerefresh = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate sync isonlinerefreshdone fileid exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    ripple_thread_lock(&incintegrate->onlinerefreshlock);

    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        onlinerefresh = (ripple_onlinerefresh_integrate*)dlnode->value;
        if ((0 == memcmp(onlinerefresh->no.data, no, RIPPLE_UUID_LEN)) 
            && (onlinerefresh->stat > RIPPLE_ONLINEREFRESH_INTEGRATE_RUNNING))
        {
           result = true;
           break;
        }
    }

    ripple_thread_unlock(&incintegrate->onlinerefreshlock);

    return result;
}

/* integrate端 事务重组线程lsn及状态 */
static void ripple_integrate_rebuildfilter_set(void* privdata, XLogRecPtr lsn)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate rebuildfilter lsn set exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->rebuild)
    {
        elog(RLOG_ERROR, "integrate rebuildfilter lsn set exception, rebuild point is NULL");
    }

    incintegrate->rebuild->filterlsn = lsn;
    incintegrate->rebuild->stat = RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_READY;
    return;
}

/* integrate端 获取sync线程状态 */
static bool ripple_integrate_issyncidle(void* privdata)
{
    ripple_increment_integrate* incintegrate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "integrate syncstate get exception, privdata point is NULL");
    }

    incintegrate = (ripple_increment_integrate*)privdata;

    if (NULL == incintegrate->syncworkstate)
    {
        elog(RLOG_ERROR, "integrate syncstate get exception, rebuild point is NULL");
    }

    if (RIPPLE_INCREMENT_INTEGRATESYNC_STATE_IDLE == incintegrate->syncworkstate->state)
    {
        return true;
    }

    return false;
}

ripple_increment_integrate* ripple_increment_integrate_init(void)
{
    ripple_increment_integrate* incintegrate = NULL;
    incintegrate = (ripple_increment_integrate*)rmalloc1(sizeof(ripple_increment_integrate));
    if (NULL == incintegrate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(incintegrate, 0, '\0', sizeof(ripple_increment_integrate));

    incintegrate->splittrailctx = ripple_increment_integratesplittrail_init();
    incintegrate->recordscache = ripple_queue_init();
    incintegrate->decodingctx = ripple_increment_integrateparsertrail_init();
    incintegrate->parser2rebuild = ripple_cache_txn_init();
    incintegrate->rebuild2sync = ripple_cache_txn_init();
    incintegrate->syncworkstate = ripple_increment_integratesync_init();
    incintegrate->integratestate = ripple_metric_integrate_init();
    incintegrate->rebuild = ripple_increment_integraterebuild_init();
    incintegrate->threads = ripple_threads_init();
    ripple_thread_mutex_init(&incintegrate->onlinerefreshlock, NULL);
    ripple_thread_mutex_init(&incintegrate->bigtxnlock, NULL);
    ripple_thread_mutex_init(&incintegrate->refreshlock, NULL);

    /*------------------loadrecords 初始化 begin-----------------------*/
    incintegrate->splittrailctx->privdata = (void*)incintegrate;
    incintegrate->splittrailctx->recordscache = incintegrate->recordscache;
    incintegrate->splittrailctx->callback.setmetricloadtrailno = ripple_increment_integrate_loadtrailno_set;
    incintegrate->splittrailctx->callback.setmetricloadtrailstart = ripple_increment_integrate_loadtrailstart_set;

    /*------------------loadrecords 初始化   end-----------------------*/

    /*------------------parser 初始化 begin----------------------------*/
    incintegrate->decodingctx->privdata = (void*)incintegrate;
    incintegrate->decodingctx->recordscache = incintegrate->recordscache;
    incintegrate->decodingctx->parsertrail.parser2txn = incintegrate->parser2rebuild;
    incintegrate->decodingctx->callback.integratestate_addrefresh = ripple_increment_integrate_addrefresh;
    incintegrate->decodingctx->callback.integratestate_isrefreshdown = ripple_increment_integrate_isrefreshdown;
    incintegrate->decodingctx->callback.setmetricloadlsn = ripple_increment_integrate_loadlsn_set;
    incintegrate->decodingctx->callback.setmetricloadtimestamp = ripple_increment_integrate_loadtimestamp_set;

    /*------------------parser 初始化   end----------------------------*/

    /*------------------rebuild 初始化 begin---------------------------*/
    incintegrate->rebuild->privdata = (void*)incintegrate;
    incintegrate->rebuild->parser2rebuild = incintegrate->parser2rebuild;
    incintegrate->rebuild->rebuild2sync = incintegrate->rebuild2sync;
    incintegrate->rebuild->callback.isonlinerefreshdone = ripple_increment_integrate_isonlinerefreshdone;
    incintegrate->rebuild->callback.isolrrefreshdone = ripple_increment_integrate_isonlinerefresh_refreshdone;
    incintegrate->rebuild->callback.setonlinerefreshfree = ripple_increment_integrate_setonlinerefreshfree;
    incintegrate->rebuild->callback.isrefreshdown = ripple_increment_integrate_isrefreshdown;
    incintegrate->rebuild->callback.isbigtxndown = ripple_increment_integrate_isbigtxndone;
    incintegrate->rebuild->callback.isbigtxnsigterm = ripple_increment_integrate_isbigtxnsigterm;
    incintegrate->rebuild->callback.addonlinerefresh = ripple_increment_integrate_addonlinerefresh;
    incintegrate->rebuild->callback.issyncidle = ripple_integrate_issyncidle;
    incintegrate->rebuild->callback.addbigtxn = ripple_increment_integrate_addbigtxnnode;

    /*------------------rebuild 初始化   end---------------------------*/

    /*------------------sync 初始化 begin------------------------------*/
    incintegrate->syncworkstate->privdata = (void*)incintegrate;
    incintegrate->syncworkstate->rebuild2sync = incintegrate->rebuild2sync;
    incintegrate->syncworkstate->callback.splittrail_fileid_emitoffse_set = ripple_increment_integrate_splittrail_fileid_emitoffset_set;
    incintegrate->syncworkstate->callback.setmetricsynclsn = ripple_increment_integrate_synclsn_set;
    incintegrate->syncworkstate->callback.setmetricsynctrailno = ripple_increment_integrate_synctrailno_set;
    incintegrate->syncworkstate->callback.setmetricsynctrailstart = ripple_increment_integrate_synctrailstart_set;
    incintegrate->syncworkstate->callback.setmetricsynctimestamp = ripple_increment_integrate_synctimestamp_set;
    incintegrate->syncworkstate->callback.integratestate_isrefreshdown = ripple_increment_integrate_isrefreshdown;
    incintegrate->syncworkstate->callback.integratestate_rebuildfilter_set = ripple_integrate_rebuildfilter_set;

    /*------------------sync 初始化   end------------------------------*/
    return incintegrate;

}

/*------------refresh 管理 begin-------------------------*/
/* 启动 refresh */
bool ripple_increment_integrate_startrefresh(ripple_increment_integrate* incintegrate)
{
    int iret = 0;
    ListCell* lc = NULL;
    ripple_refresh_integrate* rintegrate = NULL;

    iret = ripple_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    /* 遍历 refresh 节点并启动 */
    foreach(lc, incintegrate->refresh)
    {
        rintegrate = (ripple_refresh_integrate*)lfirst(lc);
        if (RIPPLE_REFRESH_INTEGRATE_STAT_INIT != rintegrate->stat)
        {
            continue;
        }

        /* 启动 refresh */
        /* 设置为启动中 */
        rintegrate->stat = RIPPLE_REFRESH_INTEGRATE_STAT_STARTING;
        /* 注册 refresh 管理线程 */
        if(false == ripple_threads_addsubmanger(incintegrate->threads,
                                                RIPPLE_THRNODE_IDENTITY_INTEGRATE_REFRESH_MGR,
                                                incintegrate->persistno,
                                                &rintegrate->thrsmgr,
                                                (void*)rintegrate,
                                                NULL,
                                                NULL,
                                                ripple_refresh_integrate_main))
        {
            elog(RLOG_WARNING, "integrate start refresh mgr failed");
            return false;
        }
    }

    ripple_thread_unlock(&incintegrate->refreshlock);
    return true;
}

/* 回收 refresh 节点 */
bool ripple_increment_integrate_tryjoinonrefresh(ripple_increment_integrate* incintegrate)
{
    int iret = 0;
    List* nl = NULL;
    ListCell* lc = NULL;
    ripple_refresh_integrate* rintegrate = NULL;

    iret = ripple_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    foreach(lc, incintegrate->refresh)
    {
        rintegrate = (ripple_refresh_integrate*)lfirst(lc);
        if (RIPPLE_REFRESH_INTEGRATE_STAT_DONE != rintegrate->stat)
        {
            nl = lappend(nl, rintegrate);
            continue;
        }

        /* 
         * refresh 已经做完了
         *  1、资源释放
         */
        ripple_refresh_integrate_free(rintegrate);
    }

    list_free(incintegrate->refresh);
    incintegrate->refresh = nl;
    ripple_thread_unlock(&incintegrate->refreshlock);
    return true;
}

/* 从状态文件加载 refresh 节点 */
bool ripple_increment_integrate_refreshload(ripple_increment_integrate* incintegrate)
{
    ripple_refresh_integrate *refresh = NULL;

    /* 读取状态文件，生成refresh任务 */
    if (false == ripple_refresh_integrate_read(&refresh))
    {
        return false;
    }
    
    /* 添加refresh任务 */
    if (NULL != refresh)
    {
        ripple_increment_integrate_addrefresh((void*)incintegrate, (void*)refresh);
    }

    return true;
}

/* 状态文件落盘 */
void ripple_increment_integrate_refreshflush(ripple_increment_integrate* incintegrate)
{
    int iret = 0;
    ListCell* lc = NULL;
    ripple_refresh_integrate* rintegrate = NULL;

    if (NULL == incintegrate)
    {
        return;
    }

    iret = ripple_thread_lock(&incintegrate->refreshlock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return;
    }

    /* 遍历 refresh 节点并落盘*/
    foreach(lc, incintegrate->refresh)
    {
        rintegrate = (ripple_refresh_integrate*)lfirst(lc);
        ripple_refresh_integrate_write(rintegrate);
    }

    ripple_thread_unlock(&incintegrate->refreshlock);

    return;
}

/*------------refresh 管理   end-------------------------*/

/*------------onlinerefresh 管理 begin-------------------*/
/* 启动 onlinerefresh 管理线程 */
bool ripple_increment_integrate_startonlinerefresh(ripple_increment_integrate* incintegrate)
{
    dlistnode* dlnode                               = NULL;
    ripple_onlinerefresh_integrate* olrintegrate    = NULL;

    if(true == dlist_isnull(incintegrate->onlinerefresh))
    {
        return true;
    }

    ripple_thread_lock(&incintegrate->onlinerefreshlock);

    /* 遍历 onlinerefresh 节点并启动 */
    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnode->next)
    {
        olrintegrate = (ripple_onlinerefresh_integrate*)dlnode->value;

        if (RIPPLE_ONLINEREFRESH_INTEGRATE_INIT != olrintegrate->stat)
        {
            continue;
        }

        if(true == ripple_onlinerefresh_integrate_isconflict(dlnode))
        {
            break;
        }

        olrintegrate->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_STARTING;

        /* 注册 onlinerefresh 管理线程 */
        if(false == ripple_threads_addsubmanger(incintegrate->threads,
                                                RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_MGR,
                                                incintegrate->persistno,
                                                &olrintegrate->thrsmgr,
                                                (void*)olrintegrate,
                                                NULL,
                                                NULL,
                                                ripple_onlinerefresh_integrate_manage))
        {
            elog(RLOG_WARNING, "start onlinerefresh mgr failed");
            ripple_thread_unlock(&incintegrate->onlinerefreshlock);
            return false;
        }
    }

    ripple_thread_unlock(&incintegrate->onlinerefreshlock);
    return true;
}

/* 回收 onlinerefresh 节点 */
bool ripple_increment_integrate_tryjoinononlinerefresh(ripple_increment_integrate* incintegrate)
{
    dlistnode* dlnode                               = NULL;
    dlistnode* dlnodenext                           = NULL;
    ripple_onlinerefresh_integrate* olrintegrate    = NULL;

    if(true == dlist_isnull(incintegrate->onlinerefresh))
    {
        return true;
    }

    ripple_thread_lock(&incintegrate->onlinerefreshlock);
    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        olrintegrate = (ripple_onlinerefresh_integrate*)dlnode->value;

        if (RIPPLE_ONLINEREFRESH_INTEGRATE_FREE != olrintegrate->stat)
        {
            continue;
        }
        incintegrate->onlinerefresh = dlist_delete(incintegrate->onlinerefresh, dlnode, ripple_onlinerefresh_integrate_free);
    }

    ripple_thread_unlock(&incintegrate->onlinerefreshlock);
    return true;
}

/* 从状态文件加载 onlinerefrerefresh 节点 */
bool ripple_increment_integrate_onlinerefreshload(ripple_increment_integrate* incintegrate)
{
    dlistnode* dlnode                                       = NULL;
    dlistnode* dlnodenext                                   = NULL;
    ripple_refresh_tables* refreshtbs                       = NULL;
    ripple_onlinerefresh_integrate* olrintegrate            = NULL;
    ripple_onlinerefresh_integratedatasetnode* datasetnode  = NULL;

    /* 加载onlinerefresh状态文件 生成 persist */
    incintegrate->rebuild->olpersist = ripple_onlinerefresh_persist_read();
    if (NULL == incintegrate->rebuild->olpersist)
    {
        elog(RLOG_WARNING, "read onlinerefresh persist error");
        return false;
    }

    ripple_thread_lock(&incintegrate->onlinerefreshlock);
    /* 根据persists构建onlinerefresh管理线程节点 */
    if (false == ripple_onlinerefresh_integrate_persist2onlinerefreshmgr(incintegrate->rebuild->olpersist, (void**)&incintegrate->onlinerefresh))
    {
        ripple_thread_unlock(&incintegrate->onlinerefreshlock);
        return false;
    }

    if(true == dlist_isnull(incintegrate->onlinerefresh))
    {
        ripple_thread_unlock(&incintegrate->onlinerefreshlock);
        return true;
    }

    for(dlnode = incintegrate->onlinerefresh->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        olrintegrate = (ripple_onlinerefresh_integrate*)dlnode->value;

        refreshtbs = ripple_refresh_table_syncstats_tablesyncing2tables(olrintegrate->tablesyncstats);

        /* 创建onlinerefresh过滤数据集 */
        ripple_onlinerefresh_integratefilterdataset_add(incintegrate->rebuild->honlinerefreshfilterdataset, refreshtbs, olrintegrate->txid);
        datasetnode = ripple_onlinerefresh_integratedatasetnode_init();
        ripple_onlinerefresh_integratedatasetnode_no_set(datasetnode, olrintegrate->no.data);
        ripple_onlinerefresh_integratedatasetnode_txid_set(datasetnode, olrintegrate->txid);
        ripple_onlinerefresh_integratedatasetnode_refreshtables_set(datasetnode, refreshtbs);
        ripple_onlinerefresh_integratedataset_add(incintegrate->rebuild->onlinerefreshdataset, datasetnode);
    }

    ripple_thread_unlock(&incintegrate->onlinerefreshlock);

    ripple_refresh_freetables(refreshtbs);

    return true;
}

/*------------onlinerefresh 管理   end-------------------*/

/*------------bigtxn 管理 begin-------------------*/
/* 启动 bigtxn 管理线程 */
bool ripple_increment_integrate_startbigtxn(ripple_increment_integrate* incintegrate)
{
    dlistnode* dlnode = NULL;
    ripple_bigtxn_integratemanager* bigtxnintegrate = NULL;

    if(true == dlist_isnull(incintegrate->bigtxnmgr))
    {
        return true;
    }

    ripple_thread_lock(&incintegrate->bigtxnlock);

    /* 遍历 refresh 节点并启动 */
    for(dlnode = incintegrate->bigtxnmgr->head; NULL != dlnode; dlnode = dlnode->next)
    {
        bigtxnintegrate = (ripple_bigtxn_integratemanager*)dlnode->value;
        if (RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_INIT != bigtxnintegrate->stat)
        {
            continue;
        }
        bigtxnintegrate->stat = RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_INPROCESS;

        /* 启动 bigtxn manager 线程 */
        if(false == ripple_threads_addsubmanger(incintegrate->threads,
                                                RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNMGR,
                                                incintegrate->persistno,
                                                &bigtxnintegrate->thrsmgr,
                                                (void*)bigtxnintegrate,
                                                NULL,
                                                NULL,
                                                ripple_bigtxn_integratemanager_main))
        {
            elog(RLOG_WARNING, "integrate start bigtxn manager failed");
            ripple_thread_unlock(&incintegrate->bigtxnlock);
            return false;
        }
    }

    ripple_thread_unlock(&incintegrate->bigtxnlock);
    return true;
}

/* 回收 bigtxn 节点 */
bool ripple_increment_integrate_tryjoinonbigtxn(ripple_increment_integrate* incintegrate)
{
    char path[MAXPGPATH]                    = {'\0'};
    dlistnode* dlnode                       = NULL;
    dlistnode* dlnodenext                   = NULL;
    ripple_bigtxn_integratemanager* bigtxnintegrate   = NULL;

    if(true == dlist_isnull(incintegrate->bigtxnmgr))
    {
        return true;
    }

    ripple_thread_lock(&incintegrate->bigtxnlock);
    for(dlnode = incintegrate->bigtxnmgr->head; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        bigtxnintegrate = (ripple_bigtxn_integratemanager*)dlnode->value;

        if (RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_FREE != bigtxnintegrate->stat)
        {
            continue;
        }

        /* 删除xid对应大事务文件夹 */
        rmemset1(path, 0, '\0', MAXPGPATH);
        snprintf(path, MAXPGPATH, "%s/%s/%lu", guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR), 
                                               RIPPLE_STORAGE_BIG_TRANSACTION_DIR, 
                                               bigtxnintegrate->xid);
        RemoveDir(path);

        incintegrate->bigtxnmgr = dlist_delete(incintegrate->bigtxnmgr, dlnode, ripple_bigtxn_integratemanager_free);
    }

    ripple_thread_unlock(&incintegrate->bigtxnlock);
    return true;
}

/*------------bigtxn 管理   end-------------------*/

void ripple_increment_integrate_destroy(ripple_increment_integrate* incintegrate)
{
    if (NULL == incintegrate)
    {
        return;
    }

    ripple_increment_integratesplittrail_free(incintegrate->splittrailctx);

    ripple_increment_integrateparsertrail_free(incintegrate->decodingctx);

    ripple_increment_integratesync_destroy(incintegrate->syncworkstate);

    ripple_increment_integraterebuild_free(incintegrate->rebuild);

    ripple_queue_destroy(incintegrate->recordscache, dlist_freevoid);

    ripple_cache_txn_destroy(incintegrate->parser2rebuild);

    ripple_cache_txn_destroy(incintegrate->rebuild2sync);

    ripple_thread_mutex_destroy(&incintegrate->onlinerefreshlock);
    
    ripple_thread_mutex_destroy(&incintegrate->bigtxnlock);

    ripple_thread_mutex_destroy(&incintegrate->refreshlock);

    ripple_refresh_integrate_listfree(incintegrate->refresh);

    ripple_metric_integrate_destroy(incintegrate->integratestate);

    ripple_threads_free(incintegrate->threads);

    dlist_free(incintegrate->onlinerefresh, ripple_onlinerefresh_integrate_free);

    dlist_free(incintegrate->bigtxnmgr, ripple_bigtxn_integratemanager_free);

    rfree(incintegrate);
    incintegrate = NULL;
}
