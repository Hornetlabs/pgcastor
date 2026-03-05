#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/mpage/mpage.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/string/stringinfo.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "serial/ripple_serial.h"
#include "parser/trail/ripple_parsertrail.h"
#include "metric/pump/ripple_statework_pump.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/p2csharding/ripple_refresh_p2csharding.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "filetransfer/pump/ripple_filetransfer_pump.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"
#include "increment/pump/serial/ripple_increment_pumpserial.h"
#include "increment/pump/split/ripple_increment_pumpsplittrail.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"
#include "onlinerefresh/pump/netincrement/ripple_onlinerefresh_pumpnet.h"
#include "onlinerefresh/pump/splittrail/ripple_onlinerefresh_pumpsplittrail.h"
#include "onlinerefresh/pump/parsertrail/ripple_onlinerefresh_pumpparsertrail.h"
#include "onlinerefresh/pump/netsharding/ripple_onlinerefresh_shardingnet.h"
#include "onlinerefresh/pump/serial/ripple_onlinerefresh_pumpserial.h"
#include "onlinerefresh/ripple_onlinerefresh_persist.h"
#include "onlinerefresh/pump/ripple_onlinerefesh_pump.h"

/* 清理缓存 */
static void ripple_onlinerefresh_pump_cacheclean(ripple_onlinerefresh_pump* olrpump)
{
    riple_file_buffer_clean_waitflush(olrpump->txn2filebuffer);
    ripple_cache_txn_clean(olrpump->parser2synctxns);
    ripple_queue_clear(olrpump->recordscache, dlist_freevoid);
}

/* 大事务设置管理线程状态为 RESET */
static void ripple_onlinerefresh_pump_setreset(void* privdata)
{
    ripple_onlinerefresh_pump* olrpump = NULL;

    olrpump = (ripple_onlinerefresh_pump*)privdata;

    if (NULL == olrpump)
    {
        elog(RLOG_ERROR, "onlinerefresh pump stat set reset exception, privdata point is NULL");
    }

    if (RIPPLE_ONLINEREFRESH_PUMP_RESET > olrpump->stat)
    {
        olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_RESET;
    }

    return;
}

/* 获取增量工作线程 */
static bool ripple_onlinerefresh_pump_getincjobthrnode(ripple_onlinerefresh_pump* olrpump,
                                                        ripple_thrnode** parserthr,
                                                        ripple_thrnode** serialthr,
                                                        ripple_thrnode** loadrecordthr,
                                                        ripple_thrnode** netthr)
{
    ListCell* lc                = NULL;
    ripple_thrref* thrref       = NULL;

    /* 获取 loadrecords 线程节点 */
    lc = olrpump->thrsmgr->childthrrefs->head;
    thrref = (ripple_thrref*)lfirst(lc);
    *loadrecordthr = ripple_threads_getthrnodebyno(olrpump->thrsmgr->parents, thrref->no);
    if(NULL == *loadrecordthr)
    {
        elog(RLOG_WARNING, "pump onlinerefresh can not get load record thread by no:%lu", thrref->no);
        return false;
    }

    /* 获取 parser 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    *parserthr = ripple_threads_getthrnodebyno(olrpump->thrsmgr->parents, thrref->no);
    if(NULL == *parserthr)
    {
        elog(RLOG_WARNING, "pump onlinerefresh can not get parser thread by no:%lu", thrref->no);
        return false;
    }

    /* 获取 serail 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    *serialthr = ripple_threads_getthrnodebyno(olrpump->thrsmgr->parents, thrref->no);
    if(NULL == *serialthr)
    {
        elog(RLOG_WARNING, "pump onlinerefresh can not get serial thread by no:%lu", thrref->no);
        return false;
    }

    /* 获取 serail 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    *netthr = ripple_threads_getthrnodebyno(olrpump->thrsmgr->parents, thrref->no);
    if(NULL == *netthr)
    {
        elog(RLOG_WARNING, "pump onlinerefresh can not get net thread by no:%lu", thrref->no);
        return false;
    }
    
    return true;
}


ripple_onlinerefresh_pump *ripple_onlinerefresh_pump_init(void)
{
    ripple_onlinerefresh_pump *olrpump = NULL;

    olrpump = (ripple_onlinerefresh_pump *)rmalloc0(sizeof(ripple_onlinerefresh_pump));
    if (NULL == olrpump)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(olrpump, 0, 0, sizeof(ripple_onlinerefresh_pump));

    olrpump->parallelcnt = guc_getConfigOptionInt(RIPPLE_CFG_KEY_MAX_WORK_PER_REFRESH);
    if(0 == olrpump->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }

    olrpump->tablesyncstats = NULL;

    olrpump->tqueue = ripple_queue_init();

    olrpump->recordscache = ripple_queue_init();

    olrpump->parser2synctxns = ripple_cache_txn_init();

    olrpump->txn2filebuffer = ripple_file_buffer_init();

    return olrpump;
}

/* pump端 添加filetransfernode */
static void ripple_onlinerefresh_pump_manage_filetransfernode_add(void* privdata, void* filetransfernode)
{
    ripple_onlinerefresh_pump* pumpstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "onlinerefresh pump filetransfernode add exception, privdata point is NULL");
    }

    pumpstate = (ripple_onlinerefresh_pump*)privdata;

    if (NULL == pumpstate->filetransfernode)
    {
        elog(RLOG_ERROR, "onlinerefresh pump filetransfernode add exception, filetransfernode point is NULL");
    }
    ripple_filetransfer_node_add(pumpstate->filetransfernode, filetransfernode);
    return;
}

/* 删除uuid下的内容，加入到队列中 */
static void ripple_onlinerefresh_pumploadrecords_adddeletedir2gap(ripple_onlinerefresh_pump* onlinerefresh)
{
    char* url = NULL;
    char* uuid = NULL;
    ripple_filetransfer_cleanpath* cleanpath = NULL;

    /* url不配置不下载文件 */
    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    if (url == NULL || url[0] == '\0')
    {
        return;
    }

    uuid = uuid2string(&onlinerefresh->no);

    /* 创建filetransfer节点加入队列 */
    cleanpath = ripple_filetransfer_cleanpath_init();
    cleanpath->base.type = RIPPLE_FILETRANSFERNODE_TYPE_DELETEDIR;
    snprintf(cleanpath->base.localpath, RIPPLE_MAXPATH, "%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME),
                                                                    RIPPLE_REFRESH_ONLINEREFRESH,
                                                                    uuid);
    snprintf(cleanpath->base.localdir, RIPPLE_MAXPATH, "%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME),
                                                                RIPPLE_REFRESH_ONLINEREFRESH);
    snprintf(cleanpath->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME),
                                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                                uuid);
    ripple_filetransfer_node_add(onlinerefresh->filetransfernode, cleanpath);
    cleanpath = NULL;
    rfree(uuid);

    return;
}

/* 生成refresh分片数文件和校验文件节点并加入到队列中 */
static void ripple_onlinerefresh_pumploadrecords_table2shardings(ripple_onlinerefresh_pump* onlinerefresh, ripple_refresh_table_syncstat *table)
{
    char* url = NULL;
    char* uuid = NULL;
    StringInfo path = NULL;
    ripple_filetransfer_refreshshards *refresh = NULL;

    /* url不配置不下载文件 */
    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    if (url == NULL || url[0] == '\0')
    {
        return;
    }

    uuid = uuid2string(&onlinerefresh->no);

    /* shardings文件下载 */
    refresh = ripple_filetransfer_refreshshards_init();
    refresh->base.type = RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESHSHARDS;
    ripple_filetransfer_download_olrefreshshards_set(refresh, uuid, table->schema, table->table);

    /* 提前创建好 schema_table 文件夹, 故障恢复时需要 */
    path = makeStringInfo();
    appendStringInfo(path, "%s/%s", refresh->base.localdir, RIPPLE_REFRESH_PARTIAL);
    if(!DirExist(path->data))
    {
        /* 创建目录 */
        MakeDir(path->data);
    }
    deleteStringInfo(path);
    ripple_filetransfer_node_add(onlinerefresh->filetransfernode, refresh);
    refresh = NULL;
    rfree(uuid);

    return;
}

/* 生成refresh分片数文件并加入到队列中 */
static void ripple_onlinerefresh_pumploadrecords_addrefresh2gapqueue(ripple_onlinerefresh_pump* onlinerefresh)
{
    char* url = NULL;
    ripple_refresh_table_syncstats* tablesyncstats = NULL;
    ripple_refresh_table_syncstat* current_table = NULL;

    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);

    if (url == NULL || url[0] == '\0')
    {
        return;
    }

    tablesyncstats = onlinerefresh->tablesyncstats;

    if (!tablesyncstats || !tablesyncstats->tablesyncall)
    {
        return;
    }

    ripple_refresh_table_syncstats_lock(tablesyncstats);
    current_table = tablesyncstats->tablesyncall;

    /* 遍历complete目录生成任务 */
    while (current_table)
    {
        ripple_onlinerefresh_pumploadrecords_table2shardings(onlinerefresh, current_table);
        current_table = current_table->next;
    }

    ripple_refresh_table_syncstats_unlock(tablesyncstats);
    return;
}

/* 注册启动 refresh 任务 */
static bool ripple_onlinerefresh_pump_startrefreshjob(ripple_onlinerefresh_pump *olrpump)
{
    int index = 0;
    ripple_onlinerefresh_shardingnet* olrpumpnet = NULL;
    for(index = 0; index < olrpump->parallelcnt; index++)
    {
        olrpumpnet = ripple_onlinerefresh_shardingnet_init();
        if(NULL == olrpumpnet)
        {
            elog(RLOG_WARNING, "pump onlinerefresh start refresh job error");
            return false;
        }
        rmemcpy1(olrpumpnet->onlinerefreshno.data, 0, olrpump->no.data, RIPPLE_UUID_LEN);
        olrpumpnet->taskqueue = olrpump->tqueue;
        olrpumpnet->syncstats = olrpump->tablesyncstats;
        olrpumpnet->privdata = (void*)olrpump;
        olrpumpnet->callback.setreset = ripple_onlinerefresh_pump_setreset;

        /* 启动 refresh 任务 */
        if(false == ripple_threads_addjobthread(olrpump->thrsmgr->parents,
                                                RIPPLE_THRNODE_IDENTITY_PUMP_ONLINEREFRESH_JOB,
                                                olrpump->thrsmgr->submgrref.no,
                                                (void*)olrpumpnet,
                                                ripple_onlinerefresh_shardingnet_free,
                                                NULL,
                                                ripple_onlinerefresh_shardingnet_main))
        {
            elog(RLOG_WARNING, "onlinerefresh pump start refresh job error");
            return false;
        }
    }
    return true;
}

/* 注册启动 increment 任务 */
static bool ripple_onlinerefresh_pump_startincrementjob(ripple_onlinerefresh_pump *olrpump)
{
    ripple_task_onlinerefreshpumpnet* pumpnet = NULL;
    ripple_task_onlinerefreshpumpsplittrail* loadrecords = NULL;
    ripple_task_onlinerefreshpumpparsertrail* parser = NULL;
    ripple_onlinerefresh_pumpserial* serial = NULL;

    if(false == olrpump->increment)
    {
        return true;
    }

    /*---------------网络发送线程 begin------------------*/
    pumpnet = ripple_onlinerefresh_pumpwrite_init();
    rmemcpy1(pumpnet->netstate->onlinerefresh.data, 0, olrpump->no.data, RIPPLE_UUID_LEN);
    pumpnet->netstate->txn2filebuffer = olrpump->txn2filebuffer;
    pumpnet->netstate->privdata = olrpump;
    pumpnet->netstate->callback.onlinerefresh_setreset = ripple_onlinerefresh_pump_setreset;
    /*---------------网络发送线程   end------------------*/

    /*---------------序列化线程 begin--------------------*/
    serial = ripple_onlinerefresh_pumpserial_init();
    serial->parser2serialtxns = olrpump->parser2synctxns;
    serial->serialstate->txn2filebuffer = olrpump->txn2filebuffer;
    /*---------------序列化线程   end--------------------*/

    /*---------------解析器线程 begin--------------------*/
    parser = ripple_onlinerefresh_pumpparsertrail_init();
    parser->decodingctx->parsertrail.parser2txn = olrpump->parser2synctxns;
    parser->decodingctx->recordscache = olrpump->recordscache;
    /*---------------解析器线程   end--------------------*/

    /*---------------loadrecords线程 begin---------------*/
    loadrecords = ripple_onlinerefresh_pumpsplittrail_init();
    rmemset1(loadrecords->splittrailctx->capturedata, 0, '\0', MAXPGPATH);
    snprintf(loadrecords->splittrailctx->capturedata, MAXPGPATH, "%s/%s" , olrpump->data, RIPPLE_STORAGE_TRAIL_DIR);

    /* 设置加载的路径 */
    if(false == ripple_loadtrailrecords_setloadsource(loadrecords->splittrailctx->loadrecords, loadrecords->splittrailctx->capturedata))
    {
        elog(RLOG_WARNING, "pump onlinerefresh set capture data error");
        return false;
    }

    rmemset1(loadrecords->onlinerefreshno.data, 0, '\0', RIPPLE_UUID_LEN);
    rmemcpy1(loadrecords->onlinerefreshno.data, 0, olrpump->no.data, RIPPLE_UUID_LEN);
    loadrecords->splittrailctx->privdata = olrpump;
    loadrecords->splittrailctx->callback.pumpstate_filetransfernode_add = ripple_onlinerefresh_pump_manage_filetransfernode_add;
    loadrecords->splittrailctx->recordscache = olrpump->recordscache;
    /*---------------loadrecords线程   end---------------*/

    /*
     * 启动各线程
     */
    /* 注册网络发送线程 */
    if(false == ripple_threads_addjobthread(olrpump->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_PUMP_OLINEREFRESH_INC_NET,
                                            olrpump->thrsmgr->submgrref.no,
                                            (void*)pumpnet,
                                            ripple_onlinerefresh_pumpnet_free,
                                            NULL,
                                            ripple_onlinerefresh_pumpnet_main))
    {
        elog(RLOG_WARNING, "onlinerefresh pump start increment net job error");
        return false;
    }

    /* 序列化线程 */
    if(false == ripple_threads_addjobthread(olrpump->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_PUMP_OLINEREFRESH_INC_SERIAL,
                                            olrpump->thrsmgr->submgrref.no,
                                            (void*)serial,
                                            ripple_onlinerefresh_pumpserial_free,
                                            NULL,
                                            ripple_onlinerefresh_pumpserial_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment serial job error");
        return false;
    }

    /* 解析器线程 */
    if(false == ripple_threads_addjobthread(olrpump->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_PUMP_OLINEREFRESH_INC_PARSER,
                                            olrpump->thrsmgr->submgrref.no,
                                            (void*)parser,
                                            ripple_onlinerefresh_pumpparsertrail_free,
                                            NULL,
                                            ripple_onlinerefresh_pumpparsertrail_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment parser job error");
        return false;
    }

    /* loadrecords 线程 */
    if(false == ripple_threads_addjobthread(olrpump->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_PUMP_OLINEREFRESH_INC_LOADRECORDS,
                                            olrpump->thrsmgr->submgrref.no,
                                            (void*)loadrecords,
                                            ripple_onlinerefresh_pumpsplittrail_free,
                                            NULL,
                                            ripple_onlinerefresh_pumpsplittrail_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment loadrecords job error");
        return false;
    }

    return true;
}

/* 根据persist生成onlinerefresh管理线程 */
dlist *ripple_onlinerefresh_pumpmanager_persist2onlinerefreshmgr(ripple_onlinerefresh_persist *persist, ripple_queue* fpt)
{
    dlist *result                       = NULL;
    dlistnode *dnode                    = NULL;
    ripple_onlinerefresh_pump *olrmgr   = NULL;

    if (!persist || true == dlist_isnull(persist->dpersistnodes))
    {
        return NULL;
    }

    /* 遍历persist */
    for (dnode = persist->dpersistnodes->head; NULL != dnode; dnode = dnode->next)
    {
        ripple_onlinerefresh_persistnode *persistnode = (ripple_onlinerefresh_persistnode *)dnode->value;

        /* 已完成的和放弃掉的不再处理 */
        if (RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_DONE == persistnode->stat
            || RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_ABANDON == persistnode->stat)
        {
            continue;
        }

        /* 包含了对txn的创建 */
        /* 构建pumpmanager并初始化 设置xid 和 begin*/
        olrmgr = ripple_onlinerefresh_pump_init();
        if(NULL == olrmgr)
        {
            elog(RLOG_WARNING, "pump onlinerefres init onlinerefresh error");
            return NULL;
        }

        /* 设置onlinerefresh信息 */
        olrmgr->increment = persistnode->increment;
        olrmgr->end.trail.fileid = persistnode->end.trail.fileid;
        olrmgr->end.trail.offset = persistnode->end.trail.offset;
        rmemcpy1(olrmgr->no.data, 0, persistnode->uuid.data, RIPPLE_UUID_LEN);
        olrmgr->stat = RIPPLE_ONLINEREFRESH_PUMP_INIT;

        olrmgr->filetransfernode = fpt;
        ripple_refresh_table_syncstats_tablesyncing2tablesyncall(persistnode->refreshtbs);
        olrmgr->tablesyncstats = persistnode->refreshtbs;
        result = dlist_put(result, olrmgr);
        olrmgr = NULL;
    }
    return result;
}

/* 设置推出位置 */
void ripple_onlinerefresh_pumpmanager_setend(ripple_onlinerefresh_pump *olrmgr, ripple_recpos *pos)
{
    olrmgr->end.trail.fileid = pos->trail.fileid;
    olrmgr->end.trail.offset = pos->trail.offset;
}

/* 线程处理入口 */
void *ripple_onlinerefresh_pump_main(void* args)
{
    bool refresh                                            = false;
    bool refreshdone                                        = false;
    int skipcnt                                             = 0;
    int jobcnt                                              = 0;
    char* uuid                                              = NULL;
    ripple_thrnode* thrnode                                 = NULL;
    ripple_thrnode* incnetthrnode                           = NULL;
    ripple_thrnode* incserialthrnode                        = NULL;
    ripple_thrnode* incparserthrnode                        = NULL;
    ripple_thrnode* incloadrecthrnode                       = NULL;
    ripple_onlinerefresh_pump* olrpump                      = NULL;

    thrnode = (ripple_thrnode *)args;
    olrpump = (ripple_onlinerefresh_pump *)thrnode->data;

    uuid = uuid2string(&olrpump->no);
    sprintf(olrpump->data, "%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                        RIPPLE_REFRESH_ONLINEREFRESH,
                                        uuid);
    rfree(uuid);

    elog(RLOG_INFO, "pump online refresh %s start", olrpump->data);

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING,"onlinerefresh pump %s stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING", olrpump->data);
        olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_DONE;
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    if (!olrpump->tablesyncstats->tablesyncall || !olrpump->tablesyncstats->tablesyncing)
    {
        elog(RLOG_INFO, "pump onlinerefresh %s sync_stats is null, onlinerefresh done.", olrpump->data);
    }

    skipcnt = 4;

    /* 网闸任务生成 */
    ripple_onlinerefresh_pumploadrecords_addrefresh2gapqueue(olrpump);

    /* 存量线程 */
    while (true)
    {
        usleep(50000);
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;

            ripple_pthread_exit(NULL);
            return NULL;
        }

        if (true == olrpump->abandon)
        {
            if (RIPPLE_ONLINEREFRESH_PUMP_WAITINCREMENTDONE > olrpump->stat
                && true == refresh)
            {
                /* 设置空闲的线程退出并统计退出的线程个数 */
                jobcnt = olrpump->parallelcnt;
                if(false == ripple_threads_setsubmgrjobthredstermandcountexit(olrpump->thrsmgr->parents,
                                                                            olrpump->thrsmgr->childthrrefs,
                                                                            0,
                                                                            &jobcnt))
                {
                    elog(RLOG_WARNING, "integrate refresh set job threads term in idle error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;;
                }

                if(jobcnt != olrpump->parallelcnt)
                {
                    continue;
                }
                olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_ABANDONED;
                break;
            }

            if (false == olrpump->increment)
            {
                olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_ABANDONED;
                break;
            }
            
            if (incnetthrnode != NULL
                && incserialthrnode != NULL
                && incparserthrnode != NULL
                && incloadrecthrnode != NULL)
            {
                /* 设置 sync 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incnetthrnode->stat)
                {
                    incnetthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }

                /* 设置 rebuild 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incserialthrnode->stat)
                {
                    incserialthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }

                /* 设置 loadrecords 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incloadrecthrnode->stat)
                {
                    incloadrecthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }

                /* 设置 parsert 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incparserthrnode->stat)
                {
                    incparserthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }

                if(RIPPLE_THRNODE_STAT_EXITED != incloadrecthrnode->stat
                    || RIPPLE_THRNODE_STAT_EXITED != incparserthrnode->stat
                    || RIPPLE_THRNODE_STAT_EXITED != incserialthrnode->stat
                    || RIPPLE_THRNODE_STAT_EXITED != incnetthrnode->stat)
                {
                    continue;
                }
            }
            olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_ABANDONED;
            break;
        }

        /* 等待子线程全部启动成功 */
        if (RIPPLE_ONLINEREFRESH_PUMP_WAITSTART == olrpump->stat)
        {
            refresh = false;
            if (olrpump->tablesyncstats->tablesyncall && olrpump->tablesyncstats->tablesyncing)
            {
                /* 启动 onlinerefresh 存量任务 */
                if(false == ripple_onlinerefresh_pump_startrefreshjob(olrpump))
                {
                    elog(RLOG_WARNING, "pump onlinerefresh %s start refresh job error.", olrpump->data);
                    olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_DONE;
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
                refresh = true;
            }

            if (true == olrpump->increment)
            {
                /* 启动 onlinerefresh 增量任务 */
                if (false == ripple_onlinerefresh_pump_startincrementjob(olrpump))
                {
                    elog(RLOG_WARNING, "pump onlinerefresh %s start increment job error.", olrpump->data);
                    olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_DONE;
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }

                if (false == ripple_onlinerefresh_pump_getincjobthrnode(olrpump,
                                                                        &incparserthrnode,
                                                                        &incserialthrnode,
                                                                        &incloadrecthrnode,
                                                                        &incnetthrnode))
                {
                    elog(RLOG_WARNING, "pump onlinerefresh can not get increment thread");
                    olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_DONE;
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
            }
            olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_STARTING;
            continue;
        }
        else if (RIPPLE_ONLINEREFRESH_PUMP_STARTING == olrpump->stat)
        {
            jobcnt = 0;
            if (false == ripple_threads_countsubmgrjobthredsabovework(olrpump->thrsmgr->parents,
                                                                      olrpump->thrsmgr->childthrrefs,
                                                                      &jobcnt))
            {
                elog(RLOG_WARNING, "pump onlinerefresh count job thread above work stat error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_onlinerefresh_pump_main_done;
            }

            if (jobcnt != olrpump->thrsmgr->childthrrefs->length)
            {
                continue;
            }

            if (true == refresh)
            {
                olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_ADDREFRESH;
            }
            else
            {
                olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_WAITINCREMENTDONE;
            }

            continue;
        }
        else if(RIPPLE_ONLINEREFRESH_PUMP_ADDREFRESH == olrpump->stat)
        {
            /* 遍历状态表和文件, 生成queue */
            if(false == ripple_refresh_table_syncstat_genqueue(olrpump->tablesyncstats, (void*)olrpump->tqueue, olrpump->data))
            {
                elog(RLOG_WARNING, "pump onlinerefresh add table shardings to queue error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_onlinerefresh_pump_main_done;
            }

            if(NULL != olrpump->tablesyncstats->tablesyncing)
            {
                /* 
                 * pump 端先接收到了 onlinerefresh 中的表, 根据表在 onlinerefesh/uuid 下读取 refresh 文件夹
                 * 此时可能会出现: 文件夹下无内容或内容不全, 而处于队列中的任务必须是分片已经存在
                 * 所以在 genqueue 中不可能一次将所有的分片加入到队列中. 而根据当前的机制是无法明确标识任务是否都加入到队列中
                 * 所以退而求其次只要同步的内容为空, 就可以标识出来没有任务需要加入到队列中.
                 */
                continue;
            }
            olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_WAITREFRESHDONE;
            continue;
        }
        else if(RIPPLE_ONLINEREFRESH_PUMP_WAITREFRESHDONE == olrpump->stat)
        {
            /*
             * 等待存量线程退出
             *  1、队列为空
             *  2、存量线程完全退出
             */
            if(false == ripple_queue_isnull(olrpump->tqueue))
            {
                /* 队列不为空, 证明还有任务需要处理 */
                continue;
            }

            /* 设置空闲的线程退出并统计退出的线程个数 */
            jobcnt = olrpump->parallelcnt;
            if(false == ripple_threads_setsubmgrjobthredstermandcountexit(olrpump->thrsmgr->parents,
                                                                        olrpump->thrsmgr->childthrrefs,
                                                                        skipcnt,
                                                                        &jobcnt))
            {
                elog(RLOG_WARNING, "pump onlinerefresh set job threads term in idle error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_onlinerefresh_pump_main_done;
            }

            if(jobcnt != olrpump->parallelcnt)
            {
                continue;
            }

            olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_WAITINCREMENTDONE;
            refreshdone = true;
            continue;
        }
        else if(RIPPLE_ONLINEREFRESH_PUMP_WAITINCREMENTDONE == olrpump->stat)
        {
            if(false == olrpump->increment)
            {
                /* 没有增量线程, 设置本线程退出 */
                ripple_onlinerefresh_pumploadrecords_adddeletedir2gap(olrpump);
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_DONE;
                break;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != incparserthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != incserialthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != incnetthrnode->stat)
            {
                /* 解析线程未退出, 等待 */
                continue;
            }

            /* 设置 loadrecords 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > incloadrecthrnode->stat)
            {
                incloadrecthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != incloadrecthrnode->stat)
            {
                continue;
            }

            /* 设置本线程退出 */
            ripple_onlinerefresh_pumploadrecords_adddeletedir2gap(olrpump);
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_DONE;
            break;
        }
        else if(RIPPLE_ONLINEREFRESH_PUMP_RESET == olrpump->stat)
        {
            /* 设置空闲的线程退出并统计退出的线程个数 */
            if (false == refreshdone)
            {
                jobcnt = olrpump->parallelcnt;
                if(false == ripple_threads_setsubmgrjobthredstermandcountexit(olrpump->thrsmgr->parents,
                                                                            olrpump->thrsmgr->childthrrefs,
                                                                            skipcnt,
                                                                            &jobcnt))
                {
                    elog(RLOG_WARNING, "pump onlinerefresh set job threads term in idle error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    goto ripple_onlinerefresh_pump_main_done;
                }

                if(jobcnt != olrpump->parallelcnt)
                {
                    continue;
                }
            }

            if (incnetthrnode != NULL
                && incserialthrnode != NULL
                && incparserthrnode != NULL
                && incloadrecthrnode != NULL)
            {
                /* 设置 sync 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incnetthrnode->stat)
                {
                    incnetthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }

                /* 设置 rebuild 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incserialthrnode->stat)
                {
                    incserialthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }

                /* 设置 loadrecords 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incloadrecthrnode->stat)
                {
                    incloadrecthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }

                /* 设置 parsert 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incparserthrnode->stat)
                {
                    incparserthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }

                if(RIPPLE_THRNODE_STAT_EXITED != incloadrecthrnode->stat
                    || RIPPLE_THRNODE_STAT_EXITED != incparserthrnode->stat
                    || RIPPLE_THRNODE_STAT_EXITED != incserialthrnode->stat
                    || RIPPLE_THRNODE_STAT_EXITED != incnetthrnode->stat)
                {
                    continue;
                }
            }
            /* 所有线程都已经退出, 管理线程可以退出了 */
            jobcnt = olrpump->thrsmgr->childthrrefs->length;
            ripple_threads_setsubmgrjobthredsfree(olrpump->thrsmgr->parents,
                                                  olrpump->thrsmgr->childthrrefs,
                                                  0,
                                                  jobcnt);
            list_free_deep(olrpump->thrsmgr->childthrrefs);
            olrpump->thrsmgr->childthrrefs = NULL;
            ripple_onlinerefresh_pump_cacheclean(olrpump);
            olrpump->stat = RIPPLE_ONLINEREFRESH_PUMP_WAITSTART;
            continue;
        }
    }

ripple_onlinerefresh_pump_main_done:
    /* 所有线程都已经退出, 管理线程可以退出了 */
    jobcnt = 0;
    if (NULL != olrpump->thrsmgr && NULL != olrpump->thrsmgr->childthrrefs)
    {
        jobcnt = olrpump->thrsmgr->childthrrefs->length;
    }

    ripple_threads_setsubmgrjobthredsfree(olrpump->thrsmgr->parents,
                                          olrpump->thrsmgr->childthrrefs,
                                          0,
                                          jobcnt);
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_pump_free(void* in_onlinerefresh)
{
    ripple_onlinerefresh_pump* onlinerefresh = NULL;
    if(NULL == in_onlinerefresh)
    {
        return;
    }
    
    onlinerefresh = (ripple_onlinerefresh_pump*)in_onlinerefresh;

    if (onlinerefresh->tablesyncstats)
    {
        ripple_refresh_table_syncstats_free(onlinerefresh->tablesyncstats);
    }

    if (onlinerefresh->tqueue)
    {
        ripple_queue_destroy(onlinerefresh->tqueue, ripple_refresh_table_sharding_queuefree);
    }

    if (onlinerefresh->recordscache)
    {
        ripple_queue_destroy(onlinerefresh->recordscache, dlist_freevoid);
    }
    
    if (onlinerefresh->parser2synctxns)
    {
        ripple_cache_txn_destroy(onlinerefresh->parser2synctxns);
    }

    if (onlinerefresh->txn2filebuffer)
    {
        ripple_file_buffer_destroy(onlinerefresh->txn2filebuffer);
    }
    
    rfree(onlinerefresh);
}