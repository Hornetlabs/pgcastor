#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/conn/conn.h"
#include "utils/string/stringinfo.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "sync/sync.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "threads/threads.h"
#include "rebuild/rebuild.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "works/syncwork/refresh_integratesync.h"
#include "refresh/sharding2db/refresh_sharding2db.h"
#include "metric/integrate/metric_integrate.h"
#include "parser/trail/parsertrail.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"
#include "increment/integrate/split/increment_integratesplittrail.h"
#include "onlinerefresh/onlinerefresh_persist.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/onlinerefresh_integrate.h"
#include "onlinerefresh/integrate/splittrail/onlinerefresh_integratesplittrail.h"
#include "onlinerefresh/integrate/parsertrail/onlinerefresh_integrateparsertrail.h"
#include "onlinerefresh/integrate/sync/onlinerefresh_integrateincsyncstate.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/integrate/bigtxn_integratemanager.h"
#include "increment/integrate/sync/increment_integratesync.h"
#include "increment/integrate/rebuild/increment_integraterebuild.h"
#include "onlinerefresh/integrate/rebuild/onlinerefresh_integraterebuild.h"
#include "increment/integrate/increment_integrate.h"


typedef enum ONLINEREFRESH_INTEGRATE_STAT
{
    ONLINEREFRESH_INTEGRATE_STAT_JOBNOP                      = 0x00,
    ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILREFRESHSTARTING     ,               /* 工作线程启动中 */
    ONLINEREFRESH_INTEGRATE_STAT_JOBWORKING                  ,               /* 存量工作线程工作状态 */
    ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTREFRESHDONE        ,               /* 等待存量工作线程工作完成 */
    ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING  ,               /* 增量工作线程启动中 */
    ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE                      /* 等待存量工作线程工作完成 */
} onlinerefresh_integrate_stat;

/* 检测abandon文件是否存在 */
static bool onlinerefresh_integrate_checkabandon(char* path)
{
    struct stat st;
    char file[MAXPATH] = {'\0'};

    sprintf(file, "%s/%s", path, ONLINEREFRESHABANDON_DAT);

    /* 校验文件是否存在，存在返回true */
    if(0 == stat(file, &st))
    {
        return true;
    }
    return false;
}

/* 向sync状态表中refresh数据，并truncate存量表 */
static bool onlinerefresh_integrate_gettrailno(onlinerefresh_integrate* onlinerefresh,
                                                      onlinerefresh_integratesplittrail* oliloadrecord,
                                                      char* name)
{
    uint64 trailno          = 0;
    uint64 emitoffset       = 0;
    XLogRecPtr lsn          = InvalidXLogRecPtr;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

    conn = conn_get(onlinerefresh->conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        return false;
    }

    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql, "select rewind_fileid, rewind_offset, lsn from \"%s\".\"%s\" where name = '%s';",
                            catalog_schema,
                            SYNC_STATUSTABLE_NAME,
                            name);
    res = conn_exec(conn, sql->data);
    if (NULL == res)
    {
        elog(RLOG_WARNING, "Execute commit failed");
        deleteStringInfo(sql);
        PQfinish(conn);
        return false;
    }
    /* 将获取的 fileid 和 offset 设置为 read 中的信息 */
    if (PQntuples(res) != 0 )
    {
        trailno = strtoul(PQgetvalue(res, 0, 0), NULL, 10);
        emitoffset = strtoul(PQgetvalue(res, 0, 1), NULL, 10);
        increment_integratesplittrail_emit_set(oliloadrecord->splittrailctx, trailno, emitoffset);

        /*lsn信息*/
        lsn = strtoul(PQgetvalue(res, 0, 2), NULL, 10);
        elog(RLOG_DEBUG, "onlinerefreshget record sync_status, trailno:%lu, emitoffset:%lu, lsn:%lu",
                          trailno,
                          emitoffset,
                          lsn);
        PQclear(res);
    }
    PQfinish(conn);
    deleteStringInfo(sql);

    return true;
}

/* 向sync状态表中添加refresh数据，并truncate存量表 */
static bool onlinerefresh_integrate_refsetsynctable(onlinerefresh_integrate* onlinerefresh, thrnode* thrnode)
{
    int index               = 0;
    char* uuid              = NULL;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

onlinerefresh_integrate_setsynctableretry:
    sleep(1);
    if(THRNODE_STAT_TERM == thrnode->stat)
    {
        return false;
    }

    conn = conn_get(onlinerefresh->conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        goto onlinerefresh_integrate_setsynctableretry;
    }

    if (false == conn_begin(conn))
    {
        elog(RLOG_WARNING, "Execute begin failed");
        PQfinish(conn);
        goto onlinerefresh_integrate_setsynctableretry;
    }
    sql = makeStringInfo();
    uuid = uuid2string(&onlinerefresh->no);
    catalog_schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);

    for (index = 0; index < onlinerefresh->parallelcnt; index++)
    {
        resetStringInfo(sql);
        appendStringInfo(sql, "INSERT INTO \"%s\".\"%s\" \n"
                              "(name, type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) \n"
                              "VALUES (\'%s-%s%d\', %hd, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (name) DO UPDATE SET \n"
                              "(type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) =  \n"
                              "(EXCLUDED.type, EXCLUDED.stat, EXCLUDED.rewind_fileid, EXCLUDED.rewind_offset, EXCLUDED.emit_fileid, EXCLUDED.emit_offset, EXCLUDED.xid, EXCLUDED.lsn); ",
                              catalog_schema,
                              SYNC_STATUSTABLE_NAME,
                              uuid,
                              REFRESH_REFRESH,
                              index,
                              2);
        res = conn_exec(conn, sql->data);
        if (NULL == res)
        {
            elog(RLOG_WARNING, "Execute commit failed");
            deleteStringInfo(sql);
            goto onlinerefresh_integrate_setsynctableretry;
        }
        PQclear(res);
    }

    /* 清空表信息 */
    if (1 == guc_getConfigOptionInt(CFG_KEY_TRUNCATETABLE))
    {
        /* 如果清空失败，重新连接数据库并执行 */
        if (false == refresh_table_syncstats_truncatetable_fromsyncstats(onlinerefresh->tablesyncstats, (void*)conn))
        {
            res = conn_exec(conn, "ROLLBACK");
            if (NULL == res)
            {
                elog(RLOG_WARNING, "Execute rollback failed");
                deleteStringInfo(sql);
                rfree(uuid);
                goto onlinerefresh_integrate_setsynctableretry;
            }
            PQclear(res);
            PQfinish(conn);
            deleteStringInfo(sql);
            rfree(uuid);
            sleep(1);
            goto onlinerefresh_integrate_setsynctableretry;
        }
    }

    if (false == conn_commit(conn))
    {
        deleteStringInfo(sql);
        PQfinish(conn);
        rfree(uuid);
        goto onlinerefresh_integrate_setsynctableretry;
    }
    PQfinish(conn);
    rfree(uuid);
    deleteStringInfo(sql);

    return true;
}

/* 向sync状态表添加增量数据 */
static bool onlinerefresh_integrate_incsetsynctable(onlinerefresh_integrate* onlinerefresh, thrnode* thrnode)
{
    char* uuid              = NULL;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

onlinerefresh_integrate_incsetsynctableretry:
    sleep(1);
    if(THRNODE_STAT_TERM == thrnode->stat)
    {
        return false;
    }

    conn = conn_get(onlinerefresh->conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        goto onlinerefresh_integrate_incsetsynctableretry;
    }

    sql = makeStringInfo();
    uuid = uuid2string(&onlinerefresh->no);
    catalog_schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql, "INSERT INTO \"%s\".\"%s\" \n"
                          "(name, type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) \n"
                          "VALUES (\'%s-%s\', %hd, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (name) DO UPDATE SET \n"
                          "(type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) =  \n"
                          "(EXCLUDED.type, EXCLUDED.stat, EXCLUDED.rewind_fileid, EXCLUDED.rewind_offset, EXCLUDED.emit_fileid, EXCLUDED.emit_offset, EXCLUDED.xid, EXCLUDED.lsn); ",
                          catalog_schema,
                          SYNC_STATUSTABLE_NAME,
                          uuid,
                          REFRESH_INCREMENT,
                          2);
    res = conn_exec(conn, sql->data);
    if (NULL == res)
    {
        elog(RLOG_WARNING, "Execute commit failed");
        deleteStringInfo(sql);
        rfree(uuid);
        goto onlinerefresh_integrate_incsetsynctableretry;
    }
    PQclear(res);
    PQfinish(conn);
    deleteStringInfo(sql);
    rfree(uuid);

    return true;
}

/* 有正在发起 onlinerefresh 中的表,那么返回 true */
bool onlinerefresh_integrate_isconflict(dlistnode* in_dlnode)
{
    dlistnode* dlnode = NULL;
    onlinerefresh_integrate* onlinerefresh_node = NULL;
    onlinerefresh_integrate* current_node = NULL;

    dlnode = in_dlnode->prev;
    if(NULL == dlnode)
    {
        return false;
    }
    current_node = (onlinerefresh_integrate*)in_dlnode->value;

    for(; NULL != dlnode; dlnode = dlnode->prev)
    {
        onlinerefresh_node = (onlinerefresh_integrate*)dlnode->value;
        
        if (false == refresh_table_syncstats_compare(onlinerefresh_node->tablesyncstats, current_node->tablesyncstats))
        {
            continue;
        }

        if (ONLINEREFRESH_INTEGRATE_DONE <= onlinerefresh_node->stat)
        {
            break;
        }
        return true;
    }
    return false;
}

bool onlinerefresh_integrate_persist2onlinerefreshmgr(onlinerefresh_persist *persist, void **onlinerefresh)
{
    dlist *result                           = NULL;
    dlistnode *dnode                        = NULL;
    onlinerefresh_integrate *olrmgr  = NULL;

    if (NULL == persist)
    {
        elog(RLOG_WARNING, "onlinerefres persist is NULL error");
        return false;
    }

    if (true == dlist_isnull(persist->dpersistnodes))
    {
        return true;
    }

    /* 遍历persist */
    for (dnode = persist->dpersistnodes->head; NULL != dnode; dnode = dnode->next)
    {
        onlinerefresh_persistnode *persistnode = (onlinerefresh_persistnode *)dnode->value;

        /* 已完成的和放弃掉的不再处理 */
        if (ONLINEREFRESH_PERSISTNODE_STAT_DONE == persistnode->stat
            || ONLINEREFRESH_PERSISTNODE_STAT_ABANDON == persistnode->stat)
        {
            continue;
        }

        /* 包含了对txn的创建 */
        /* 构建manager并初始化 设置xid 和 begin*/
        olrmgr = onlinerefresh_integrate_init(persistnode->increment);
        if(NULL == olrmgr)
        {
            elog(RLOG_WARNING, "onlinerefres init onlinerefresh error");
            return false;
        }

        rmemcpy1(olrmgr->no.data, 0, persistnode->uuid.data, UUID_LEN);
        olrmgr->stat = ONLINEREFRESH_INTEGRATE_INIT;
        olrmgr->increment = persistnode->increment;
        olrmgr->txid = persistnode->txid;
        olrmgr->begin.trail.fileid = persistnode->begin.trail.fileid;
        olrmgr->begin.trail.offset = persistnode->begin.trail.offset;
        refresh_table_syncstats_tablesyncing2tablesyncall(persistnode->refreshtbs);
        olrmgr->tablesyncstats = persistnode->refreshtbs;
        result = dlist_put(result, olrmgr);
        olrmgr = NULL;
    }

    *onlinerefresh = (void*)result;
    return true;
}

/* 启动 refresh 工作线程 */
static bool onlinerefresh_integrate_startrefreshjob(onlinerefresh_integrate* olintegrate)
{
    int index                               = 0;
    char* uuid                              = NULL;
    refresh_sharding2db *shard2db    = NULL;

    /* 为每一个线程分配空间 */
    for (index = 0; index < olintegrate->parallelcnt; index++)
    {
        /* 分配空间和初始化 */
        shard2db = refresh_sharding2db_init();
        if (NULL == shard2db)
        {
            return false;
        }
        shard2db->name = (char*)rmalloc0(NAMEDATALEN);
        if(NULL == shard2db->name)
        {
            elog(RLOG_WARNING, "malloc sharding2dbname out of memory");
            refresh_sharding2db_free(shard2db);
            return false;
        }
        rmemset0(shard2db->name, 0, '\0', NAMEDATALEN);
        uuid = uuid2string(&olintegrate->no);
        sprintf(shard2db->name, "%s-%s%d", uuid, REFRESH_REFRESH, index);
        rfree(uuid);
        shard2db->syncstats->base.conn = NULL;
        shard2db->refresh_path = olintegrate->data;
        shard2db->syncstats->base.conninfo = olintegrate->conninfo;
        shard2db->syncstats->tablesyncstats = olintegrate->tablesyncstats;
        shard2db->syncstats->queue = olintegrate->tqueue;

        /* 注册工作线程 */
        if(false == threads_addjobthread(olintegrate->thrsmgr->parents,
                                                THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_JOB,
                                                olintegrate->thrsmgr->submgrref.no,
                                                (void*)shard2db,
                                                refresh_sharding2db_free,
                                                NULL,
                                                refresh_sharding2db_work))
        {
            elog(RLOG_WARNING, "refresh integrate start job error");
            return false;
        }
    }
    return true;
}

/* 启动增量工作线程 */
static bool onlinerefresh_integrate_startincrementjob(onlinerefresh_integrate* olintegrate)
{
    char* uuid                                                      = NULL;
    onlinerefresh_integratesplittrail* oliloadrecord         = NULL;
    onlinerefresh_integrateparsertrail* oliparsertrail       = NULL;
    onlinerefresh_integrateincsync* olisync                  = NULL;
    onlinerefresh_integraterebuild* olirebuild               = NULL;

    if(false == olintegrate->increment)
    {
        /* 不需要启动增量 */
        return true;
    }

    /*---------------------应用线程 begin------------------*/
    olisync = onlinerefresh_integrateincsync_init();
    if(NULL == olisync)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate sync init error");
        return false;
    }
    olisync->base.name = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == olisync->base.name)
    {
        elog(RLOG_WARNING, "malloc sharding2dbname out of memory");
        onlinerefresh_integrateincsync_free(olisync);
        return false;
    }
    rmemset0(olisync->base.name, 0, '\0', NAMEDATALEN);
    uuid = uuid2string(&olintegrate->no);
    sprintf(olisync->base.name, "%s-%s", uuid, REFRESH_INCREMENT);
    rfree(uuid);
    olisync->rebuild2sync = olintegrate->rebuild2sync;
    olisync->base.conninfo = guc_getConfigOption(CFG_KEY_URL);

    /*---------------------应用线程   end------------------*/

    /*---------------------事务重组线程 begin----------------*/
    olirebuild = onlinerefresh_integraterebuild_init();
    if(NULL == olirebuild)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate rebuild init error");
        return false;
    }
    olirebuild->parser2rebuild = olintegrate->parser2rebuild;
    olirebuild->rebuild2sync = olintegrate->rebuild2sync;
    /*---------------------事务重组线程   end----------------*/

    /*---------------------解析器线程 begin----------------*/
    /* 
     * 解析器初始化
     * parserwal回调设置
    */
    oliparsertrail = onlinerefresh_integrateparsertrail_init();
    if(NULL == olirebuild)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate parsertrail init error");
        return false;
    }
    oliparsertrail->decodingctx->parsertrail.parser2txn = olintegrate->parser2rebuild;
    oliparsertrail->decodingctx->recordscache = olintegrate->recordscache;
    /*---------------------解析器线程   end----------------*/

    /*---------------------trail拆分线程 begin--------------*/
    oliloadrecord = onlinerefresh_integratesplittrail_init();
    if(NULL == oliloadrecord)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate init splittrail error");
        return false;
    }
    rmemset1(oliloadrecord->splittrailctx->capturedata, 0, '\0', MAXPGPATH);
    snprintf(oliloadrecord->splittrailctx->capturedata, MAXPGPATH, "%s/%s" , olintegrate->data, STORAGE_TRAIL_DIR);

    /* 设置加载的路径 */
    if(false == loadtrailrecords_setloadsource(oliloadrecord->splittrailctx->loadrecords, oliloadrecord->splittrailctx->capturedata))
    {
        elog(RLOG_WARNING, "integrate onlinerefresh set capture data error");
        return false;
    }
    oliloadrecord->splittrailctx->recordscache = olintegrate->recordscache;
    onlinerefresh_integrate_gettrailno(olintegrate, oliloadrecord, olisync->base.name);
    /*---------------------trail拆分线程   end--------------*/

    /*
     * 启动各线程
     */
    /* 注册应用线程 */
    if(false == threads_addjobthread(olintegrate->thrsmgr->parents,
                                            THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_SYNC,
                                            olintegrate->thrsmgr->submgrref.no,
                                            (void*)olisync,
                                            onlinerefresh_integrateincsync_free,
                                            NULL,
                                            onlinerefresh_integrateincsync_main))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start increment sync job error");
        return false;
    }
    
    /* 注册事务重组线程 */
    if(false == threads_addjobthread(olintegrate->thrsmgr->parents,
                                            THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_REBUILD,
                                            olintegrate->thrsmgr->submgrref.no,
                                            (void*)olirebuild,
                                            onlinerefresh_integraterebuild_free,
                                            NULL,
                                            onlinerefresh_integraterebuild_main))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start increment rebuild job error");
        return false;
    }

    /* 注册解析器线程 */
    if(false == threads_addjobthread(olintegrate->thrsmgr->parents,
                                            THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_PARSER,
                                            olintegrate->thrsmgr->submgrref.no,
                                            (void*)oliparsertrail,
                                            onlinerefresh_integrateparsertrail_free,
                                            NULL,
                                            onlinerefresh_integrateparsertrail_main))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start increment parsertrail job error");
        return false;
    }

    /* 注册 loadrecords线程 */
    if(false == threads_addjobthread(olintegrate->thrsmgr->parents,
                                            THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_LOADRECORDS,
                                            olintegrate->thrsmgr->submgrref.no,
                                            (void*)oliloadrecord,
                                            onlinerefresh_integratesplittrail_free,
                                            NULL,
                                            onlinerefresh_integratesplittrail_main))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start increment splittrail job error");
        return false;
    }
    return true;
}

/* 线程处理入口 */
void *onlinerefresh_integrate_manage(void* args)
{
    int jobcnt                                      = 0;
    onlinerefresh_integrate_stat jobstat     = ONLINEREFRESH_INTEGRATE_STAT_JOBNOP;
    char* uuid                                      = NULL;
    ListCell* lc                                    = NULL;
    thrref* thr_ref                           = NULL;
    thrnode* thr_node                         = NULL;
    thrnode* incsync_thrnode                  = NULL;
    thrnode* increbuild_thrnode               = NULL;
    thrnode* incparser_thrnode                = NULL;
    thrnode* incloadrec_thrnode               = NULL;
    onlinerefresh_integrate *olintegrate     = NULL;

    elog(RLOG_INFO, "start integrate online refresh manage");

    thr_node = (thrnode *)args;
    olintegrate = (onlinerefresh_integrate *)thr_node->data;

    uuid = uuid2string(&olintegrate->no);
    sprintf(olintegrate->data, "%s/%s/%s", guc_getConfigOption(CFG_KEY_TRAIL_DIR),
                                             REFRESH_ONLINEREFRESH,
                                             uuid);

    rfree(uuid);

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate stat exception, expected stat is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        osal_thread_exit(NULL);
        return NULL;
    }

    thr_node->stat = THRNODE_STAT_WORK;

    olintegrate->stat = ONLINEREFRESH_INTEGRATE_RUNNING;

    if(false == onlinerefresh_integrate_refsetsynctable(olintegrate, thr_node))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate set synctable error");
        thr_node->stat = THRNODE_STAT_ABORT;
        osal_thread_exit(NULL);
        return NULL;
    }

    /* 
     * 存量注册工作线程
     */
    if(false == onlinerefresh_integrate_startrefreshjob(olintegrate))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start refresh job thread error");
        thr_node->stat = THRNODE_STAT_ABORT;
        osal_thread_exit(NULL);
        return NULL;
    }

    jobstat = ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILREFRESHSTARTING;
    /* 主循环 */
    while (true)
    {
        /* 
         * 首先判断是否接收到退出信号
         *  对于子管理线程，收到 TERM 信号有两种场景:
         *  1、子管理线程的上级常驻线程退出
         *  2、接收到了退出标识
         * 
         * 上述两种场景, 都不需要子管理线程设置工作线程为 FREE 状态
         */
        usleep(50000);
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            osal_thread_exit(NULL);
            break;
        }

        if (true == onlinerefresh_integrate_checkabandon(olintegrate->data))
        {
            if (ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING > jobstat)
            {
                /* 设置空闲的线程退出并统计退出的线程个数 */
                jobcnt = olintegrate->parallelcnt;
                if(false == threads_setsubmgrjobthredstermandcountexit(olintegrate->thrsmgr->parents,
                                                                            olintegrate->thrsmgr->childthrrefs,
                                                                            0,
                                                                            &jobcnt))
                {
                    elog(RLOG_WARNING, "integrate refresh set job threads term in idle error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    goto onlinerefresh_integrate_main_done;
                }

                if(jobcnt != olintegrate->parallelcnt)
                {
                    continue;
                }
                olintegrate->stat = ONLINEREFRESH_INTEGRATE_ABANDONED;
                break;
            }

            if (false == olintegrate->increment)
            {
                olintegrate->stat = ONLINEREFRESH_INTEGRATE_ABANDONED;
                break;
            }
            
            if (incsync_thrnode != NULL
                && increbuild_thrnode != NULL
                && incparser_thrnode != NULL
                && incloadrec_thrnode != NULL)
            {
                /* 设置 sync 线程退出 */
                if(THRNODE_STAT_TERM > incsync_thrnode->stat)
                {
                    incsync_thrnode->stat = THRNODE_STAT_TERM;
                    continue;
                }
                
                /* 设置 rebuild 线程退出 */
                if(THRNODE_STAT_TERM > increbuild_thrnode->stat)
                {
                    increbuild_thrnode->stat = THRNODE_STAT_TERM;
                    continue;
                }

                /* 设置 loadrecords 线程退出 */
                if(THRNODE_STAT_TERM > incloadrec_thrnode->stat)
                {
                    incloadrec_thrnode->stat = THRNODE_STAT_TERM;
                    continue;
                }

                /* 设置 parsert 线程退出 */
                if(THRNODE_STAT_TERM > incparser_thrnode->stat)
                {
                    incparser_thrnode->stat = THRNODE_STAT_TERM;
                    continue;
                }

                if(THRNODE_STAT_EXITED != incloadrec_thrnode->stat
                    || THRNODE_STAT_EXITED != incparser_thrnode->stat
                    || THRNODE_STAT_EXITED != incsync_thrnode->stat
                    || THRNODE_STAT_EXITED != increbuild_thrnode->stat)
                {
                    continue;
                }
            }
            olintegrate->stat = ONLINEREFRESH_INTEGRATE_ABANDONED;
            break;
        }

        /* 等待子线程全部启动成功 */
        if(ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILREFRESHSTARTING == jobstat)
        {
            /* 查看是否已经启动成功 */
            jobcnt = 0;
            if(false == threads_countsubmgrjobthredsabovework(olintegrate->thrsmgr->parents,
                                                                     olintegrate->thrsmgr->childthrrefs,
                                                                     &jobcnt))
            {
                elog(RLOG_WARNING, "capture onlinerefresh count job thread above work stat error");
                thr_node->stat = THRNODE_STAT_ABORT;
                goto onlinerefresh_integrate_main_done;
            }

            if(jobcnt != olintegrate->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = ONLINEREFRESH_INTEGRATE_STAT_JOBWORKING;
            continue;
        }
        else if(ONLINEREFRESH_INTEGRATE_STAT_JOBWORKING == jobstat)
        {
            /* 工作线程已经启动, 那么向队列中加入工作任务 */
            if(false == refresh_table_syncstat_genqueue(olintegrate->tablesyncstats, (void*)olintegrate->tqueue, olintegrate->data))
            {
                /* 向队列中加入任务失败, 那么管理线程退出, 子线程的回收由主线程处理 */
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }

            /* 首先判断是否存在任务和待同步表 */
            if (NULL ==  olintegrate->tablesyncstats->tablesyncing)
            {
                jobstat = ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTREFRESHDONE;
            }
            continue;
        }
        else if (ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTREFRESHDONE == jobstat)
        {
            /*
             * 等待存量线程退出
             *  1、队列为空
             *  2、存量线程完全退出
             */
            if(false == queue_isnull(olintegrate->tqueue))
            {
                /* 队列不为空, 证明还有任务需要处理 */
                continue;
            }

            /* 设置空闲的线程退出并统计退出的线程个数 */
            jobcnt = olintegrate->parallelcnt;
            if(false == threads_setsubmgrjobthredstermandcountexit(olintegrate->thrsmgr->parents,
                                                                         olintegrate->thrsmgr->childthrrefs,
                                                                         0,
                                                                         &jobcnt))
            {
                elog(RLOG_WARNING, "integrate refresh set job threads term in idle error");
                thr_node->stat = THRNODE_STAT_ABORT;
                goto onlinerefresh_integrate_main_done;
            }

            if(jobcnt != olintegrate->parallelcnt)
            {
                continue;
            }

            jobstat = ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING;

            olintegrate->stat = ONLINEREFRESH_INTEGRATE_REFRESHDONE;
            continue;
        }
        else if(ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING == jobstat)
        {
            if(true == olintegrate->increment)
            {
                if(false == onlinerefresh_integrate_incsetsynctable(olintegrate, thr_node))
                {
                    elog(RLOG_WARNING, "onlinerefresh integrate increment set synctable error");
                    continue;
                }

                /* 启动增量工作线程 */
                if(false == onlinerefresh_integrate_startincrementjob(olintegrate))
                {
                    elog(RLOG_WARNING, "onlinerefresh integrate start increment thread error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }

                /* 预先获取到 increment->parser 线程, 方面后续的逻辑判断 */
                /* 获取 loadrecord 线程 */
                lc = olintegrate->thrsmgr->childthrrefs->head;
                thr_ref = (thrref*)lfirst(lc);
                incloadrec_thrnode = threads_getthrnodebyno(olintegrate->thrsmgr->parents, thr_ref->no);
                if(NULL == incloadrec_thrnode)
                {
                    elog(RLOG_WARNING, "integrate onlinerefresh can not get load record thread by no:%lu", thr_ref->no);
                    thr_node->stat = THRNODE_STAT_ABORT;
                    osal_thread_exit(NULL);
                }

                /* 获取 parser 线程 */
                lc = lc->next;
                thr_ref = (thrref*)lfirst(lc);
                incparser_thrnode = threads_getthrnodebyno(olintegrate->thrsmgr->parents, thr_ref->no);
                if(NULL == incparser_thrnode)
                {
                    elog(RLOG_WARNING, "integrate onlinerefresh can not get parser thread by no:%lu", thr_ref->no);
                    thr_node->stat = THRNODE_STAT_ABORT;
                    osal_thread_exit(NULL);
                }

                /* 获取 serail 线程 */
                lc = lc->next;
                thr_ref = (thrref*)lfirst(lc);
                increbuild_thrnode = threads_getthrnodebyno(olintegrate->thrsmgr->parents, thr_ref->no);
                if(NULL == increbuild_thrnode)
                {
                    elog(RLOG_WARNING, "integrate onlinerefresh can not get rebuild thread by no:%lu", thr_ref->no);
                    thr_node->stat = THRNODE_STAT_ABORT;
                    osal_thread_exit(NULL);
                }

                /* 获取 flush 线程 */
                lc = lc->next;
                thr_ref = (thrref*)lfirst(lc);
                incsync_thrnode = threads_getthrnodebyno(olintegrate->thrsmgr->parents, thr_ref->no);
                if(NULL == incsync_thrnode)
                {
                    elog(RLOG_WARNING, "integrate onlinerefresh can not get sync thread by no:%lu", thr_ref->no);
                    thr_node->stat = THRNODE_STAT_ABORT;
                    osal_thread_exit(NULL);
                }
            }

            jobstat = ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE;
        }
        else if (ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE == jobstat)
        {
            /* 
             * 在 onlinerefresh 中, 需要先等待 increment->parser 线程退出
             *  parser 在同步过程解析到onlinerefreshend的事务后就会退出
             *  sync 应用完onlinerefreshend的事务后就会退出
             *  等待 解析、应用线程退出后, 设置 loadrecord、rebuild 线程为 TERM
             */
            if(false == olintegrate->increment)
            {
                /* 没有增量线程, 设置本线程退出 */
                thr_node->stat = THRNODE_STAT_EXIT;
                break;
            }

            if(THRNODE_STAT_EXITED != incparser_thrnode->stat
                || THRNODE_STAT_EXITED != incsync_thrnode->stat)
            {
                /* 解析线程未退出, 等待 */
                continue;
            }

            /* 设置 loadrecords 线程退出 */
            if(THRNODE_STAT_TERM > incloadrec_thrnode->stat)
            {
                incloadrec_thrnode->stat = THRNODE_STAT_TERM;
                continue;
            }

            /* 设置 rebuild 线程退出 */
            if(THRNODE_STAT_TERM > increbuild_thrnode->stat)
            {
                increbuild_thrnode->stat = THRNODE_STAT_TERM;
                continue;
            }

            if(THRNODE_STAT_EXITED != incloadrec_thrnode->stat
                || THRNODE_STAT_EXITED != increbuild_thrnode->stat)
            {
                continue;
            }
            
            /* 设置本线程退出 */
            thr_node->stat = THRNODE_STAT_EXIT;
            olintegrate->stat = ONLINEREFRESH_INTEGRATE_DONE;
            break;
        }
        
    }

onlinerefresh_integrate_main_done:
    /* 所有线程都已经退出, 管理线程可以退出了 */
    jobcnt = olintegrate->thrsmgr->childthrrefs->length;
    threads_setsubmgrjobthredsfree(olintegrate->thrsmgr->parents,
                                          olintegrate->thrsmgr->childthrrefs,
                                          0,
                                          jobcnt);
    /* make compiler happy */
    return NULL;
}

onlinerefresh_integrate *onlinerefresh_integrate_init(bool increment)
{
    onlinerefresh_integrate *onlinerefresh = NULL;

    onlinerefresh = (onlinerefresh_integrate *)rmalloc0(sizeof(onlinerefresh_integrate));
    if (NULL == onlinerefresh)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(onlinerefresh, 0, 0, sizeof(onlinerefresh_integrate));

    onlinerefresh->tablesyncstats = NULL;
    onlinerefresh->txid = InvalidFullTransactionId;

    onlinerefresh->parallelcnt = guc_getConfigOptionInt(CFG_KEY_MAX_WORK_PER_REFRESH);
    if(0 == onlinerefresh->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }

    onlinerefresh->conninfo = guc_getConfigOption(CFG_KEY_URL);

    onlinerefresh->thrsmgr = NULL;

    onlinerefresh->tqueue = queue_init();

    if(increment)
    {
        onlinerefresh->recordscache = queue_init();

        onlinerefresh->parser2rebuild = cache_txn_init();

        onlinerefresh->rebuild2sync = cache_txn_init();
    }


    return onlinerefresh;
}

void onlinerefresh_integrate_free(void* in_onlinerefresh)
{
    onlinerefresh_integrate* onlinerefresh = NULL;
    if(NULL == in_onlinerefresh)
    {
        return;
    }

    onlinerefresh = (onlinerefresh_integrate*)in_onlinerefresh;

    if (onlinerefresh->tablesyncstats)
    {
        refresh_table_syncstats_free(onlinerefresh->tablesyncstats);
    }

    if (onlinerefresh->tqueue)
    {
        queue_destroy(onlinerefresh->tqueue, refresh_table_sharding_queuefree);
    }

    if (onlinerefresh->recordscache)
    {
        queue_destroy(onlinerefresh->recordscache, dlist_freevoid);
    }
    
    if (onlinerefresh->parser2rebuild)
    {
        cache_txn_destroy(onlinerefresh->parser2rebuild);
    }

    if (onlinerefresh->rebuild2sync)
    {
        cache_txn_destroy(onlinerefresh->rebuild2sync);
    }

    rfree(onlinerefresh);
}
