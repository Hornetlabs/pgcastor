#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/conn/ripple_conn.h"
#include "utils/string/stringinfo.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "sync/ripple_sync.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "threads/ripple_threads.h"
#include "rebuild/ripple_rebuild.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "works/syncwork/ripple_refresh_integratesync.h"
#include "refresh/sharding2db/ripple_refresh_sharding2db.h"
#include "metric/integrate/ripple_metric_integrate.h"
#include "parser/trail/ripple_parsertrail.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "increment/integrate/parser/ripple_increment_integrateparsertrail.h"
#include "increment/integrate/split/ripple_increment_integratesplittrail.h"
#include "onlinerefresh/ripple_onlinerefresh_persist.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/ripple_onlinerefresh_integrate.h"
#include "onlinerefresh/integrate/splittrail/ripple_onlinerefresh_integratesplittrail.h"
#include "onlinerefresh/integrate/parsertrail/ripple_onlinerefresh_integrateparsertrail.h"
#include "onlinerefresh/integrate/sync/ripple_onlinerefresh_integrateincsyncstate.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/integrate/ripple_bigtxn_integratemanager.h"
#include "increment/integrate/sync/ripple_increment_integratesync.h"
#include "increment/integrate/rebuild/ripple_increment_integraterebuild.h"
#include "onlinerefresh/integrate/rebuild/ripple_onlinerefresh_integraterebuild.h"
#include "increment/integrate/ripple_increment_integrate.h"


typedef enum RIPPLE_ONLINEREFRESH_INTEGRATE_STAT
{
    RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBNOP                      = 0x00,
    RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILREFRESHSTARTING     ,               /* 工作线程启动中 */
    RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBWORKING                  ,               /* 存量工作线程工作状态 */
    RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTREFRESHDONE        ,               /* 等待存量工作线程工作完成 */
    RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING  ,               /* 增量工作线程启动中 */
    RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE                      /* 等待存量工作线程工作完成 */
} ripple_onlinerefresh_integrate_stat;

/* 检测abandon文件是否存在 */
static bool ripple_onlinerefresh_integrate_checkabandon(char* path)
{
    struct stat st;
    char file[RIPPLE_MAXPATH] = {'\0'};

    sprintf(file, "%s/%s", path, RIPPLE_ONLINEREFRESHABANDON_DAT);

    /* 校验文件是否存在，存在返回true */
    if(0 == stat(file, &st))
    {
        return true;
    }
    return false;
}

/* 向sync状态表中refresh数据，并truncate存量表 */
static bool ripple_onlinerefresh_integrate_gettrailno(ripple_onlinerefresh_integrate* onlinerefresh,
                                                      ripple_onlinerefresh_integratesplittrail* oliloadrecord,
                                                      char* name)
{
    uint64 trailno          = 0;
    uint64 emitoffset       = 0;
    XLogRecPtr lsn          = InvalidXLogRecPtr;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

    conn = ripple_conn_get(onlinerefresh->conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        return false;
    }

    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql, "select rewind_fileid, rewind_offset, lsn from \"%s\".\"%s\" where name = '%s';",
                            catalog_schema,
                            RIPPLE_SYNC_STATUSTABLE_NAME,
                            name);
    res = ripple_conn_exec(conn, sql->data);
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
        ripple_increment_integratesplittrail_emit_set(oliloadrecord->splittrailctx, trailno, emitoffset);

        /*lsn信息*/
        lsn = strtoul(PQgetvalue(res, 0, 2), NULL, 10);
        elog(RLOG_DEBUG, "onlinerefreshget record ripple_sync_status, trailno:%lu, emitoffset:%lu, lsn:%lu",
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
static bool ripple_onlinerefresh_integrate_refsetsynctable(ripple_onlinerefresh_integrate* onlinerefresh, ripple_thrnode* thrnode)
{
    int index               = 0;
    char* uuid              = NULL;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

ripple_onlinerefresh_integrate_setsynctableretry:
    sleep(1);
    if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
    {
        return false;
    }

    conn = ripple_conn_get(onlinerefresh->conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        goto ripple_onlinerefresh_integrate_setsynctableretry;
    }

    if (false == ripple_conn_begin(conn))
    {
        elog(RLOG_WARNING, "Execute begin failed");
        PQfinish(conn);
        goto ripple_onlinerefresh_integrate_setsynctableretry;
    }
    sql = makeStringInfo();
    uuid = uuid2string(&onlinerefresh->no);
    catalog_schema = guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA);

    for (index = 0; index < onlinerefresh->parallelcnt; index++)
    {
        resetStringInfo(sql);
        appendStringInfo(sql, "INSERT INTO \"%s\".\"%s\" \n"
                              "(name, type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) \n"
                              "VALUES (\'%s-%s%d\', %hd, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (name) DO UPDATE SET \n"
                              "(type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) =  \n"
                              "(EXCLUDED.type, EXCLUDED.stat, EXCLUDED.rewind_fileid, EXCLUDED.rewind_offset, EXCLUDED.emit_fileid, EXCLUDED.emit_offset, EXCLUDED.xid, EXCLUDED.lsn); ",
                              catalog_schema,
                              RIPPLE_SYNC_STATUSTABLE_NAME,
                              uuid,
                              RIPPLE_REFRESH_REFRESH,
                              index,
                              2);
        res = ripple_conn_exec(conn, sql->data);
        if (NULL == res)
        {
            elog(RLOG_WARNING, "Execute commit failed");
            deleteStringInfo(sql);
            goto ripple_onlinerefresh_integrate_setsynctableretry;
        }
        PQclear(res);
    }

    /* 清空表信息 */
    if (1 == guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRUNCATETABLE))
    {
        /* 如果清空失败，重新连接数据库并执行 */
        if (false == ripple_refresh_table_syncstats_truncatetable_fromsyncstats(onlinerefresh->tablesyncstats, (void*)conn))
        {
            res = ripple_conn_exec(conn, "ROLLBACK");
            if (NULL == res)
            {
                elog(RLOG_WARNING, "Execute rollback failed");
                deleteStringInfo(sql);
                rfree(uuid);
                goto ripple_onlinerefresh_integrate_setsynctableretry;
            }
            PQclear(res);
            PQfinish(conn);
            deleteStringInfo(sql);
            rfree(uuid);
            sleep(1);
            goto ripple_onlinerefresh_integrate_setsynctableretry;
        }
    }

    if (false == ripple_conn_commit(conn))
    {
        deleteStringInfo(sql);
        PQfinish(conn);
        rfree(uuid);
        goto ripple_onlinerefresh_integrate_setsynctableretry;
    }
    PQfinish(conn);
    rfree(uuid);
    deleteStringInfo(sql);

    return true;
}

/* 向sync状态表添加增量数据 */
static bool ripple_onlinerefresh_integrate_incsetsynctable(ripple_onlinerefresh_integrate* onlinerefresh, ripple_thrnode* thrnode)
{
    char* uuid              = NULL;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

onlinerefresh_integrate_incsetsynctableretry:
    sleep(1);
    if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
    {
        return false;
    }

    conn = ripple_conn_get(onlinerefresh->conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        goto onlinerefresh_integrate_incsetsynctableretry;
    }

    sql = makeStringInfo();
    uuid = uuid2string(&onlinerefresh->no);
    catalog_schema = guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql, "INSERT INTO \"%s\".\"%s\" \n"
                          "(name, type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) \n"
                          "VALUES (\'%s-%s\', %hd, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (name) DO UPDATE SET \n"
                          "(type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) =  \n"
                          "(EXCLUDED.type, EXCLUDED.stat, EXCLUDED.rewind_fileid, EXCLUDED.rewind_offset, EXCLUDED.emit_fileid, EXCLUDED.emit_offset, EXCLUDED.xid, EXCLUDED.lsn); ",
                          catalog_schema,
                          RIPPLE_SYNC_STATUSTABLE_NAME,
                          uuid,
                          RIPPLE_REFRESH_INCREMENT,
                          2);
    res = ripple_conn_exec(conn, sql->data);
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
bool ripple_onlinerefresh_integrate_isconflict(dlistnode* in_dlnode)
{
    dlistnode* dlnode = NULL;
    ripple_onlinerefresh_integrate* onlinerefresh_node = NULL;
    ripple_onlinerefresh_integrate* current_node = NULL;

    dlnode = in_dlnode->prev;
    if(NULL == dlnode)
    {
        return false;
    }
    current_node = (ripple_onlinerefresh_integrate*)in_dlnode->value;

    for(; NULL != dlnode; dlnode = dlnode->prev)
    {
        onlinerefresh_node = (ripple_onlinerefresh_integrate*)dlnode->value;
        
        if (false == ripple_refresh_table_syncstats_compare(onlinerefresh_node->tablesyncstats, current_node->tablesyncstats))
        {
            continue;
        }

        if (RIPPLE_ONLINEREFRESH_INTEGRATE_DONE <= onlinerefresh_node->stat)
        {
            break;
        }
        return true;
    }
    return false;
}

bool ripple_onlinerefresh_integrate_persist2onlinerefreshmgr(ripple_onlinerefresh_persist *persist, void **onlinerefresh)
{
    dlist *result                           = NULL;
    dlistnode *dnode                        = NULL;
    ripple_onlinerefresh_integrate *olrmgr  = NULL;

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
        ripple_onlinerefresh_persistnode *persistnode = (ripple_onlinerefresh_persistnode *)dnode->value;

        /* 已完成的和放弃掉的不再处理 */
        if (RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_DONE == persistnode->stat
            || RIPPLE_ONLINEREFRESH_PERSISTNODE_STAT_ABANDON == persistnode->stat)
        {
            continue;
        }

        /* 包含了对txn的创建 */
        /* 构建manager并初始化 设置xid 和 begin*/
        olrmgr = ripple_onlinerefresh_integrate_init(persistnode->increment);
        if(NULL == olrmgr)
        {
            elog(RLOG_WARNING, "onlinerefres init onlinerefresh error");
            return false;
        }

        rmemcpy1(olrmgr->no.data, 0, persistnode->uuid.data, RIPPLE_UUID_LEN);
        olrmgr->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_INIT;
        olrmgr->increment = persistnode->increment;
        olrmgr->txid = persistnode->txid;
        olrmgr->begin.trail.fileid = persistnode->begin.trail.fileid;
        olrmgr->begin.trail.offset = persistnode->begin.trail.offset;
        ripple_refresh_table_syncstats_tablesyncing2tablesyncall(persistnode->refreshtbs);
        olrmgr->tablesyncstats = persistnode->refreshtbs;
        result = dlist_put(result, olrmgr);
        olrmgr = NULL;
    }

    *onlinerefresh = (void*)result;
    return true;
}

/* 启动 refresh 工作线程 */
static bool ripple_onlinerefresh_integrate_startrefreshjob(ripple_onlinerefresh_integrate* olintegrate)
{
    int index                               = 0;
    char* uuid                              = NULL;
    ripple_refresh_sharding2db *shard2db    = NULL;

    /* 为每一个线程分配空间 */
    for (index = 0; index < olintegrate->parallelcnt; index++)
    {
        /* 分配空间和初始化 */
        shard2db = ripple_refresh_sharding2db_init();
        if (NULL == shard2db)
        {
            return false;
        }
        shard2db->name = (char*)rmalloc0(RIPPLE_NAMEDATALEN);
        if(NULL == shard2db->name)
        {
            elog(RLOG_WARNING, "malloc sharding2dbname out of memory");
            ripple_refresh_sharding2db_free(shard2db);
            return false;
        }
        rmemset0(shard2db->name, 0, '\0', RIPPLE_NAMEDATALEN);
        uuid = uuid2string(&olintegrate->no);
        sprintf(shard2db->name, "%s-%s%d", uuid, RIPPLE_REFRESH_REFRESH, index);
        rfree(uuid);
        shard2db->syncstats->base.conn = NULL;
        shard2db->refresh_path = olintegrate->data;
        shard2db->syncstats->base.conninfo = olintegrate->conninfo;
        shard2db->syncstats->tablesyncstats = olintegrate->tablesyncstats;
        shard2db->syncstats->queue = olintegrate->tqueue;

        /* 注册工作线程 */
        if(false == ripple_threads_addjobthread(olintegrate->thrsmgr->parents,
                                                RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_JOB,
                                                olintegrate->thrsmgr->submgrref.no,
                                                (void*)shard2db,
                                                ripple_refresh_sharding2db_free,
                                                NULL,
                                                ripple_refresh_sharding2db_work))
        {
            elog(RLOG_WARNING, "refresh integrate start job error");
            return false;
        }
    }
    return true;
}

/* 启动增量工作线程 */
static bool ripple_onlinerefresh_integrate_startincrementjob(ripple_onlinerefresh_integrate* olintegrate)
{
    char* uuid                                                      = NULL;
    ripple_onlinerefresh_integratesplittrail* oliloadrecord         = NULL;
    ripple_onlinerefresh_integrateparsertrail* oliparsertrail       = NULL;
    ripple_onlinerefresh_integrateincsync* olisync                  = NULL;
    ripple_onlinerefresh_integraterebuild* olirebuild               = NULL;

    if(false == olintegrate->increment)
    {
        /* 不需要启动增量 */
        return true;
    }

    /*---------------------应用线程 begin------------------*/
    olisync = ripple_onlinerefresh_integrateincsync_init();
    if(NULL == olisync)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate sync init error");
        return false;
    }
    olisync->base.name = (char*)rmalloc0(RIPPLE_NAMEDATALEN);
    if(NULL == olisync->base.name)
    {
        elog(RLOG_WARNING, "malloc sharding2dbname out of memory");
        ripple_onlinerefresh_integrateincsync_free(olisync);
        return false;
    }
    rmemset0(olisync->base.name, 0, '\0', RIPPLE_NAMEDATALEN);
    uuid = uuid2string(&olintegrate->no);
    sprintf(olisync->base.name, "%s-%s", uuid, RIPPLE_REFRESH_INCREMENT);
    rfree(uuid);
    olisync->rebuild2sync = olintegrate->rebuild2sync;
    olisync->base.conninfo = guc_getConfigOption(RIPPLE_CFG_KEY_URL);

    /*---------------------应用线程   end------------------*/

    /*---------------------事务重组线程 begin----------------*/
    olirebuild = ripple_onlinerefresh_integraterebuild_init();
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
    oliparsertrail = ripple_onlinerefresh_integrateparsertrail_init();
    if(NULL == olirebuild)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate parsertrail init error");
        return false;
    }
    oliparsertrail->decodingctx->parsertrail.parser2txn = olintegrate->parser2rebuild;
    oliparsertrail->decodingctx->recordscache = olintegrate->recordscache;
    /*---------------------解析器线程   end----------------*/

    /*---------------------trail拆分线程 begin--------------*/
    oliloadrecord = ripple_onlinerefresh_integratesplittrail_init();
    if(NULL == oliloadrecord)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate init splittrail error");
        return false;
    }
    rmemset1(oliloadrecord->splittrailctx->capturedata, 0, '\0', MAXPGPATH);
    snprintf(oliloadrecord->splittrailctx->capturedata, MAXPGPATH, "%s/%s" , olintegrate->data, RIPPLE_STORAGE_TRAIL_DIR);

    /* 设置加载的路径 */
    if(false == ripple_loadtrailrecords_setloadsource(oliloadrecord->splittrailctx->loadrecords, oliloadrecord->splittrailctx->capturedata))
    {
        elog(RLOG_WARNING, "integrate onlinerefresh set capture data error");
        return false;
    }
    oliloadrecord->splittrailctx->recordscache = olintegrate->recordscache;
    ripple_onlinerefresh_integrate_gettrailno(olintegrate, oliloadrecord, olisync->base.name);
    /*---------------------trail拆分线程   end--------------*/

    /*
     * 启动各线程
     */
    /* 注册应用线程 */
    if(false == ripple_threads_addjobthread(olintegrate->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_SYNC,
                                            olintegrate->thrsmgr->submgrref.no,
                                            (void*)olisync,
                                            ripple_onlinerefresh_integrateincsync_free,
                                            NULL,
                                            ripple_onlinerefresh_integrateincsync_main))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start increment sync job error");
        return false;
    }
    
    /* 注册事务重组线程 */
    if(false == ripple_threads_addjobthread(olintegrate->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_REBUILD,
                                            olintegrate->thrsmgr->submgrref.no,
                                            (void*)olirebuild,
                                            ripple_onlinerefresh_integraterebuild_free,
                                            NULL,
                                            ripple_onlinerefresh_integraterebuild_main))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start increment rebuild job error");
        return false;
    }

    /* 注册解析器线程 */
    if(false == ripple_threads_addjobthread(olintegrate->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_PARSER,
                                            olintegrate->thrsmgr->submgrref.no,
                                            (void*)oliparsertrail,
                                            ripple_onlinerefresh_integrateparsertrail_free,
                                            NULL,
                                            ripple_onlinerefresh_integrateparsertrail_main))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start increment parsertrail job error");
        return false;
    }

    /* 注册 loadrecords线程 */
    if(false == ripple_threads_addjobthread(olintegrate->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_LOADRECORDS,
                                            olintegrate->thrsmgr->submgrref.no,
                                            (void*)oliloadrecord,
                                            ripple_onlinerefresh_integratesplittrail_free,
                                            NULL,
                                            ripple_onlinerefresh_integratesplittrail_main))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start increment splittrail job error");
        return false;
    }
    return true;
}

/* 线程处理入口 */
void *ripple_onlinerefresh_integrate_manage(void* args)
{
    int jobcnt                                      = 0;
    ripple_onlinerefresh_integrate_stat jobstat     = RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBNOP;
    char* uuid                                      = NULL;
    ListCell* lc                                    = NULL;
    ripple_thrref* thrref                           = NULL;
    ripple_thrnode* thrnode                         = NULL;
    ripple_thrnode* incsyncthrnode                  = NULL;
    ripple_thrnode* increbuildthrnode               = NULL;
    ripple_thrnode* incparserthrnode                = NULL;
    ripple_thrnode* incloadrecthrnode               = NULL;
    ripple_onlinerefresh_integrate *olintegrate     = NULL;

    elog(RLOG_INFO, "start integrate online refresh manage");

    thrnode = (ripple_thrnode *)args;
    olintegrate = (ripple_onlinerefresh_integrate *)thrnode->data;

    uuid = uuid2string(&olintegrate->no);
    sprintf(olintegrate->data, "%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR),
                                             RIPPLE_REFRESH_ONLINEREFRESH,
                                             uuid);

    rfree(uuid);

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate stat exception, expected stat is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    olintegrate->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_RUNNING;

    if(false == ripple_onlinerefresh_integrate_refsetsynctable(olintegrate, thrnode))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate set synctable error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 
     * 存量注册工作线程
     */
    if(false == ripple_onlinerefresh_integrate_startrefreshjob(olintegrate))
    {
        elog(RLOG_WARNING, "onlinerefresh integrate start refresh job thread error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    jobstat = RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILREFRESHSTARTING;
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
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            ripple_pthread_exit(NULL);
            break;
        }

        if (true == ripple_onlinerefresh_integrate_checkabandon(olintegrate->data))
        {
            if (RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING > jobstat)
            {
                /* 设置空闲的线程退出并统计退出的线程个数 */
                jobcnt = olintegrate->parallelcnt;
                if(false == ripple_threads_setsubmgrjobthredstermandcountexit(olintegrate->thrsmgr->parents,
                                                                            olintegrate->thrsmgr->childthrrefs,
                                                                            0,
                                                                            &jobcnt))
                {
                    elog(RLOG_WARNING, "integrate refresh set job threads term in idle error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    goto ripple_onlinerefresh_integrate_main_done;
                }

                if(jobcnt != olintegrate->parallelcnt)
                {
                    continue;
                }
                olintegrate->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_ABANDONED;
                break;
            }

            if (false == olintegrate->increment)
            {
                olintegrate->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_ABANDONED;
                break;
            }
            
            if (incsyncthrnode != NULL
                && increbuildthrnode != NULL
                && incparserthrnode != NULL
                && incloadrecthrnode != NULL)
            {
                /* 设置 sync 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > incsyncthrnode->stat)
                {
                    incsyncthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                    continue;
                }
                
                /* 设置 rebuild 线程退出 */
                if(RIPPLE_THRNODE_STAT_TERM > increbuildthrnode->stat)
                {
                    increbuildthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
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
                    || RIPPLE_THRNODE_STAT_EXITED != incsyncthrnode->stat
                    || RIPPLE_THRNODE_STAT_EXITED != increbuildthrnode->stat)
                {
                    continue;
                }
            }
            olintegrate->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_ABANDONED;
            break;
        }

        /* 等待子线程全部启动成功 */
        if(RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILREFRESHSTARTING == jobstat)
        {
            /* 查看是否已经启动成功 */
            jobcnt = 0;
            if(false == ripple_threads_countsubmgrjobthredsabovework(olintegrate->thrsmgr->parents,
                                                                     olintegrate->thrsmgr->childthrrefs,
                                                                     &jobcnt))
            {
                elog(RLOG_WARNING, "capture onlinerefresh count job thread above work stat error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_onlinerefresh_integrate_main_done;
            }

            if(jobcnt != olintegrate->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBWORKING;
            continue;
        }
        else if(RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBWORKING == jobstat)
        {
            /* 工作线程已经启动, 那么向队列中加入工作任务 */
            if(false == ripple_refresh_table_syncstat_genqueue(olintegrate->tablesyncstats, (void*)olintegrate->tqueue, olintegrate->data))
            {
                /* 向队列中加入任务失败, 那么管理线程退出, 子线程的回收由主线程处理 */
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }

            /* 首先判断是否存在任务和待同步表 */
            if (NULL ==  olintegrate->tablesyncstats->tablesyncing)
            {
                jobstat = RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTREFRESHDONE;
            }
            continue;
        }
        else if (RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTREFRESHDONE == jobstat)
        {
            /*
             * 等待存量线程退出
             *  1、队列为空
             *  2、存量线程完全退出
             */
            if(false == ripple_queue_isnull(olintegrate->tqueue))
            {
                /* 队列不为空, 证明还有任务需要处理 */
                continue;
            }

            /* 设置空闲的线程退出并统计退出的线程个数 */
            jobcnt = olintegrate->parallelcnt;
            if(false == ripple_threads_setsubmgrjobthredstermandcountexit(olintegrate->thrsmgr->parents,
                                                                         olintegrate->thrsmgr->childthrrefs,
                                                                         0,
                                                                         &jobcnt))
            {
                elog(RLOG_WARNING, "integrate refresh set job threads term in idle error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_onlinerefresh_integrate_main_done;
            }

            if(jobcnt != olintegrate->parallelcnt)
            {
                continue;
            }

            jobstat = RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING;

            olintegrate->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_REFRESHDONE;
            continue;
        }
        else if(RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING == jobstat)
        {
            if(true == olintegrate->increment)
            {
                if(false == ripple_onlinerefresh_integrate_incsetsynctable(olintegrate, thrnode))
                {
                    elog(RLOG_WARNING, "onlinerefresh integrate increment set synctable error");
                    continue;
                }

                /* 启动增量工作线程 */
                if(false == ripple_onlinerefresh_integrate_startincrementjob(olintegrate))
                {
                    elog(RLOG_WARNING, "onlinerefresh integrate start increment thread error");
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }

                /* 预先获取到 increment->parser 线程, 方面后续的逻辑判断 */
                /* 获取 loadrecord 线程 */
                lc = olintegrate->thrsmgr->childthrrefs->head;
                thrref = (ripple_thrref*)lfirst(lc);
                incloadrecthrnode = ripple_threads_getthrnodebyno(olintegrate->thrsmgr->parents, thrref->no);
                if(NULL == incloadrecthrnode)
                {
                    elog(RLOG_WARNING, "integrate onlinerefresh can not get load record thread by no:%lu", thrref->no);
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    ripple_pthread_exit(NULL);
                }

                /* 获取 parser 线程 */
                lc = lc->next;
                thrref = (ripple_thrref*)lfirst(lc);
                incparserthrnode = ripple_threads_getthrnodebyno(olintegrate->thrsmgr->parents, thrref->no);
                if(NULL == incparserthrnode)
                {
                    elog(RLOG_WARNING, "integrate onlinerefresh can not get parser thread by no:%lu", thrref->no);
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    ripple_pthread_exit(NULL);
                }

                /* 获取 serail 线程 */
                lc = lc->next;
                thrref = (ripple_thrref*)lfirst(lc);
                increbuildthrnode = ripple_threads_getthrnodebyno(olintegrate->thrsmgr->parents, thrref->no);
                if(NULL == increbuildthrnode)
                {
                    elog(RLOG_WARNING, "integrate onlinerefresh can not get rebuild thread by no:%lu", thrref->no);
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    ripple_pthread_exit(NULL);
                }

                /* 获取 flush 线程 */
                lc = lc->next;
                thrref = (ripple_thrref*)lfirst(lc);
                incsyncthrnode = ripple_threads_getthrnodebyno(olintegrate->thrsmgr->parents, thrref->no);
                if(NULL == incsyncthrnode)
                {
                    elog(RLOG_WARNING, "integrate onlinerefresh can not get sync thread by no:%lu", thrref->no);
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    ripple_pthread_exit(NULL);
                }
            }

            jobstat = RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE;
        }
        else if (RIPPLE_ONLINEREFRESH_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE == jobstat)
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
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                break;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != incparserthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != incsyncthrnode->stat)
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

            /* 设置 rebuild 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > increbuildthrnode->stat)
            {
                increbuildthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != incloadrecthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != increbuildthrnode->stat)
            {
                continue;
            }
            
            /* 设置本线程退出 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            olintegrate->stat = RIPPLE_ONLINEREFRESH_INTEGRATE_DONE;
            break;
        }
        
    }

ripple_onlinerefresh_integrate_main_done:
    /* 所有线程都已经退出, 管理线程可以退出了 */
    jobcnt = olintegrate->thrsmgr->childthrrefs->length;
    ripple_threads_setsubmgrjobthredsfree(olintegrate->thrsmgr->parents,
                                          olintegrate->thrsmgr->childthrrefs,
                                          0,
                                          jobcnt);
    /* make compiler happy */
    return NULL;
}

ripple_onlinerefresh_integrate *ripple_onlinerefresh_integrate_init(bool increment)
{
    ripple_onlinerefresh_integrate *onlinerefresh = NULL;

    onlinerefresh = (ripple_onlinerefresh_integrate *)rmalloc0(sizeof(ripple_onlinerefresh_integrate));
    if (NULL == onlinerefresh)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(onlinerefresh, 0, 0, sizeof(ripple_onlinerefresh_integrate));

    onlinerefresh->tablesyncstats = NULL;
    onlinerefresh->txid = InvalidFullTransactionId;

    onlinerefresh->parallelcnt = guc_getConfigOptionInt(RIPPLE_CFG_KEY_MAX_WORK_PER_REFRESH);
    if(0 == onlinerefresh->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }

    onlinerefresh->conninfo = guc_getConfigOption(RIPPLE_CFG_KEY_URL);

    onlinerefresh->thrsmgr = NULL;

    onlinerefresh->tqueue = ripple_queue_init();

    if(increment)
    {
        onlinerefresh->recordscache = ripple_queue_init();

        onlinerefresh->parser2rebuild = ripple_cache_txn_init();

        onlinerefresh->rebuild2sync = ripple_cache_txn_init();
    }


    return onlinerefresh;
}

void ripple_onlinerefresh_integrate_free(void* in_onlinerefresh)
{
    ripple_onlinerefresh_integrate* onlinerefresh = NULL;
    if(NULL == in_onlinerefresh)
    {
        return;
    }

    onlinerefresh = (ripple_onlinerefresh_integrate*)in_onlinerefresh;

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
    
    if (onlinerefresh->parser2rebuild)
    {
        ripple_cache_txn_destroy(onlinerefresh->parser2rebuild);
    }

    if (onlinerefresh->rebuild2sync)
    {
        ripple_cache_txn_destroy(onlinerefresh->rebuild2sync);
    }

    rfree(onlinerefresh);
}
