#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/mpage/mpage.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/conn/ripple_conn.h"
#include "utils/string/stringinfo.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "sync/ripple_sync.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "parser/trail/ripple_parsertrail.h"
#include "rebuild/ripple_rebuild.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "increment/integrate/parser/ripple_increment_integrateparsertrail.h"
#include "increment/integrate/split/ripple_increment_integratesplittrail.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratefilterdataset.h"
#include "onlinerefresh/ripple_onlinerefresh_persist.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/integrate/split/ripple_bigtxn_integratesplittrail.h"
#include "bigtransaction/integrate/parser/ripple_bigtxn_integrateparsertrail.h"
#include "bigtransaction/integrate/sync/ripple_bigtxn_integratesync.h"
#include "bigtransaction/integrate/rebuild/ripple_bigtxn_integraterebuild.h"
#include "bigtransaction/integrate/ripple_bigtxn_integratemanager.h"
#include "increment/integrate/sync/ripple_increment_integratesync.h"
#include "increment/integrate/rebuild/ripple_increment_integraterebuild.h"
#include "metric/integrate/ripple_metric_integrate.h"
#include "increment/integrate/ripple_increment_integrate.h"

typedef enum RIPPLE_BIGTXN_INTEGRATE_STAT
{
    RIPPLE_BIGTXN_INTEGRATE_STAT_JOBNOP                         = 0x00,
    RIPPLE_BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING     ,               /* 工作线程启动中 */
    RIPPLE_BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE                         /* 等待工作线程工作完成 */
} ripple_bigtxn_integrate_stat;

/* 设置状态 */
void ripple_bigtxn_integratemanager_stat_set(ripple_bigtxn_integratemanager* bigtxnmgr, int stat)
{
    bigtxnmgr->stat = stat;
}

/* 向sync状态表中添加增量数据 */
static bool ripple_bigtxn_integratemanager_setsynctable(ripple_bigtxn_integratemanager* bigtxnmgr, ripple_thrnode* thrnode)
{
    char* conninfo          = NULL;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

    conninfo = guc_getConfigOption(RIPPLE_CFG_KEY_URL);

ripple_bigtxn_integratemanager_setsynctableretry:
    sleep(1);
    if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
    {
        return false;
    }

    conn = ripple_conn_get(conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        goto ripple_bigtxn_integratemanager_setsynctableretry;
    }

    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql, "INSERT INTO \"%s\".\"%s\" \n"
                          "(name, type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) \n"
                          "VALUES (\'%s-%lu\', %hd, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (name) DO UPDATE SET \n"
                          "(type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) =  \n"
                          "(EXCLUDED.type, EXCLUDED.stat, EXCLUDED.rewind_fileid, EXCLUDED.rewind_offset, EXCLUDED.emit_fileid, EXCLUDED.emit_offset, EXCLUDED.xid, EXCLUDED.lsn); ",
                          catalog_schema,
                          RIPPLE_SYNC_STATUSTABLE_NAME,
                          RIPPLE_STORAGE_BIG_TRANSACTION_DIR,
                          bigtxnmgr->xid,
                          3);
    res = ripple_conn_exec(conn, sql->data);
    if (NULL == res)
    {
        elog(RLOG_WARNING, "Execute commit failed");
        deleteStringInfo(sql);;
        goto ripple_bigtxn_integratemanager_setsynctableretry;
    }
    PQclear(res);
    PQfinish(conn);
    deleteStringInfo(sql);

    return true;
}

/* 向sync状态表中添加增量数据 */
static bool ripple_bigtxn_integratemanager_checksyncstat(ripple_bigtxn_integratemanager* bigtxnmgr, int16* stat)
{
    char* conninfo          = NULL;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

    *stat = 0;

    conninfo = guc_getConfigOption(RIPPLE_CFG_KEY_URL);

    conn = ripple_conn_get(conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        return false;
    }

    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(RIPPLE_CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql, "select stat from \"%s\".\"%s\" where name = \'%s-%lu\';",
                          catalog_schema,
                          RIPPLE_SYNC_STATUSTABLE_NAME,
                          RIPPLE_STORAGE_BIG_TRANSACTION_DIR,
                          bigtxnmgr->xid);

    res = ripple_conn_exec(conn, sql->data);
    if (NULL == res)
    {
        elog(RLOG_WARNING, "Execute commit failed");
        deleteStringInfo(sql);
        return false;
    }

    if (PQntuples(res) == 1)
    {
        /*lsn信息*/
        sscanf(PQgetvalue(res, 0, 0), "%hd", stat);
        elog(RLOG_WARNING, " stat %hd", *stat);
    }

    PQclear(res);
    PQfinish(conn);
    deleteStringInfo(sql);

    return true;
}

/* 启动增量工作线程 */
static bool ripple_bigtxn_integratemanager_startincrementjob(ripple_bigtxn_integratemanager* bigtxnmgr)
{
    ripple_bigtxn_integratesplittrail* bigtxnloadrecord         = NULL;
    ripple_bigtxn_integrateparsertrail* bigtxnparser            = NULL;
    ripple_bigtxn_integrateincsync* bigtxnsync                  = NULL;
    ripple_bigtxn_integraterebuild* bigtxnrebuild               = NULL;

    /*---------------------应用线程 begin------------------*/
    bigtxnsync = ripple_bigtxn_integrateincsync_init();
    if(NULL == bigtxnsync)
    {
        elog(RLOG_WARNING, "bigtxn integrate sync init error");
        return false;
    }
    bigtxnsync->rebuild2sync = bigtxnmgr->rebuild2sync;
    bigtxnsync->base.conninfo = guc_getConfigOption(RIPPLE_CFG_KEY_URL);
    bigtxnsync->base.name = (char*)rmalloc0(RIPPLE_NAMEDATALEN);
    if(NULL == bigtxnsync->base.name)
    {
        elog(RLOG_WARNING, "malloc bigtxnsync out of memory");
        ripple_bigtxn_integrateincsync_free(bigtxnsync);
        return false;
    }
    rmemset0(bigtxnsync->base.name, 0, '\0', RIPPLE_NAMEDATALEN);
    sprintf(bigtxnsync->base.name, "%s-%lu", RIPPLE_STORAGE_BIG_TRANSACTION_DIR, bigtxnmgr->xid);

    /*---------------------应用线程   end------------------*/

    /*---------------------事务重组线程 begin----------------*/
    bigtxnrebuild = ripple_bigtxn_integraterebuild_init();
    if(NULL == bigtxnrebuild)
    {
        elog(RLOG_WARNING, "bigtxn integrate rebuild init error");
        return false;
    }
    bigtxnrebuild->parser2rebuild = bigtxnmgr->parser2rebuild;
    bigtxnrebuild->rebuild2sync = bigtxnmgr->rebuild2sync;
    bigtxnrebuild->honlinerefreshfilterdataset = bigtxnmgr->honlinerefreshfilterdataset;
    bigtxnrebuild->onlinerefreshdataset = bigtxnmgr->onlinerefreshdataset;
    /*---------------------事务重组线程   end----------------*/

    /*---------------------解析器线程 begin----------------*/
    /* 
     * 解析器初始化
     * parserwal回调设置
    */
    bigtxnparser = ripple_bigtxn_integrateparsertrail_init();
    if(NULL == bigtxnparser)
    {
        elog(RLOG_WARNING, "bigtxn integrate parsertrail init error");
        return false;
    }
    bigtxnparser->decodingctx->parsertrail.parser2txn = bigtxnmgr->parser2rebuild;
    bigtxnparser->decodingctx->recordscache = bigtxnmgr->recordscache;

    /*---------------------trail拆分线程 begin--------------*/
    bigtxnloadrecord = ripple_bigtxn_integratesplittrail_init();
    if(NULL == bigtxnloadrecord)
    {
        elog(RLOG_WARNING, "bigtxn integrate init splittrail error");
        return false;
    }
    rmemset1(bigtxnloadrecord->splittrailctx->capturedata, 0, '\0', MAXPGPATH);
    snprintf(bigtxnloadrecord->splittrailctx->capturedata, MAXPGPATH, "%s/%s/%lu", guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR), RIPPLE_STORAGE_BIG_TRANSACTION_DIR, bigtxnmgr->xid);
   
    if(false ==  ripple_loadtrailrecords_setloadsource(bigtxnloadrecord->splittrailctx->loadrecords, bigtxnloadrecord->splittrailctx->capturedata))
    {
        elog(RLOG_WARNING, "integrate bigtxn set capture data error");
        return false;
    }
    bigtxnloadrecord->splittrailctx->recordscache = bigtxnmgr->recordscache;
    /*---------------------trail拆分线程   end--------------*/

    /*
     * 启动各线程
     */
    /* 注册应用线程 */
    if(false == ripple_threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNSYNC,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnsync,
                                            ripple_bigtxn_integrateincsync_free,
                                            NULL,
                                            ripple_bigtxn_integrateincsync_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment sync job error");
        return false;
    }
    
    /* 注册事务重组线程 */
    if(false == ripple_threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNREBUILD,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnrebuild,
                                            ripple_bigtxn_integraterebuild_free,
                                            NULL,
                                            ripple_bigtxn_integraterebuild_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment rebuild job error");
        return false;
    }

    /* 注册解析器线程 */
    if(false == ripple_threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNPARSER,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnparser,
                                            ripple_bigtxn_integrateparsertrail_free,
                                            NULL,
                                            ripple_bigtxn_integrateparsertrail_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment parsertrail job error");
        return false;
    }

    /* 注册 loadrecords线程 */
    if(false == ripple_threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNLOADRECORDS,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnloadrecord,
                                            ripple_bigtxn_integratesplittrail_free,
                                            NULL,
                                            ripple_bigtxn_integratesplittrail_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment splittrail job error");
        return false;
    }
    return true;
}

ripple_bigtxn_integratemanager* ripple_bigtxn_integratemanager_init(void)
{
    ripple_bigtxn_integratemanager* bigtxnmgr= NULL;

    bigtxnmgr = (ripple_bigtxn_integratemanager*)rmalloc0(sizeof(ripple_bigtxn_integratemanager));
    if (NULL == bigtxnmgr)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
  }

    rmemset0(bigtxnmgr, 0, '\0', sizeof(ripple_bigtxn_integratemanager));
    ripple_bigtxn_integratemanager_stat_set(bigtxnmgr, RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_NOP);
    bigtxnmgr->thrsmgr = NULL;
    bigtxnmgr->honlinerefreshfilterdataset = NULL;
    bigtxnmgr->onlinerefreshdataset = NULL;
    bigtxnmgr->recordscache = ripple_queue_init();
    bigtxnmgr->parser2rebuild = ripple_cache_txn_init();
    bigtxnmgr->rebuild2sync = ripple_cache_txn_init();
    return bigtxnmgr;
}

void* ripple_bigtxn_integratemanager_main(void *args)
{
    int16 syncstat                                      = 0;
    int jobcnt                                          = 0;
    ripple_bigtxn_integrate_stat jobstat                = RIPPLE_BIGTXN_INTEGRATE_STAT_JOBNOP;
    ListCell* lc                                        = NULL;
    ripple_thrref* thrref                               = NULL;
    ripple_thrnode* thrnode                             = NULL;
    ripple_thrnode* bigtxnsyncthrnode                   = NULL;
    ripple_thrnode* bigtxnrebuildthrnode                = NULL;
    ripple_thrnode* bigtxnparsertrailthrnode            = NULL;
    ripple_thrnode* bigtxnloadrecthrnode                = NULL;
    ripple_bigtxn_integratemanager* integratemgr        = NULL;

    thrnode = (ripple_thrnode*)args;

    integratemgr = (ripple_bigtxn_integratemanager*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn mgr stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    ripple_bigtxn_integratemanager_checksyncstat(integratemgr, &syncstat);
    if (1 == syncstat)
    {
        elog(RLOG_DEBUG, "integrate bigtxn %lu already completed", integratemgr->xid);
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    if(false == ripple_bigtxn_integratemanager_setsynctable(integratemgr, thrnode))
    {
        elog(RLOG_WARNING, "bigtxn integrate set synctable error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    if(false == ripple_bigtxn_integratemanager_startincrementjob(integratemgr))
    {
        elog(RLOG_WARNING, "bigtxn integrate start job thread error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 预先获取到 increment->parser 线程, 方面后续的逻辑判断 */
    /* 获取 loadrecord 线程 */
    lc = integratemgr->thrsmgr->childthrrefs->head;
    thrref = (ripple_thrref*)lfirst(lc);
    bigtxnloadrecthrnode = ripple_threads_getthrnodebyno(integratemgr->thrsmgr->parents, thrref->no);
    if(NULL == bigtxnloadrecthrnode)
    {
        elog(RLOG_WARNING, "bigtxn integratemanager can not get load record thread by no:%lu", thrref->no);
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 获取 parser 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    bigtxnparsertrailthrnode = ripple_threads_getthrnodebyno(integratemgr->thrsmgr->parents, thrref->no);
    if(NULL == bigtxnparsertrailthrnode)
    {
        elog(RLOG_WARNING, "bigtxn integratemanager can not get parser thread by no:%lu", thrref->no);
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 获取 rebuild 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    bigtxnrebuildthrnode = ripple_threads_getthrnodebyno(integratemgr->thrsmgr->parents, thrref->no);
    if(NULL == bigtxnrebuildthrnode)
    {
        elog(RLOG_WARNING, "bigtxn integratemanager can not get rebuild thread by no:%lu", thrref->no);
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 获取 sync 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    bigtxnsyncthrnode = ripple_threads_getthrnodebyno(integratemgr->thrsmgr->parents, thrref->no);
    if(NULL == bigtxnsyncthrnode)
    {
        elog(RLOG_WARNING, "bigtxn integratemanager can not get sync thread by no:%lu", thrref->no);
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    jobstat = RIPPLE_BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING;
    /* 进入工作 */
    while(1)
    {
        usleep(50000);
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            integratemgr->stat = RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_SIGTERM;
            ripple_bigtxn_integratemanager_checksyncstat(integratemgr, &syncstat);
            if (1 == syncstat)
            {
                integratemgr->stat = RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_EXIT;
            }
            elog(RLOG_DEBUG, "bigtxn integratemgr %d", integratemgr->stat);

            break;
        }

        /* 等待子线程全部启动成功 */
        if(RIPPLE_BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING == jobstat)
        {
            /* 查看是否已经启动成功 */
            jobcnt = 0;
            if(false == ripple_threads_countsubmgrjobthredsabovework(integratemgr->thrsmgr->parents,
                                                                     integratemgr->thrsmgr->childthrrefs,
                                                                     &jobcnt))
            {
                elog(RLOG_WARNING, "integrate bigtxn count job thread above work stat error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }

            if(jobcnt != integratemgr->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = RIPPLE_BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE;
            continue;
        }
        else if(RIPPLE_BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE == jobstat)
        {
            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnsyncthrnode->stat
              ||RIPPLE_THRNODE_STAT_EXITED != bigtxnparsertrailthrnode->stat)
            {
                /* 解析线程未退出, 等待 */
                continue;
            }

            /* 设置 loadrecords 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnloadrecthrnode->stat)
            {
                bigtxnloadrecthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            /* 设置 rebuild 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > bigtxnrebuildthrnode->stat)
            {
                bigtxnrebuildthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                continue;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != bigtxnloadrecthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != bigtxnrebuildthrnode->stat)
            {
                /* loadrecords、rebuild线程未退出, 等待 */
                continue;
            }

            /* 所有线程都已经退出, 管理线程可以退出了 */
            jobcnt = integratemgr->thrsmgr->childthrrefs->length;
            ripple_threads_setsubmgrjobthredsfree(integratemgr->thrsmgr->parents,
                                                integratemgr->thrsmgr->childthrrefs,
                                                0,
                                                jobcnt);
            
            /* 设置本线程退出 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            integratemgr->stat = RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_EXIT;
            break;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

/* 释放资源 */
void ripple_bigtxn_integratemanager_free(void* args)
{
    ripple_bigtxn_integratemanager* bigtxnmgr   = NULL;

    bigtxnmgr = (ripple_bigtxn_integratemanager*)args;

    if(NULL == bigtxnmgr)
    {
        return ;
    }

    ripple_queue_destroy(bigtxnmgr->recordscache, dlist_freevoid);

    ripple_cache_txn_destroy(bigtxnmgr->parser2rebuild);

    ripple_cache_txn_destroy(bigtxnmgr->rebuild2sync);

    if (bigtxnmgr->honlinerefreshfilterdataset)
    {
        hash_destroy(bigtxnmgr->honlinerefreshfilterdataset);
    }

    if (bigtxnmgr->onlinerefreshdataset)
    {
        dlist_free(bigtxnmgr->onlinerefreshdataset->onlinerefresh, ripple_onlinerefresh_integratedataset_free);
        rfree(bigtxnmgr->onlinerefreshdataset);
    }
    
    rfree(bigtxnmgr);
    return;
}
