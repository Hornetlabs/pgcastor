#include "app_incl.h"
#include "libpq-fe.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/mpage/mpage.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/conn/conn.h"
#include "utils/string/stringinfo.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "sync/sync.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "parser/trail/parsertrail.h"
#include "rebuild/rebuild.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"
#include "increment/integrate/split/increment_integratesplittrail.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratefilterdataset.h"
#include "onlinerefresh/onlinerefresh_persist.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/integrate/split/bigtxn_integratesplittrail.h"
#include "bigtransaction/integrate/parser/bigtxn_integrateparsertrail.h"
#include "bigtransaction/integrate/sync/bigtxn_integratesync.h"
#include "bigtransaction/integrate/rebuild/bigtxn_integraterebuild.h"
#include "bigtransaction/integrate/bigtxn_integratemanager.h"
#include "increment/integrate/sync/increment_integratesync.h"
#include "increment/integrate/rebuild/increment_integraterebuild.h"
#include "metric/integrate/metric_integrate.h"
#include "increment/integrate/increment_integrate.h"

typedef enum BIGTXN_INTEGRATE_STAT
{
    BIGTXN_INTEGRATE_STAT_JOBNOP                         = 0x00,
    BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING     ,               /* 工作线程启动中 */
    BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE                         /* 等待工作线程工作完成 */
} bigtxn_integrate_stat;

/* 设置状态 */
void bigtxn_integratemanager_stat_set(bigtxn_integratemanager* bigtxnmgr, int stat)
{
    bigtxnmgr->stat = stat;
}

/* 向sync状态表中添加增量数据 */
static bool bigtxn_integratemanager_setsynctable(bigtxn_integratemanager* bigtxnmgr, thrnode* thrnode)
{
    char* conninfo          = NULL;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

    conninfo = guc_getConfigOption(CFG_KEY_URL);

bigtxn_integratemanager_setsynctableretry:
    sleep(1);
    if(THRNODE_STAT_TERM == thrnode->stat)
    {
        return false;
    }

    conn = conn_get(conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        goto bigtxn_integratemanager_setsynctableretry;
    }

    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql, "INSERT INTO \"%s\".\"%s\" \n"
                          "(name, type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) \n"
                          "VALUES (\'%s-%lu\', %hd, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (name) DO UPDATE SET \n"
                          "(type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) =  \n"
                          "(EXCLUDED.type, EXCLUDED.stat, EXCLUDED.rewind_fileid, EXCLUDED.rewind_offset, EXCLUDED.emit_fileid, EXCLUDED.emit_offset, EXCLUDED.xid, EXCLUDED.lsn); ",
                          catalog_schema,
                          SYNC_STATUSTABLE_NAME,
                          STORAGE_BIG_TRANSACTION_DIR,
                          bigtxnmgr->xid,
                          3);
    res = conn_exec(conn, sql->data);
    if (NULL == res)
    {
        elog(RLOG_WARNING, "Execute commit failed");
        deleteStringInfo(sql);;
        goto bigtxn_integratemanager_setsynctableretry;
    }
    PQclear(res);
    PQfinish(conn);
    deleteStringInfo(sql);

    return true;
}

/* 向sync状态表中添加增量数据 */
static bool bigtxn_integratemanager_checksyncstat(bigtxn_integratemanager* bigtxnmgr, int16* stat)
{
    char* conninfo          = NULL;
    char* catalog_schema    = NULL;
    PGconn *conn            = NULL;
    PGresult *res           = NULL;
    StringInfo sql          = NULL;

    *stat = 0;

    conninfo = guc_getConfigOption(CFG_KEY_URL);

    conn = conn_get(conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        return false;
    }

    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);

    resetStringInfo(sql);
    appendStringInfo(sql, "select stat from \"%s\".\"%s\" where name = \'%s-%lu\';",
                          catalog_schema,
                          SYNC_STATUSTABLE_NAME,
                          STORAGE_BIG_TRANSACTION_DIR,
                          bigtxnmgr->xid);

    res = conn_exec(conn, sql->data);
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
static bool bigtxn_integratemanager_startincrementjob(bigtxn_integratemanager* bigtxnmgr)
{
    bigtxn_integratesplittrail* bigtxnloadrecord         = NULL;
    bigtxn_integrateparsertrail* bigtxnparser            = NULL;
    bigtxn_integrateincsync* bigtxnsync                  = NULL;
    bigtxn_integraterebuild* bigtxnrebuild               = NULL;

    /*---------------------应用线程 begin------------------*/
    bigtxnsync = bigtxn_integrateincsync_init();
    if(NULL == bigtxnsync)
    {
        elog(RLOG_WARNING, "bigtxn integrate sync init error");
        return false;
    }
    bigtxnsync->rebuild2sync = bigtxnmgr->rebuild2sync;
    bigtxnsync->base.conninfo = guc_getConfigOption(CFG_KEY_URL);
    bigtxnsync->base.name = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == bigtxnsync->base.name)
    {
        elog(RLOG_WARNING, "malloc bigtxnsync out of memory");
        bigtxn_integrateincsync_free(bigtxnsync);
        return false;
    }
    rmemset0(bigtxnsync->base.name, 0, '\0', NAMEDATALEN);
    sprintf(bigtxnsync->base.name, "%s-%lu", STORAGE_BIG_TRANSACTION_DIR, bigtxnmgr->xid);

    /*---------------------应用线程   end------------------*/

    /*---------------------事务重组线程 begin----------------*/
    bigtxnrebuild = bigtxn_integraterebuild_init();
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
    bigtxnparser = bigtxn_integrateparsertrail_init();
    if(NULL == bigtxnparser)
    {
        elog(RLOG_WARNING, "bigtxn integrate parsertrail init error");
        return false;
    }
    bigtxnparser->decodingctx->parsertrail.parser2txn = bigtxnmgr->parser2rebuild;
    bigtxnparser->decodingctx->recordscache = bigtxnmgr->recordscache;

    /*---------------------trail拆分线程 begin--------------*/
    bigtxnloadrecord = bigtxn_integratesplittrail_init();
    if(NULL == bigtxnloadrecord)
    {
        elog(RLOG_WARNING, "bigtxn integrate init splittrail error");
        return false;
    }
    rmemset1(bigtxnloadrecord->splittrailctx->capturedata, 0, '\0', MAXPGPATH);
    snprintf(bigtxnloadrecord->splittrailctx->capturedata, MAXPGPATH, "%s/%s/%lu", guc_getConfigOption(CFG_KEY_TRAIL_DIR), STORAGE_BIG_TRANSACTION_DIR, bigtxnmgr->xid);
   
    if(false ==  loadtrailrecords_setloadsource(bigtxnloadrecord->splittrailctx->loadrecords, bigtxnloadrecord->splittrailctx->capturedata))
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
    if(false == threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNSYNC,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnsync,
                                            bigtxn_integrateincsync_free,
                                            NULL,
                                            bigtxn_integrateincsync_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment sync job error");
        return false;
    }
    
    /* 注册事务重组线程 */
    if(false == threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNREBUILD,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnrebuild,
                                            bigtxn_integraterebuild_free,
                                            NULL,
                                            bigtxn_integraterebuild_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment rebuild job error");
        return false;
    }

    /* 注册解析器线程 */
    if(false == threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNPARSER,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnparser,
                                            bigtxn_integrateparsertrail_free,
                                            NULL,
                                            bigtxn_integrateparsertrail_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment parsertrail job error");
        return false;
    }

    /* 注册 loadrecords线程 */
    if(false == threads_addjobthread(bigtxnmgr->thrsmgr->parents,
                                            THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNLOADRECORDS,
                                            bigtxnmgr->thrsmgr->submgrref.no,
                                            (void*)bigtxnloadrecord,
                                            bigtxn_integratesplittrail_free,
                                            NULL,
                                            bigtxn_integratesplittrail_main))
    {
        elog(RLOG_WARNING, "bigtxn integrate start increment splittrail job error");
        return false;
    }
    return true;
}

bigtxn_integratemanager* bigtxn_integratemanager_init(void)
{
    bigtxn_integratemanager* bigtxnmgr= NULL;

    bigtxnmgr = (bigtxn_integratemanager*)rmalloc0(sizeof(bigtxn_integratemanager));
    if (NULL == bigtxnmgr)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
  }

    rmemset0(bigtxnmgr, 0, '\0', sizeof(bigtxn_integratemanager));
    bigtxn_integratemanager_stat_set(bigtxnmgr, BIGTXN_INTEGRATEMANAGER_STAT_NOP);
    bigtxnmgr->thrsmgr = NULL;
    bigtxnmgr->honlinerefreshfilterdataset = NULL;
    bigtxnmgr->onlinerefreshdataset = NULL;
    bigtxnmgr->recordscache = queue_init();
    bigtxnmgr->parser2rebuild = cache_txn_init();
    bigtxnmgr->rebuild2sync = cache_txn_init();
    return bigtxnmgr;
}

void* bigtxn_integratemanager_main(void *args)
{
    int16 syncstat                                      = 0;
    int jobcnt                                          = 0;
    bigtxn_integrate_stat jobstat                = BIGTXN_INTEGRATE_STAT_JOBNOP;
    ListCell* lc                                        = NULL;
    thrref* thr_ref                               = NULL;
    thrnode* thr_node                             = NULL;
    thrnode* bigtxnsyncthrnode                   = NULL;
    thrnode* bigtxnrebuildthrnode                = NULL;
    thrnode* bigtxnparsertrailthrnode            = NULL;
    thrnode* bigtxnloadrecthrnode                = NULL;
    bigtxn_integratemanager* integratemgr        = NULL;

    thr_node = (thrnode*)args;

    integratemgr = (bigtxn_integratemanager*)thr_node->data;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn mgr stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    bigtxn_integratemanager_checksyncstat(integratemgr, &syncstat);
    if (1 == syncstat)
    {
        elog(RLOG_DEBUG, "integrate bigtxn %lu already completed", integratemgr->xid);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    if(false == bigtxn_integratemanager_setsynctable(integratemgr, thr_node))
    {
        elog(RLOG_WARNING, "bigtxn integrate set synctable error");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    if(false == bigtxn_integratemanager_startincrementjob(integratemgr))
    {
        elog(RLOG_WARNING, "bigtxn integrate start job thread error");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* 预先获取到 increment->parser 线程, 方面后续的逻辑判断 */
    /* 获取 loadrecord 线程 */
    lc = integratemgr->thrsmgr->childthrrefs->head;
    thr_ref = (thrref*)lfirst(lc);
    bigtxnloadrecthrnode = threads_getthrnodebyno(integratemgr->thrsmgr->parents, thr_ref->no);
    if(NULL == bigtxnloadrecthrnode)
    {
        elog(RLOG_WARNING, "bigtxn integratemanager can not get load record thread by no:%lu", thr_ref->no);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 获取 parser 线程 */
    lc = lc->next;
    thr_ref = (thrref*)lfirst(lc);
    bigtxnparsertrailthrnode = threads_getthrnodebyno(integratemgr->thrsmgr->parents, thr_ref->no);
    if(NULL == bigtxnparsertrailthrnode)
    {
        elog(RLOG_WARNING, "bigtxn integratemanager can not get parser thread by no:%lu", thr_ref->no);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 获取 rebuild 线程 */
    lc = lc->next;
    thr_ref = (thrref*)lfirst(lc);
    bigtxnrebuildthrnode = threads_getthrnodebyno(integratemgr->thrsmgr->parents, thr_ref->no);
    if(NULL == bigtxnrebuildthrnode)
    {
        elog(RLOG_WARNING, "bigtxn integratemanager can not get rebuild thread by no:%lu", thr_ref->no);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 获取 sync 线程 */
    lc = lc->next;
    thr_ref = (thrref*)lfirst(lc);
    bigtxnsyncthrnode = threads_getthrnodebyno(integratemgr->thrsmgr->parents, thr_ref->no);
    if(NULL == bigtxnsyncthrnode)
    {
        elog(RLOG_WARNING, "bigtxn integratemanager can not get sync thread by no:%lu", thr_ref->no);
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    jobstat = BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING;
    /* 进入工作 */
    while(1)
    {
        usleep(50000);
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            /* 序列化/落盘 */
            thr_node->stat = THRNODE_STAT_EXIT;
            integratemgr->stat = BIGTXN_INTEGRATEMANAGER_STAT_SIGTERM;
            bigtxn_integratemanager_checksyncstat(integratemgr, &syncstat);
            if (1 == syncstat)
            {
                integratemgr->stat = BIGTXN_INTEGRATEMANAGER_STAT_EXIT;
            }
            elog(RLOG_DEBUG, "bigtxn integratemgr %d", integratemgr->stat);

            break;
        }

        /* 等待子线程全部启动成功 */
        if(BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTSTARTING == jobstat)
        {
            /* 查看是否已经启动成功 */
            jobcnt = 0;
            if(false == threads_countsubmgrjobthredsabovework(integratemgr->thrsmgr->parents,
                                                                     integratemgr->thrsmgr->childthrrefs,
                                                                     &jobcnt))
            {
                elog(RLOG_WARNING, "integrate bigtxn count job thread above work stat error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }

            if(jobcnt != integratemgr->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE;
            continue;
        }
        else if(BIGTXN_INTEGRATE_STAT_JOBTRAILTINCREMENTDONE == jobstat)
        {
            if(THRNODE_STAT_EXITED != bigtxnsyncthrnode->stat
              ||THRNODE_STAT_EXITED != bigtxnparsertrailthrnode->stat)
            {
                /* 解析线程未退出, 等待 */
                continue;
            }

            /* 设置 loadrecords 线程退出 */
            if(THRNODE_STAT_TERM > bigtxnloadrecthrnode->stat)
            {
                bigtxnloadrecthrnode->stat = THRNODE_STAT_TERM;
                continue;
            }

            /* 设置 rebuild 线程退出 */
            if(THRNODE_STAT_TERM > bigtxnrebuildthrnode->stat)
            {
                bigtxnrebuildthrnode->stat = THRNODE_STAT_TERM;
                continue;
            }

            if(THRNODE_STAT_EXITED != bigtxnloadrecthrnode->stat
                || THRNODE_STAT_EXITED != bigtxnrebuildthrnode->stat)
            {
                /* loadrecords、rebuild线程未退出, 等待 */
                continue;
            }

            /* 所有线程都已经退出, 管理线程可以退出了 */
            jobcnt = integratemgr->thrsmgr->childthrrefs->length;
            threads_setsubmgrjobthredsfree(integratemgr->thrsmgr->parents,
                                                integratemgr->thrsmgr->childthrrefs,
                                                0,
                                                jobcnt);
            
            /* 设置本线程退出 */
            thr_node->stat = THRNODE_STAT_EXIT;
            integratemgr->stat = BIGTXN_INTEGRATEMANAGER_STAT_EXIT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

/* 释放资源 */
void bigtxn_integratemanager_free(void* args)
{
    bigtxn_integratemanager* bigtxnmgr   = NULL;

    bigtxnmgr = (bigtxn_integratemanager*)args;

    if(NULL == bigtxnmgr)
    {
        return ;
    }

    queue_destroy(bigtxnmgr->recordscache, dlist_freevoid);

    cache_txn_destroy(bigtxnmgr->parser2rebuild);

    cache_txn_destroy(bigtxnmgr->rebuild2sync);

    if (bigtxnmgr->honlinerefreshfilterdataset)
    {
        hash_destroy(bigtxnmgr->honlinerefreshfilterdataset);
    }

    if (bigtxnmgr->onlinerefreshdataset)
    {
        dlist_free(bigtxnmgr->onlinerefreshdataset->onlinerefresh, onlinerefresh_integratedataset_free);
        rfree(bigtxnmgr->onlinerefreshdataset);
    }
    
    rfree(bigtxnmgr);
    return;
}
