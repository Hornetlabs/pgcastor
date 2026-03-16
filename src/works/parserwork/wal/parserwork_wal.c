#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/conn/ripple_conn.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/guc/guc.h"
#include "loadrecords/ripple_record.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "misc/ripple_misc_stat.h"
#include "misc/ripple_misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "net/netpacket/ripple_netpacket.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "cache/ripple_fpwcache.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_control.h"
#include "catalog/ripple_class.h"
#include "stmts/ripple_txnstmt.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "works/parserwork/wal/ripple_parserwork_wal.h"
#include "works/parserwork/wal/ripple_decode_checkpoint.h"
#include "metric/capture/ripple_metric_capture.h"
#include "utils/init/ripple_databaserecv.h"
#include "works/splitwork/wal/ripple_splitwork_wal.h"
#include "utils/regex/ripple_regex.h"
#include "strategy/ripple_filter_dataset.h"

static void ripple_parserwork_wal_reload(ripple_decodingcontext* decodingctx);

void ripple_parserwork_stat_setpause(ripple_decodingcontext* decodingctx)
{
    decodingctx->stat = RIPPLE_DECODINGCONTEXT_SET_PAUSE;

    while (decodingctx->stat == RIPPLE_DECODINGCONTEXT_SET_PAUSE)
    {
        usleep(10000);
    }
    return;
}

void ripple_parserwork_stat_setresume(ripple_decodingcontext* decodingctx)
{
    decodingctx->stat = RIPPLE_DECODINGCONTEXT_RESUME;
    return;
}

HTAB *decodingcontext_stat_getsyncdataset(ripple_decodingcontext* decodingctx)
{
    return decodingctx->transcache->hsyncdataset;
}

/* 删除 onlinerefresh */
void ripple_parserwork_decodingctx_removeonlinerefresh(ripple_decodingcontext* ctx, ripple_onlinerefresh* onlinerefresh)
{
    ctx->onlinerefresh = dlist_deletebyvalue(ctx->onlinerefresh,
                                             onlinerefresh,
                                             ripple_onlinerefresh_cmp,
                                             ripple_onlinerefresh_destroyvoid);

    if (NULL != ctx->refreshtxn)
    {
        ripple_txn_free(ctx->refreshtxn);
        rfree(ctx->refreshtxn);
        ctx->refreshtxn = NULL;
    }
}


void ripple_parserwork_decodingctx_addonlinerefresh(ripple_decodingcontext* ctx, ripple_onlinerefresh* onlinerefresh, ripple_txn* txn)
{
    ctx->onlinerefresh = dlist_put(ctx->onlinerefresh, onlinerefresh);
    ctx->refreshtxn = txn;
}

static void ripple_parserwork_wal_check_reloadstate(ripple_decodingcontext* decodingctx, int state)
{
    if (RIPPLE_CAPTURERELOAD_STATUS_RELOADING_PARSERWAL == state)
    {
        ripple_parserwork_wal_reload(decodingctx);
        g_gotsigreload = RIPPLE_CAPTURERELOAD_STATUS_RELOADING_WRITE;
    }
    return;
}

void ripple_parserwork_stat_setrewind(ripple_decodingcontext* decodingctx)
{
    decodingctx->stat = RIPPLE_DECODINGCONTEXT_REWIND;
    return;
}

void ripple_parserwork_stat_setrunning(ripple_decodingcontext* decodingctx)
{
    decodingctx->stat = RIPPLE_DECODINGCONTEXT_RUNNING;
    return;
}

ripple_decodingcontext* ripple_parserwork_walinitphase1(void)
{
    ripple_decodingcontext*  decodingctx = NULL;
    HASHCTL		hash_ctl;
    if(NULL != decodingctx)
    {
        return decodingctx;
    }

    decodingctx = (ripple_decodingcontext*)rmalloc1(sizeof(ripple_decodingcontext));
    if(NULL == decodingctx)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx, 0, '\0', sizeof(ripple_decodingcontext));
    decodingctx->parselsn = RIPPLE_FRISTVALID_LSN;
    decodingctx->decode_record = NULL;
    decodingctx->stat = RIPPLE_DECODINGCONTEXT_INIT;

     /* 在磁盘中加载 BASE 信息 */
    ripple_misc_stat_loaddecode((void*)&decodingctx->base);
    decodingctx->decode_record = NULL;

    decodingctx->transcache = (ripple_transcache*)rmalloc0(sizeof(ripple_transcache));
    if(NULL == decodingctx->transcache)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->transcache, 0, '\0', sizeof(ripple_transcache));

    /* 
     * 加载规则 
     *  1、同步规则
     *  2、过滤规则
     *  3、新增表过滤规则
    */
    decodingctx->transcache->tableincludes = ripple_filter_dataset_inittableinclude(decodingctx->transcache->tableincludes);
    decodingctx->transcache->tableexcludes = ripple_filter_dataset_inittableexclude(decodingctx->transcache->tableexcludes);
    decodingctx->transcache->addtablepattern = ripple_filter_dataset_initaddtablepattern(decodingctx->transcache->addtablepattern);

    /* 添加解析库需要的信息 */
    decodingctx->walpre.m_dbtype = g_idbtype;
    decodingctx->walpre.m_dbversion = guc_getConfigOption(RIPPLE_CFG_KEY_DBVERION);
    decodingctx->walpre.m_debugLevel = 0;
    decodingctx->walpre.m_pagesize = g_blocksize;
    decodingctx->walpre.m_walLevel = XK_PG_PARSER_WALLEVEL_LOGICAL;
    decodingctx->walpre.m_record = NULL;

    /* 初始化链表结构 */
    decodingctx->transcache->transdlist = (ripple_txn_dlist*)rmalloc0(sizeof(ripple_txn_dlist));
    if(NULL == decodingctx->transcache->transdlist)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->transcache->transdlist, 0, '\0', sizeof(ripple_txn_dlist));
    decodingctx->transcache->transdlist->head = NULL;
    decodingctx->transcache->transdlist->tail = NULL;

    /* 初始化capture_buffer 并转换为字节大小 */
    decodingctx->transcache->capture_buffer = RIPPLE_MB2BYTE(guc_getConfigOptionInt(RIPPLE_CFG_KEY_CAPTURE_BUFFER));

    /* 初始化事务HASH表 */
    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(FullTransactionId);
    hash_ctl.entrysize = sizeof(ripple_txn);
    decodingctx->transcache->by_txns = hash_create("transaction hash", 8192, &hash_ctl,
                                                        HASH_ELEM | HASH_BLOBS);
    /* 初始化sysdicts */
    decodingctx->transcache->sysdicts = (ripple_cache_sysdicts*)rmalloc0(sizeof(ripple_cache_sysdicts));
    if(NULL == decodingctx->transcache->sysdicts)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->transcache->sysdicts, 0, '\0', sizeof(ripple_cache_sysdicts));

    /* rewind初始化 */
    decodingctx->rewind = (ripple_rewind*)rmalloc0(sizeof(ripple_rewind));
    if(NULL == decodingctx->rewind)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->rewind, 0, '\0', sizeof(ripple_rewind));

    /* fpw 初始化 */
    decodingctx->transcache->by_fpwtuples = ripple_fpwcache_init(decodingctx->transcache);

    /* 设置大事务过滤标识 */
    decodingctx->filterbigtrans = true;

    return decodingctx;
}

void ripple_parserwork_walinitphase2(ripple_decodingcontext* decodingctx)
{
    Oid dbid = InvalidOid;

    /* 在磁盘中加载 BASE 信息 */
    ripple_misc_stat_loaddecode((void*)&decodingctx->base);

    decodingctx->database = ripple_misc_controldata_database_get(NULL);
    decodingctx->monetary = rstrdup(ripple_misc_controldata_monetary_get());
    decodingctx->numeric = rstrdup(ripple_misc_controldata_numeric_get());
    decodingctx->tzname = rstrdup(ripple_misc_controldata_timezone_get());
    decodingctx->orgdbcharset = rstrdup(ripple_misc_controldata_orgencoding_get());
    decodingctx->tgtdbcharset = ripple_misc_controldata_dstencoding_get();

    /*加载字典表*/
    dbid = ripple_misc_controldata_database_get(NULL);
    ripple_cache_sysdictsload((void**)&decodingctx->transcache->sysdicts);

    decodingctx->transcache->hsyncdataset = ripple_filter_dataset_load(decodingctx->transcache->sysdicts->by_namespace,
                                                                        decodingctx->transcache->sysdicts->by_class);

    decodingctx->transcache->htxnfilterdataset = ripple_filter_dataset_txnfilterload(decodingctx->transcache->sysdicts->by_namespace,
                                                                                     decodingctx->transcache->sysdicts->by_class);

    decodingctx->transcache->sysdicts->by_relfilenode = ripple_cache_sysdicts_buildrelfilenode2oid(dbid,
                                                                                    (void*)decodingctx->transcache->sysdicts);
    /* checkpoint 节点初始化 */
    ripple_decode_chkpt_init(decodingctx, decodingctx->base.redolsn);

    /* 初始化capture_buffer 并转换为字节大小 */
    decodingctx->transcache->capture_buffer = RIPPLE_MB2BYTE(guc_getConfigOptionInt(RIPPLE_CFG_KEY_CAPTURE_BUFFER));


    elog(RLOG_INFO, "ripple parser from, redolsn %X/%X, restartlsn %X/%X, last commit lsn:%X/%X, fileid:%lu, offset:%u, timeline:%u",
                        (uint32)(decodingctx->base.redolsn>>32), (uint32)decodingctx->base.redolsn,
                        (uint32)(decodingctx->base.restartlsn>>32), (uint32)decodingctx->base.restartlsn,
                        (uint32)(decodingctx->base.confirmedlsn>>32), (uint32)decodingctx->base.confirmedlsn,
                        decodingctx->base.fileid,
                        decodingctx->base.fileoffset,
                        decodingctx->base.curtlid);
}

bool ripple_parserwork_wal_initfromdb(ripple_decodingcontext* decodingctx)
{
    const char*    url = NULL;

    PGconn* conn = NULL;
    ripple_checkpoint* checkpoint = NULL;

    /*获取连接信息*/
    url = guc_getConfigOption("url");

    /*连接数据库*/
    conn = ripple_conn_get(url);

    /* 连接错误退出 */
    if(NULL == conn)
    {
        return false;
    }

    checkpoint = ripple_databaserecv_checkpoint_get(conn);

    decodingctx->database = ripple_databaserecv_database_get(conn);
    decodingctx->monetary = ripple_databaserecv_monetary_get(conn);
    decodingctx->numeric = ripple_databaserecv_numeric_get(conn);
    decodingctx->tzname = ripple_databaserecv_timezone_get(conn);
    decodingctx->orgdbcharset = ripple_databaserecv_orgencoding_get(conn);
    decodingctx->tgtdbcharset = RIPPLE_DSTENCODING;

    /*设置controlfile信息*/
    ripple_misc_controldata_database_set(decodingctx->database);
    ripple_misc_controldata_dbname_set(PQdb(conn));
    ripple_misc_controldata_monetary_set(decodingctx->monetary);
    ripple_misc_controldata_numeric_set(decodingctx->numeric);
    ripple_misc_controldata_timezone_set(decodingctx->tzname);
    ripple_misc_controldata_orgencoding_set(decodingctx->orgdbcharset);
    ripple_misc_controldata_dstencoding_set(decodingctx->tgtdbcharset);

    /* 填充base文件信息 */
    decodingctx->base.curtlid = checkpoint->tlid;
    decodingctx->base.redolsn = checkpoint->redolsn;

    decodingctx->rewind->redolsn = checkpoint->redolsn;

    elog(RLOG_INFO, "ripple redolsn fromdb, redolsn %X/%X, timeline:%u",
                    (uint32)(checkpoint->redolsn>>32),
                    (uint32)checkpoint->redolsn,
                    decodingctx->base.curtlid);

    /* checkpoint 节点初始化 */
    ripple_decode_chkpt_init(decodingctx, decodingctx->base.redolsn);

    /* 创建触发器 */
    if(!ripple_databaserecv_trigger_set(conn))
    {
        return false;
    }

    ripple_databaserecv_checkpoint(conn);

    /*关闭 conn*/
    PQfinish(conn);
    conn = NULL;

    rfree(checkpoint);

    return true;

}

/* 遍历解析 record */
static void  ripple_parserwork_wal_work(ripple_decodingcontext* decodingctx, dlist* record_dlist)
{
    dlistnode* dlnode = NULL;
    ripple_record *record = NULL;

    dlnode = record_dlist->head;

    while (dlnode)
    {
        record = (ripple_record*)dlnode->value;

        /* 结束 LSN */
        decodingctx->callback.setmetricparselsn(decodingctx->privdata, record->end.wal.lsn);

        decodingctx->parselsn = record->end.wal.lsn;

        /* 解析 record */
        decodingctx->decode_record = record;

        /* 调用解析函数 */
        g_parserecno++;
        ripple_parserwork_waldecode(decodingctx);
        decodingctx->decode_record = NULL;

        dlnode = dlnode->next;
    }
}

/* 遍历record执行rewind, 查找checkpoint 没用*/
static void ripple_parserwork_wal_rewind(ripple_decodingcontext* decodingctx, dlist* record_dlist)
{
    dlistnode* dlnode = NULL;
    ripple_record* record = NULL;

    dlnode = record_dlist->head;

    /* 遍历链表 */
    while (dlnode)
    {
        record = (ripple_record*)dlnode->value;

        /* 结束 LSN */
        decodingctx->callback.setmetricparselsn(decodingctx->privdata, record->end.wal.lsn);

        decodingctx->parselsn = record->end.wal.lsn;

        /* 解析 record */
        decodingctx->decode_record = record;

        if (decodingctx->rewind->stat == RIPPLE_REWIND_EMITING)
        {
            if (decodingctx->decode_record->start.wal.lsn >= decodingctx->rewind->currentlsn)
            {
                /* 找到了大于xmax的事务的commit */
                decodingctx->base.confirmedlsn = decodingctx->decode_record->start.wal.lsn - 1;
                decodingctx->base.restartlsn = decodingctx->rewind->redolsn;
                decodingctx->stat = RIPPLE_DECODINGCONTEXT_RUNNING;
                if (decodingctx->callback.setparserlsn)
                {
                    decodingctx->callback.setparserlsn(decodingctx->privdata, decodingctx->base.confirmedlsn, decodingctx->base.restartlsn, decodingctx->base.restartlsn);
                }
                else
                {
                    elog(RLOG_WARNING, "be carefull! setparserlsn is null");
                }
                
                elog(RLOG_INFO, "parserwork wal rewind end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                                (uint32)(decodingctx->base.redolsn>>32), (uint32)decodingctx->base.redolsn,
                                (uint32)(decodingctx->base.restartlsn>>32), (uint32)decodingctx->base.restartlsn,
                                (uint32)(decodingctx->base.confirmedlsn>>32), (uint32)decodingctx->base.confirmedlsn);
                ripple_rewind_stat_setemited(decodingctx->rewind);
            }
        }

        /* 调用rewind函数 */
        //g_parserecno++;
        if (decodingctx->rewind->stat == RIPPLE_REWIND_SEARCHCHECKPOINT)
        {
            ripple_rewind_fastrewind(decodingctx);
            /* 如果是rewinding状态, 重新设置split的起点 */
            if (decodingctx->rewind->stat == RIPPLE_REWIND_REWINDING)
            {
                decodingctx->callback.setloadlsn(decodingctx->privdata, decodingctx->rewind->redolsn, InvalidXLogRecPtr);
                break;
            }
        }
        else if (decodingctx->rewind->stat == RIPPLE_REWIND_EMITING)
        {
            ripple_rewind_fastrewind_emit(decodingctx);
        }
        else if (decodingctx->rewind->stat == RIPPLE_REWIND_EMITED)
        {
            /* 这里是有可能走到的, 此时走正常逻辑 */
            ripple_parserwork_waldecode(decodingctx);
        }
        decodingctx->decode_record = NULL;

        dlnode = dlnode->next;
    }
}

void ripple_parserwork_wal_getpos(ripple_decodingcontext* decodingctx, uint64* fileid, uint64* fileoffset)
{
    if(NULL == decodingctx)
    {
        return;
    }

    *fileid = decodingctx->base.fileid;
    *fileoffset = decodingctx->base.fileoffset;
    *fileid = 0;
    *fileoffset = 0;
    return;
}

void* ripple_parserwork_wal_main(void *args)
{
    int timeout = 0;
    ripple_thrnode* thrnode                     = NULL;
    ripple_decodingcontext* decodingctx         = NULL;
    dlist* record_dlist                         = NULL;

    thrnode = (ripple_thrnode*)args;
    decodingctx = (ripple_decodingcontext*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment capture parser stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;
    while(1)
    {
        record_dlist = NULL;
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 解析器 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        if (decodingctx->stat == RIPPLE_DECODINGCONTEXT_REWIND
         && !ripple_rewind_check_stat_allow_get_entry(decodingctx->rewind))
        {
            /* 睡眠10ms */
            usleep(10000);
            continue;
        }

        if (decodingctx->stat == RIPPLE_DECODINGCONTEXT_SET_PAUSE)
        {
            decodingctx->stat = RIPPLE_DECODINGCONTEXT_PAUSE;
        }

        /* 暂停状态, 休眠 */
        if (decodingctx->stat == RIPPLE_DECODINGCONTEXT_PAUSE)
        {
            /* 睡眠50ms */
            usleep(50000);
            continue;
        }

        /* 恢复状态, online refresh begin事务放到带序列化缓存 */
        if (decodingctx->stat == RIPPLE_DECODINGCONTEXT_RESUME)
        {
            /* 先将refreshtxn放入缓存 */
            if (decodingctx->refreshtxn)
            {
                ripple_cache_txn_add(decodingctx->parser2txns, decodingctx->refreshtxn);
                decodingctx->refreshtxn = NULL;
            }

            decodingctx->stat = RIPPLE_DECODINGCONTEXT_RUNNING;
        }

        /* 重新加载参数 */
        ripple_parserwork_wal_check_reloadstate(decodingctx, g_gotsigreload);

        /* 获取数据 */
        record_dlist = ripple_queue_get(decodingctx->recordqueue, &timeout);
        if(NULL == record_dlist)
        {
            /* 需要退出，等待 worknode->status 变为 RIPPLE_WORK_STATUS_TERM 后退出*/
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                /* 睡眠 10 毫秒 */
                continue;
            }

            elog(RLOG_WARNING, "capture parser get records from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if (decodingctx->stat == RIPPLE_DECODINGCONTEXT_REWIND)
        {
            if (decodingctx->refreshtxn)
            {
                ripple_cache_txn_add(decodingctx->parser2txns, decodingctx->refreshtxn);
                decodingctx->refreshtxn = NULL;
            }

            if (decodingctx->rewind->stat == RIPPLE_REWIND_INIT)
            {
                ripple_rewind_stat_setsearchcheckpoint(decodingctx->rewind);
            }
            /* 执行rewind查找checkpoint和查找emit点的逻辑 */
            ripple_parserwork_wal_rewind(decodingctx, record_dlist);
        }
        else if(decodingctx->stat >= RIPPLE_DECODINGCONTEXT_RUNNING)
        {
            /* 根据 entry 的内容获取数据 */
            ripple_parserwork_wal_work(decodingctx, record_dlist);
        }

        /* record 双向链表 内存释放 */
        dlist_free(record_dlist, (dlistvaluefree )ripple_record_free);
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_parserwork_wal_getparserinfo(ripple_decodingcontext* decodingctx, XLogRecPtr* prestartlsn, XLogRecPtr* pconfirmlsn)
{
    if(NULL == decodingctx)
    {
        return;
    }

    *prestartlsn = decodingctx->base.restartlsn;
    *pconfirmlsn = decodingctx->base.confirmedlsn;
}

static void ripple_parserwork_wal_reload(ripple_decodingcontext* decodingctx)
{
    /* 加载guc参数 */
    guc_loadcfg(g_profilepath, true);

    /* 加载规则 */
    decodingctx->transcache->tableincludes = ripple_filter_dataset_inittableinclude(decodingctx->transcache->tableincludes);
    decodingctx->transcache->tableexcludes = ripple_filter_dataset_inittableexclude(decodingctx->transcache->tableexcludes);
    decodingctx->transcache->addtablepattern = ripple_filter_dataset_initaddtablepattern(decodingctx->transcache->addtablepattern);

    decodingctx->transcache->hsyncdataset = ripple_filter_dataset_reload(decodingctx->transcache->sysdicts->by_namespace,
                                                                        decodingctx->transcache->sysdicts->by_class,
                                                                        decodingctx->transcache->hsyncdataset);
    return;
}

bool ripple_parserwork_buildrefreshtransaction(ripple_decodingcontext* decodingctx, ripple_refresh_tables* tables)
{
    ripple_txn *refreshtxn = NULL;
    ripple_txnstmt* stmt = NULL;

    if (!tables || 0 == tables->cnt)
    {
        ripple_refresh_freetables(tables);
        decodingctx->refreshtxn = NULL;
        return true;
    }

    refreshtxn = ripple_txn_init(RIPPLE_REFRESH_TXNID, 1, InvalidXLogRecPtr);
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (ripple_txnstmt *) rmalloc0(sizeof(ripple_txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));

    stmt->type = RIPPLE_TXNSTMT_TYPE_REFRESH;
    stmt->stmt = (void*)tables;
    stmt->extra0.wal.lsn = RIPPLE_REFRESH_LSN;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    ripple_txn_addcommit(refreshtxn);

    decodingctx->refreshtxn = refreshtxn;
    return true;
}

ripple_txn *ripple_parserwork_build_onlinerefresh_end_txn(unsigned char *uuid, XLogRecPtr parserlsn)
{
    ripple_txn *refreshtxn = NULL;
    ripple_txnstmt *stmt = NULL;

    refreshtxn = ripple_txn_init(RIPPLE_REFRESH_TXNID, InvalidXLogRecPtr, (parserlsn + 1));
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (ripple_txnstmt *) rmalloc0(sizeof(ripple_txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));

    stmt->type = RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_END;
    
    stmt->stmt = rmalloc0(RIPPLE_UUID_LEN);
    if (!stmt->stmt)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(stmt->stmt, 0, 0, RIPPLE_UUID_LEN);
    rmemcpy0(stmt->stmt, 0, uuid, RIPPLE_UUID_LEN);
    
    /* 设置 onlinerefreshend->stmt 的 lsn */
    stmt->extra0.wal.lsn = parserlsn + 1;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    ripple_txn_addcommit(refreshtxn);

    return refreshtxn;
}

ripple_txn *ripple_parserwork_build_onlinerefresh_increment_end_txn(unsigned char *uuid)
{
    ripple_txn *refreshtxn = NULL;
    ripple_txnstmt *stmt = NULL;

    refreshtxn = ripple_txn_init(RIPPLE_REFRESH_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (ripple_txnstmt *) rmalloc0(sizeof(ripple_txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));

    stmt->type = RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END;
    
    stmt->stmt = rmalloc0(RIPPLE_UUID_LEN);
    if (!stmt->stmt)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(stmt->stmt, 0, 0, RIPPLE_UUID_LEN);
    rmemcpy0(stmt->stmt, 0, uuid, RIPPLE_UUID_LEN);
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    ripple_txn_addcommit(refreshtxn);

    return refreshtxn;
}

ripple_txn *ripple_parserwork_build_onlinerefresh_begin_txn(ripple_txnstmt_onlinerefresh *olstmt, XLogRecPtr parserlsn)
{
    ripple_txn *refreshtxn = NULL;
    ripple_txnstmt *stmt = NULL;

    refreshtxn = ripple_txn_init(RIPPLE_REFRESH_TXNID, InvalidXLogRecPtr, (parserlsn + 1));
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (ripple_txnstmt *) rmalloc0(sizeof(ripple_txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(ripple_txnstmt));

    stmt->type = RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_BEGIN;
    /* 设置 onlinerefreshbegin->stmt 的 lsn */
    stmt->extra0.wal.lsn = parserlsn + 1;
    
    stmt->stmt = (void*)olstmt;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    ripple_txn_addcommit(refreshtxn);

    return refreshtxn;
}

/* 缓存清理 */
void ripple_parserwork_wal_destroy(ripple_decodingcontext* decodingctx)
{
    if(NULL == decodingctx)
    {
        return;
    }

    if(NULL != decodingctx->rewind)
    {
        if (NULL != decodingctx->rewind->conn)
        {
            PQfinish(decodingctx->rewind->conn);
            decodingctx->rewind->conn = NULL;
        }

        if (NULL != decodingctx->rewind->strategy.xips)
        {
            hash_destroy(decodingctx->rewind->strategy.xips);
        }
        rfree(decodingctx->rewind);
    }

    if (NULL != decodingctx->monetary)
    {
        rfree(decodingctx->monetary);
    }

    if (NULL != decodingctx->numeric)
    {
        rfree(decodingctx->numeric);
    }

    if (NULL != decodingctx->tzname)
    {
        rfree(decodingctx->tzname);
    }

    if (NULL != decodingctx->orgdbcharset)
    {
        rfree(decodingctx->orgdbcharset);
    }

    /* 清理 transcache */
    ripple_transcache_free(decodingctx->transcache);
    rfree(decodingctx->transcache);
    decodingctx->transcache = NULL;

    decodingctx->privdata = NULL;
    decodingctx->recordqueue = NULL;
    decodingctx->parser2txns = NULL;

    rfree(decodingctx);
}
