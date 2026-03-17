#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/conn/conn.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/guc/guc.h"
#include "loadrecords/record.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "misc/misc_stat.h"
#include "misc/misc_control.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "net/netpacket/netpacket.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "cache/fpwcache.h"
#include "catalog/catalog.h"
#include "catalog/control.h"
#include "catalog/class.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "works/parserwork/wal/parserwork_wal.h"
#include "works/parserwork/wal/decode_checkpoint.h"
#include "metric/capture/metric_capture.h"
#include "utils/init/databaserecv.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "utils/regex/regex.h"
#include "strategy/filter_dataset.h"

static void parserwork_wal_reload(decodingcontext* decodingctx);

void parserwork_stat_setpause(decodingcontext* decodingctx)
{
    decodingctx->stat = DECODINGCONTEXT_SET_PAUSE;

    while (decodingctx->stat == DECODINGCONTEXT_SET_PAUSE)
    {
        usleep(10000);
    }
    return;
}

void parserwork_stat_setresume(decodingcontext* decodingctx)
{
    decodingctx->stat = DECODINGCONTEXT_RESUME;
    return;
}

HTAB *decodingcontext_stat_getsyncdataset(decodingcontext* decodingctx)
{
    return decodingctx->trans_cache->hsyncdataset;
}

/* 删除 onlinerefresh */
void parserwork_decodingctx_removeonlinerefresh(decodingcontext* ctx, onlinerefresh* onlinerefresh)
{
    ctx->onlinerefresh = dlist_deletebyvalue(ctx->onlinerefresh,
                                             onlinerefresh,
                                             onlinerefresh_cmp,
                                             onlinerefresh_destroyvoid);

    if (NULL != ctx->refreshtxn)
    {
        txn_free(ctx->refreshtxn);
        rfree(ctx->refreshtxn);
        ctx->refreshtxn = NULL;
    }
}


void parserwork_decodingctx_addonlinerefresh(decodingcontext* ctx, onlinerefresh* onlinerefresh, txn* txn)
{
    ctx->onlinerefresh = dlist_put(ctx->onlinerefresh, onlinerefresh);
    ctx->refreshtxn = txn;
}

static void parserwork_wal_check_reloadstate(decodingcontext* decodingctx, int state)
{
    if (CAPTURERELOAD_STATUS_RELOADING_PARSERWAL == state)
    {
        parserwork_wal_reload(decodingctx);
        g_gotsigreload = CAPTURERELOAD_STATUS_RELOADING_WRITE;
    }
    return;
}

void parserwork_stat_setrewind(decodingcontext* decodingctx)
{
    decodingctx->stat = DECODINGCONTEXT_REWIND;
    return;
}

void parserwork_stat_setrunning(decodingcontext* decodingctx)
{
    decodingctx->stat = DECODINGCONTEXT_RUNNING;
    return;
}

decodingcontext* parserwork_walinitphase1(void)
{
    decodingcontext*  decodingctx = NULL;
    HASHCTL		hash_ctl;
    if(NULL != decodingctx)
    {
        return decodingctx;
    }

    decodingctx = (decodingcontext*)rmalloc1(sizeof(decodingcontext));
    if(NULL == decodingctx)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx, 0, '\0', sizeof(decodingcontext));
    decodingctx->parselsn = FRISTVALID_LSN;
    decodingctx->decode_record = NULL;
    decodingctx->stat = DECODINGCONTEXT_INIT;

     /* 在磁盘中加载 BASE 信息 */
    misc_stat_loaddecode((void*)&decodingctx->base);
    decodingctx->decode_record = NULL;

    decodingctx->trans_cache = (transcache*)rmalloc0(sizeof(transcache));
    if(NULL == decodingctx->trans_cache)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->trans_cache, 0, '\0', sizeof(transcache));

    /* 
     * 加载规则 
     *  1、同步规则
     *  2、过滤规则
     *  3、新增表过滤规则
    */
    decodingctx->trans_cache->tableincludes = filter_dataset_inittableinclude(decodingctx->trans_cache->tableincludes);
    decodingctx->trans_cache->tableexcludes = filter_dataset_inittableexclude(decodingctx->trans_cache->tableexcludes);
    decodingctx->trans_cache->addtablepattern = filter_dataset_initaddtablepattern(decodingctx->trans_cache->addtablepattern);

    /* 添加解析库需要的信息 */
    decodingctx->walpre.m_dbtype = g_idbtype;
    decodingctx->walpre.m_dbversion = guc_getConfigOption(CFG_KEY_DBVERION);
    decodingctx->walpre.m_debugLevel = 0;
    decodingctx->walpre.m_pagesize = g_blocksize;
    decodingctx->walpre.m_walLevel = XK_PG_PARSER_WALLEVEL_LOGICAL;
    decodingctx->walpre.m_record = NULL;

    /* 初始化链表结构 */
    decodingctx->trans_cache->transdlist = (txn_dlist*)rmalloc0(sizeof(txn_dlist));
    if(NULL == decodingctx->trans_cache->transdlist)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(decodingctx->trans_cache->transdlist, 0, '\0', sizeof(txn_dlist));
    decodingctx->trans_cache->transdlist->head = NULL;
    decodingctx->trans_cache->transdlist->tail = NULL;

    /* 初始化capture_buffer 并转换为字节大小 */
    decodingctx->trans_cache->capture_buffer = MB2BYTE(guc_getConfigOptionInt(CFG_KEY_CAPTURE_BUFFER));

    /* 初始化事务HASH表 */
    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(FullTransactionId);
    hash_ctl.entrysize = sizeof(txn);
    decodingctx->trans_cache->by_txns = hash_create("transaction hash", 8192, &hash_ctl,
                                                        HASH_ELEM | HASH_BLOBS);
    /* 初始化sysdicts */
    decodingctx->trans_cache->sysdicts = (cache_sysdicts*)rmalloc0(sizeof(cache_sysdicts));
    if(NULL == decodingctx->trans_cache->sysdicts)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->trans_cache->sysdicts, 0, '\0', sizeof(cache_sysdicts));

    /* rewind_ptr初始化 */
    decodingctx->rewind_ptr = (rewind_info*)rmalloc0(sizeof(rewind_info));
    if(NULL == decodingctx->rewind_ptr)
    {
        elog(RLOG_ERROR, "out of memeory");
    }
    rmemset0(decodingctx->rewind_ptr, 0, '\0', sizeof(rewind_info));

    /* fpw 初始化 */
    decodingctx->trans_cache->by_fpwtuples = fpwcache_init(decodingctx->trans_cache);

    /* 设置大事务过滤标识 */
    decodingctx->filterbigtrans = true;

    return decodingctx;
}

void parserwork_walinitphase2(decodingcontext* decodingctx)
{
    Oid dbid = InvalidOid;

    /* 在磁盘中加载 BASE 信息 */
    misc_stat_loaddecode((void*)&decodingctx->base);

    decodingctx->database = misc_controldata_database_get(NULL);
    decodingctx->monetary = rstrdup(misc_controldata_monetary_get());
    decodingctx->numeric = rstrdup(misc_controldata_numeric_get());
    decodingctx->tzname = rstrdup(misc_controldata_timezone_get());
    decodingctx->orgdbcharset = rstrdup(misc_controldata_orgencoding_get());
    decodingctx->tgtdbcharset = misc_controldata_dstencoding_get();

    /*加载字典表*/
    dbid = misc_controldata_database_get(NULL);
    cache_sysdictsload((void**)&decodingctx->trans_cache->sysdicts);

    decodingctx->trans_cache->hsyncdataset = filter_dataset_load(decodingctx->trans_cache->sysdicts->by_namespace,
                                                                        decodingctx->trans_cache->sysdicts->by_class);

    decodingctx->trans_cache->htxnfilterdataset = filter_dataset_txnfilterload(decodingctx->trans_cache->sysdicts->by_namespace,
                                                                                     decodingctx->trans_cache->sysdicts->by_class);

    decodingctx->trans_cache->sysdicts->by_relfilenode = cache_sysdicts_buildrelfilenode2oid(dbid,
                                                                                    (void*)decodingctx->trans_cache->sysdicts);
    /* checkpoint 节点初始化 */
    decode_chkpt_init(decodingctx, decodingctx->base.redolsn);

    /* 初始化capture_buffer 并转换为字节大小 */
    decodingctx->trans_cache->capture_buffer = MB2BYTE(guc_getConfigOptionInt(CFG_KEY_CAPTURE_BUFFER));


    elog(RLOG_INFO, "ripple parser from, redolsn %X/%X, restartlsn %X/%X, last commit lsn:%X/%X, fileid:%lu, offset:%u, timeline:%u",
                        (uint32)(decodingctx->base.redolsn>>32), (uint32)decodingctx->base.redolsn,
                        (uint32)(decodingctx->base.restartlsn>>32), (uint32)decodingctx->base.restartlsn,
                        (uint32)(decodingctx->base.confirmedlsn>>32), (uint32)decodingctx->base.confirmedlsn,
                        decodingctx->base.fileid,
                        decodingctx->base.fileoffset,
                        decodingctx->base.curtlid);
}

bool parserwork_wal_initfromdb(decodingcontext* decodingctx)
{
    const char*    url = NULL;

    PGconn* conn = NULL;
    checkpoint* checkpoint = NULL;

    /*获取连接信息*/
    url = guc_getConfigOption("url");

    /*连接数据库*/
    conn = conn_get(url);

    /* 连接错误退出 */
    if(NULL == conn)
    {
        return false;
    }

    checkpoint = databaserecv_checkpoint_get(conn);

    decodingctx->database = databaserecv_database_get(conn);
    decodingctx->monetary = databaserecv_monetary_get(conn);
    decodingctx->numeric = databaserecv_numeric_get(conn);
    decodingctx->tzname = databaserecv_timezone_get(conn);
    decodingctx->orgdbcharset = databaserecv_orgencoding_get(conn);
    decodingctx->tgtdbcharset = DSTENCODING;

    /*设置controlfile信息*/
    misc_controldata_database_set(decodingctx->database);
    misc_controldata_dbname_set(PQdb(conn));
    misc_controldata_monetary_set(decodingctx->monetary);
    misc_controldata_numeric_set(decodingctx->numeric);
    misc_controldata_timezone_set(decodingctx->tzname);
    misc_controldata_orgencoding_set(decodingctx->orgdbcharset);
    misc_controldata_dstencoding_set(decodingctx->tgtdbcharset);

    /* 填充base文件信息 */
    decodingctx->base.curtlid = checkpoint->tlid;
    decodingctx->base.redolsn = checkpoint->redolsn;

    decodingctx->rewind_ptr->redolsn = checkpoint->redolsn;

    elog(RLOG_INFO, "ripple redolsn fromdb, redolsn %X/%X, timeline:%u",
                    (uint32)(checkpoint->redolsn>>32),
                    (uint32)checkpoint->redolsn,
                    decodingctx->base.curtlid);

    /* checkpoint 节点初始化 */
    decode_chkpt_init(decodingctx, decodingctx->base.redolsn);

    /* 创建触发器 */
    if(!databaserecv_trigger_set(conn))
    {
        return false;
    }

    databaserecv_checkpoint(conn);

    /*关闭 conn*/
    PQfinish(conn);
    conn = NULL;

    rfree(checkpoint);

    return true;

}

/* 遍历解析 record */
static void  parserwork_wal_work(decodingcontext* decodingctx, dlist* record_dlist)
{
    dlistnode* dlnode = NULL;
    record *record_obj = NULL;

    dlnode = record_dlist->head;

    while (dlnode)
    {
        record_obj = (record*)dlnode->value;

        /* 结束 LSN */
        decodingctx->callback.setmetricparselsn(decodingctx->privdata, record_obj->end.wal.lsn);

        decodingctx->parselsn = record_obj->end.wal.lsn;

        /* 解析 record */
        decodingctx->decode_record = record_obj;

        /* 调用解析函数 */
        g_parserecno++;
        parserwork_waldecode(decodingctx);
        decodingctx->decode_record = NULL;

        dlnode = dlnode->next;
    }
}

/* 遍历record执行rewind_ptr, 查找checkpoint 没用*/
static void parserwork_wal_rewind_ptr(decodingcontext* decodingctx, dlist* record_dlist)
{
    dlistnode* dlnode = NULL;
    record* record_obj = NULL;

    dlnode = record_dlist->head;

    /* 遍历链表 */
    while (dlnode)
    {
        record_obj = (record*)dlnode->value;

        /* 结束 LSN */
        decodingctx->callback.setmetricparselsn(decodingctx->privdata, record_obj->end.wal.lsn);

        decodingctx->parselsn = record_obj->end.wal.lsn;

        /* 解析 record */
        decodingctx->decode_record = record_obj;

        if (decodingctx->rewind_ptr->stat == REWIND_EMITING)
        {
            if (decodingctx->decode_record->start.wal.lsn >= decodingctx->rewind_ptr->currentlsn)
            {
                /* 找到了大于xmax的事务的commit */
                decodingctx->base.confirmedlsn = decodingctx->decode_record->start.wal.lsn - 1;
                decodingctx->base.restartlsn = decodingctx->rewind_ptr->redolsn;
                decodingctx->stat = DECODINGCONTEXT_RUNNING;
                if (decodingctx->callback.setparserlsn)
                {
                    decodingctx->callback.setparserlsn(decodingctx->privdata, decodingctx->base.confirmedlsn, decodingctx->base.restartlsn, decodingctx->base.restartlsn);
                }
                else
                {
                    elog(RLOG_WARNING, "be carefull! setparserlsn is null");
                }
                
                elog(RLOG_INFO, "parserwork wal rewind_ptr end, redolsn: %X/%X, restartlsn: %X/%X, confirmedlsn: %X/%X",
                                (uint32)(decodingctx->base.redolsn>>32), (uint32)decodingctx->base.redolsn,
                                (uint32)(decodingctx->base.restartlsn>>32), (uint32)decodingctx->base.restartlsn,
                                (uint32)(decodingctx->base.confirmedlsn>>32), (uint32)decodingctx->base.confirmedlsn);
                rewind_stat_setemited(decodingctx->rewind_ptr);
            }
        }

        /* 调用rewind_ptr函数 */
        //g_parserecno++;
        if (decodingctx->rewind_ptr->stat == REWIND_SEARCHCHECKPOINT)
        {
            rewind_fastrewind(decodingctx);
            /* 如果是rewind_ptring状态, 重新设置split的起点 */
            if (decodingctx->rewind_ptr->stat == REWIND_REWINDING)
            {
                decodingctx->callback.setloadlsn(decodingctx->privdata, decodingctx->rewind_ptr->redolsn, InvalidXLogRecPtr);
                break;
            }
        }
        else if (decodingctx->rewind_ptr->stat == REWIND_EMITING)
        {
            rewind_fastrewind_emit(decodingctx);
        }
        else if (decodingctx->rewind_ptr->stat == REWIND_EMITED)
        {
            /* 这里是有可能走到的, 此时走正常逻辑 */
            parserwork_waldecode(decodingctx);
        }
        decodingctx->decode_record = NULL;

        dlnode = dlnode->next;
    }
}

void parserwork_wal_getpos(decodingcontext* decodingctx, uint64* fileid, uint64* fileoffset)
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

void* parserwork_wal_main(void *args)
{
    int timeout = 0;
    thrnode* thrnode_ptr                     = NULL;
    decodingcontext* decodingctx         = NULL;
    dlist* record_dlist                         = NULL;

    thrnode_ptr = (thrnode*)args;
    decodingctx = (decodingcontext*)thrnode_ptr->data;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thrnode_ptr->stat)
    {
        elog(RLOG_WARNING, "increment capture parser stat exception, expected state is THRNODE_STAT_STARTING");
        thrnode_ptr->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode_ptr->stat = THRNODE_STAT_WORK;
    while(1)
    {
        record_dlist = NULL;
        if(THRNODE_STAT_TERM == thrnode_ptr->stat)
        {
            /* 解析器 */
            thrnode_ptr->stat = THRNODE_STAT_EXIT;
            break;
        }

        if (decodingctx->stat == DECODINGCONTEXT_REWIND
         && !rewind_check_stat_allow_get_entry(decodingctx->rewind_ptr))
        {
            /* 睡眠10ms */
            usleep(10000);
            continue;
        }

        if (decodingctx->stat == DECODINGCONTEXT_SET_PAUSE)
        {
            decodingctx->stat = DECODINGCONTEXT_PAUSE;
        }

        /* 暂停状态, 休眠 */
        if (decodingctx->stat == DECODINGCONTEXT_PAUSE)
        {
            /* 睡眠50ms */
            usleep(50000);
            continue;
        }

        /* 恢复状态, online refresh begin事务放到带序列化缓存 */
        if (decodingctx->stat == DECODINGCONTEXT_RESUME)
        {
            /* 先将refreshtxn放入缓存 */
            if (decodingctx->refreshtxn)
            {
                cache_txn_add(decodingctx->parser2txns, decodingctx->refreshtxn);
                decodingctx->refreshtxn = NULL;
            }

            decodingctx->stat = DECODINGCONTEXT_RUNNING;
        }

        /* 重新加载参数 */
        parserwork_wal_check_reloadstate(decodingctx, g_gotsigreload);

        /* 获取数据 */
        record_dlist = queue_get(decodingctx->recordqueue, &timeout);
        if(NULL == record_dlist)
        {
            /* 需要退出，等待 worknode->status 变为 WORK_STATUS_TERM 后退出*/
            if(ERROR_TIMEOUT == timeout)
            {
                /* 睡眠 10 毫秒 */
                continue;
            }

            elog(RLOG_WARNING, "capture parser get records from queue error");
            thrnode_ptr->stat = THRNODE_STAT_ABORT;
            break;
        }

        if (decodingctx->stat == DECODINGCONTEXT_REWIND)
        {
            if (decodingctx->refreshtxn)
            {
                cache_txn_add(decodingctx->parser2txns, decodingctx->refreshtxn);
                decodingctx->refreshtxn = NULL;
            }

            if (decodingctx->rewind_ptr->stat == REWIND_INIT)
            {
                rewind_stat_setsearchcheckpoint(decodingctx->rewind_ptr);
            }
            /* 执行rewind_ptr查找checkpoint和查找emit点的逻辑 */
            parserwork_wal_rewind_ptr(decodingctx, record_dlist);
        }
        else if(decodingctx->stat >= DECODINGCONTEXT_RUNNING)
        {
            /* 根据 entry 的内容获取数据 */
            parserwork_wal_work(decodingctx, record_dlist);
        }

        /* record 双向链表 内存释放 */
        dlist_free(record_dlist, (dlistvaluefree )record_free);
    }

    pthread_exit(NULL);
    return NULL;
}

void parserwork_wal_getparserinfo(decodingcontext* decodingctx, XLogRecPtr* prestartlsn, XLogRecPtr* pconfirmlsn)
{
    if(NULL == decodingctx)
    {
        return;
    }

    *prestartlsn = decodingctx->base.restartlsn;
    *pconfirmlsn = decodingctx->base.confirmedlsn;
}

static void parserwork_wal_reload(decodingcontext* decodingctx)
{
    /* 加载guc参数 */
    guc_loadcfg(g_profilepath, true);

    /* 加载规则 */
    decodingctx->trans_cache->tableincludes = filter_dataset_inittableinclude(decodingctx->trans_cache->tableincludes);
    decodingctx->trans_cache->tableexcludes = filter_dataset_inittableexclude(decodingctx->trans_cache->tableexcludes);
    decodingctx->trans_cache->addtablepattern = filter_dataset_initaddtablepattern(decodingctx->trans_cache->addtablepattern);

    decodingctx->trans_cache->hsyncdataset = filter_dataset_reload(decodingctx->trans_cache->sysdicts->by_namespace,
                                                                        decodingctx->trans_cache->sysdicts->by_class,
                                                                        decodingctx->trans_cache->hsyncdataset);
    return;
}

bool parserwork_buildrefreshtransaction(decodingcontext* decodingctx, refresh_tables* tables)
{
    txn *refreshtxn = NULL;
    txnstmt* stmt = NULL;

    if (!tables || 0 == tables->cnt)
    {
        refresh_freetables(tables);
        decodingctx->refreshtxn = NULL;
        return true;
    }

    refreshtxn = txn_init(REFRESH_TXNID, 1, InvalidXLogRecPtr);
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt *) rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_REFRESH;
    stmt->stmt = (void*)tables;
    stmt->extra0.wal.lsn = REFRESH_LSN;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    txn_addcommit(refreshtxn);

    decodingctx->refreshtxn = refreshtxn;
    return true;
}

txn *parserwork_build_onlinerefresh_end_txn(unsigned char *uuid, XLogRecPtr parserlsn)
{
    txn *refreshtxn = NULL;
    txnstmt *stmt = NULL;

    refreshtxn = txn_init(REFRESH_TXNID, InvalidXLogRecPtr, (parserlsn + 1));
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt *) rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_ONLINEREFRESH_END;
    
    stmt->stmt = rmalloc0(UUID_LEN);
    if (!stmt->stmt)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(stmt->stmt, 0, 0, UUID_LEN);
    rmemcpy0(stmt->stmt, 0, uuid, UUID_LEN);
    
    /* 设置 onlinerefreshend->stmt 的 lsn */
    stmt->extra0.wal.lsn = parserlsn + 1;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    txn_addcommit(refreshtxn);

    return refreshtxn;
}

txn *parserwork_build_onlinerefresh_increment_end_txn(unsigned char *uuid)
{
    txn *refreshtxn = NULL;
    txnstmt *stmt = NULL;

    refreshtxn = txn_init(REFRESH_TXNID, InvalidXLogRecPtr, InvalidXLogRecPtr);
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt *) rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END;
    
    stmt->stmt = rmalloc0(UUID_LEN);
    if (!stmt->stmt)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(stmt->stmt, 0, 0, UUID_LEN);
    rmemcpy0(stmt->stmt, 0, uuid, UUID_LEN);
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    txn_addcommit(refreshtxn);

    return refreshtxn;
}

txn *parserwork_build_onlinerefresh_begin_txn(txnstmt_onlinerefresh *olstmt, XLogRecPtr parserlsn)
{
    txn *refreshtxn = NULL;
    txnstmt *stmt = NULL;

    refreshtxn = txn_init(REFRESH_TXNID, InvalidXLogRecPtr, (parserlsn + 1));
    if (NULL == refreshtxn)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }

    stmt = (txnstmt *) rmalloc0(sizeof(txnstmt));
    if (NULL == stmt)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stmt, 0, 0, sizeof(txnstmt));

    stmt->type = TXNSTMT_TYPE_ONLINEREFRESH_BEGIN;
    /* 设置 onlinerefreshbegin->stmt 的 lsn */
    stmt->extra0.wal.lsn = parserlsn + 1;
    
    stmt->stmt = (void*)olstmt;
    refreshtxn->stmts = lappend(refreshtxn->stmts, stmt);
    txn_addcommit(refreshtxn);

    return refreshtxn;
}

/* 缓存清理 */
void parserwork_wal_destroy(decodingcontext* decodingctx)
{
    if(NULL == decodingctx)
    {
        return;
    }

    if(NULL != decodingctx->rewind_ptr)
    {
        if (NULL != decodingctx->rewind_ptr->conn)
        {
            PQfinish(decodingctx->rewind_ptr->conn);
            decodingctx->rewind_ptr->conn = NULL;
        }

        if (NULL != decodingctx->rewind_ptr->strategy.xips)
        {
            hash_destroy(decodingctx->rewind_ptr->strategy.xips);
        }
        rfree(decodingctx->rewind_ptr);
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

    /* 清理 trans_cache */
    transcache_free(decodingctx->trans_cache);
    rfree(decodingctx->trans_cache);
    decodingctx->trans_cache = NULL;

    decodingctx->privdata = NULL;
    decodingctx->recordqueue = NULL;
    decodingctx->parser2txns = NULL;

    rfree(decodingctx);
}
