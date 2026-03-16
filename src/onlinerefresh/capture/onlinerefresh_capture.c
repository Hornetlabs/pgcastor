#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "port/net/ripple_net.h"
#include "net/netiomp/ripple_netiomp.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
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
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "misc/ripple_misc_stat.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "snapshot/ripple_snapshot.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/capture/ripple_refresh_capture.h"
#include "refresh/sharding2file/ripple_refresh_sharding2file.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "onlinerefresh/capture/ripple_onlinerefresh_capture.h"
#include "works/splitwork/wal/ripple_splitwork_wal.h"
#include "works/splitwork/wal/ripple_wal_define.h"
#include "serial/ripple_serial.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "onlinerefresh/capture/parserwal/ripple_onlinerefresh_capture_parser.h"
#include "onlinerefresh/capture/serial/ripple_onlinerefresh_captureserial.h"
#include "onlinerefresh/capture/loadrecord/ripple_onlinerefresh_captureloadrecord.h"
#include "onlinerefresh/capture/flush/ripple_onlinerefresh_captureflush.h"
#include "utils/conn/ripple_conn.h"
#include "works/parserwork/wal/ripple_parserwork_wal.h"

typedef enum RIPPLE_ONLINEREFRESH_CAPTURE_STAT
{
    RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBNOP                = 0x00,
    RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBSTARTING           ,               /* 工作线程启动中 */
    RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWORKING            ,               /* 工作线程工作状态 */
    RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWAITREFRESHDONE    ,               /* 等待存量工作线程工作完成 */
    RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWAITINCREMENTDONE                  /* 等待存量工作线程工作完成 */
} ripple_onlinerefresh_capture_stat;

/*---------------------基础初始化和设置 begin--------------------------------*/

/* 打印onlinerefresh信息 */
static void ripple_onlinerefresh_capture_print(ripple_onlinerefresh_capture *olcapture)
{
    HASH_SEQ_STATUS snap_status;
    char* uuid                      = NULL;
    dlistnode *dlnode               = NULL;
    FullTransactionId *xid_p        = NULL;
    ripple_snapshot_xid* entry      = NULL;
    ripple_refresh_table* table     = NULL;

    uuid = uuid2string(olcapture->no);
    elog(RLOG_INFO, "onlinerefresh capture :%s, increment: %d, txid:%lu, redo:%X/%X", 
                    uuid,
                    olcapture->increment,
                    olcapture->txid,
                    (uint32)(olcapture->redo.wal.lsn >> 32),
                    (uint32)(olcapture->redo.wal.lsn));

    elog(RLOG_INFO, "onlinerefresh capture:%s, snapshot: %s, xmin:%lu, xmax:%lu", 
                    uuid,
                    olcapture->snapshot->name,
                    olcapture->snapshot->xmin,
                    olcapture->snapshot->xmax);
    hash_seq_init(&snap_status, olcapture->snapshot->xids);
    while (NULL != (entry = hash_seq_search(&snap_status)))
    {
        /* 拷贝 hash */
        elog(RLOG_INFO, "onlinerefresh snapshotxids:%s - %lu", uuid, entry->xid);
    }

    table = olcapture->tables->tables;
    while(NULL != table)
    {
        elog(RLOG_INFO, "onlinerefresh refresh table:%s - %s.%s", uuid, table->schema, table->table);
        table = table->next;
    }

    if (dlist_isnull(olcapture->xids))
    {
        rfree(uuid);
        return;
    }

    dlnode = olcapture->xids->head;
    while (dlnode)
    {
        xid_p = (FullTransactionId *)dlnode->value;
        elog(RLOG_INFO, "onlinerefresh xids:%s - %lu", uuid, *xid_p);
        dlnode = dlnode->next;
    }

    rfree(uuid);
    return;
}

/* 初始化onlinerefresh 节点 */
ripple_onlinerefresh_capture *ripple_onlinerefresh_capture_init(bool increment)
{
    ripple_onlinerefresh_capture *olrefresh = NULL;

    olrefresh = rmalloc0(sizeof(ripple_onlinerefresh_capture));
    if (!olrefresh)
    {
        elog(RLOG_WARNING, "oom");
        return NULL;
    }
    rmemset0(olrefresh, 0, 0, sizeof(ripple_onlinerefresh_capture));
    olrefresh->parallelcnt = guc_getConfigOptionInt(RIPPLE_CFG_KEY_MAX_WORK_PER_REFRESH);
    if(0 == olrefresh->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }
    olrefresh->thrsmgr = NULL;

    /* 初始化存量数据任务队列 */
    olrefresh->refreshtqueue = ripple_queue_init();

    if (increment)
    {
        /* 缓存初始化 */
        olrefresh->parser2serialtxns = ripple_cache_txn_init();
        olrefresh->txn2filebuffer = ripple_file_buffer_init();
        olrefresh->recordqueue = ripple_queue_init();
    }

    return olrefresh;
}

/* 比较函数 */
int ripple_onlinerefresh_capture_cmp(void* s1, void* s2)
{
    ripple_onlinerefresh_capture* olrefresh1 = NULL;
    ripple_onlinerefresh_capture* olrefresh2 = NULL;

    olrefresh1 = (ripple_onlinerefresh_capture*)s1;
    olrefresh2 = (ripple_onlinerefresh_capture*)s2;

    if (0 == memcmp(olrefresh1->no->data, olrefresh2->no->data, RIPPLE_UUID_LEN))
    {
        return 0;
    }
    return 1;
}

/* 在解析器中获取时间线 */
static bool ripple_onlinerefresh_capture_gettlidfromparser(void* args, TimeLineID* tlid)
{
    ListCell* lc = NULL;
    ripple_thrref* thrref = NULL;
    ripple_thrnode* thrnode = NULL;
    ripple_decodingcontext *decodingctx;
    ripple_onlinerefresh_capture* olcapture = NULL;
    ripple_onlinerefresh_captureparser *cparser = NULL;

    if (NULL == args)
    {
        /* never come here */
        elog(RLOG_WARNING, "capture parserwal rewinding state exception, privdata point is NULL");
        return false;
    }
    olcapture = (ripple_onlinerefresh_capture*)args;

    /* 获取 loadrecord 线程 */
    lc = olcapture->thrsmgr->childthrrefs->head;

    /* 获取 parser 线程 */
    lc = lc->next;
    thrref = (ripple_thrref*)lfirst(lc);
    thrnode = ripple_threads_getthrnodebyno(olcapture->thrsmgr->parents, thrref->no);
    if(NULL == thrnode)
    {
        elog(RLOG_WARNING, "capture onlinerefresh can not get parser thread by no:%lu", thrref->no);
        return false;
    }

    /* 获取 onlinerefresh parser 节点 */
    cparser = (ripple_onlinerefresh_captureparser *)thrnode->data;
    if (NULL == cparser)
    {
        elog(RLOG_WARNING, "olcapture_serial point is NULL");
        return false;
    }
    decodingctx = cparser->decodingctx;

    *tlid = decodingctx->base.curtlid;
    return true;
}

/* 设置工作目录 */
static void ripple_onlinerefresh_capture_setdata(ripple_onlinerefresh_capture *olcapture, char *data)
{
    olcapture->data = data;
}


void ripple_onlinerefresh_capture_increment_set(ripple_onlinerefresh_capture *onlinerefresh_capture, bool increment)
{
    onlinerefresh_capture->increment = increment;
}

void ripple_onlinerefresh_capture_redo_set(ripple_onlinerefresh_capture *onlinerefresh_capture, XLogRecPtr redo)
{
    onlinerefresh_capture->redo.wal.lsn = redo;
}

void ripple_onlinerefresh_capture_conninfo_set(ripple_onlinerefresh_capture *onlinerefresh_capture, char* conninfo)
{
    onlinerefresh_capture->conninfo = conninfo;
}

void ripple_onlinerefresh_capture_snapshot_set(ripple_onlinerefresh_capture *onlinerefresh_capture, ripple_snapshot *snapshot)
{
    onlinerefresh_capture->snapshot = snapshot;
}

void ripple_onlinerefresh_capture_conn_set(ripple_onlinerefresh_capture *onlinerefresh_capture, PGconn *conn)
{
    onlinerefresh_capture->conn = conn;
}

void ripple_onlinerefresh_capture_snap_conn_set(ripple_onlinerefresh_capture *onlinerefresh_capture, PGconn *snap_conn)
{
    onlinerefresh_capture->snap_conn = snap_conn;
}

void ripple_onlinerefresh_capture_no_set(ripple_onlinerefresh_capture *onlinerefresh_capture, ripple_uuid_t *no)
{
    onlinerefresh_capture->no = no;
}

void ripple_onlinerefresh_capture_txid_set(ripple_onlinerefresh_capture *onlinerefresh_capture, FullTransactionId txid)
{
    onlinerefresh_capture->txid = txid;
}

void ripple_onlinerefresh_capture_xids_append(ripple_onlinerefresh_capture *onlinerefresh_capture, TransactionId xid)
{
    FullTransactionId *xid_p = NULL;
    dlistnode *xid_dlnode = NULL;

    /* 由于xid是不可重复的, 因此在这里先检查是否有重复的xid */
    if (onlinerefresh_capture->xids)
    {
        xid_dlnode = onlinerefresh_capture->xids->head;
        while (xid_dlnode)
        {
            FullTransactionId *xid_temp = (FullTransactionId *) xid_dlnode->value;
            if (*xid_temp == (FullTransactionId)xid)
            {
                return;
            }
            xid_dlnode = xid_dlnode->next;
        }
    }

    xid_p = rmalloc0(sizeof(FullTransactionId));
    if (!xid_p)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(xid_p, 0, 0, sizeof(FullTransactionId));
    *xid_p = (FullTransactionId)xid;
    onlinerefresh_capture->xids = dlist_put(onlinerefresh_capture->xids, xid_p);
}

void ripple_onlinerefresh_capture_add_xids_from_snapshot(ripple_onlinerefresh_capture *onlinerefresh_capture, ripple_snapshot *snap)
{
    HASH_SEQ_STATUS status;
    ripple_snapshot_xid* entry = NULL;
    TransactionId xid = InvalidTransactionId;

    hash_seq_init(&status, snap->xids);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        xid = entry->xid;
        ripple_onlinerefresh_capture_xids_append(onlinerefresh_capture, xid);
    }
}

bool ripple_onlinerefresh_capture_isxidinsnapshot(ripple_onlinerefresh_capture* onlinerefresh_capture, FullTransactionId xid)
{
    TransactionId txid = (TransactionId) xid;
    bool find = false;
    if (!onlinerefresh_capture || !onlinerefresh_capture->snapshot->xids)
    {
        return false;
    }

    /* 注意: snapshot的xids哈希里的key实际上是TransactionId */
    hash_search(onlinerefresh_capture->snapshot->xids, &txid, HASH_FIND, &find);

    return find;
}

bool ripple_onlinerefresh_capture_isxidinxids(ripple_onlinerefresh_capture* onlinerefresh_capture, FullTransactionId xid)
{
    dlistnode *dlnode = NULL;
    FullTransactionId *xid_p = NULL;

    dlnode = onlinerefresh_capture->xids->head;
    while (dlnode)
    {
        xid_p = (FullTransactionId *)dlnode->value;
        if (*xid_p == xid)
        {
            return true;
        }
        dlnode = dlnode->next;
    }
    return false;
}


static void ripple_onlinerefresh_capture_free_xid(void *xid_p)
{
    if (xid_p)
    {
        rfree(xid_p);
    }
}

void ripple_onlinerefresh_capture_xids_delete(ripple_onlinerefresh_capture *olcapture, dlistnode *dlnode)
{
    olcapture->xids = dlist_delete(olcapture->xids, dlnode, ripple_onlinerefresh_capture_free_xid);
}

bool ripple_onlinerefresh_capture_xids_isnull(ripple_onlinerefresh_capture* olcapture)
{
    return dlist_isnull(olcapture->xids);
}

void ripple_onlinerefresh_capture_tables_set(ripple_onlinerefresh_capture *onlinerefresh_capture, ripple_refresh_tables *tables)
{
    onlinerefresh_capture->tables = tables;
}

/*---------------------基础初始化和设置   end--------------------------------*/

/* 创建工作目录 */
static bool ripple_onlinerefresh_capture_trymkdatadir(ripple_onlinerefresh_capture *olcapture)
{
    char path[RIPPLE_MAXPATH] = {'\0'};
    char *uuid_str = NULL;
    if(NULL == olcapture->data)
    {
        /* 生成主目录 */
        uuid_str = uuid2string(olcapture->no);
        sprintf(path, "%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA), RIPPLE_REFRESH_ONLINEREFRESH, uuid_str);
        ripple_onlinerefresh_capture_setdata(olcapture, rstrdup(path));
        rfree(uuid_str);
    }

    /* 创建onlinerefresh uuid主目录 */
    if (!DirExist(olcapture->data))
    {
        if(0 != MakeDir(olcapture->data))
        {
            elog(RLOG_WARNING, "could not create directory:%s, %s", olcapture->data, strerror(errno));
            return false;
        }
    }

    return true;
}

/* 创建 onlinerefresh->refresh 存储目录 */
static bool ripple_onlinerefresh_capture_trymkrefreshdir(ripple_onlinerefresh_capture *olcapture)
{
    StringInfo path = NULL;
    StringInfo path_partial = NULL;
    StringInfo path_complete = NULL;
    ripple_refresh_table *table = NULL;
    ripple_refresh_tables *tables = olcapture->tables;

    path = makeStringInfo();
    path_partial = makeStringInfo();
    path_complete = makeStringInfo();

    /* 创建存量表目录 */
    for (table = tables->tables; table != NULL; table = table->next)
    {
        resetStringInfo(path);
        appendStringInfo(path, "%s/%s/%s_%s",
                                     olcapture->data,
                                     RIPPLE_REFRESH_REFRESH,
                                     table->schema,
                                     table->table);
        if (!DirExist(path->data))
        {
            resetStringInfo(path_partial);
            resetStringInfo(path_complete);

            if(0 != MakeDir(path->data))
            {
                elog(RLOG_WARNING, "could not create directory:%s, %s", path, strerror(errno));
                return false;
            }
            appendStringInfo(path_partial, "%s/%s",
                                            path->data,
                                            RIPPLE_REFRESH_PARTIAL);
            appendStringInfo(path_complete, "%s/%s",
                                            path->data,
                                            RIPPLE_REFRESH_COMPLETE);

            if(0 != MakeDir(path_partial->data))
            {
                elog(RLOG_WARNING, "could not create directory:%s, %s", path_partial->data, strerror(errno));
                return false;
            }

            if(0 != MakeDir(path_complete->data))
            {
                elog(RLOG_WARNING, "could not create directory:%s, %s", path_complete->data, strerror(errno));
                return false;
            }
        }
    }

    deleteStringInfo(path);
    deleteStringInfo(path_complete);
    deleteStringInfo(path_partial);
    return true;
}

/* 创建增量存储目录 */
static bool ripple_onlinerefresh_capture_trymkincrementdir(ripple_onlinerefresh_capture *olcapture)
{
    StringInfo path = NULL;
    if(false == olcapture->increment)
    {
        /* 此次 onlinerefresh 不涉及到增量的同步 */
        return true;
    }

    /* 创建增量表目录 */
    path = makeStringInfo();
    appendStringInfo(path, "%s/%s", olcapture->data,
                                    RIPPLE_STORAGE_TRAIL_DIR);
    if (!DirExist(path->data))
    {
        if(0 != MakeDir(path->data))
        {
            elog(RLOG_WARNING, "could not create directory:%s, %s", path, strerror(errno));
            deleteStringInfo(path);
            return false;
        }
    }
    deleteStringInfo(path);
    return true;
}

/* 启动 refresh 工作线程 */
static bool ripple_onlinerefresh_capture_startrefreshjob(ripple_onlinerefresh_capture* olcapture)
{
    int index                                           = 0;
    ripple_task_refresh_sharding2file* sharding2file    = NULL;
    char refreshpath[RIPPLE_ABSPATH]                    = { 0 };
    for(index = 0; index < olcapture->parallelcnt; index++)
    {
        /* 分配空间和初始化 */
        sharding2file = ripple_refresh_sharding2file_init();
        sharding2file->conn = NULL;
        sharding2file->conn_info = olcapture->conninfo;

        /* 生成存储目录 */
        rmemset1(refreshpath, 0, '\0', RIPPLE_ABSPATH);
        snprintf(refreshpath,
                RIPPLE_ABSPATH,
                "%s/%s",
                olcapture->data,
                RIPPLE_REFRESH_REFRESH);
        sharding2file->refresh_path = rstrdup(refreshpath);

        /* 设置快照名称用于导出数据和任务队列 */
        sharding2file->snap_shot_name = olcapture->snapshot->name;
        sharding2file->tqueue = olcapture->refreshtqueue;

        /* 注册工作线程 */
        if(false == ripple_threads_addjobthread(olcapture->thrsmgr->parents,
                                                RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_JOB,
                                                olcapture->thrsmgr->submgrref.no,
                                                (void*)sharding2file,
                                                ripple_refresh_sharding2file_free,
                                                NULL,
                                                ripple_refresh_sharding2file_work))
        {
            elog(RLOG_WARNING, "onlinerefresh capture start job error");
            return false;
        }
    }
    return true;
}

/* 启动增量工作线程 */
static bool ripple_onlinerefresh_capture_startincrementjob(ripple_onlinerefresh_capture* olcapture)
{
    ripple_onlinerefresh_captureflush* cflush = NULL;
    ripple_onlinerefresh_captureserial* cserial = NULL;
    ripple_onlinerefresh_captureparser* cparser = NULL;
    ripple_onlinerefresh_captureloadrecord *cloadrecord = NULL;
    char path[RIPPLE_MAXPATH] = {'\0'};
    if(false == olcapture->increment)
    {
        /* 不需要启动增量 */
        return true;
    }

    /*---------------------刷新线程 begin------------------*/
    cflush = ripple_onlinerefresh_captureflush_init();
    if(NULL == cflush)
    {
        elog(RLOG_WARNING, "onlinerefresh capture flush init error");
        return false;
    }
    cflush->txn2filebuffer = olcapture->txn2filebuffer;
    rmemset1(path, 0, 0, RIPPLE_MAXPATH);
    sprintf(path, "%s/%s", olcapture->data, RIPPLE_STORAGE_TRAIL_DIR);
    cflush->trail = rstrdup(path);

    /*---------------------刷新线程   end------------------*/

    /*---------------------序列化线程 begin----------------*/
    cserial = ripple_onlinerefresh_captureserial_init();
    if(NULL == cserial)
    {
        elog(RLOG_WARNING, "onlinerefresh capture serial init error");
        return false;
    }
    cserial->privdata = (void*)olcapture;
    cserial->parser2serialtxns = olcapture->parser2serialtxns;
    cserial->serialstate->txn2filebuffer = olcapture->txn2filebuffer;
    cserial->callback.parserstat_curtlid_get = ripple_onlinerefresh_capture_gettlidfromparser;
    /*---------------------序列化线程   end----------------*/

    /*---------------------解析器线程 begin----------------*/
    /* 
     * 解析器初始化
     * parserwal回调设置
    */
    cparser = ripple_onlinerefresh_captureparser_init();
    cparser->decodingctx->privdata = (void*)olcapture;
    cparser->decodingctx->recordqueue = olcapture->recordqueue;
    cparser->decodingctx->parser2txns = olcapture->parser2serialtxns;

    /* 加载解析元数据信息, 时区字符集数据字典等 */
    ripple_onlinerefresh_captureparser_loadmetadata(cparser);

    /* 生成同步集合 */
    if(false == ripple_onlinerefresh_captureparser_datasetinit(cparser->decodingctx, olcapture))
    {
        elog(RLOG_WARNING, "onlinerefresh capture init sync dataset error");
        return false;
    }

    /* 
     * 设置 split lsn 的回调设置函数为空
     *  onlinerefresh 的解析的起点为: increment 常量中的 redolsn/restartlsn/confirmlsn
     */
    cparser->decodingctx->callback.setloadlsn = NULL;

    /*---------------------解析器线程   end----------------*/

    /*---------------------日志拆分线程 begin--------------*/
    cloadrecord = ripple_onlinerefresh_captureloadrecord_init();
    if(NULL == cloadrecord)
    {
        elog(RLOG_WARNING, "onlinerefresh capture init splitwanl error");
        return false;
    }
    cloadrecord->splitwalctx->privdata = (void *)olcapture;
    cloadrecord->splitwalctx->recordqueue = olcapture->recordqueue;

    /* 设置解析的时间线 */
    cloadrecord->splitwalctx->loadrecords->timeline = cparser->decodingctx->base.curtlid;
    cloadrecord->splitwalctx->loadrecords->startptr = olcapture->redo.wal.lsn;
    cloadrecord->splitwalctx->callback.parserwal_rewindstat_setemiting = NULL;
    /*---------------------日志拆分线程   end--------------*/

    /*
     * 启动各线程
     */
    /* 注册刷新线程 */
    if(false == ripple_threads_addjobthread(olcapture->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_FLUSH,
                                            olcapture->thrsmgr->submgrref.no,
                                            (void*)cflush,
                                            ripple_onlinerefresh_captureflush_free,
                                            NULL,
                                            ripple_onlinerefresh_captureflush_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment flush job error");
        return false;
    }
    
    /* 注册序列化线程 */
    if(false == ripple_threads_addjobthread(olcapture->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_SERIAL,
                                            olcapture->thrsmgr->submgrref.no,
                                            (void*)cserial,
                                            ripple_onlinerefresh_captureserial_free,
                                            NULL,
                                            ripple_onlinerefresh_captureserial_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment serial job error");
        return false;
    }

    /* 注册解析器线程 */
    if(false == ripple_threads_addjobthread(olcapture->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_PARSER,
                                            olcapture->thrsmgr->submgrref.no,
                                            (void*)cparser,
                                            ripple_onlinerefresh_captureparser_free,
                                            NULL,
                                            ripple_onlinerefresh_captureparser_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment parser job error");
        return false;
    }

    /* 注册 loadrecords线程 */
    if(false == ripple_threads_addjobthread(olcapture->thrsmgr->parents,
                                            RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_LOADRECORDS,
                                            olcapture->thrsmgr->submgrref.no,
                                            (void*)cloadrecord,
                                            ripple_onlinerefresh_captureloadrecord_free,
                                            NULL,
                                            ripple_onlinerefresh_captureloadrecord_main))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment parser job error");
        return false;
    }
    return true;
}

/* 遍历待refresh表, 生成queue */
static bool ripple_onlinerefresh_capture_tables2shardings(ripple_onlinerefresh_capture *olcapture)
{
    StringInfo  str = NULL;
    PGresult   *res = NULL;
    PGconn     *conn = olcapture->conn;
    uint32      ctid_blkid_max = 0;
    ripple_refresh_table *table = NULL;
    int         max_shard_num = guc_getConfigOptionInt(RIPPLE_CFG_KEY_MAX_PAGE_PER_REFRESHSHARDING);

    str = makeStringInfo();

    elog(RLOG_DEBUG, "capture refresh mgr, gen queue begin");

    /* 先判断连接是否存在, 不存在打开连接 */
    while(!conn)
    {
        conn = ripple_conn_get(olcapture->conninfo);

        if (NULL != conn)
        {
            olcapture->conn = conn;
            break;
        }
    }

    /* 遍历待同步表链表, 查询最大ctid估值数, 进行分片后生成任务 */
    for (table = olcapture->tables->tables; table != NULL; table = table->next)
    {
        uint32 left = 0;
        uint32 right = 0;
        uint32 remain = 0;
        int    shard_no = 1;

        appendStringInfo(str, "select pg_relation_size('\"%s\".\"%s\"')/%d;", table->schema, table->table, g_blocksize);
        res = PQexec(conn, str->data);
        if (PGRES_TUPLES_OK != PQresultStatus(res))
        {
            elog(RLOG_WARNING,"Failed execute in compute max ctid: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            olcapture->conn = NULL;
            deleteStringInfo(str);
            return false;
        }

        if (PQntuples(res) != 0 )
        {
            ctid_blkid_max = (uint32) atoi(PQgetvalue(res, 0, 0));
        }
        else
        {
            elog(RLOG_WARNING,"Failed execute in compute max ctid: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            olcapture->conn = NULL;
            deleteStringInfo(str);
            return false;
        }

        /* 重置 */
        resetStringInfo(str);
        PQclear(res);

        /* 没有数据的情况下直接设置分片为0 */
        if (ctid_blkid_max == 0)
        {
            ripple_refresh_table_sharding *table_shard = NULL;

            table_shard = ripple_refresh_table_sharding_init();

            /* 分片 */
            ripple_refresh_table_sharding_set_schema(table_shard, table->schema);
            ripple_refresh_table_sharding_set_table(table_shard, table->table);
            ripple_refresh_table_sharding_set_shardings(table_shard, 0);
            ripple_refresh_table_sharding_set_shardno(table_shard, 0);
            ripple_refresh_table_sharding_set_condition(table_shard, NULL);

            elog(RLOG_DEBUG, "capture refresh mgr, queue: %s.%s %4d %4d", table_shard->schema,
                                                                          table_shard->table,
                                                                          table_shard->shardings,
                                                                          table_shard->sharding_no);
            /* 添加到缓存中 */
            ripple_queue_put(olcapture->refreshtqueue, (void *)table_shard);
            continue;
        }

        /* 第一次计算分片值 */
        right = ctid_blkid_max < max_shard_num ? ctid_blkid_max : max_shard_num;
        remain = ctid_blkid_max;

        /* 生成queue */
        do
        {
            ripple_refresh_table_sharding *table_shard = NULL;
            ripple_refresh_table_condition *cond = NULL;

            table_shard = ripple_refresh_table_sharding_init();
            cond = ripple_refresh_table_sharding_condition_init();

            /* 分片 */
            ripple_refresh_table_sharding_set_schema(table_shard, table->schema);
            ripple_refresh_table_sharding_set_table(table_shard, table->table);
            ripple_refresh_table_sharding_set_shardings(table_shard, ((ctid_blkid_max - 1) / max_shard_num) + 1);
            ripple_refresh_table_sharding_set_shardno(table_shard, shard_no++);
            ripple_refresh_table_sharding_set_condition(table_shard, cond);

            cond->left_condition = left;
            cond->right_condition = right;

            elog(RLOG_DEBUG, "capture refresh mgr, queue: %s.%s %4d %4d", table_shard->schema,
                                                                          table_shard->table,
                                                                          table_shard->shardings,
                                                                          table_shard->sharding_no);

            /* 添加到缓存中 */
            ripple_queue_put(olcapture->refreshtqueue, (void *)table_shard);

            remain = ctid_blkid_max - right;
            left = right;
            right += remain > max_shard_num ? max_shard_num : remain;
        } while (remain);
    }

    /* 清理工作 */
    deleteStringInfo(str);

    return true;
}

static void recordqueue_dlist_free(dlist* record_dlist)
{
    dlist_free(record_dlist, (dlistvaluefree)ripple_record_free);
}

static bool ripple_onlinerefresh_capture_keep_alive(PGconn* conn)
{
    PGresult  *res = NULL;
    res = PQexec(conn, "SELECT 1;");
    if (PGRES_TUPLES_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,"Failed in snapshot keep alive: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

/* 主处理流程 */
void *ripple_onlinerefresh_capture_main(void* args)
{
    int skipcnt = 0;
    int jobcnt = 0;
    uint32 delay = 200;
    ripple_onlinerefresh_capture_stat jobstat = RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBNOP;
    ListCell* lc = NULL;
    ripple_thrref* thrref = NULL;
    ripple_thrnode* thrnode = NULL;
    ripple_thrnode* incflushthrnode = NULL;
    ripple_thrnode* incserialthrnode = NULL;
    ripple_thrnode* incparserthrnode = NULL;
    ripple_thrnode* incloadrecthrnode = NULL;
    ripple_onlinerefresh_capture *olcapture = NULL;

    thrnode = (ripple_thrnode *)args;
    olcapture = (ripple_onlinerefresh_capture *)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh capture stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 没有待同步的表的情况下可以退出 */
    if (NULL == olcapture)
    {
        elog(RLOG_WARNING, "no tables need do onlinerefresh");
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为启动状态 */
    elog(RLOG_DEBUG, "onlinerefresh capture start");
    ripple_onlinerefresh_capture_print(olcapture);

    /* 创建主目录 */
    if(false == ripple_onlinerefresh_capture_trymkdatadir(olcapture))
    {
        /* 创建工作目录失败 */
        elog(RLOG_WARNING, "capture onlinerefresh mk data dir error");
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 
     * 创建存量目录, 并注册工作线程
     */
    ripple_onlinerefresh_capture_trymkrefreshdir(olcapture);
    if(false == ripple_onlinerefresh_capture_startrefreshjob(olcapture))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start refresh job thread error");
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /*
     * 创建增量目录，并启动增量工作线程
     */
    if(false == ripple_onlinerefresh_capture_trymkincrementdir(olcapture))
    {
        elog(RLOG_WARNING, "onlinerefresh capture make increment job thread data error");
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 启动增量工作线程 */
    if(false == ripple_onlinerefresh_capture_startincrementjob(olcapture))
    {
        elog(RLOG_WARNING, "onlinerefresh capture start increment thread error");
        thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    if(true == olcapture->increment)
    {
        skipcnt = 4;
        /* 预先获取到 increment->parser 线程, 方面后续的逻辑判断 */
        /* 获取 loadrecord 线程 */
        lc = olcapture->thrsmgr->childthrrefs->head;
        thrref = (ripple_thrref*)lfirst(lc);
        incloadrecthrnode = ripple_threads_getthrnodebyno(olcapture->thrsmgr->parents, thrref->no);
        if(NULL == incloadrecthrnode)
        {
            elog(RLOG_WARNING, "capture onlinerefresh can not get load record thread by no:%lu", thrref->no);
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            ripple_pthread_exit(NULL);
        }

        /* 获取 parser 线程 */
        lc = lc->next;
        thrref = (ripple_thrref*)lfirst(lc);
        incparserthrnode = ripple_threads_getthrnodebyno(olcapture->thrsmgr->parents, thrref->no);
        if(NULL == incparserthrnode)
        {
            elog(RLOG_WARNING, "capture onlinerefresh can not get parser thread by no:%lu", thrref->no);
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            ripple_pthread_exit(NULL);
        }

        /* 获取 serail 线程 */
        lc = lc->next;
        thrref = (ripple_thrref*)lfirst(lc);
        incserialthrnode = ripple_threads_getthrnodebyno(olcapture->thrsmgr->parents, thrref->no);
        if(NULL == incserialthrnode)
        {
            elog(RLOG_WARNING, "capture onlinerefresh can not get serial thread by no:%lu", thrref->no);
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            ripple_pthread_exit(NULL);
        }

        /* 获取 flush 线程 */
        lc = lc->next;
        thrref = (ripple_thrref*)lfirst(lc);
        incflushthrnode = ripple_threads_getthrnodebyno(olcapture->thrsmgr->parents, thrref->no);
        if(NULL == incflushthrnode)
        {
            elog(RLOG_WARNING, "capture onlinerefresh can not get inc flush thread by no:%lu", thrref->no);
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            ripple_pthread_exit(NULL);
        }
    }

    jobstat = RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBSTARTING;
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

        /* snapshot 保活 */
        if (delay >= 200)
        {
            delay = 0;
            ripple_onlinerefresh_capture_keep_alive(olcapture->snap_conn);
        }

        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 等待子线程全部启动成功 */
        if(RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBSTARTING == jobstat)
        {
            /* 查看是否已经启动成功 */
            jobcnt = 0;
            delay++;
            if(false == ripple_threads_countsubmgrjobthredsabovework(olcapture->thrsmgr->parents,
                                                                    olcapture->thrsmgr->childthrrefs,
                                                                    &jobcnt))
            {
                elog(RLOG_WARNING, "capture onlinerefresh count job thread above work stat error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_onlinerefresh_capture_main_done;
            }

            if(jobcnt != olcapture->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWORKING;
            continue;
        }
        else if(RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWORKING == jobstat)
        {
            /* 都启动了, 此时将存量任务加入到队列中 */
            if(false == ripple_onlinerefresh_capture_tables2shardings(olcapture))
            {
                /* 向队列中加入任务失败, 那么管理线程退出, 子线程的回收由主线程处理 */
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
            jobstat = RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWAITREFRESHDONE;
            delay++;
            continue;
        }
        else if(RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWAITREFRESHDONE == jobstat)
        {
            /*
             * 等待存量线程退出
             *  1、队列为空
             *  2、存量线程完全退出
             */
            if(false == ripple_queue_isnull(olcapture->refreshtqueue))
            {
                /* 队列不为空, 证明还有任务需要处理 */
                delay++;
                continue;
            }

            /* 设置空闲的线程退出并统计退出的线程个数 */
            jobcnt = olcapture->parallelcnt;
            if(false == ripple_threads_setsubmgrjobthredstermandcountexit(olcapture->thrsmgr->parents,
                                                                        olcapture->thrsmgr->childthrrefs,
                                                                        skipcnt,
                                                                        &jobcnt))
            {
                elog(RLOG_WARNING, "capture refresh set job threads term in idle error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_onlinerefresh_capture_main_done;
            }

            if(jobcnt != olcapture->parallelcnt)
            {
                continue;
            }

            /* 设置存量线程退出, 跳过前面的 4 个增量线程 */
            ripple_threads_setsubmgrjobthredsfree(olcapture->thrsmgr->parents,
                                                olcapture->thrsmgr->childthrrefs,
                                                skipcnt,
                                                jobcnt);

            jobstat = RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWAITINCREMENTDONE;

            /* 清理掉连接 */
            if (olcapture->conn)
            {
                PQfinish(olcapture->conn);
                olcapture->conn = NULL;
            }

            if (olcapture->snap_conn)
            {
                PQfinish(olcapture->snap_conn);
                olcapture->snap_conn = NULL;
            }
            continue;
        }
        else if(RIPPLE_ONLINEREFRESH_CAPTURE_STAT_JOBWAITINCREMENTDONE == jobstat)
        {
            /* 
             * 在 onlinerefresh 中, 需要先等待 increment->parser 线程退出
             *  parser 在同步过程中捕获完快照中的事务后就会退出
             *  解析线程在退出时会设置 onlinerefreshend, 此后 序列化线程和刷新线程也会相继退出
             *  等待 解析、序列化、刷新线程退出后, 设置 loadrecord 线程为 TERM
             */
            if(false == olcapture->increment)
            {
                /* 没有增量线程, 设置本线程退出 */
                thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
                break;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != incparserthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != incserialthrnode->stat
                || RIPPLE_THRNODE_STAT_EXITED != incflushthrnode->stat)
            {
                /* 解析线程未退出, 等待 */
                continue;
            }

            /* 设置 loadrecords 线程退出 */
            if(RIPPLE_THRNODE_STAT_TERM > incloadrecthrnode->stat)
            {
                incloadrecthrnode->stat = RIPPLE_THRNODE_STAT_TERM;
                ripple_queue_destroy(olcapture->recordqueue, (dlistvaluefree )recordqueue_dlist_free);
                olcapture->recordqueue = NULL;
                continue;
            }

            if(RIPPLE_THRNODE_STAT_EXITED != incloadrecthrnode->stat)
            {
                continue;
            }

            /* 所有线程都已经退出, 管理线程可以退出了 */
            jobcnt = olcapture->thrsmgr->childthrrefs->length - olcapture->parallelcnt;
            ripple_threads_setsubmgrjobthredsfree(olcapture->thrsmgr->parents,
                                                olcapture->thrsmgr->childthrrefs,
                                                0,
                                                jobcnt);
            
            /* 设置本线程退出 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
    }

ripple_onlinerefresh_capture_main_done:
    ripple_pthread_exit(NULL);
    return NULL;
}

/* 释放 */
void ripple_onlinerefresh_capture_destroy(void* privdata)
{
    ripple_onlinerefresh_capture *olcapture = NULL;

    olcapture = (ripple_onlinerefresh_capture *)privdata;
    if(NULL == olcapture)
    {
        return;
    }

    /* 在父结构体的 olrefreshing 中删除本节点 */
    olcapture->removeolrefresh(olcapture->privdata, privdata);

    if (olcapture->conn)
    {
        PQfinish(olcapture->conn);
    }

    if (olcapture->snap_conn)
    {
        PQfinish(olcapture->snap_conn);
    }

    if (olcapture->snapshot)
    {
        if (olcapture->snapshot->name)
        {
            rfree(olcapture->snapshot->name);
        }
        if (olcapture->snapshot->xids)
        {
            hash_destroy(olcapture->snapshot->xids);
        }
        rfree(olcapture->snapshot);
    }

    if (olcapture->data)
    {
        rfree(olcapture->data);
    }

    if (olcapture->no)
    {
        ripple_uuid_free(olcapture->no);
    }

    if (olcapture->refreshtqueue)
    {
        ripple_queue_destroy(olcapture->refreshtqueue, NULL);
    }

    /* 清理refresh tables */
    if (olcapture->tables)
    {
        ripple_refresh_freetables(olcapture->tables);
    }

    if (olcapture->recordqueue)
    {
        ripple_queue_destroy(olcapture->recordqueue, (dlistvaluefree)recordqueue_dlist_free);
    }

    if (olcapture->parser2serialtxns)
    {
        ripple_cache_txn_destroy(olcapture->parser2serialtxns);
    }

    if (olcapture->txn2filebuffer)
    {
        ripple_file_buffer_destroy(olcapture->txn2filebuffer);
    }

    rfree(olcapture);
}
