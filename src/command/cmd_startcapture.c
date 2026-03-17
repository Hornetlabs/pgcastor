#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/uuid/uuid.h"
#include "utils/conn/conn.h"
#include "utils/init/datainit.h"
#include "utils/daemon/process.h"
#include "utils/regex/regex.h"
#include "utils/init/databaserecv.h"
#include "misc/misc_stat.h"
#include "misc/misc_lockfiles.h"
#include "misc/misc_control.h"
#include "signal/app_signal.h"
#include "storage/file_buffer.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "snapshot/snapshot.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "serial/serial.h"
#include "threads/threads.h"
#include "xmanager/xmanager_msg.h"
#include "net/netpacket/netpacket.h"
#include "refresh/refresh_tables.h"
#include "refresh/capture/refresh_capture.h"
#include "metric/capture/metric_capture.h"
#include "increment/capture/flush/increment_captureflush.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/bigtxn.h"
#include "bigtransaction/capture/flush/bigtxn_captureflush.h"
#include "bigtransaction/capture/serial/bigtxn_captureserial.h"
#include "onlinerefresh/capture/onlinerefresh_capture.h"
#include "increment/capture/serial/increment_captureserial.h"
#include "strategy/filter_dataset.h"
#include "works/splitwork/wal/wal_define.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/parserwork_wal.h"
#include "increment/capture/increment_capture.h"
#include "command/cmd_startcapture.h"

/* 启动常驻线程 */
static bool cmd_startcapturethreads(increment_capture* inccapture)
{
    thrnode* thr_node                 = NULL;

    /*-------------------------------启动常驻工作线程 begin---------------------------------*/
    /* 启动的顺序为退出的逆序, 即先启动的后退出 */
    /* 启动落盘线程 */
    if(false == threads_addpersistthread(inccapture->threads,
                                                &thr_node,
                                                THRNODE_IDENTITY_INC_CAPTURE_BIGTXNFLUSH,
                                                inccapture->persistno,
                                                (void*)inccapture->bigtxnwritestate,
                                                NULL,
                                                NULL,
                                                bigtxn_captureflush_main))
    {
        elog(RLOG_WARNING, "add capture increment bigtxn flush persist to threads error");
        return false;
    }

    /* 启动大事务序列化线程 */
    if(false == threads_addpersistthread(inccapture->threads,
                                                &thr_node,
                                                THRNODE_IDENTITY_INC_CAPTURE_BIGTXNSERIAL,
                                                inccapture->persistno,
                                                (void*)inccapture->bigtxnserialstate,
                                                NULL,
                                                NULL,
                                                bigtxn_captureserial_main))
    {
        elog(RLOG_WARNING, "add capture increment bigtxn serial persist to threads error");
        return false;
    }

    /* 启动增量落盘线程 */
    if(false == threads_addpersistthread(inccapture->threads,
                                                &thr_node,
                                                THRNODE_IDENTITY_INC_CAPTURE_FLUSH,
                                                inccapture->persistno,
                                                (void*)inccapture->writestate,
                                                NULL,
                                                NULL,
                                                increment_captureflush_main))
    {
        elog(RLOG_WARNING, "add capture increment flush persist to threads error");
        return false;
    }

    /* 启动增量序列化线程 */
    if(false == threads_addpersistthread(inccapture->threads,
                                                &thr_node,
                                                THRNODE_IDENTITY_INC_CAPTURE_SERIAL,
                                                inccapture->persistno,
                                                (void*)inccapture->serialstate,
                                                NULL,
                                                NULL,
                                                increment_captureserial_main))
    {
        elog(RLOG_WARNING, "add capture increment serial persist to threads error");
        return false;
    }

    /* 启动解析器线程 */
    if(false == threads_addpersistthread(inccapture->threads,
                                                &thr_node,
                                                THRNODE_IDENTITY_INC_CAPTURE_PARSER,
                                                inccapture->persistno,
                                                (void*)inccapture->decodingctx,
                                                NULL,
                                                NULL,
                                                parserwork_wal_main))
    {
        elog(RLOG_WARNING, "add capture increment parser persist to threads error");
        return false;
    }

    /* 启动 walwork 线程 */
    if(false == threads_addpersistthread(inccapture->threads,
                                                &thr_node,
                                                THRNODE_IDENTITY_INC_CAPTURE_LOADRECORD,
                                                inccapture->persistno,
                                                (void*)inccapture->splitwalctx,
                                                NULL,
                                                NULL,
                                                splitwork_wal_main))
    {
        elog(RLOG_WARNING, "add capture increment splitwal persist to threads error");
        return false;
    }

    /* 启动指标线程 */
    if(false == threads_addpersistthread(inccapture->threads,
                                                &thr_node,
                                                THRNODE_IDENTITY_CAPTURE_METRIC,
                                                inccapture->persistno,
                                                (void*)inccapture->metric,
                                                NULL,
                                                NULL,
                                                metric_capture_main))
    {
        elog(RLOG_WARNING, "add capture increment metric persist to threads error");
        return false;
    }

    /*-------------------------------启动常驻工作线程   end---------------------------------*/
    return true;
}

/* 将指定的 onlinrefresh 节点在 olrefreshing 节点中删除 */
static void cmd_startcapture_removeonlinerefresh(void* pinccapture, void* polrefresh)
{
    increment_capture* inccapture                    = NULL;

    inccapture = (increment_capture*)pinccapture;

    osal_thread_lock(&inccapture->olrefreshlock);
    dlist_deletebyvalue(inccapture->olrefreshing, polrefresh, onlinerefresh_capture_cmp, NULL);
    osal_thread_unlock(&inccapture->olrefreshlock);
}

/* 构建 onlinerefresh 返回包到 xmanager */
static void cmd_startcaputre_assembleolrefreshpacket(increment_capture* inccapture,
                                                            refresh_tables* rtables,
                                                            bool result,
                                                            int errcode,
                                                            char* msg)
{
    /*
     * 1、构建网络包
     * 2、将包挂载到 metric 上
     */
    uint8 u8value                   = 0;
    int ivalue                      = 0;
    int msglen                      = 0;
    int resultlen                   = 0;
    uint8* uptr                     = NULL;
    netpacket* npacket       = NULL;
    refresh_table* rtable    = NULL;

    /* 总长度 + crc32 + 类型 + 标识 + 错误长度 + 错误码 */
    msglen = (4 + 4 + 4 + 1 + 4 + 4);

    resultlen = strlen("start refresh: ");
    for (rtable = rtables->tables; NULL != rtable; rtable = rtable->next)
    {
        resultlen += strlen(rtable->schema);

        /* '.' */
        resultlen += 1;
        resultlen += strlen(rtable->table);
    }

    /* ' ' */
    resultlen += 1;
    /* 总长度 */
    if (false == result)
    {
        resultlen += strlen("failed, ");
        resultlen += strlen(msg);
        resultlen += 1;
    }
    else
    {
        resultlen += strlen(msg);
    }

    msglen += resultlen;

    npacket = netpacket_init();
    if (NULL == npacket)
    {
        return;
    }

    npacket->used = msglen;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        goto cmd_startcaputre_assembleolrefreshpacket_error;
    }

    uptr = npacket->data;

    /* 添加长度 */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* 类型 */
    ivalue = XMANAGER_MSG_CAPTUREREFRESH;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* flag 标识 */
    if (false == result)
    {
        u8value = 1;
    }
    else
    {
        u8value = 0;
    }
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;

    /* 总长度 */
    ivalue = (resultlen + 8);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* 错误码 */
    ivalue = errcode;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    ivalue = strlen("start refresh: ");
    rmemcpy1(uptr, 0, "start refresh: ", ivalue);
    uptr += ivalue;

    /* 填充表信息 */
    for (rtable = rtables->tables; NULL != rtable; rtable = rtable->next)
    {
        /* 模式名 */
        ivalue = strlen(rtable->schema);
        rmemcpy1(uptr, 0, rtable->schema, ivalue);
        uptr += ivalue;

        /* 添加分隔符号 */
        *uptr = '.';
        uptr++;

        /* 表名 */
        ivalue = strlen(rtable->table);
        rmemcpy1(uptr, 0, rtable->table, ivalue);
        uptr += ivalue;
    }

    *uptr = ' ';
    uptr++;
    if (false == result)
    {
        ivalue = strlen("failed, ");
        rmemcpy1(uptr, 0, "failed, ", ivalue);
        uptr += ivalue;

        ivalue = strlen(msg);
        rmemcpy1(uptr, 0, msg, ivalue);
    }
    else
    {
        ivalue = strlen(msg);
        rmemcpy1(uptr, 0, msg, ivalue);
    }
    uptr += ivalue;

    /* 挂载到待发送队列中 */
    metric_capture_addpackets(inccapture->metric, npacket);
    return;
cmd_startcaputre_assembleolrefreshpacket_error:

    netpacket_destroy(npacket);
    return;
}

/* 启动 onlinerefresh 节点 */
static bool cmd_startcapture_startonlinerefresh(increment_capture* inccapture)
{
    bool bmatch                                     = true;
    bool increment                                  = true;
    TransactionId olxid                             = InvalidTransactionId;
    PGconn *snapconn                                = NULL;
    List *ntables                                   = NULL;
    HTAB* hnamespace                                = NULL;
    HTAB* hclass                                    = NULL;
    HTAB* hsyncdataset                              = NULL;
    dlistnode *dnode                                = NULL;
    uuid_t *uuid                             = NULL;
    txn *olbegin_txn                         = NULL;
    snapshot *olsnapshot                     = NULL;
    refresh_table* rtable                    = NULL;
    refresh_tables *rtables                  = NULL;
    onlinerefresh *olinerefresh              = NULL;
    txnstmt_onlinerefresh *olrtxnstmt        = NULL;
    onlinerefresh_capture *olcapture         = NULL;
    capturebase temp_base                    = { '\0' };
    char errmsg[1024]                               = { 0 };

    /* 检查onlinerefresh表是否重复 */
    osal_thread_lock(&inccapture->olrefreshlock);
    /* 查看是否需要发起 onlinerefresh */

    if (true == dlist_isnull(inccapture->olrefreshtables))
    {
        osal_thread_unlock(&inccapture->olrefreshlock);
        return true;
    }

    /* 暂停parser */
    parserwork_stat_setpause(inccapture->decodingctx);

    hnamespace = inccapture->decodingctx->trans_cache->sysdicts->by_namespace;
    hclass = inccapture->decodingctx->trans_cache->sysdicts->by_class;
    hsyncdataset = inccapture->decodingctx->trans_cache->hsyncdataset;
    for (dnode = inccapture->olrefreshtables->head; NULL != dnode;)
    {
        inccapture->olrefreshtables->head = dnode->next;
        inccapture->olrefreshtables->length--;
        rtables = (refresh_tables*)dnode->value;

        /* 填充 refreshtables 中的 oid */
        if (false == onlinerefresh_rebuildrefreshtables(rtables,
                                                               hnamespace,
                                                               hclass,
                                                               &bmatch))
        {
            parserwork_stat_setresume(inccapture->decodingctx);

            /* 构建 onlinerefresh 失败信息 */
            osal_thread_unlock(&inccapture->olrefreshlock);
            snprintf(errmsg, 1024, "ERROR: can not rebuild refresh tables.");
            cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_NOENT, errmsg);
            refresh_freetables(rtables);
            return true;
        }

        inccapture->olrefreshtables = dlist_delete(inccapture->olrefreshtables, dnode, NULL);
        break;
    }

    /* 没有待同步表 */
    if (!rtables || false == bmatch)
    {
        /* TODO 构建 onlinerefresh 成功信息 */
        snprintf(errmsg, 1024, "No table match refresh!!!");
        elog(RLOG_WARNING, errmsg);
        parserwork_stat_setresume(inccapture->decodingctx);
        osal_thread_unlock(&inccapture->olrefreshlock);
        cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, true, ERROR_SUCCESS, errmsg);
        refresh_freetables(rtables);
        return true;
    }

    /* 检查onlinerefresh表是否重复 */
    if (false == dlist_isnull(inccapture->olrefreshing))
    {
        dnode = inccapture->olrefreshing->head;
        while (dnode)
        {
            onlinerefresh_capture *olcapture = (onlinerefresh_capture *)dnode->value;
            if(false == refresh_tables_hasrepeat(olcapture->tables, rtables, &rtable))
            {
                dnode = dnode->next;
                continue;
            }
            snprintf(errmsg, 1024, "ERROR: %s.%s refreshing.", rtable->schema, rtable->table);
            elog(RLOG_WARNING, "%s, repeat table when do online refresh", errmsg);
            osal_thread_unlock(&inccapture->olrefreshlock);
            parserwork_stat_setresume(inccapture->decodingctx);
            cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_MSGEXIST, errmsg);
            refresh_freetables(rtables);
            return true;
        }
    }
    osal_thread_unlock(&inccapture->olrefreshlock);

    /* 生成新增表链表 */
    ntables = onlinerefresh_get_newtable(hsyncdataset, rtables);
    if (ntables)
    {
        /* 加入到待同步集合中 */
        filter_dataset_updatedatasets_onlinerefresh(hsyncdataset, ntables);
    }

    /* 
     * 使用可重复读连接数据库并获取快照
     */
    /* 连接数据库 */
    snapconn = conn_get(guc_getConfigOption("url"));
    if(NULL == snapconn)
    {
        snprintf(errmsg, 1024, "ERROR: connect database error.");
        elog(RLOG_WARNING, errmsg);
        parserwork_stat_setresume(inccapture->decodingctx);
        cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_DISCONN, errmsg);
        refresh_freetables(rtables);
        return false;
    }

    /* 设置连接为可重复读 */
    conn_settxnisolationlevel(snapconn, TXNISOLVL_REPEATABLE_READ);

    /* 获取快照 */
    olsnapshot = snapshot_buildfromdb(snapconn);

    /* 获取当前事务号, 用于过滤事务 */
    olxid = databaserecv_transactionid_get(snapconn);
    /* 判断txid和xmax */
    if (increment)
    {
        /* 
         * 通过判断当前数据库最新的事务号和快照中的 xmin 来看是否含有活跃的事务
         *  1、快照中 xmin 的含义, 活跃的最小事务, 当没有活跃事务时, xmin = xmax = 将要被使用的事务号
         *  2、xmin == olxid 时, 代表在获取快照这一刻是没有事务在运行的, 也就不需要做 增量 的同步
         */
        increment = (olxid == olsnapshot->xmin) ? false : true;
    }

    elog(RLOG_DEBUG, "online refresh: %s increment data", increment ? "need do" : "needn't do");

    /* 生成事务 */
    olrtxnstmt = txnstmt_onlinerefresh_init();

    /* 设置增量标志 */
    txnstmt_onlinerefresh_set_increment(olrtxnstmt, increment);

    /* 设置txid */
    txnstmt_onlinerefresh_set_txid(olrtxnstmt, olxid);

    /* 设置uuid */
    uuid = random_uuid();
    txnstmt_onlinerefresh_set_no(olrtxnstmt, uuid);

    /* 设置tables */
    txnstmt_onlinerefresh_set_refreshtables(olrtxnstmt, rtables);

    /* 生成onlinerefresh节点并设置值 */
    olinerefresh = onlinerefresh_init();
    onlinerefresh_state_setsearchmax(olinerefresh);
    onlinerefresh_no_set(olinerefresh, uuid_copy(uuid));
    onlinerefresh_txid_set(olinerefresh, olxid);
    onlinerefresh_snapshot_set(olinerefresh, olsnapshot);
    onlinerefresh_increment_set(olinerefresh, increment);
    onlinerefresh_newtables_set(olinerefresh, ntables);

    /* 只需要做存量的时候xmin不需要添加 */
    if (increment)
    {
        /* 将xmin加入到xids中 */
        onlinerefresh_xids_append(olinerefresh, olsnapshot->xmin);

        /* 将snapshot中的xiplist添加到xids中 */
        onlinerefresh_add_xids_from_snapshot(olinerefresh, olsnapshot);
    }

    transcache_make_xids_from_txn(inccapture->decodingctx, olinerefresh);
    /* 构建begin txn */
    olbegin_txn = parserwork_build_onlinerefresh_begin_txn(olrtxnstmt, inccapture->decodingctx->parselsn);

    /* 将 onlinerefresh 事务和节点放入增量解析中 */
    parserwork_decodingctx_addonlinerefresh(inccapture->decodingctx, olinerefresh, olbegin_txn);

    /*-------------------onlinerefresh capture 管理线程 begin--------------------------*/
    /* 初始化onlinerefresh capture管理线程 */
    olcapture = onlinerefresh_capture_init(increment);
    if(NULL == olcapture)
    {
        snprintf(errmsg, 1024, "ERROR: add onlinerefresh error, capture out of memory.");
        elog(RLOG_WARNING, errmsg);
        cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_OOM, errmsg);
        parserwork_stat_setresume(inccapture->decodingctx);
        return false;
    }

    /* 设置onlinerefresh capture管理线程 */
    onlinerefresh_capture_increment_set(olcapture, increment);
    misc_stat_loaddecode((void*)&temp_base);
    onlinerefresh_capture_redo_set(olcapture, temp_base.redolsn);
    onlinerefresh_capture_conninfo_set(olcapture, guc_getConfigOption("url"));
    onlinerefresh_capture_snapshot_set(olcapture, snapshot_copy(olsnapshot));
    onlinerefresh_capture_snap_conn_set(olcapture, snapconn);
    onlinerefresh_capture_no_set(olcapture, uuid_copy(uuid));
    onlinerefresh_capture_tables_set(olcapture, refresh_tables_copy(rtables));
    onlinerefresh_capture_txid_set(olcapture, (FullTransactionId) olxid);

    /* 只需要做存量的时候xmin不需要添加 */
    if (increment)
    {
        /* 加入最小的事务 */
        onlinerefresh_capture_xids_append(olcapture, olsnapshot->xmin);

        /* 加入快照中 xlist 事务 */
        onlinerefresh_capture_add_xids_from_snapshot(olcapture, olsnapshot);
    }

    olcapture->privdata = inccapture;
    olcapture->removeolrefresh = cmd_startcapture_removeonlinerefresh;
    osal_thread_lock(&inccapture->olrefreshlock);
    inccapture->olrefreshing = dlist_put(inccapture->olrefreshing, olcapture);
    osal_thread_unlock(&inccapture->olrefreshlock);

    /* 注册启动onlinerefresh capture管理线程 */
    if(false == threads_addsubmanger(inccapture->threads,
                                            THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_MGR,
                                            inccapture->persistno,
                                            &olcapture->thrsmgr,
                                            (void*)olcapture,
                                            onlinerefresh_capture_destroy,
                                            NULL,
                                            onlinerefresh_capture_main))
    {
        snprintf(errmsg, 1024, "ERROR: start onlinerefresh work threads error.");
        elog(RLOG_WARNING, errmsg);
        cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, ERROR_STARTTHREAD, errmsg);

        /* 
         * 1、在 increment capture 中移除 onlinerefresh 
         * 2、在 increment->parser 线程中移除 onlinerefresh 事务
         */
        dlist_deletebyvalue(inccapture->olrefreshing,
                            olcapture,
                            onlinerefresh_capture_cmp,
                            onlinerefresh_capture_destroy);

        /* 删除 onlinerefresh */
        parserwork_decodingctx_removeonlinerefresh(inccapture->decodingctx, olinerefresh);
        parserwork_stat_setresume(inccapture->decodingctx);
        return false;
    }

    /*-------------------onlinerefresh capture 管理线程   end--------------------------*/
    /* 恢复parser */
    snprintf(errmsg, 1024, "success.");
    cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, true, ERROR_SUCCESS, errmsg);
    parserwork_stat_setresume(inccapture->decodingctx);
    return true;
}

/* capture 启动 */
bool cmd_startcapture(void)
{
    /*
     * 1、切换工作目录
     * 2、创建锁文件
     * 3、初始化 log 信息
     * 4、加载 Control 信息
     * 5、临时文件删除
     * 6、Control 文件锁初始化
     * 7、同步策略初始化
     */
    bool bret                               = true;
    int gctime                              = 0;
    int forcefree                           = 0;
    int refreshstragety                     = 0;
    XLogRecPtr endlsn                       = InvalidXLogRecPtr;
    XLogRecPtr startlsn                     = InvalidXLogRecPtr;
    char* wdata                             = NULL;
    char* parserddl                         = NULL;
    snapshot* snapshot               = NULL;
    decodingcontext* decodingctx     = NULL;
    refresh_capture *rcapture      = NULL;
    refresh_tables* refreshtables    = NULL;
    refresh_tables* mgr_tables       = NULL;
    increment_capture* inccapture    = NULL;

    /* 获取工作目录 */
    wdata = guc_getdata();

    /* 检测 data 目录是否存在 */
    if(false == osal_dir_exist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        bret = false;
        goto cmd_startcapture_done;
    }

    /* 切换工作目录 */
    chdir(wdata);

    /* 设置为后台运行 */
    makedaemon();

    /* 获取主线程号 */
    g_mainthrid = pthread_self();

    /* 在 wdata 查看锁文件是否存在,不存在则创建,存在则检测进程是否启动 */
    misc_lockfiles_create(LOCK_FILE);

    /* log 初始化 */
    log_init();

    /* 获取内存回收时间 */
    gctime = guc_getConfigOptionInt(CFG_KEY_GCTIME);

    /* inccapture 初始化*/
    inccapture = increment_capture_init();

    /* 加载 ControlData */
    misc_controldata_load();

    g_xsynchstat = misc_controldata_stat_get();

    /* 临时文件删除 */
    datainit_clear(CATALOG_DIR);

    /* 
     * 启动工作线程
     */
    /* 设置信号处理函数 */
    signal_init();

    refreshstragety = guc_getConfigOptionInt(CFG_KEY_REFRESHSTRAGETY);

    /* parser 线程初始化 */
    decodingctx = inccapture->decodingctx;

    if(XSYNCHSTAT_REWIND == g_xsynchstat)
    {
        /* 设置stat */
        parserwork_stat_setrewind(decodingctx);

        /* 加载数据库信息 */
        if(!parserwork_wal_initfromdb(decodingctx))
        {
            bret = false;
            goto cmd_startcapture_done;
        }

        /* 获取到基础数据后, 先落盘 */
        misc_stat_decodewrite(&(inccapture->decodingctx->base), &inccapture->writestate->basefd);

        /* 临时设置拆分线程 timeline*/
        inccapture->splitwalctx->loadrecords->timeline = decodingctx->base.curtlid;

        /*
         * 开启新连接
         *  1、设置连接的事务级别为可重复读
         *  2、获取数据字典
         *      新开连接, 对数据字典开启 FULL 模式
         *  3、获取快照
         */
        decodingctx->rewind_ptr->conn = conn_get(guc_getConfigOption("url"));
        if(NULL == decodingctx->rewind_ptr->conn)
        {
            elog(RLOG_WARNING, "capture can't conn database:%s", guc_getConfigOption("url"));
            bret = false;
            goto cmd_startcapture_done;
        }

        /* 开启事务, 并设置事务的级别为可重复读 */
        conn_settxnisolationlevel(decodingctx->rewind_ptr->conn, TXNISOLVL_REPEATABLE_READ);

        /* 加载字典表 初始化同步数据集*/
        catalog_sysdict_getfromdb(decodingctx->rewind_ptr->conn, decodingctx->trans_cache->sysdicts);

        /*新开连接设置full用完关闭*/
        if(false == catalog_sysdict_setfullmode(decodingctx->trans_cache->sysdicts->by_class))
        {
            elog(RLOG_WARNING, "capture set table replica identity full error");
            bret = false;
            goto cmd_startcapture_done;
        }

        /* 持久化系统字典, 下次启动时使用 */
        sysdictscache_write(decodingctx->trans_cache->sysdicts, decodingctx->base.redolsn);

        /* 生成同步数据集, 将同步数据集落盘 */
        filter_dataset_init(decodingctx->trans_cache->tableincludes,
                                decodingctx->trans_cache->tableexcludes,
                                decodingctx->trans_cache->sysdicts->by_namespace, 
                                decodingctx->trans_cache->sysdicts->by_class);

        decodingctx->trans_cache->hsyncdataset = filter_dataset_load(decodingctx->trans_cache->sysdicts->by_namespace,
                                                                            decodingctx->trans_cache->sysdicts->by_class);

        decodingctx->trans_cache->htxnfilterdataset = filter_dataset_txnfilterload(decodingctx->trans_cache->sysdicts->by_namespace,
                                                                                         decodingctx->trans_cache->sysdicts->by_class);

        decodingctx->trans_cache->sysdicts->by_relfilenode = cache_sysdicts_buildrelfilenode2oid(decodingctx->database,
                                                                                        (void*)decodingctx->trans_cache->sysdicts);
        snapshot = snapshot_buildfromdb(decodingctx->rewind_ptr->conn);

        decodingctx->rewind_ptr->currentlsn = databaserecv_currentlsn_get(decodingctx->rewind_ptr->conn);
        decodingctx->rewind_ptr->currentxid = databaserecv_transactionid_get(decodingctx->rewind_ptr->conn);
        
        if (refreshstragety)
        {
            refreshtables = filter_dataset_buildrefreshtables(decodingctx->trans_cache->hsyncdataset);
            mgr_tables = refresh_tables_copy(refreshtables);
            parserwork_buildrefreshtransaction(decodingctx, refreshtables);

            /* 初始化refresh mgr线程的相关结构 */
            rcapture = refresh_capture_init();
            if(NULL == rcapture)
            {
                bret = false;
                elog(RLOG_WARNING, "init refresh error");
                goto cmd_startcapture_done;
            }
            refresh_capture_setsnapshotname(rcapture, snapshot->name);

            /* todo, tables生成后的接入 */
            refresh_capture_setrefreshtables(mgr_tables, rcapture);
            refresh_capture_setconn(decodingctx->rewind_ptr->conn, rcapture);
            decodingctx->rewind_ptr->conn = NULL;
        }
        else
        {
            conn_close(decodingctx->rewind_ptr->conn);
            decodingctx->rewind_ptr->conn = NULL;
        }

        /*设置快照到rewind中*/
        rewind_strategy_setfastrewind(snapshot, decodingctx);

        startlsn = GetXlogSegmentBegin(decodingctx->rewind_ptr->redolsn, (g_walsegsize * 1048576));

        endlsn = decodingctx->rewind_ptr->currentlsn;

        /* 清理snapshot不清理snapshot->xids */
        snapshot_free(snapshot);
    }
    else
    {
        /* 设置stat */
        parserwork_stat_setrunning(decodingctx);

        /* 加载decodingctx信息*/
        parserwork_walinitphase2(decodingctx);

        /* 设置拆分线程 timeline*/
        inccapture->splitwalctx->loadrecords->timeline = decodingctx->base.curtlid;

        startlsn = decodingctx->base.redolsn;
        endlsn = InvalidXLogRecPtr;
    }

    /*设置splitwork的拆分的起点和终点*/
    inccapture->decodingctx->callback.setloadlsn((void*)inccapture, startlsn, endlsn);

    if(NULL != parserddl)
    {
        parserddl = guc_getConfigOption(CFG_KEY_DDL);
        if(strlen("on") == strlen(parserddl)
            && 0 == strcmp("on", parserddl))
        {
            g_parserddl = 1;
        }
        else
        {
            g_parserddl = 0;
        }
    }

    /*
     * 添加主常驻线程
     */
    if(false == threads_addpersist(inccapture->threads, &inccapture->persistno, "CAPTURE INCREMENT"))
    {
        bret = false;
        elog(RLOG_WARNING, "add capture increment persist to threads error");
        goto cmd_startcapture_done;
    }

    /* 启动常驻工作线程 */
    if(false == cmd_startcapturethreads(inccapture))
    {
        bret = false;
        elog(RLOG_WARNING, "start capture increment persist job threads error");
        goto cmd_startcapture_done;
    }

    /* 启动refresh mgr */
    if(NULL != rcapture)
    {
        /* 注册 refresh 管理线程 */
        if(false == threads_addsubmanger(inccapture->threads,
                                                THRNODE_IDENTITY_CAPTURE_REFRESH_MGR,
                                                inccapture->persistno,
                                                &rcapture->thrsmgr,
                                                (void*)rcapture,
                                                refresh_capture_free,
                                                NULL,
                                                refresh_capture_main))
        {
            bret = false;
            elog(RLOG_WARNING, "start refresh mgr failed");
            goto cmd_startcapture_done;
        }
    }

    /* 解除信号屏蔽 */
    singal_setmask();

    elog(RLOG_INFO, "capture start, pid:%d", getpid());

    log_destroyerrorstack();
    /* 关闭标准输入/输出/错误 */
    closestd();

    while(1)
    {
        /* 日志信息打印 */
        if(true == g_gotsigterm)
        {
            /* 捕获到 sigterm 信号, 设置线程退出 */
            threads_exit(inccapture->threads);
            break;
        }

        /* 启动 onlinerefresh */
        if(false == cmd_startcapture_startonlinerefresh(inccapture))
        {
            elog(RLOG_WARNING, "capture add onlinerefresh error");
            continue;
        }

        /* 启动线程 */
        threads_startthread(inccapture->threads);

        /* 尝试捕获异常线程 */
        threads_tryjoin(inccapture->threads);

        /* 回收 FREE 节点 */
        threads_thrnoderecycle(inccapture->threads);

        if(false == threads_hasthrnode(inccapture->threads))
        {
            /* 所有的线程退出, 主线程退出 */
            /* 记录 */
            misc_stat_decodewrite(&(inccapture->writestate->base), &inccapture->writestate->basefd);
            break;
        }

        if(0 == gctime)
        {
            ;
        }
        else if(gctime > forcefree)
        {
            forcefree++;
        }
        else
        {
            /* 回收内存 */
            malloc_trim(0);
            forcefree = 0;
        }
        usleep(100000);
        continue;
    }

    /* 所有的线程都退出了, 那么主线程也退出 */
    /* 正常退出时, 不需要向 xmanager 发送信息 */
    /* 将新地址落盘 */
    elog(RLOG_INFO, "capture writestate persistent trail, redolsn: %X/%X, restartlsn %X/%X, last commit lsn:%X/%X, fileid:%lu, offset:%u, timeline:%u",
                    (uint32)(inccapture->writestate->base.redolsn>>32), (uint32)inccapture->writestate->base.redolsn,
                    (uint32)(inccapture->writestate->base.restartlsn>>32), (uint32)inccapture->writestate->base.restartlsn,
                    (uint32)(inccapture->writestate->base.confirmedlsn>>32), (uint32)inccapture->writestate->base.confirmedlsn,
                    inccapture->writestate->base.fileid,
                    inccapture->writestate->base.fileoffset,
                    inccapture->writestate->base.curtlid);


cmd_startcapture_done:

    /* inccapture 资源回收*/
    increment_capture_destroy(inccapture);

    /* control 文件内存释放 */
    misc_controldata_destroy();

    /* 锁文件释放 */
    misc_lockfiles_unlink(0, NULL);

    guc_destroy();

    /* 泄露内存打印 */
    mem_print(MEMPRINT_ALL);
    if (true == bret)
    {
        /* 已经进入过逻辑处理中, 直接退出即可 */
        exit(0);
    }

    /* 向 xmanager 反馈失败信息 */
    return bret;
}
