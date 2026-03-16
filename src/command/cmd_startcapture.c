#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/conn/ripple_conn.h"
#include "utils/init/ripple_datainit.h"
#include "utils/daemon/ripple_process.h"
#include "utils/regex/ripple_regex.h"
#include "utils/init/ripple_databaserecv.h"
#include "misc/ripple_misc_stat.h"
#include "misc/ripple_misc_lockfiles.h"
#include "misc/ripple_misc_control.h"
#include "signal/ripple_signal.h"
#include "storage/ripple_file_buffer.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "snapshot/ripple_snapshot.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "serial/ripple_serial.h"
#include "threads/ripple_threads.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "net/netpacket/ripple_netpacket.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/capture/ripple_refresh_capture.h"
#include "metric/capture/ripple_metric_capture.h"
#include "increment/capture/flush/ripple_increment_captureflush.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/ripple_bigtxn.h"
#include "bigtransaction/capture/flush/ripple_bigtxn_captureflush.h"
#include "bigtransaction/capture/serial/ripple_bigtxn_captureserial.h"
#include "onlinerefresh/capture/ripple_onlinerefresh_capture.h"
#include "increment/capture/serial/ripple_increment_captureserial.h"
#include "strategy/ripple_filter_dataset.h"
#include "works/splitwork/wal/ripple_wal_define.h"
#include "works/splitwork/wal/ripple_splitwork_wal.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_parserwork_wal.h"
#include "increment/capture/ripple_increment_capture.h"
#include "command/ripple_cmd_startcapture.h"

/* 启动常驻线程 */
static bool ripple_cmd_startcapturethreads(ripple_increment_capture* inccapture)
{
    ripple_thrnode* thrnode                 = NULL;

    /*-------------------------------启动常驻工作线程 begin---------------------------------*/
    /* 启动的顺序为退出的逆序, 即先启动的后退出 */
    /* 启动落盘线程 */
    if(false == ripple_threads_addpersistthread(inccapture->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_BIGTXNFLUSH,
                                                inccapture->persistno,
                                                (void*)inccapture->bigtxnwritestate,
                                                NULL,
                                                NULL,
                                                ripple_bigtxn_captureflush_main))
    {
        elog(RLOG_WARNING, "add capture increment bigtxn flush persist to threads error");
        return false;
    }

    /* 启动大事务序列化线程 */
    if(false == ripple_threads_addpersistthread(inccapture->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_BIGTXNSERIAL,
                                                inccapture->persistno,
                                                (void*)inccapture->bigtxnserialstate,
                                                NULL,
                                                NULL,
                                                ripple_bigtxn_captureserial_main))
    {
        elog(RLOG_WARNING, "add capture increment bigtxn serial persist to threads error");
        return false;
    }

    /* 启动增量落盘线程 */
    if(false == ripple_threads_addpersistthread(inccapture->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_FLUSH,
                                                inccapture->persistno,
                                                (void*)inccapture->writestate,
                                                NULL,
                                                NULL,
                                                ripple_increment_captureflush_main))
    {
        elog(RLOG_WARNING, "add capture increment flush persist to threads error");
        return false;
    }

    /* 启动增量序列化线程 */
    if(false == ripple_threads_addpersistthread(inccapture->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_SERIAL,
                                                inccapture->persistno,
                                                (void*)inccapture->serialstate,
                                                NULL,
                                                NULL,
                                                ripple_increment_captureserial_main))
    {
        elog(RLOG_WARNING, "add capture increment serial persist to threads error");
        return false;
    }

    /* 启动解析器线程 */
    if(false == ripple_threads_addpersistthread(inccapture->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_PARSER,
                                                inccapture->persistno,
                                                (void*)inccapture->decodingctx,
                                                NULL,
                                                NULL,
                                                ripple_parserwork_wal_main))
    {
        elog(RLOG_WARNING, "add capture increment parser persist to threads error");
        return false;
    }

    /* 启动 walwork 线程 */
    if(false == ripple_threads_addpersistthread(inccapture->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_CAPTURE_LOADRECORD,
                                                inccapture->persistno,
                                                (void*)inccapture->splitwalctx,
                                                NULL,
                                                NULL,
                                                ripple_splitwork_wal_main))
    {
        elog(RLOG_WARNING, "add capture increment splitwal persist to threads error");
        return false;
    }

    /* 启动指标线程 */
    if(false == ripple_threads_addpersistthread(inccapture->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_CAPTURE_METRIC,
                                                inccapture->persistno,
                                                (void*)inccapture->metric,
                                                NULL,
                                                NULL,
                                                ripple_metric_capture_main))
    {
        elog(RLOG_WARNING, "add capture increment metric persist to threads error");
        return false;
    }

    /*-------------------------------启动常驻工作线程   end---------------------------------*/
    return true;
}

/* 将指定的 onlinrefresh 节点在 olrefreshing 节点中删除 */
static void ripple_cmd_startcapture_removeonlinerefresh(void* pinccapture, void* polrefresh)
{
    ripple_increment_capture* inccapture                    = NULL;

    inccapture = (ripple_increment_capture*)pinccapture;

    ripple_thread_lock(&inccapture->olrefreshlock);
    dlist_deletebyvalue(inccapture->olrefreshing, polrefresh, ripple_onlinerefresh_capture_cmp, NULL);
    ripple_thread_unlock(&inccapture->olrefreshlock);
}

/* 构建 onlinerefresh 返回包到 xmanager */
static void ripple_cmd_startcaputre_assembleolrefreshpacket(ripple_increment_capture* inccapture,
                                                            ripple_refresh_tables* rtables,
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
    ripple_netpacket* npacket       = NULL;
    ripple_refresh_table* rtable    = NULL;

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

    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        return;
    }

    npacket->used = msglen;
    npacket->data = ripple_netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        goto ripple_cmd_startcaputre_assembleolrefreshpacket_error;
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
    ivalue = RIPPLE_XMANAGER_MSG_CAPTUREREFRESH;
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
    ripple_metric_capture_addpackets(inccapture->metric, npacket);
    return;
ripple_cmd_startcaputre_assembleolrefreshpacket_error:

    ripple_netpacket_destroy(npacket);
    return;
}

/* 启动 onlinerefresh 节点 */
static bool ripple_cmd_startcapture_startonlinerefresh(ripple_increment_capture* inccapture)
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
    ripple_uuid_t *uuid                             = NULL;
    ripple_txn *olbegin_txn                         = NULL;
    ripple_snapshot *olsnapshot                     = NULL;
    ripple_refresh_table* rtable                    = NULL;
    ripple_refresh_tables *rtables                  = NULL;
    ripple_onlinerefresh *olinerefresh              = NULL;
    ripple_txnstmt_onlinerefresh *olrtxnstmt        = NULL;
    ripple_onlinerefresh_capture *olcapture         = NULL;
    ripple_capturebase temp_base                    = { '\0' };
    char errmsg[1024]                               = { 0 };

    /* 检查onlinerefresh表是否重复 */
    ripple_thread_lock(&inccapture->olrefreshlock);
    /* 查看是否需要发起 onlinerefresh */

    if (true == dlist_isnull(inccapture->olrefreshtables))
    {
        ripple_thread_unlock(&inccapture->olrefreshlock);
        return true;
    }

    /* 暂停parser */
    ripple_parserwork_stat_setpause(inccapture->decodingctx);

    hnamespace = inccapture->decodingctx->transcache->sysdicts->by_namespace;
    hclass = inccapture->decodingctx->transcache->sysdicts->by_class;
    hsyncdataset = inccapture->decodingctx->transcache->hsyncdataset;
    for (dnode = inccapture->olrefreshtables->head; NULL != dnode;)
    {
        inccapture->olrefreshtables->head = dnode->next;
        inccapture->olrefreshtables->length--;
        rtables = (ripple_refresh_tables*)dnode->value;

        /* 填充 refreshtables 中的 oid */
        if (false == ripple_onlinerefresh_rebuildrefreshtables(rtables,
                                                               hnamespace,
                                                               hclass,
                                                               &bmatch))
        {
            ripple_parserwork_stat_setresume(inccapture->decodingctx);

            /* 构建 onlinerefresh 失败信息 */
            ripple_thread_unlock(&inccapture->olrefreshlock);
            snprintf(errmsg, 1024, "ERROR: can not rebuild refresh tables.");
            ripple_cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, RIPPLE_ERROR_NOENT, errmsg);
            ripple_refresh_freetables(rtables);
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
        ripple_parserwork_stat_setresume(inccapture->decodingctx);
        ripple_thread_unlock(&inccapture->olrefreshlock);
        ripple_cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, true, RIPPLE_ERROR_SUCCESS, errmsg);
        ripple_refresh_freetables(rtables);
        return true;
    }

    /* 检查onlinerefresh表是否重复 */
    if (false == dlist_isnull(inccapture->olrefreshing))
    {
        dnode = inccapture->olrefreshing->head;
        while (dnode)
        {
            ripple_onlinerefresh_capture *olcapture = (ripple_onlinerefresh_capture *)dnode->value;
            if(false == ripple_refresh_tables_hasrepeat(olcapture->tables, rtables, &rtable))
            {
                dnode = dnode->next;
                continue;
            }
            snprintf(errmsg, 1024, "ERROR: %s.%s refreshing.", rtable->schema, rtable->table);
            elog(RLOG_WARNING, "%s, repeat table when do online refresh", errmsg);
            ripple_thread_unlock(&inccapture->olrefreshlock);
            ripple_parserwork_stat_setresume(inccapture->decodingctx);
            ripple_cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, RIPPLE_ERROR_MSGEXIST, errmsg);
            ripple_refresh_freetables(rtables);
            return true;
        }
    }
    ripple_thread_unlock(&inccapture->olrefreshlock);

    /* 生成新增表链表 */
    ntables = ripple_onlinerefresh_get_newtable(hsyncdataset, rtables);
    if (ntables)
    {
        /* 加入到待同步集合中 */
        ripple_filter_dataset_updatedatasets_onlinerefresh(hsyncdataset, ntables);
    }

    /* 
     * 使用可重复读连接数据库并获取快照
     */
    /* 连接数据库 */
    snapconn = ripple_conn_get(guc_getConfigOption("url"));
    if(NULL == snapconn)
    {
        snprintf(errmsg, 1024, "ERROR: connect database error.");
        elog(RLOG_WARNING, errmsg);
        ripple_parserwork_stat_setresume(inccapture->decodingctx);
        ripple_cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, RIPPLE_ERROR_DISCONN, errmsg);
        ripple_refresh_freetables(rtables);
        return false;
    }

    /* 设置连接为可重复读 */
    ripple_conn_settxnisolationlevel(snapconn, RIPPLE_TXNISOLVL_REPEATABLE_READ);

    /* 获取快照 */
    olsnapshot = ripple_snapshot_buildfromdb(snapconn);

    /* 获取当前事务号, 用于过滤事务 */
    olxid = ripple_databaserecv_transactionid_get(snapconn);
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
    olrtxnstmt = ripple_txnstmt_onlinerefresh_init();

    /* 设置增量标志 */
    ripple_txnstmt_onlinerefresh_set_increment(olrtxnstmt, increment);

    /* 设置txid */
    ripple_txnstmt_onlinerefresh_set_txid(olrtxnstmt, olxid);

    /* 设置uuid */
    uuid = ripple_random_uuid();
    ripple_txnstmt_onlinerefresh_set_no(olrtxnstmt, uuid);

    /* 设置tables */
    ripple_txnstmt_onlinerefresh_set_refreshtables(olrtxnstmt, rtables);

    /* 生成onlinerefresh节点并设置值 */
    olinerefresh = ripple_onlinerefresh_init();
    ripple_onlinerefresh_state_setsearchmax(olinerefresh);
    ripple_onlinerefresh_no_set(olinerefresh, ripple_uuid_copy(uuid));
    ripple_onlinerefresh_txid_set(olinerefresh, olxid);
    ripple_onlinerefresh_snapshot_set(olinerefresh, olsnapshot);
    ripple_onlinerefresh_increment_set(olinerefresh, increment);
    ripple_onlinerefresh_newtables_set(olinerefresh, ntables);

    /* 只需要做存量的时候xmin不需要添加 */
    if (increment)
    {
        /* 将xmin加入到xids中 */
        ripple_onlinerefresh_xids_append(olinerefresh, olsnapshot->xmin);

        /* 将snapshot中的xiplist添加到xids中 */
        ripple_onlinerefresh_add_xids_from_snapshot(olinerefresh, olsnapshot);
    }

    ripple_transcache_make_xids_from_txn(inccapture->decodingctx, olinerefresh);
    /* 构建begin txn */
    olbegin_txn = ripple_parserwork_build_onlinerefresh_begin_txn(olrtxnstmt, inccapture->decodingctx->parselsn);

    /* 将 onlinerefresh 事务和节点放入增量解析中 */
    ripple_parserwork_decodingctx_addonlinerefresh(inccapture->decodingctx, olinerefresh, olbegin_txn);

    /*-------------------onlinerefresh capture 管理线程 begin--------------------------*/
    /* 初始化onlinerefresh capture管理线程 */
    olcapture = ripple_onlinerefresh_capture_init(increment);
    if(NULL == olcapture)
    {
        snprintf(errmsg, 1024, "ERROR: add onlinerefresh error, capture out of memory.");
        elog(RLOG_WARNING, errmsg);
        ripple_cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, RIPPLE_ERROR_OOM, errmsg);
        ripple_parserwork_stat_setresume(inccapture->decodingctx);
        return false;
    }

    /* 设置onlinerefresh capture管理线程 */
    ripple_onlinerefresh_capture_increment_set(olcapture, increment);
    ripple_misc_stat_loaddecode((void*)&temp_base);
    ripple_onlinerefresh_capture_redo_set(olcapture, temp_base.redolsn);
    ripple_onlinerefresh_capture_conninfo_set(olcapture, guc_getConfigOption("url"));
    ripple_onlinerefresh_capture_snapshot_set(olcapture, ripple_snapshot_copy(olsnapshot));
    ripple_onlinerefresh_capture_snap_conn_set(olcapture, snapconn);
    ripple_onlinerefresh_capture_no_set(olcapture, ripple_uuid_copy(uuid));
    ripple_onlinerefresh_capture_tables_set(olcapture, ripple_refresh_tables_copy(rtables));
    ripple_onlinerefresh_capture_txid_set(olcapture, (FullTransactionId) olxid);

    /* 只需要做存量的时候xmin不需要添加 */
    if (increment)
    {
        /* 加入最小的事务 */
        ripple_onlinerefresh_capture_xids_append(olcapture, olsnapshot->xmin);

        /* 加入快照中 xlist 事务 */
        ripple_onlinerefresh_capture_add_xids_from_snapshot(olcapture, olsnapshot);
    }

    olcapture->privdata = inccapture;
    olcapture->removeolrefresh = ripple_cmd_startcapture_removeonlinerefresh;
    ripple_thread_lock(&inccapture->olrefreshlock);
    inccapture->olrefreshing = dlist_put(inccapture->olrefreshing, olcapture);
    ripple_thread_unlock(&inccapture->olrefreshlock);

    /* 注册启动onlinerefresh capture管理线程 */
    if(false == ripple_threads_addsubmanger(inccapture->threads,
                                            RIPPLE_THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_MGR,
                                            inccapture->persistno,
                                            &olcapture->thrsmgr,
                                            (void*)olcapture,
                                            ripple_onlinerefresh_capture_destroy,
                                            NULL,
                                            ripple_onlinerefresh_capture_main))
    {
        snprintf(errmsg, 1024, "ERROR: start onlinerefresh work threads error.");
        elog(RLOG_WARNING, errmsg);
        ripple_cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, false, RIPPLE_ERROR_STARTTHREAD, errmsg);

        /* 
         * 1、在 increment capture 中移除 onlinerefresh 
         * 2、在 increment->parser 线程中移除 onlinerefresh 事务
         */
        dlist_deletebyvalue(inccapture->olrefreshing,
                            olcapture,
                            ripple_onlinerefresh_capture_cmp,
                            ripple_onlinerefresh_capture_destroy);

        /* 删除 onlinerefresh */
        ripple_parserwork_decodingctx_removeonlinerefresh(inccapture->decodingctx, olinerefresh);
        ripple_parserwork_stat_setresume(inccapture->decodingctx);
        return false;
    }

    /*-------------------onlinerefresh capture 管理线程   end--------------------------*/
    /* 恢复parser */
    snprintf(errmsg, 1024, "success.");
    ripple_cmd_startcaputre_assembleolrefreshpacket(inccapture, rtables, true, RIPPLE_ERROR_SUCCESS, errmsg);
    ripple_parserwork_stat_setresume(inccapture->decodingctx);
    return true;
}

/* capture 启动 */
bool ripple_cmd_startcapture(void)
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
    ripple_snapshot* snapshot               = NULL;
    ripple_decodingcontext* decodingctx     = NULL;
    ripple_refresh_capture *rcapture      = NULL;
    ripple_refresh_tables* refreshtables    = NULL;
    ripple_refresh_tables* mgr_tables       = NULL;
    ripple_increment_capture* inccapture    = NULL;

    /* 获取工作目录 */
    wdata = guc_getdata();

    /* 检测 data 目录是否存在 */
    if(false == DirExist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        bret = false;
        goto ripple_cmd_startcapture_done;
    }

    /* 切换工作目录 */
    chdir(wdata);

    /* 设置为后台运行 */
    ripple_makedaemon();

    /* 获取主线程号 */
    g_mainthrid = pthread_self();

    /* 在 wdata 查看锁文件是否存在,不存在则创建,存在则检测进程是否启动 */
    ripple_misc_lockfiles_create(RIPPLE_LOCK_FILE);

    /* log 初始化 */
    ripple_log_init();

    /* 获取内存回收时间 */
    gctime = guc_getConfigOptionInt(RIPPLE_CFG_KEY_GCTIME);

    /* inccapture 初始化*/
    inccapture = ripple_increment_capture_init();

    /* 加载 ControlData */
    ripple_misc_controldata_load();

    g_xsynchstat = ripple_misc_controldata_stat_get();

    /* 临时文件删除 */
    ripple_datainit_clear(RIPPLE_CATALOG_DIR);

    /* 
     * 启动工作线程
     */
    /* 设置信号处理函数 */
    ripple_signal_init();

    refreshstragety = guc_getConfigOptionInt(RIPPLE_CFG_KEY_REFRESHSTRAGETY);

    /* parser 线程初始化 */
    decodingctx = inccapture->decodingctx;

    if(RIPPLE_XSYNCHSTAT_REWIND == g_xsynchstat)
    {
        /* 设置stat */
        ripple_parserwork_stat_setrewind(decodingctx);

        /* 加载数据库信息 */
        if(!ripple_parserwork_wal_initfromdb(decodingctx))
        {
            bret = false;
            goto ripple_cmd_startcapture_done;
        }

        /* 获取到基础数据后, 先落盘 */
        ripple_misc_stat_decodewrite(&(inccapture->decodingctx->base), &inccapture->writestate->basefd);

        /* 临时设置拆分线程 timeline*/
        inccapture->splitwalctx->loadrecords->timeline = decodingctx->base.curtlid;

        /*
         * 开启新连接
         *  1、设置连接的事务级别为可重复读
         *  2、获取数据字典
         *      新开连接, 对数据字典开启 FULL 模式
         *  3、获取快照
         */
        decodingctx->rewind->conn = ripple_conn_get(guc_getConfigOption("url"));
        if(NULL == decodingctx->rewind->conn)
        {
            elog(RLOG_WARNING, "capture can't conn database:%s", guc_getConfigOption("url"));
            bret = false;
            goto ripple_cmd_startcapture_done;
        }

        /* 开启事务, 并设置事务的级别为可重复读 */
        ripple_conn_settxnisolationlevel(decodingctx->rewind->conn, RIPPLE_TXNISOLVL_REPEATABLE_READ);

        /* 加载字典表 初始化同步数据集*/
        ripple_catalog_sysdict_getfromdb(decodingctx->rewind->conn, decodingctx->transcache->sysdicts);

        /*新开连接设置full用完关闭*/
        if(false == ripple_catalog_sysdict_setfullmode(decodingctx->transcache->sysdicts->by_class))
        {
            elog(RLOG_WARNING, "capture set table replica identity full error");
            bret = false;
            goto ripple_cmd_startcapture_done;
        }

        /* 持久化系统字典, 下次启动时使用 */
        ripple_sysdictscache_write(decodingctx->transcache->sysdicts, decodingctx->base.redolsn);

        /* 生成同步数据集, 将同步数据集落盘 */
        ripple_filter_dataset_init(decodingctx->transcache->tableincludes,
                                decodingctx->transcache->tableexcludes,
                                decodingctx->transcache->sysdicts->by_namespace, 
                                decodingctx->transcache->sysdicts->by_class);

        decodingctx->transcache->hsyncdataset = ripple_filter_dataset_load(decodingctx->transcache->sysdicts->by_namespace,
                                                                            decodingctx->transcache->sysdicts->by_class);

        decodingctx->transcache->htxnfilterdataset = ripple_filter_dataset_txnfilterload(decodingctx->transcache->sysdicts->by_namespace,
                                                                                         decodingctx->transcache->sysdicts->by_class);

        decodingctx->transcache->sysdicts->by_relfilenode = ripple_cache_sysdicts_buildrelfilenode2oid(decodingctx->database,
                                                                                        (void*)decodingctx->transcache->sysdicts);
        snapshot = ripple_snapshot_buildfromdb(decodingctx->rewind->conn);

        decodingctx->rewind->currentlsn = ripple_databaserecv_currentlsn_get(decodingctx->rewind->conn);
        decodingctx->rewind->currentxid = ripple_databaserecv_transactionid_get(decodingctx->rewind->conn);
        
        if (refreshstragety)
        {
            refreshtables = ripple_filter_dataset_buildrefreshtables(decodingctx->transcache->hsyncdataset);
            mgr_tables = ripple_refresh_tables_copy(refreshtables);
            ripple_parserwork_buildrefreshtransaction(decodingctx, refreshtables);

            /* 初始化refresh mgr线程的相关结构 */
            rcapture = ripple_refresh_capture_init();
            if(NULL == rcapture)
            {
                bret = false;
                elog(RLOG_WARNING, "init refresh error");
                goto ripple_cmd_startcapture_done;
            }
            ripple_refresh_capture_setsnapshotname(rcapture, snapshot->name);

            /* todo, tables生成后的接入 */
            ripple_refresh_capture_setrefreshtables(mgr_tables, rcapture);
            ripple_refresh_capture_setconn(decodingctx->rewind->conn, rcapture);
            decodingctx->rewind->conn = NULL;
        }
        else
        {
            ripple_conn_close(decodingctx->rewind->conn);
            decodingctx->rewind->conn = NULL;
        }

        /*设置快照到rewind中*/
        ripple_rewind_strategy_setfastrewind(snapshot, decodingctx);

        startlsn = GetXlogSegmentBegin(decodingctx->rewind->redolsn, (g_walsegsize * 1048576));

        endlsn = decodingctx->rewind->currentlsn;

        /* 清理snapshot不清理snapshot->xids */
        ripple_snapshot_free(snapshot);
    }
    else
    {
        /* 设置stat */
        ripple_parserwork_stat_setrunning(decodingctx);

        /* 加载decodingctx信息*/
        ripple_parserwork_walinitphase2(decodingctx);

        /* 设置拆分线程 timeline*/
        inccapture->splitwalctx->loadrecords->timeline = decodingctx->base.curtlid;

        startlsn = decodingctx->base.redolsn;
        endlsn = InvalidXLogRecPtr;
    }

    /*设置splitwork的拆分的起点和终点*/
    inccapture->decodingctx->callback.setloadlsn((void*)inccapture, startlsn, endlsn);

    if(NULL != parserddl)
    {
        parserddl = guc_getConfigOption(RIPPLE_CFG_KEY_DDL);
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
    if(false == ripple_threads_addpersist(inccapture->threads, &inccapture->persistno, "CAPTURE INCREMENT"))
    {
        bret = false;
        elog(RLOG_WARNING, "add capture increment persist to threads error");
        goto ripple_cmd_startcapture_done;
    }

    /* 启动常驻工作线程 */
    if(false == ripple_cmd_startcapturethreads(inccapture))
    {
        bret = false;
        elog(RLOG_WARNING, "start capture increment persist job threads error");
        goto ripple_cmd_startcapture_done;
    }

    /* 启动refresh mgr */
    if(NULL != rcapture)
    {
        /* 注册 refresh 管理线程 */
        if(false == ripple_threads_addsubmanger(inccapture->threads,
                                                RIPPLE_THRNODE_IDENTITY_CAPTURE_REFRESH_MGR,
                                                inccapture->persistno,
                                                &rcapture->thrsmgr,
                                                (void*)rcapture,
                                                ripple_refresh_capture_free,
                                                NULL,
                                                ripple_refresh_capture_main))
        {
            bret = false;
            elog(RLOG_WARNING, "start refresh mgr failed");
            goto ripple_cmd_startcapture_done;
        }
    }

    /* 解除信号屏蔽 */
    ripple_singal_setmask();

    elog(RLOG_INFO, "capture start, pid:%d", getpid());

    ripple_log_destroyerrorstack();
    /* 关闭标准输入/输出/错误 */
    ripple_closestd();

    while(1)
    {
        /* 日志信息打印 */
        if(true == g_gotsigterm)
        {
            /* 捕获到 sigterm 信号, 设置线程退出 */
            ripple_threads_exit(inccapture->threads);
            break;
        }

        /* 启动 onlinerefresh */
        if(false == ripple_cmd_startcapture_startonlinerefresh(inccapture))
        {
            elog(RLOG_WARNING, "capture add onlinerefresh error");
            continue;
        }

        /* 启动线程 */
        ripple_threads_startthread(inccapture->threads);

        /* 尝试捕获异常线程 */
        ripple_threads_tryjoin(inccapture->threads);

        /* 回收 FREE 节点 */
        ripple_threads_thrnoderecycle(inccapture->threads);

        if(false == ripple_threads_hasthrnode(inccapture->threads))
        {
            /* 所有的线程退出, 主线程退出 */
            /* 记录 */
            ripple_misc_stat_decodewrite(&(inccapture->writestate->base), &inccapture->writestate->basefd);
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


ripple_cmd_startcapture_done:

    /* inccapture 资源回收*/
    ripple_increment_capture_destroy(inccapture);

    /* control 文件内存释放 */
    ripple_misc_controldata_destroy();

    /* 锁文件释放 */
    ripple_misc_lockfiles_unlink(0, NULL);

    guc_destroy();

    /* 泄露内存打印 */
    ripple_mem_print(RIPPLE_MEMPRINT_ALL);
    if (true == bret)
    {
        /* 已经进入过逻辑处理中, 直接退出即可 */
        exit(0);
    }

    /* 向 xmanager 反馈失败信息 */
    return bret;
}
