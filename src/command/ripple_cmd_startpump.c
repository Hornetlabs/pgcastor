#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/guc/guc.h"
#include "utils/mpage/mpage.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/license/license.h"
#include "utils/daemon/ripple_process.h"
#include "threads/ripple_threads.h"
#include "misc/ripple_misc_lockfiles.h"
#include "signal/ripple_signal.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "queue/ripple_queue.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/ripple_netclient.h"
#include "net/ripple_netserver.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "works/ripple_workthreadmgr.h"
#include "works/dyworks/ripple_dyworks.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "serial/ripple_serial.h"
#include "parser/trail/ripple_parsertrail.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "filetransfer/pump/ripple_filetransfer_pump.h"
#include "task/ripple_task_slot.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "onlinerefresh/ripple_onlinerefresh_persist.h"
#include "onlinerefresh/pump/ripple_onlinerefesh_pump.h"
#include "metric/pump/ripple_statework_pump.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/pump/ripple_bigtxn_pumpmanager.h"
#include "bigtransaction/ripple_bigtxn.h"
#include "increment/pump/net/ripple_increment_pumpnet.h"
#include "increment/pump/serial/ripple_increment_pumpserial.h"
#include "increment/pump/split/ripple_increment_pumpsplittrail.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"
#include "increment/pump/ripple_increment_pump.h"
#include "command/ripple_cmd_startpump.h"


static bool ripple_cmd_startpumpthreads(ripple_increment_pump* incpump)
{
    ripple_thrnode* thrnode                 = NULL;

    /*-------------------------------启动常驻工作线程 begin---------------------------------*/
    /* 启动的顺序为退出的逆序, 即先启动的后退出 */
    /* 启动发送线程 */
    if(false == ripple_threads_addpersistthread(incpump->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_PUMP_NETCLIENT,
                                                incpump->persistno,
                                                (void*)incpump->clientstate,
                                                NULL,
                                                NULL,
                                                ripple_increment_pumpnet_main))
    {
        elog(RLOG_WARNING, "add pump increment net client persist to threads error");
        return false;
    }

    /* 启动序列化线程 */
    if(false == ripple_threads_addpersistthread(incpump->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_PUMP_SERIAL,
                                                incpump->persistno,
                                                (void*)incpump->serialstate,
                                                NULL,
                                                NULL,
                                                ripple_increment_pumpserial_main))
    {
        elog(RLOG_WARNING, "add pump increment serial persist to threads error");
        return false;
    }

    /* 启动增量解析 */
    if(false == ripple_threads_addpersistthread(incpump->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_PUMP_PARSER,
                                                incpump->persistno,
                                                (void*)incpump->pumpparsertrail,
                                                NULL,
                                                NULL,
                                                ripple_increment_pumpparsertrail_main))
    {
        elog(RLOG_WARNING, "add pump increment flush persist to threads error");
        return false;
    }

    /* 启动加载records线程 */
    if(false == ripple_threads_addpersistthread(incpump->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_INC_PUMP_LOADRECORD,
                                                incpump->persistno,
                                                (void*)incpump->splittrailctx,
                                                NULL,
                                                NULL,
                                                ripple_increment_pumpsplitrail_main))
    {
        elog(RLOG_WARNING, "add pump increment loadrecords persist to threads error");
        return false;
    }

    /* 启动网闸线程 */
    if(false == ripple_threads_addpersistthread(incpump->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_GAP,
                                                incpump->persistno,
                                                (void*)incpump->ftptransfer,
                                                NULL,
                                                NULL,
                                                ripple_filetransfer_pump_main))
    {
        elog(RLOG_WARNING, "add pump gap persist to threads error");
        return false;
    }

    /* 启动指标线程 */
    if(false == ripple_threads_addpersistthread(incpump->threads,
                                                &thrnode,
                                                RIPPLE_THRNODE_IDENTITY_PUMP_METRIC,
                                                incpump->persistno,
                                                (void*)incpump->pumpstate,
                                                NULL,
                                                NULL,
                                                ripple_state_pump_main))
    {
        elog(RLOG_WARNING, "add pump increment metric persist to threads error");
        return false;
    }

    /*-------------------------------启动常驻工作线程   end---------------------------------*/
    return true;
}

/* pump 启动 */
bool ripple_cmd_startpump(void)
{
    /*
     * 1、license check
     * 2、切换工作目录
     * 3、创建锁文件
     * 4、初始化 log 信息
     */
    bool bret = true;
    int gctime = 0;
    int forcefree = 0;
    char* wdata = NULL;
    ripple_increment_pump* incpump = NULL;

    /* 获取工作目录 */
    wdata = guc_getdata();

    /* 校验license */
    if (false == ripple_license_check(g_cfgpath))
    {
        elog(RLOG_WARNING, "license expired");
        bret = false;
        goto ripple_cmd_startpump_done;
    }

    /* 检测 data 目录是否存在 */
    if(false == DirExist(wdata))
    {
        elog(RLOG_WARNING, "work data not exist:%s", wdata);
        bret = false;
        goto ripple_cmd_startpump_done;
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

    /* 
     * 启动工作线程
     */
    /* 设置信号处理函数 */
    ripple_signal_init();

    /* 获取内存回收时间 */
    gctime = guc_getConfigOptionInt(RIPPLE_CFG_KEY_GCTIME);

    /* incpump 缓存初始化 */
    incpump = ripple_increment_pump_init();

    /* 加载大事务状态文件 生成 persist */
    incpump->txnpersist = ripple_bigtxn_read_persist();
    if (NULL == incpump->txnpersist)
    {
        elog(RLOG_WARNING, "read bigtxn persist error");
        bret = false;
        goto ripple_cmd_startpump_done;
    }
    /* 根据persists构建大事务管理线程节点 */
    incpump->bigtxn = ripple_bigtxn_pumpmanager_persist2pumpmanager(incpump->txnpersist, incpump->filetransfernode);

    /* 加载onlinerefresh状态文件 生成 persist */
    incpump->olrpersist = ripple_onlinerefresh_persist_read();
    if (NULL == incpump->olrpersist)
    {
        elog(RLOG_WARNING, "read onlinerefresh persist error");
        bret = false;
        goto ripple_cmd_startpump_done;
    }
    /* 根据persists构建onlinerefresh管理线程节点 */
    incpump->onlinerefresh = ripple_onlinerefresh_pumpmanager_persist2onlinerefreshmgr(incpump->olrpersist, incpump->filetransfernode);

    /*
     * 添加主常驻线程
     */
    if(false == ripple_threads_addpersist(incpump->threads, &incpump->persistno, "CAPTURE INCREMENT"))
    {
        elog(RLOG_WARNING, "add pump increment persist to threads error");
        bret = false;
        goto ripple_cmd_startpump_done;
    }

    /* 启动常驻线程 */
    if(false == ripple_cmd_startpumpthreads(incpump))
    {
        elog(RLOG_WARNING, "add pump increment persist to threads error");
        bret = false;
        goto ripple_cmd_startpump_done;
    }

    /* 解除信号屏蔽 */
    ripple_singal_setmask();

    elog(RLOG_INFO, "xsynch pump start, pid:%d", getpid());

    ripple_log_destroyerrorstack();

    /* 关闭标准输入/输出/错误 */
    ripple_closestd();

    while(1)
    {
        /* 日志信息打印 */
        if(true == g_gotsigterm)
        {
            /* 捕获到 sigterm 信号, 设置线程退出 */
            ripple_threads_exit(incpump->threads);
            break;
        }

        /*
         * refresh 处理
         *  1、启动 refresh 节点
         *  2、回收完成的 refresh 节点
         */
        /* 启动 refresh 节点 */
        if(false == ripple_increment_pump_startrefresh(incpump))
        {
            elog(RLOG_WARNING, "start refresh error");
            break;
        }

        /* 尝试回收 refresh 节点 */
        if(false == ripple_increment_pump_tryjoinonrefresh(incpump))
        {
            elog(RLOG_WARNING, "try join on refresh error");
            break;
        }

        /* 
         * onlinerefresh 处理
         *  1、启动 onlinerefesh 节点
         *  2、回收完成的 onlinerefresh 节点
         */
        /* 启动 onlinerefesh 节点 */
        if(false == ripple_increment_pump_startonlinerefresh(incpump))
        {
            elog(RLOG_WARNING, "start onlinerefresh error");
            break;
        }

        /* 尝试回收 onlinerefresh 节点 */
        if(false == ripple_increment_pump_tryjoinononlinerefresh(incpump))
        {
            elog(RLOG_WARNING, "try join on onlinerefresh error");
            break;
        }

        /* 
         * bigtxn 处理
         *  1、启动 bigtxn 节点
         *  2、回收完成的 bigtxn 节点
         */
        /* 启动 bigtxn 节点 */
        if(false == ripple_increment_pump_startbigtxn(incpump))
        {
            elog(RLOG_WARNING, "start bigtxn error");
            break;
        }

        /* 尝试回收 bigtxn 节点 */
        if(false == ripple_increment_pump_tryjoinonbigtxn(incpump))
        {
            elog(RLOG_WARNING, "try join on bigtxn error");
            break;
        }

        /* 启动线程 */
        ripple_threads_startthread(incpump->threads);

        /* 尝试捕获异常线程 */
        ripple_threads_tryjoin(incpump->threads);

        /* 回收 FREE 节点 */
        ripple_threads_thrnoderecycle(incpump->threads);

        if(false == ripple_threads_hasthrnode(incpump->threads))
        {
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
        sleep(1);
        continue;
    }

    ripple_onlinerefresh_persist_write(incpump->olrpersist);

ripple_cmd_startpump_done:

    ripple_increment_pumpstate_destroy(incpump);

    ripple_misc_lockfiles_unlink(0, NULL);

    /* 泄露内存打印 */
    ripple_mem_print(RIPPLE_MEMPRINT_ALL);
    return bret;
}
