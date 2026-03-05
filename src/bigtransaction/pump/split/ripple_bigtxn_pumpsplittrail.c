#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/mpage/mpage.h"
#include "utils/dlist/dlist.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "filetransfer/pump/ripple_filetransfer_pump.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "increment/pump/split/ripple_increment_pumpsplittrail.h"
#include "bigtransaction/pump/split/ripple_bigtxn_pumpsplittrail.h"

/* 设置文件重新从开始读取, 网闸使用 */
static void ripple_bigtxn_pumpsplittrail_reread(ripple_increment_pumpsplittrail* splittrail)
{
    if (false == splittrail->remote)
    {
        return;
    }
    
    /* 重置过滤的起点 */
    splittrail->emitoffset = splittrail->loadrecords->foffset;

    /* 重置下载的起点 */
    splittrail->loadrecords->foffset = 0;
    ripple_loadtrailrecords_setloadposition(splittrail->loadrecords, splittrail->loadrecords->fileid, splittrail->loadrecords->foffset);
    sleep(10);
}

/* 将要下载的文件加入队列 */
static void ripple_bigtxn_pumpsplittrail_downloadfile(ripple_increment_pumpsplittrail* splittrail, FullTransactionId xid)
{
    ripple_filetransfer_bigtxninc* filetransfer_inc = NULL;

    if (false == splittrail->remote)
    {
        return;
    }

    /* 创建filetransfer节点加入队列 */
    filetransfer_inc = ripple_filetransfer_bigtxninc_init();
    ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)filetransfer_inc, splittrail->loadrecords->fileid);
    ripple_filetransfer_download_bigtxnincpath_set(filetransfer_inc, splittrail->capturedata, xid, splittrail->jobname);
    splittrail->callback.pumpstate_filetransfernode_add(splittrail->privdata, (void*)filetransfer_inc);
    filetransfer_inc = NULL;
    return;
}

/* 将要使用完的文件生成删除任务，加入到队列中 */
static void ripple_bigtxn_pumpsplittrail_deletefile_add(ripple_increment_pumpsplittrail* splittrail, FullTransactionId xid)
{
    ripple_filetransfer_cleanpath* cleanpath = NULL;

    if (false == splittrail->remote)
    {
        return;
    }

    /* 创建filetransfer节点加入队列 */
    cleanpath = ripple_filetransfer_cleanpath_init();
    ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)cleanpath, splittrail->loadrecords->fileid - 1);
    snprintf(cleanpath->base.localpath, RIPPLE_MAXPATH, "%s/%016lX", splittrail->capturedata, cleanpath->base.trail);
    snprintf(cleanpath->base.localdir, RIPPLE_MAXPATH, "%s", splittrail->capturedata);
    snprintf(cleanpath->prefixpath, RIPPLE_MAXPATH, "%s/%s/%lu", splittrail->jobname, RIPPLE_STORAGE_BIG_TRANSACTION_DIR, xid);
    splittrail->callback.pumpstate_filetransfernode_add(splittrail->privdata, (void*)cleanpath);

    return;
}

/* 网闸场景下, 需要重新读取下载文件的判断 */
static void ripple_bigtxn_pumpsplittrail_gapreset(ripple_increment_pumpsplittrail* splittrail, FullTransactionId xid)
{
    /* 
     * 非网闸环境, 不需要处理
     */
    if(false == splittrail->remote)
    {
        return;
    }

    if(RIPPLE_ERROR_BLK_INCOMPLETE != splittrail->loadrecords->loadrecords.error)
    {
        return;
    }

    /* 只有在块不完整时才需要重置 */
    ripple_loadtrailrecords_fileclose(splittrail->loadrecords);
    ripple_bigtxn_pumpsplittrail_reread(splittrail);
    ripple_bigtxn_pumpsplittrail_downloadfile(splittrail, xid);
    return;
}

/* 初始化信息,包含设置 loadtrail中的基础信息 */
static ripple_increment_pumpsplittrail* ripple_bigtxn_increment_pumpsplittrail_init(FullTransactionId xid)
{
    char* url = NULL;
    char* cdata = NULL;
    ripple_increment_pumpsplittrail* splittrail = NULL;

    splittrail = (ripple_increment_pumpsplittrail*)rmalloc1(sizeof(ripple_increment_pumpsplittrail));
    if(NULL == splittrail)
    {
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }
    rmemset0(splittrail, 0, '\0', sizeof(ripple_increment_pumpsplittrail));

    /* 根据配置文件设置 loadtrail信息 */
    splittrail->capturedata = rmalloc0(MAXPGPATH);
    if (NULL == splittrail->capturedata)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(splittrail->capturedata, 0, '\0', MAXPGPATH);
    cdata = guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR);
    snprintf(splittrail->capturedata, MAXPGPATH, "%s/%s/%lu", cdata, RIPPLE_STORAGE_BIG_TRANSACTION_DIR, xid);

    /*------------------------load record 模块初始化 begin---------------------------*/
    splittrail->loadrecords = ripple_loadtrailrecords_init();
    if(NULL == splittrail->loadrecords)
    {
        elog(RLOG_WARNING, "pump increment load records error");
        return NULL;
    }

    if(false == ripple_loadtrailrecords_setloadpageroutine(splittrail->loadrecords, RIPPLE_LOADPAGE_TYPE_FILE))
    {
        elog(RLOG_WARNING, "pump increment set load page error");
        return NULL;
    }

    if(false == ripple_loadtrailrecords_setloadsource(splittrail->loadrecords, splittrail->capturedata))
    {
        elog(RLOG_WARNING, "pump increment set capture data error");
        return NULL;
    }
    ripple_loadtrailrecords_setloadposition(splittrail->loadrecords, PUMP_INFO_FILEID, 0);
    /*------------------------load record 模块初始化   end---------------------------*/

    splittrail->filter = true;
    rmemset1(splittrail->jobname, 0, '\0', 128);
    snprintf(splittrail->jobname, 128, guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME));

    /* url不配置不下载文件 */
    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    if (!(url == NULL || url[0] == '\0'))
    {
        splittrail->remote = true;
    }

    ripple_increment_pumpsplittrail_state_set(splittrail, RIPPLE_PUMP_STATUS_SPLIT_RESET);
    return splittrail;
}

/* 将 records 加入到队列中 */
static bool ripple_bigtxn_pumpsplitrail_addrecords2queue(ripple_increment_pumpsplittrail* splittrail)
{
    /* 加入到队列中 */
    while(RIPPLE_PUMP_STATUS_SPLIT_WORKING == splittrail->state)
    {
        if(false == ripple_queue_put(splittrail->recordscache, splittrail->loadrecords->records))
        {
            if(RIPPLE_ERROR_QUEUE_FULL == splittrail->recordscache->error)
            {
                usleep(50000);
                continue;
            }
            elog(RLOG_WARNING, "pump add records 2 queue error");
            break;
        }
        splittrail->loadrecords->records = NULL;
        return true;
    }

    return false;
}

ripple_bigtxn_pumpsplittrail* ripple_bigtxn_pumpsplittrail_init(FullTransactionId xid)
{
    ripple_bigtxn_pumpsplittrail* stctx = NULL;

    stctx = (ripple_bigtxn_pumpsplittrail*)rmalloc0(sizeof(ripple_bigtxn_pumpsplittrail));
    if(NULL == stctx)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stctx, 0, 0, sizeof(ripple_bigtxn_pumpsplittrail));

    stctx->xid = xid;
    stctx->splittrailctx = ripple_bigtxn_increment_pumpsplittrail_init(xid);
    return stctx;
}

/* 处理主入口 */
void *ripple_bigtxn_pumpsplittrail_main(void *args)
{
    /*
     * 状态转换:
     *  1、split 线程初始状态为 RESET, 此时还未连接上collector
     *  2、当 net 线程连接到 collector 后, 由 collector 函数设置 split 线程的状态为 init
     *  3、split 检测到状态为 INIT 后, 会设置自己的状态为 READY , 并设置 parser 的状态为 init
     *  4、在 ready 状态下, split 线程不做任务操作，等待 parser 线程设置为 work
     *  5、work 状态即工作状态
     * 
     *  RESET---->INIT
     *          /|\ |
     *          /   |
     *         /    |
     *        /     |
     *       /      |
     *      /       |
     *     /       \|/
     *  WORK<-----READY
     * 
     */
    uint64 fileid                                       = 0;
    ripple_thrnode* thrnode                             = NULL;
    ripple_bigtxn_pumpsplittrail* bigtxn_splittrail     = NULL;
    ripple_increment_pumpsplittrail* splittrail         = NULL;

    thrnode = (ripple_thrnode*)args;

    bigtxn_splittrail = (ripple_bigtxn_pumpsplittrail*)thrnode->data;
    splittrail = (ripple_increment_pumpsplittrail*)bigtxn_splittrail->splittrailctx;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "bigtxn pump splittrail stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    splittrail->state = RIPPLE_PUMP_STATUS_SPLIT_WORKING;

    ripple_bigtxn_pumpsplittrail_downloadfile(splittrail, bigtxn_splittrail->xid);

    while(true)
    {
        /* 打开文件 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取 records */
        /* 预保留 fileid, 在 loadrecords 时, 会自动切换文件 */
        fileid = splittrail->loadrecords->fileid;
        if(false == ripple_loadtrailrecords_load(splittrail->loadrecords))
        {
            elog(RLOG_WARNING, "load trail records error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(true == dlist_isnull(splittrail->loadrecords->records))
        {
            /* 
             * 没有读取到数据, 追上了最新的, 所以需要重新读取该块
             *  网闸场景, 可能需要重新下载文件
             */
            ripple_bigtxn_pumpsplittrail_gapreset(splittrail, bigtxn_splittrail->xid);
            usleep(10000);
            continue;
        }

        /* 这里会存在一个时间差问题, 如果后续出现问题, 优先考虑此处 */
        if(true == splittrail->filter)
        {
            /* 
             * 连接上 collector 后, collector 会重新发送需要的 pump 解析的起点,并同时设置 filter 为 true
             *  pump 根据最新解析的起点, 找到一个事务的起点,找到事务的起点后 设置 filter 为 false
             */
            if(true == ripple_loadtrailrecords_filterfortransbegin(splittrail->loadrecords))
            {
                ripple_bigtxn_pumpsplittrail_gapreset(splittrail, bigtxn_splittrail->xid);
                continue;
            }
            splittrail->filter = false;
        }

        /* 在网闸的场景下, 需要根据最新的 offset 进行过滤 */
        if(true == splittrail->remote)
        {
            ripple_loadtrailrecords_filter(splittrail->loadrecords, fileid, splittrail->emitoffset);
        }

        /* 经过上述的过滤,再次检查是否为空, 若为空, 那么证明没有新的数据, 那么去加载新的数据 */
        if(true == dlist_isnull(splittrail->loadrecords->records))
        {
            ripple_bigtxn_pumpsplittrail_gapreset(splittrail, bigtxn_splittrail->xid);
            continue;
        }

        /* 将 records 加入到队列中 */
        dlist_setfree(splittrail->loadrecords->records, ripple_record_freevoid);
        if(false == ripple_bigtxn_pumpsplitrail_addrecords2queue(splittrail))
        {
            elog(RLOG_WARNING, "pump add records 2 queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 只有在工作状态才会切文件，
         * 原因：当recordscache内存占满之后会卡在 add函数中等待，
         * 这时重启parser会清理recordscache缓存且splittrail会重置--状态(init)、fileid、blkid、offset(回调)，
         * 设置完会继续执行会更新blkid或fileid 会导致读取的内容不对*/
        if(RIPPLE_PUMP_STATUS_SPLIT_WORKING != splittrail->state)
        {
            continue;
        }

        /* 切换文件 */
        if(fileid == splittrail->loadrecords->fileid)
        {
            ripple_bigtxn_pumpsplittrail_gapreset(splittrail, bigtxn_splittrail->xid);
            continue;
        }

        /* 产生了文件切换 */
        splittrail->emitoffset = 0;
        splittrail->filter = false;

        if(false == splittrail->remote)
        {
            continue;
        }

        /* 网闸处理 */
        ripple_bigtxn_pumpsplittrail_downloadfile(splittrail, bigtxn_splittrail->xid);
        ripple_bigtxn_pumpsplittrail_deletefile_add(splittrail, bigtxn_splittrail->xid);
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_bigtxn_pumpsplittrail_free(void *args)
{
    ripple_bigtxn_pumpsplittrail* bigtxn_splittrail     = NULL;

    bigtxn_splittrail = (ripple_bigtxn_pumpsplittrail*)args;

    if (NULL == bigtxn_splittrail)
    {
        return;
    }
    
    ripple_increment_pumpsplittrail_free(bigtxn_splittrail->splittrailctx);

    rfree(bigtxn_splittrail);
}