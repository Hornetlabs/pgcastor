#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/mpage/mpage.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "increment/pump/split/ripple_increment_pumpsplittrail.h"
#include "onlinerefresh/pump/splittrail/ripple_onlinerefresh_pumpsplittrail.h"


/* 设置文件重新从开始读取 */
static void ripple_onlinerefresh_pumpsplittrail_reread(ripple_increment_pumpsplittrail* splittrail)
{
    if (false == splittrail->remote)
    {
        return;
    }
    
    splittrail->emitoffset = splittrail->loadrecords->foffset;

    /* 重置下载的起点 */
    splittrail->loadrecords->foffset = 0;
    ripple_loadtrailrecords_setloadposition(splittrail->loadrecords, splittrail->loadrecords->fileid, splittrail->loadrecords->foffset);
    sleep(10);
}

/* 将要下载的文件加入队列 */
static void ripple_onlinerefresh_pumpsplittrail_downloadfile(ripple_increment_pumpsplittrail* splittrail, ripple_uuid_t* onlinerefreshno)
{
    char* uuid = NULL;
    ripple_filetransfer_onlinerefreshinc* filetransfer_inc = NULL;

    if (false == splittrail->remote)
    {
        return;
    }

    uuid = uuid2string(onlinerefreshno);

    /* 创建filetransfer节点加入队列 */
    filetransfer_inc = ripple_filetransfer_onlinerefreshinc_init();
    ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)filetransfer_inc, splittrail->loadrecords->fileid);
    ripple_filetransfer_download_olincpath_set(filetransfer_inc, splittrail->capturedata, uuid, splittrail->jobname);
    splittrail->callback.pumpstate_filetransfernode_add(splittrail->privdata, (void*)filetransfer_inc);
    filetransfer_inc = NULL;
    rfree(uuid);
    return;
}

/* 网闸场景下, 需要重新读取下载文件的判断 */
static void ripple_onlinerefresh_pumpsplittrail_gapreset(ripple_increment_pumpsplittrail* splittrail, ripple_uuid_t* onlinerefreshno)
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
    ripple_onlinerefresh_pumpsplittrail_reread(splittrail);
    ripple_onlinerefresh_pumpsplittrail_downloadfile(splittrail, onlinerefreshno);
    return;
}

/* 逻辑读取主线程 */
ripple_task_onlinerefreshpumpsplittrail* ripple_onlinerefresh_pumpsplittrail_init(void)
{
    ripple_task_onlinerefreshpumpsplittrail* stctx = NULL;

    stctx = (ripple_task_onlinerefreshpumpsplittrail*)rmalloc0(sizeof(ripple_task_onlinerefreshpumpsplittrail));
    if(NULL == stctx)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(stctx, 0, 0, sizeof(ripple_task_onlinerefreshpumpsplittrail));

    stctx->splittrailctx = ripple_increment_pumpsplittrail_init();
    stctx->splittrailctx->state = RIPPLE_PUMP_STATUS_SPLIT_WORKING;
    return stctx;
}

/* 逻辑读取主线程 */
void* ripple_onlinerefresh_pumpsplittrail_main(void* args)
{
    uint64 fileid                                       = 0;
    ripple_thrnode *thrnode                             = NULL;
    ripple_increment_pumpsplittrail* splittrail         = NULL;
    ripple_task_onlinerefreshpumpsplittrail* task_stctx = NULL;

    thrnode = (ripple_thrnode*)args;
    task_stctx = (ripple_task_onlinerefreshpumpsplittrail*)thrnode->data;
    splittrail = task_stctx->splittrailctx;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh pump incrment loadrecords stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 获取第一个增量文件 */
    ripple_onlinerefresh_pumpsplittrail_downloadfile(splittrail, &task_stctx->onlinerefreshno);
    while(true)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取 records */
        /* 预保留 fileid, 在 loadrecords 时, 会自动切换文件 */
        fileid = splittrail->loadrecords->fileid;
        if(false == ripple_loadtrailrecords_load(splittrail->loadrecords))
        {
            elog(RLOG_WARNING, "load trail records error");
            break;
        }

        if(true == dlist_isnull(splittrail->loadrecords->records))
        {
            /* 
             * 没有读取到数据, 追上了最新的, 所以需要重新读取该块
             */
            ripple_onlinerefresh_pumpsplittrail_gapreset(splittrail, &task_stctx->onlinerefreshno);
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
                ripple_onlinerefresh_pumpsplittrail_gapreset(splittrail, &task_stctx->onlinerefreshno);
                continue;
            }
            splittrail->filter = false;
        }

        /* 在网闸的场景下, 需要根据最新的 offset 进行过滤 */
        if(true == splittrail->remote)
        {
            ripple_loadtrailrecords_filter(splittrail->loadrecords, splittrail->loadrecords->fileid, splittrail->emitoffset);
        }

        /* 经过上述的过滤,再次检查是否为空, 若为空, 那么证明没有新的数据, 那么去加载新的数据 */
        if(true == dlist_isnull(splittrail->loadrecords->records))
        {
            ripple_onlinerefresh_pumpsplittrail_gapreset(splittrail, &task_stctx->onlinerefreshno);
            continue;
        }

        /* 将 records 加入到队列中 */
        dlist_setfree(splittrail->loadrecords->records, ripple_record_freevoid);

        while(RIPPLE_THRNODE_STAT_TERM != thrnode->stat)
        {
            if(false == ripple_queue_put(splittrail->recordscache, splittrail->loadrecords->records))
            {
                if(RIPPLE_ERROR_QUEUE_FULL == splittrail->recordscache->error)
                {
                    usleep(50000);
                    continue;
                }

                elog(RLOG_WARNING, "integrate add records 2 queue error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                goto ripple_onlinerefresh_pumpsplittrail_main_done;
            }
            splittrail->loadrecords->records = NULL;
            break;
        }

        /* 文件切换判断 */
        if(fileid == splittrail->loadrecords->fileid)
        {
            ripple_onlinerefresh_pumpsplittrail_gapreset(splittrail, &task_stctx->onlinerefreshno);
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
        ripple_onlinerefresh_pumpsplittrail_reread(splittrail);
        ripple_onlinerefresh_pumpsplittrail_downloadfile(splittrail, &task_stctx->onlinerefreshno);
        splittrail->filter = false;
    }

ripple_onlinerefresh_pumpsplittrail_main_done:
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_pumpsplittrail_free(void* args)
{
    ripple_task_onlinerefreshpumpsplittrail* olrsplittrail = NULL;

    olrsplittrail = (ripple_task_onlinerefreshpumpsplittrail*)args;

    if (olrsplittrail->splittrailctx)
    {
        ripple_increment_pumpsplittrail_free(olrsplittrail->splittrailctx);
    }

    rfree(olrsplittrail);
}
