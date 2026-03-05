#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "increment/integrate/split/ripple_increment_integratesplittrail.h"
#include "onlinerefresh/integrate/splittrail/ripple_onlinerefresh_integratesplittrail.h"

/* 逻辑读取主线程 */
ripple_onlinerefresh_integratesplittrail* ripple_onlinerefresh_integratesplittrail_init(void)
{
    ripple_onlinerefresh_integratesplittrail* stctx = NULL;

    stctx = (ripple_onlinerefresh_integratesplittrail*)rmalloc0(sizeof(ripple_onlinerefresh_integratesplittrail));
    if(NULL == stctx)
    {
        elog(RLOG_WARNING, "onlinerefresh integratesplittrail malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stctx, 0, 0, sizeof(ripple_onlinerefresh_integratesplittrail));

    stctx->splittrailctx = ripple_increment_integratesplittrail_init();
    stctx->splittrailctx->state = RIPPLE_INTEGRATE_STATUS_SPLIT_WAITSET;
    return stctx;
}

/* 将 records 加入到队列中 */
static bool ripple_onlinerefresh_integratesplittrail_addrecords2queue(ripple_increment_integratesplittrail* splittrail,
                                                                      ripple_thrnode* thrnode)
{
    /* 加入到队列中 */
    while(RIPPLE_THRNODE_STAT_WORK == thrnode->stat)
    {
        if(false == ripple_queue_put(splittrail->recordscache, splittrail->loadrecords->records))
        {
            if(RIPPLE_ERROR_QUEUE_FULL == splittrail->recordscache->error)
            {
                usleep(50000);
                continue;
            }
            elog(RLOG_WARNING, "integrate add records 2 queue error");
            break;
        }
        splittrail->loadrecords->records = NULL;
        return true;
    }

    return false;
}

/* 逻辑读取主线程 */
void *ripple_onlinerefresh_integratesplittrail_main(void* args)
{
    uint64 fileid                                                       = 0;
    ripple_thrnode* thrnode                                             = NULL;
    ripple_increment_integratesplittrail* splittrail                    = NULL;
    ripple_onlinerefresh_integratesplittrail* olintegratesplittrail     = NULL;

    thrnode = (ripple_thrnode*)args;

    olintegratesplittrail = (ripple_onlinerefresh_integratesplittrail*)thrnode->data;

    splittrail = olintegratesplittrail->splittrailctx;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate spliittrail stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while(1)
    {
        /*
         * 1、打开文件，打开文件时，遇到的场景， 文件不存在，那么等待文件存在，在等待文件的过程中也要检测是否接收到了退出的信号
         * 2、检测是否接收到退出的信号，接收到，那么退出
         * 3、根据 blockid 换算偏移量，根据此内容读取数据
        */
        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 加载records */
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
             */
            usleep(10000);
            continue;
        }

        /* 是否需要过滤, 不需要过滤则加入到队列中 */
        if(false == splittrail->filter)
        {
            /* 加入到队列中 */
            if(false == ripple_onlinerefresh_integratesplittrail_addrecords2queue(splittrail, thrnode))
            {
                elog(RLOG_WARNING, "integrate add records 2 queue error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if(false == ripple_loadtrailrecords_filterremainmetadata(splittrail->loadrecords, fileid, splittrail->emitoffset))
        {
            /* 过滤完成了 */
            splittrail->filter = false;
        }

        if(true == dlist_isnull(splittrail->loadrecords->records))
        {
            /* 
             * 没有读取到数据, 追上了最新的, 所以需要重新读取该块
             */
            continue;
        }

        if(false == ripple_onlinerefresh_integratesplittrail_addrecords2queue(splittrail, thrnode))
        {
            elog(RLOG_WARNING, "integrate add records 2 queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_integratesplittrail_free(void *args)
{
    ripple_onlinerefresh_integratesplittrail* stctx = NULL;

    stctx = (ripple_onlinerefresh_integratesplittrail*)args;

    if (NULL == stctx)
    {
        return;
    }

    if (stctx->splittrailctx)
    {
        ripple_increment_integratesplittrail_free(stctx->splittrailctx);
    }

    rfree(stctx);
}
