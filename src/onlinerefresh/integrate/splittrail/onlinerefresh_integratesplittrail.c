#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "increment/integrate/split/increment_integratesplittrail.h"
#include "onlinerefresh/integrate/splittrail/onlinerefresh_integratesplittrail.h"

/* 逻辑读取主线程 */
onlinerefresh_integratesplittrail* onlinerefresh_integratesplittrail_init(void)
{
    onlinerefresh_integratesplittrail* stctx = NULL;

    stctx = (onlinerefresh_integratesplittrail*)rmalloc0(sizeof(onlinerefresh_integratesplittrail));
    if(NULL == stctx)
    {
        elog(RLOG_WARNING, "onlinerefresh integratesplittrail malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stctx, 0, 0, sizeof(onlinerefresh_integratesplittrail));

    stctx->splittrailctx = increment_integratesplittrail_init();
    stctx->splittrailctx->state = INTEGRATE_STATUS_SPLIT_WAITSET;
    return stctx;
}

/* 将 records 加入到队列中 */
static bool onlinerefresh_integratesplittrail_addrecords2queue(increment_integratesplittrail* splittrail,
                                                                      thrnode* thrnode)
{
    /* 加入到队列中 */
    while(THRNODE_STAT_WORK == thrnode->stat)
    {
        if(false == queue_put(splittrail->recordscache, splittrail->loadrecords->records))
        {
            if(ERROR_QUEUE_FULL == splittrail->recordscache->error)
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
void *onlinerefresh_integratesplittrail_main(void* args)
{
    uint64 fileid                                                       = 0;
    thrnode* thr_node                                             = NULL;
    increment_integratesplittrail* splittrail                    = NULL;
    onlinerefresh_integratesplittrail* olintegratesplittrail     = NULL;

    thr_node = (thrnode*)args;

    olintegratesplittrail = (onlinerefresh_integratesplittrail*)thr_node->data;

    splittrail = olintegratesplittrail->splittrailctx;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh integrate spliittrail stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    while(1)
    {
        /*
         * 1、打开文件，打开文件时，遇到的场景， 文件不存在，那么等待文件存在，在等待文件的过程中也要检测是否接收到了退出的信号
         * 2、检测是否接收到退出的信号，接收到，那么退出
         * 3、根据 blockid 换算偏移量，根据此内容读取数据
        */
        /* 首先判断是否接收到退出信号 */
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 加载records */
        /* 预保留 fileid, 在 loadrecords 时, 会自动切换文件 */
        fileid = splittrail->loadrecords->fileid;
        if(false == loadtrailrecords_load(splittrail->loadrecords))
        {
            elog(RLOG_WARNING, "load trail records error");
            thr_node->stat = THRNODE_STAT_ABORT;
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
            if(false == onlinerefresh_integratesplittrail_addrecords2queue(splittrail, thr_node))
            {
                elog(RLOG_WARNING, "integrate add records 2 queue error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if(false == loadtrailrecords_filterremainmetadata(splittrail->loadrecords, fileid, splittrail->emitoffset))
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

        if(false == onlinerefresh_integratesplittrail_addrecords2queue(splittrail, thr_node))
        {
            elog(RLOG_WARNING, "integrate add records 2 queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_integratesplittrail_free(void *args)
{
    onlinerefresh_integratesplittrail* stctx = NULL;

    stctx = (onlinerefresh_integratesplittrail*)args;

    if (NULL == stctx)
    {
        return;
    }

    if (stctx->splittrailctx)
    {
        increment_integratesplittrail_free(stctx->splittrailctx);
    }

    rfree(stctx);
}
