#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
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
#include "bigtransaction/integrate/split/bigtxn_integratesplittrail.h"


/* 将 records 加入到队列中 */
static bool bigtxn_integratesplittrail_addrecords2queue(increment_integratesplittrail* splittrail,
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
bigtxn_integratesplittrail* bigtxn_integratesplittrail_init(void)
{
    bigtxn_integratesplittrail* stctx = NULL;

    stctx = (bigtxn_integratesplittrail*)rmalloc0(sizeof(bigtxn_integratesplittrail));
    if(NULL == stctx)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stctx, 0, 0, sizeof(bigtxn_integratesplittrail));

    stctx->splittrailctx = increment_integratesplittrail_init();
    stctx->splittrailctx->state = INTEGRATE_STATUS_SPLIT_WAITSET;
    return stctx;
}


/* 逻辑读取主线程 */
void *bigtxn_integratesplittrail_main(void* args)
{
    uint64 fileid                                       = 0;
    thrnode* thr_node                             = NULL;
    increment_integratesplittrail* stctx         = NULL;
    bigtxn_integratesplittrail* bigtxn_stctx     = NULL;

    thr_node = (thrnode*)args;

    bigtxn_stctx = (bigtxn_integratesplittrail*)thr_node->data;

    stctx = bigtxn_stctx->splittrailctx;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn splittrail stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
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
        /* 打开文件 */
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            /* 序列化/落盘 */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
        fileid = stctx->loadrecords->fileid;

        if(false == loadtrailrecords_load(stctx->loadrecords))
        {
            elog(RLOG_WARNING, "bigtxn load trail records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if(true == dlist_isnull(stctx->loadrecords->records))
        {
            /* 
             * 没有读取到数据, 追上了最新的, 所以需要重新读取该块
             */
            /* 需要退出，等待 thr_node->stat 变为 TERM 后退出*/
            if(THRNODE_STAT_TERM != thr_node->stat)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                /* 重启重复发送文件会rename文件，需要关闭文件描述符 */
                loadtrailrecords_fileclose(stctx->loadrecords);
                continue;
            }
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 是否需要过滤, 不需要过滤则加入到队列中 */
        if(false == stctx->filter)
        {
            /* 加入到队列中 */
            if(false == bigtxn_integratesplittrail_addrecords2queue(stctx, thr_node))
            {
                elog(RLOG_WARNING, "integrate bigtxn add records 2 queue error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if(false == loadtrailrecords_filterremainmetadata(stctx->loadrecords, fileid, stctx->emitoffset))
        {
            stctx->filter = false;
        }

        if(true == dlist_isnull(stctx->loadrecords->records))
        {
            /* 
             * 没有读取到数据, 追上了最新的, 所以需要重新读取该块
             */
            continue;
        }

        /* 加入到队列中 */
        if(false == bigtxn_integratesplittrail_addrecords2queue(stctx, thr_node))
        {
            elog(RLOG_WARNING, "integrate bigtxn add records 2 queue error");
            break;
        }
        /* TODO chkpoint 逻辑 */
    }

    pthread_exit(NULL);
    return NULL;
}

void bigtxn_integratesplittrail_free(void *args)
{
    bigtxn_integratesplittrail* stctx = NULL;

    stctx = (bigtxn_integratesplittrail*)args;

    if (stctx->splittrailctx)
    {
        increment_integratesplittrail_free(stctx->splittrailctx);
    }

    rfree(stctx);
}
