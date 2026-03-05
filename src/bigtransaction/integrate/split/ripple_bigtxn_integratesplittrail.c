#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
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
#include "bigtransaction/integrate/split/ripple_bigtxn_integratesplittrail.h"


/* 将 records 加入到队列中 */
static bool ripple_bigtxn_integratesplittrail_addrecords2queue(ripple_increment_integratesplittrail* splittrail,
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
ripple_bigtxn_integratesplittrail* ripple_bigtxn_integratesplittrail_init(void)
{
    ripple_bigtxn_integratesplittrail* stctx = NULL;

    stctx = (ripple_bigtxn_integratesplittrail*)rmalloc0(sizeof(ripple_bigtxn_integratesplittrail));
    if(NULL == stctx)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stctx, 0, 0, sizeof(ripple_bigtxn_integratesplittrail));

    stctx->splittrailctx = ripple_increment_integratesplittrail_init();
    stctx->splittrailctx->state = RIPPLE_INTEGRATE_STATUS_SPLIT_WAITSET;
    return stctx;
}


/* 逻辑读取主线程 */
void *ripple_bigtxn_integratesplittrail_main(void* args)
{
    uint64 fileid                                       = 0;
    ripple_thrnode* thrnode                             = NULL;
    ripple_increment_integratesplittrail* stctx         = NULL;
    ripple_bigtxn_integratesplittrail* bigtxn_stctx     = NULL;

    thrnode = (ripple_thrnode*)args;

    bigtxn_stctx = (ripple_bigtxn_integratesplittrail*)thrnode->data;

    stctx = bigtxn_stctx->splittrailctx;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn splittrail stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
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
        /* 打开文件 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
        fileid = stctx->loadrecords->fileid;

        if(false == ripple_loadtrailrecords_load(stctx->loadrecords))
        {
            elog(RLOG_WARNING, "bigtxn load trail records error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(true == dlist_isnull(stctx->loadrecords->records))
        {
            /* 
             * 没有读取到数据, 追上了最新的, 所以需要重新读取该块
             */
            /* 需要退出，等待 thrnode->stat 变为 TERM 后退出*/
            if(RIPPLE_THRNODE_STAT_TERM != thrnode->stat)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                /* 重启重复发送文件会rename文件，需要关闭文件描述符 */
                ripple_loadtrailrecords_fileclose(stctx->loadrecords);
                continue;
            }
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 是否需要过滤, 不需要过滤则加入到队列中 */
        if(false == stctx->filter)
        {
            /* 加入到队列中 */
            if(false == ripple_bigtxn_integratesplittrail_addrecords2queue(stctx, thrnode))
            {
                elog(RLOG_WARNING, "integrate bigtxn add records 2 queue error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if(false == ripple_loadtrailrecords_filterremainmetadata(stctx->loadrecords, fileid, stctx->emitoffset))
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
        if(false == ripple_bigtxn_integratesplittrail_addrecords2queue(stctx, thrnode))
        {
            elog(RLOG_WARNING, "integrate bigtxn add records 2 queue error");
            break;
        }
        /* TODO chkpoint 逻辑 */
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_bigtxn_integratesplittrail_free(void *args)
{
    ripple_bigtxn_integratesplittrail* stctx = NULL;

    stctx = (ripple_bigtxn_integratesplittrail*)args;

    if (stctx->splittrailctx)
    {
        ripple_increment_integratesplittrail_free(stctx->splittrailctx);
    }

    rfree(stctx);
}
