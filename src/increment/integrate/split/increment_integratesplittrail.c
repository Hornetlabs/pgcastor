#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/mpage/mpage.h"
#include "utils/hash/hash_search.h"
#include "queue/queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "threads/threads.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "increment/integrate/split/increment_integratesplittrail.h"


/* 设置 integrate 拆分线程的状态 */
void increment_integratesplittrail_state_set(increment_integratesplittrail* splittrail, int state)
{
    splittrail->state = state;
}

/*
  * 入参说明:
  *    fileid 设置为 loadtrail->fileid, loadtrail->offset 设置为 0
  *    emitoffset 设置为 emitoffset
  */
void increment_integratesplittrail_emit_set(increment_integratesplittrail* splittrail, uint64 fileid, uint64 emitoffset)
{
    /* 重置过滤的起点 */
    splittrail->loadrecords->fileid = fileid;
    splittrail->loadrecords->foffset = 0;
    splittrail->emitoffset = emitoffset;

    /* 重置解析的起点和过滤的起点 */
    loadtrailrecords_setloadposition(splittrail->loadrecords, splittrail->loadrecords->fileid, splittrail->loadrecords->foffset);
}

static bool increment_integratesplittrail_state_checkandset(increment_integratesplittrail* splittrail)
{
    if (INTEGRATE_STATUS_SPLIT_WORKING == splittrail->state)
    {
        return true;
    }
    return false;
}

/* 初始化信息,包含设置 loadtrail中的基础信息 */
increment_integratesplittrail* increment_integratesplittrail_init(void)
{
    char* cdata = NULL;
    increment_integratesplittrail* splittrail = NULL;

    splittrail = (increment_integratesplittrail*)rmalloc1(sizeof(increment_integratesplittrail));
    if(NULL == splittrail)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(splittrail, 0, '\0', sizeof(increment_integratesplittrail));

    /* 根据配置文件设置 loadtrail信息 */
    splittrail->capturedata = rmalloc0(MAXPGPATH);
    if (NULL == splittrail->capturedata)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(splittrail->capturedata, 0, '\0', MAXPGPATH);
    cdata = guc_getConfigOption(CFG_KEY_TRAIL_DIR);
    snprintf(splittrail->capturedata, MAXPGPATH, "%s/%s", cdata, STORAGE_TRAIL_DIR);

    /*------------------------load record 模块初始化 begin---------------------------*/
    splittrail->loadrecords = loadtrailrecords_init();
    if(NULL == splittrail->loadrecords)
    {
        elog(RLOG_WARNING, "integrate increment load records error");
        return NULL;
    }

    if(false == loadtrailrecords_setloadpageroutine(splittrail->loadrecords, LOADPAGE_TYPE_FILE))
    {
        elog(RLOG_WARNING, "integrate increment set load page error");
        return NULL;
    }

    if(false == loadtrailrecords_setloadsource(splittrail->loadrecords, splittrail->capturedata))
    {
        elog(RLOG_WARNING, "integrate increment set capture data error");
        return NULL;
    }
    loadtrailrecords_setloadposition(splittrail->loadrecords, 0, 0);
    /*------------------------load record 模块初始化   end---------------------------*/

    /* 设置状态为等待设置 */
    increment_integratesplittrail_state_set(splittrail, INTEGRATE_STATUS_SPLIT_WAITSET);
    splittrail->filter = true;
    return splittrail;
}

/* 将 records 加入到队列中 */
static bool increment_integratesplitrail_addrecords2queue(increment_integratesplittrail* splittrail)
{
    /* 加入到队列中 */
    while(INTEGRATE_STATUS_SPLIT_WORKING == splittrail->state)
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

/* 处理主入口 */
void* increment_integratesplitrail_main(void* args)
{
    uint64 fileid                                           = 0;
    thrnode* thr_node                                 = NULL;
    increment_integratesplittrail* splittrail        = NULL;

    thr_node = (thrnode*)args;
    /* 入参转换 */
    splittrail = (increment_integratesplittrail*)thr_node->data;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "increment integrate splittrail exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    while(true)
    {
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            /* 序列化/落盘 */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        if (!increment_integratesplittrail_state_checkandset(splittrail))
        {
            /* 睡眠 10 毫秒 */
            usleep(10000);
            continue;
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

        splittrail->callback.setmetricloadtrailno(splittrail->privdata, splittrail->loadrecords->fileid);
        splittrail->callback.setmetricloadtrailstart(splittrail->privdata, splittrail->loadrecords->foffset);

        if(true == dlist_isnull(splittrail->loadrecords->records))
        {
            /* 
             * 没有读取到数据, 追上了最新的, 所以需要重新读取该块
             */
            /* 需要退出，等待 thr_node->stat 变为 TERM 后退出*/
            if(THRNODE_STAT_TERM != thr_node->stat)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 是否需要过滤, 不需要过滤则加入到队列中 */
        if(false == splittrail->filter)
        {
            /* 加入到队列中 */
            if(false == increment_integratesplitrail_addrecords2queue(splittrail))
            {
                elog(RLOG_WARNING, "integrate add records 2 queue error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if(false == loadtrailrecords_filterremainmetadata(splittrail->loadrecords, fileid, splittrail->emitoffset))
        {
            splittrail->filter = false;
        }

        if(true == dlist_isnull(splittrail->loadrecords->records))
        {
            /* 
             * 数据全部被过滤，继续获取数据
             */
            continue;
        }

        /* 加入到队列中 */
        if(false == increment_integratesplitrail_addrecords2queue(splittrail))
        {
            elog(RLOG_WARNING, "integrate add records 2 queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void increment_integratesplittrail_free(increment_integratesplittrail* splitrail)
{
    if (NULL == splitrail)
    {
        return;
    }

    if (splitrail->capturedata)
    {
        rfree(splitrail->capturedata);
    }

    if (NULL != splitrail->loadrecords)
    {
        loadtrailrecords_free(splitrail->loadrecords);
    }
    splitrail->recordscache = NULL;
    rfree(splitrail);
}
