#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/mpage/mpage.h"
#include "utils/hash/hash_search.h"
#include "queue/ripple_queue.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "threads/ripple_threads.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadtrailrecords.h"
#include "increment/integrate/split/ripple_increment_integratesplittrail.h"


/* 设置 integrate 拆分线程的状态 */
void ripple_increment_integratesplittrail_state_set(ripple_increment_integratesplittrail* splittrail, int state)
{
    splittrail->state = state;
}

/*
  * 入参说明:
  *    fileid 设置为 loadtrail->fileid, loadtrail->offset 设置为 0
  *    emitoffset 设置为 emitoffset
  */
void ripple_increment_integratesplittrail_emit_set(ripple_increment_integratesplittrail* splittrail, uint64 fileid, uint64 emitoffset)
{
    /* 重置过滤的起点 */
    splittrail->loadrecords->fileid = fileid;
    splittrail->loadrecords->foffset = 0;
    splittrail->emitoffset = emitoffset;

    /* 重置解析的起点和过滤的起点 */
    ripple_loadtrailrecords_setloadposition(splittrail->loadrecords, splittrail->loadrecords->fileid, splittrail->loadrecords->foffset);
}

static bool ripple_increment_integratesplittrail_state_checkandset(ripple_increment_integratesplittrail* splittrail)
{
    if (RIPPLE_INTEGRATE_STATUS_SPLIT_WORKING == splittrail->state)
    {
        return true;
    }
    return false;
}

/* 初始化信息,包含设置 loadtrail中的基础信息 */
ripple_increment_integratesplittrail* ripple_increment_integratesplittrail_init(void)
{
    char* cdata = NULL;
    ripple_increment_integratesplittrail* splittrail = NULL;

    splittrail = (ripple_increment_integratesplittrail*)rmalloc1(sizeof(ripple_increment_integratesplittrail));
    if(NULL == splittrail)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(splittrail, 0, '\0', sizeof(ripple_increment_integratesplittrail));

    /* 根据配置文件设置 loadtrail信息 */
    splittrail->capturedata = rmalloc0(MAXPGPATH);
    if (NULL == splittrail->capturedata)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(splittrail->capturedata, 0, '\0', MAXPGPATH);
    cdata = guc_getConfigOption(RIPPLE_CFG_KEY_TRAIL_DIR);
    snprintf(splittrail->capturedata, MAXPGPATH, "%s/%s", cdata, RIPPLE_STORAGE_TRAIL_DIR);

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

    /* 设置状态为等待设置 */
    ripple_increment_integratesplittrail_state_set(splittrail, RIPPLE_INTEGRATE_STATUS_SPLIT_WAITSET);
    splittrail->filter = true;
    return splittrail;
}

/* 将 records 加入到队列中 */
static bool ripple_increment_integratesplitrail_addrecords2queue(ripple_increment_integratesplittrail* splittrail)
{
    /* 加入到队列中 */
    while(RIPPLE_INTEGRATE_STATUS_SPLIT_WORKING == splittrail->state)
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

/* 处理主入口 */
void* ripple_increment_integratesplitrail_main(void* args)
{
    uint64 fileid                                           = 0;
    ripple_thrnode* thrnode                                 = NULL;
    ripple_increment_integratesplittrail* splittrail        = NULL;

    thrnode = (ripple_thrnode*)args;
    /* 入参转换 */
    splittrail = (ripple_increment_integratesplittrail*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment integrate splittrail exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while(true)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        if (!ripple_increment_integratesplittrail_state_checkandset(splittrail))
        {
            /* 睡眠 10 毫秒 */
            usleep(10000);
            continue;
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

        splittrail->callback.setmetricloadtrailno(splittrail->privdata, splittrail->loadrecords->fileid);
        splittrail->callback.setmetricloadtrailstart(splittrail->privdata, splittrail->loadrecords->foffset);

        if(true == dlist_isnull(splittrail->loadrecords->records))
        {
            /* 
             * 没有读取到数据, 追上了最新的, 所以需要重新读取该块
             */
            /* 需要退出，等待 thrnode->stat 变为 TERM 后退出*/
            if(RIPPLE_THRNODE_STAT_TERM != thrnode->stat)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 是否需要过滤, 不需要过滤则加入到队列中 */
        if(false == splittrail->filter)
        {
            /* 加入到队列中 */
            if(false == ripple_increment_integratesplitrail_addrecords2queue(splittrail))
            {
                elog(RLOG_WARNING, "integrate add records 2 queue error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if(false == ripple_loadtrailrecords_filterremainmetadata(splittrail->loadrecords, fileid, splittrail->emitoffset))
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
        if(false == ripple_increment_integratesplitrail_addrecords2queue(splittrail))
        {
            elog(RLOG_WARNING, "integrate add records 2 queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_increment_integratesplittrail_free(ripple_increment_integratesplittrail* splitrail)
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
        ripple_loadtrailrecords_free(splitrail->loadrecords);
    }
    splitrail->recordscache = NULL;
    rfree(splitrail);
}
