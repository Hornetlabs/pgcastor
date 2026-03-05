#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/dlist/dlist.h"
#include "misc/ripple_misc_stat.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "storage/ripple_file_buffer.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/ripple_netserver.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "metric/collector/ripple_metric_collector.h"
#include "increment/collector/net/ripple_increment_collectornetsvr.h"
#include "increment/collector/flush/ripple_increment_collectorflush.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"
#include "filetransfer/collector/ripple_filetransfer_collector.h"
#include "increment/collector/ripple_increment_collector.h"

/* collector端 设置metric接收到 pump 的 lsn */
static void ripple_increment_collector_recvlsn_set(void* privdata, char* name, XLogRecPtr recvlsn)
{
    ListCell* lc = NULL;
    ripple_metric_collectornode* mcollector = NULL;
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector metric recvlsn set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->collectorstate)
    {
        elog(RLOG_ERROR, "collector metric recvlsn set exception, collectorstate point is NULL");
    }

    if (RIPPLE_MAX_LSN == recvlsn || RIPPLE_FRISTVALID_LSN == recvlsn)
    {
        return;
    }
    

    foreach(lc, collectorstate->collectorstate->pumps)
    {
        mcollector = (ripple_metric_collectornode*)lfirst(lc);
        if (strlen(name) == strlen(mcollector->jobname)
            && 0 == strcmp(name, mcollector->jobname))
        {
            mcollector->recvlsn = recvlsn;
            break;
        }
    }
    return;
}

/* collector端 设置metric接收到 trail 文件编号 */
static void ripple_increment_collector_recvtrailno_set(void* privdata, char* name, uint64 recvtrailno)
{
    ListCell* lc = NULL;
    ripple_metric_collectornode* mcollector = NULL;
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector metric recvtrailno set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->collectorstate)
    {
        elog(RLOG_ERROR, "collector metric recvtrailno set exception, collectorstate point is NULL");
    }

    foreach(lc, collectorstate->collectorstate->pumps)
    {
        mcollector = (ripple_metric_collectornode*)lfirst(lc);
        if (strlen(name) == strlen(mcollector->jobname)
            && 0 == strcmp(name, mcollector->jobname))
        {
            mcollector->recvtrailno = recvtrailno;
            break;
        }
    }
    return;
}

/* collector端 设置metric接收到 trail 文件内的偏移 */
static void ripple_increment_collector_recvtrailstart_set(void* privdata, char* name, uint64 recvtrailstart)
{
    ListCell* lc = NULL;
    ripple_metric_collectornode* mcollector = NULL;
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector metric recvtrailstart set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->collectorstate)
    {
        elog(RLOG_ERROR, "collector metric recvtrailstart set exception, collectorstate point is NULL");
    }

    foreach(lc, collectorstate->collectorstate->pumps)
    {
        mcollector = (ripple_metric_collectornode*)lfirst(lc);
        if (strlen(name) == strlen(mcollector->jobname)
            && 0 == strcmp(name, mcollector->jobname))
        {
            mcollector->recvtrailstart = recvtrailstart;
            break;
        }
    }
    return;
}


/* collector端 设置metric接收到 时间戳 */
static void ripple_increment_collector_recvtimestamp_set(void* privdata, char* name, TimestampTz recvtimestamp)
{
    ListCell* lc = NULL;
    ripple_metric_collectornode* mcollector = NULL;
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector metric recvtimestamp set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->collectorstate)
    {
        elog(RLOG_ERROR, "collector metric recvtimestamp set exception, collectorstate point is NULL");
    }

    if (0 == recvtimestamp)
    {
        return;
    }

    foreach(lc, collectorstate->collectorstate->pumps)
    {
        mcollector = (ripple_metric_collectornode*)lfirst(lc);
        if (strlen(name) == strlen(mcollector->jobname)
            && 0 == strcmp(name, mcollector->jobname))
        {
            mcollector->recvtimestamp = recvtimestamp;
            break;
        }
    }
    return;
}

/* collector端 设置metric写入到文件中的 lsn */
static void ripple_increment_collector_flushlsn_set(void* privdata, char* pumpname, XLogRecPtr flushlsn)
{
    ListCell* lc = NULL;
    ripple_metric_collectornode* mcollector = NULL;
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector metric flushlsn set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->collectorstate)
    {
        elog(RLOG_ERROR, "collector metric flushlsn set exception, collectorstate point is NULL");
    }

    if (RIPPLE_MAX_LSN == flushlsn || RIPPLE_FRISTVALID_LSN == flushlsn)
    {
        return;
    }

    foreach(lc, collectorstate->collectorstate->pumps)
    {
        mcollector = (ripple_metric_collectornode*)lfirst(lc);
        if (strlen(pumpname) == strlen(mcollector->jobname)
            && 0 == strcmp(pumpname, mcollector->jobname))
        {
            mcollector->flushlsn = flushlsn;
            break;
        }
    }

    return;
}

/* collector端 设置metric 持久化到磁盘的 trail 文件编号 */
static void ripple_increment_collector_flushtrailno_set(void* privdata, char* pumpname, uint64 flushtrailno)
{
    ListCell* lc = NULL;
    ripple_metric_collectornode* mcollector = NULL;
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector metric flushtrailno set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->collectorstate)
    {
        elog(RLOG_ERROR, "collector metric flushtrailno set exception, collectorstate point is NULL");
    }

    foreach(lc, collectorstate->collectorstate->pumps)
    {
        mcollector = (ripple_metric_collectornode*)lfirst(lc);
        if (strlen(pumpname) == strlen(mcollector->jobname)
            && 0 == strcmp(pumpname, mcollector->jobname))
        {
            mcollector->flushtrailno = flushtrailno;
            break;
        }
    }

    return;
}

/* collector端 设置metric持久化到磁盘的 trail 文件内的偏移 */
static void ripple_increment_collector_flushtrailstart_set(void* privdata, char* pumpname, uint64 flushtrailstart)
{
    ListCell* lc = NULL;
    ripple_metric_collectornode* mcollector = NULL;
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector metric flushtrailstart set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->collectorstate)
    {
        elog(RLOG_ERROR, "collector metric flushtrailstart set exception, collectorstate point is NULL");
    }

    foreach(lc, collectorstate->collectorstate->pumps)
    {
        mcollector = (ripple_metric_collectornode*)lfirst(lc);
        if (strlen(pumpname) == strlen(mcollector->jobname)
            && 0 == strcmp(pumpname, mcollector->jobname))
        {
            mcollector->flushtrailstart = flushtrailstart;
            break;
        }
    }

    return;
}

/* collector端 设置metric持久化到 trail 文件中的时间戳 */
static void ripple_increment_collector_flushtimestamp_set(void* privdata, char* pumpname, TimestampTz flushtimestamp)
{
    ListCell* lc = NULL;
    ripple_metric_collectornode* mcollector = NULL;
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector metric flushtimestamp set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->collectorstate)
    {
        elog(RLOG_ERROR, "collector metric flushtimestamp set exception, collectorstate point is NULL");
    }

    if (0 == flushtimestamp)
    {
        return;
    }

    foreach(lc, collectorstate->collectorstate->pumps)
    {
        mcollector = (ripple_metric_collectornode*)lfirst(lc);
        if (strlen(pumpname) == strlen(mcollector->jobname)
            && 0 == strcmp(pumpname, mcollector->jobname))
        {
            mcollector->flushtimestamp = flushtimestamp;
            break;
        }
    }

    return;
}

/* collector端 添加filetransfernode */
static void ripple_collector_filetransfernode_add(void* privdata, void* filetransfernode)
{
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector filetransfernode add exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->filetransfernode)
    {
        elog(RLOG_ERROR, "collector filetransfernode add exception, filetransfernode point is NULL");
    }

    ripple_filetransfer_metadatafile_set(filetransfernode);

    ripple_filetransfer_node_add((void*)collectorstate->filetransfernode, filetransfernode);

    return;
}

/* collector端 添加netclient */
static bool ripple_collector_netclient_add(void* privdata, void* netclient)
{
    ripple_increment_collector* collectorstate = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector netclient add exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    if (NULL == collectorstate->netsvr)
    {
        elog(RLOG_ERROR, "collector netclient add exception, netsvr point is NULL");
    }

    /* 注册 netclient 线程 */
    if(false == ripple_threads_addsubmanger(collectorstate->threads,
                                            RIPPLE_THRNODE_IDENTITY_INC_COLLECTOR_NETCLINT,
                                            collectorstate->persistno,
                                            &collectorstate->netsvr->thrsmgr,
                                            (void*)netclient,
                                            ripple_increment_collectornetclient_free,
                                            NULL,
                                            ripple_increment_collectornetclient_main))
    {
        elog(RLOG_WARNING, "start collector netclient failed");
        return false;
    }
    return true;
}

static ripple_increment_collectornetbuffernode* ripple_increment_collector_netbuffernode_init(char* name)
{
    ripple_increment_collectornetbuffernode* netbuffernode = NULL;
    netbuffernode = (ripple_increment_collectornetbuffernode*)rmalloc0(sizeof(ripple_increment_collectornetbuffernode));
    if(NULL == netbuffernode)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(netbuffernode, 0, '\0', sizeof(ripple_increment_collectornetbuffernode));
    snprintf(netbuffernode->name, 128, "%s", name);
    netbuffernode->netbuffer = ripple_file_buffer_init();
    return netbuffernode;
}

/* 向ripple_increment_collector添加flush */
static bool ripple_increment_collector_addflush(void* privdata, char* jobname)
{
    int iret = 0;
    ListCell* lc = NULL;
    ripple_metric_collectornode* collectornode = NULL;
    ripple_increment_collector* collectorstate = NULL;
    ripple_increment_collectorflushnode* flushnode = NULL;
    ripple_increment_collectornetbuffernode* netbuffernode = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector collectorstate recvlsn set exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    iret = ripple_thread_lock(&collectorstate->flushlock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    foreach(lc, collectorstate->flushthreads)
    {
        flushnode = (ripple_increment_collectorflushnode*)lfirst(lc);

        if(strlen(jobname) == strlen(flushnode->name)
           && strcmp(jobname, flushnode->name) == 0)
        {
            ripple_thread_unlock(&collectorstate->flushlock);
            return true;
        }
    }

    netbuffernode = ripple_increment_collector_netbuffernode_init(jobname);
    collectorstate->netbuffers = lappend(collectorstate->netbuffers, netbuffernode);

    flushnode = ripple_increment_collectorflushnode_init(jobname);

    flushnode->stat = RIPPLE_INCREMENT_COLLECTORFLUSHNODE_STAT_INIT;
    flushnode->flush->privdata = (void*)collectorstate;
    flushnode->flush->netdata2filebuffer = netbuffernode->netbuffer;
    flushnode->flush->callback.setmetricflushlsn = ripple_increment_collector_flushlsn_set;
    flushnode->flush->callback.setmetricflushtrailno = ripple_increment_collector_flushtrailno_set;
    flushnode->flush->callback.setmetricflushtrailstart = ripple_increment_collector_flushtrailstart_set;
    flushnode->flush->callback.setmetricflushtimestamp = ripple_increment_collector_flushtimestamp_set;
    flushnode->flush->callback.collector_filetransfernode_add = ripple_collector_filetransfernode_add;
    snprintf(flushnode->flush->name, 128, "%s", jobname);
    collectorstate->flushthreads = lappend(collectorstate->flushthreads, flushnode);

    collectornode = ripple_metric_collectornode_init(jobname);

    collectorstate->collectorstate->pumps = lappend(collectorstate->collectorstate->pumps, collectornode);

    ripple_thread_unlock(&collectorstate->flushlock);
    return true;
}

/* networksvr获取自身privdata的netbuffer */
ripple_file_buffers* ripple_increment_collector_netsvr_netbuffer_get(void* privdata, char* name)
{
    ListCell* lc = NULL;
    ripple_increment_collector* collectorstate = NULL;
    ripple_increment_collectornetbuffernode* netbuffernode = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector networksvr netbuffer get exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    foreach(lc, collectorstate->netbuffers)
    {
        netbuffernode = (ripple_increment_collectornetbuffernode*)lfirst(lc);

        if(strlen(name) != strlen(netbuffernode->name))
        {
            continue;
        }

        if(0 != strcmp(name, netbuffernode->name))
        {
            continue;
        }

        return netbuffernode->netbuffer;
    }

    elog(RLOG_WARNING, "collector netsvr buffer get exception, unknown %s client", name);
    return NULL;
}

void ripple_increment_collector_cflush_fileid_get(void* privdata, char* name, uint64* pfileid, uint64* cfileid)
{
    ListCell* lc = NULL;
    ripple_increment_collector* collectorstate = NULL;
    ripple_increment_collectorflushnode* flushnode = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "collector cflush fileid  get exception, privdata point is NULL");
    }

    collectorstate = (ripple_increment_collector*)privdata;

    foreach(lc, collectorstate->flushthreads)
    {
        flushnode = (ripple_increment_collectorflushnode*)lfirst(lc);

        if(strlen(name) != strlen(flushnode->flush->name))
        {
            continue;
        }

        if(0 != strcmp(name, flushnode->flush->name))
        {
            continue;
        }
        *pfileid = flushnode->flush->collectorbase.pfileid;
        *cfileid = flushnode->flush->collectorbase.cfileid;
        return;
    }

    elog(RLOG_WARNING, "collector cflush fileid get exception, unknown %s client", name);
}

ripple_increment_collector* ripple_increment_collector_init(void)
{
    ripple_increment_collector*                 collectorstate = NULL;

    collectorstate = (ripple_increment_collector*)rmalloc1(sizeof(ripple_increment_collector));
    if (NULL == collectorstate)
    {
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }
    rmemset0(collectorstate, 0, '\0', sizeof(ripple_increment_collector));

    ripple_thread_mutex_init(&collectorstate->flushlock, NULL);

    /* collector 状态初始化 */
    collectorstate->collectorstate = ripple_metric_collector_init();
    collectorstate->filetransfernode = ripple_queue_init();
    collectorstate->threads = ripple_threads_init();
    collectorstate->ftptransfer = ripple_filetransfer_collector_init();
    collectorstate->ftptransfer->filetransfernode = collectorstate->filetransfernode;

    /* collector 服务端初始化 */
    collectorstate->netsvr = ripple_increment_collectornetsvr_init();
    if (NULL == collectorstate->netsvr)
    {
        elog(RLOG_WARNING, "collector net server init error");
        return NULL;
    }

    collectorstate->netsvr->privdata = (void*)collectorstate;
    collectorstate->netsvr->callback.netsvr_netbuffer_get = ripple_increment_collector_netsvr_netbuffer_get;
    collectorstate->netsvr->callback.setmetricrecvlsn = ripple_increment_collector_recvlsn_set;
    collectorstate->netsvr->callback.setmetricrecvtrailno = ripple_increment_collector_recvtrailno_set;
    collectorstate->netsvr->callback.setmetricrecvtrailstart = ripple_increment_collector_recvtrailstart_set;
    collectorstate->netsvr->callback.setmetricrecvtimestamp = ripple_increment_collector_recvtimestamp_set;
    collectorstate->netsvr->callback.writestate_fileid_get = ripple_increment_collector_cflush_fileid_get;
    collectorstate->netsvr->callback.collector_increment_addflush = ripple_increment_collector_addflush;
    collectorstate->netsvr->callback.collectornetsvr_filetransfernode_add = ripple_collector_filetransfernode_add;
    collectorstate->netsvr->callback.collectornetsvr_netclient_add = ripple_collector_netclient_add;
    return collectorstate;
}

void ripple_increment_collector_destroy(ripple_increment_collector* collector)
{
    ListCell* lc = NULL;
    ripple_increment_collectorflushnode* flushnode = NULL;
    ripple_increment_collectornetbuffernode* netbuffernode = NULL;
    if (NULL == collector)
    {
        return;
    }

    ripple_thread_mutex_destroy(&collector->flushlock);

    foreach(lc, collector->flushthreads)
    {
        flushnode = (ripple_increment_collectorflushnode*)lfirst(lc);
        ripple_increment_collectorflushnode_destroy(flushnode);
    }
    list_free(collector->flushthreads);
    collector->flushthreads = NULL;

    foreach(lc, collector->netbuffers)
    {
        netbuffernode = (ripple_increment_collectornetbuffernode*)lfirst(lc);
        ripple_file_buffer_destroy(netbuffernode->netbuffer);
        rfree(netbuffernode);
    }
    list_free(collector->netbuffers);
    collector->netbuffers = NULL;

    ripple_filetransfer_collector_free(collector->ftptransfer);

    ripple_increment_collectornetsvr_destroy(collector->netsvr);

    ripple_threads_free(collector->threads);

    ripple_metric_collector_destroy(collector->collectorstate);

    ripple_queue_destroy(collector->filetransfernode, ripple_filetransfer_queuefree);

    rfree(collector);
    collector = NULL;
}
