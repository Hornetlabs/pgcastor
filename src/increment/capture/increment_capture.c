#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "misc/ripple_misc_stat.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/dlist/dlist.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "net/netpacket/ripple_netpacket.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/ripple_file_buffer.h"
#include "threads/ripple_threads.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "refresh/ripple_refresh_tables.h"
#include "stmts/ripple_txnstmt_refresh.h"
#include "stmts/ripple_txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "works/parserwork/wal/ripple_parserwork_wal.h"
#include "increment/capture/flush/ripple_increment_captureflush.h"
#include "serial/ripple_serial.h"
#include "bigtransaction/persist/ripple_bigtxn_persist.h"
#include "bigtransaction/ripple_bigtxn.h"
#include "bigtransaction/capture/serial/ripple_bigtxn_captureserial.h"
#include "bigtransaction/capture/flush/ripple_bigtxn_captureflush.h"
#include "works/splitwork/wal/ripple_splitwork_wal.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "increment/capture/serial/ripple_increment_captureserial.h"
#include "metric/capture/ripple_metric_capture.h"
#include "increment/capture/ripple_increment_capture.h"


static void ripple_capturestate_write_set_misc_callback(void *state, 
                                                        XLogRecPtr confirm,
                                                        XLogRecPtr restart,
                                                        XLogRecPtr redo)
{
    ripple_increment_capture *inccapture = state;
    inccapture->writestate->base.confirmedlsn = confirm;
    inccapture->writestate->base.restartlsn = restart;
    inccapture->writestate->base.redolsn = redo;
    ripple_misc_stat_decodewrite(&(inccapture->writestate->base), &inccapture->writestate->basefd);
}

/*------------metric 信息设置   begin-------------------*/

/* capture端 设置capturestate解析到的lsn */
static void ripple_capturestate_parselsn_set(void* privdata, XLogRecPtr pareslsn)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric  pareslsn set exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric pareslsn set exception, capturestate point is NULL");
    }

    inccapture->metric->parselsn = pareslsn;

    return;
}

/* capture端 设置capturestate拆分到的lsn */
static void ripple_capturestate_loadlsn_set(void* privdata, XLogRecPtr loadlasn)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric splitlsn set exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric splitlsn set exception, capturestate point is NULL");
    }

    inccapture->metric->loadlsn = loadlasn;

    return;
}

/* capture端 设置capturestate写入到文件中的 lsn */
static void ripple_capturestate_flushlsn_set(void* privdata, XLogRecPtr flushlsn)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric flushlsn set exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric flushlsn set exception, capturestate point is NULL");
    }

    inccapture->metric->flushlsn = flushlsn;

    return;
}

/* capture端 设置capturestate 的trail 文件编号*/
static void ripple_capturestate_trailnoset(void* privdata, uint64 trailno)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric trailno set exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric trailno set exception, capturestate point is NULL");
    }

    inccapture->metric->trailno = trailno;

    return;
}

/* capture端 设置capturestate的trail 文件内的偏移 */
static void ripple_capturestate_trailstartset(void* privdata, uint64 trailstart)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric trailstart set exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric trailstart set exception, capturestate point is NULL");
    }

    inccapture->metric->trailstart = trailstart;

    return;
}

/* capture端 设置capturestate的 confirmlsn */
static void ripple_capturestate_walsynclsn_set(void* privdata, 
                                               XLogRecPtr redolsn,
                                               XLogRecPtr restartlsn,
                                               XLogRecPtr confirmlsn)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "increment capture metric walsynclsn set exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "increment capture metric walsynclsn set exception, capturestate point is NULL");
    }

    inccapture->metric->redolsn = redolsn;
    inccapture->metric->restartlsn = restartlsn;
    inccapture->metric->confirmlsn = confirmlsn;

    return;
}

/* capture端 设置capturestate的trail 文件内的偏移 */
static void ripple_capturestate_parsetimestamp_set(void* privdata, TimestampTz parsetimestamp)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric parsetimestamp set exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric parsetimestamp set exception, capturestate point is NULL");
    }
    if (0 != parsetimestamp)
    {
        inccapture->metric->parsetimestamp = parsetimestamp;
    }

    return;
}

/* capture端 设置capturestate的trail 文件内的偏移 */
static void ripple_capturestate_flushtimestamp_set(void* privdata, TimestampTz flushtimestamp)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric flushtimestamp set exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric flushtimestamp set exception, capturestate point is NULL");
    }

    if (0 != flushtimestamp)
    {
        inccapture->metric->flushtimestamp = flushtimestamp;
    }

    return;
}

/*------------metric 信息设置  end-------------------*/

/* capture端 设置splitwork的拆分的起点和终点 */
void ripple_increment_capture_splitwal_lsn_set(void* privdata, XLogRecPtr startlsn, XLogRecPtr endlsn)
{
    ripple_loadwalrecords *loadrecords = NULL;
    ripple_increment_capture* inccapture = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture split lsn exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;
    if (NULL == inccapture->splitwalctx)
    {
        elog(RLOG_ERROR, "capture split state exception, splitwalctx point is NULL");
    }

    loadrecords = inccapture->splitwalctx->loadrecords;
    if (!loadrecords)
    {
        elog(RLOG_ERROR, "loadrecords is NULL");
    }

    if (startlsn && endlsn)
    {
        inccapture->splitwalctx->rewind_start = startlsn;
        loadrecords->startptr = startlsn;
        loadrecords->endptr = endlsn;
        inccapture->splitwalctx->status = RIPPLE_SPLITWORK_WAL_STATUS_REWIND;
    }
    else if (startlsn && endlsn == InvalidXLogRecPtr)
    {
        /* 如果是切换的, 此时还不能直接赋值startptr, 因为record可能还在划分 */
        if (inccapture->splitwalctx->status == RIPPLE_SPLITWORK_WAL_STATUS_REWIND)
        {
            inccapture->splitwalctx->change = true;
            inccapture->splitwalctx->change_startptr = startlsn;
        }
        else
        {
            loadrecords->startptr = startlsn;
        }

        loadrecords->endptr = InvalidXLogRecPtr;
        inccapture->splitwalctx->status = RIPPLE_SPLITWORK_WAL_STATUS_NORMAL;
    }
    else
    {
        elog(RLOG_ERROR, "error startlsn or endlsn in ");
    }

    return;

}

/* splitwork 设置解析线程状态为emiting */
void ripple_increment_capture_parserwal_rewindingstat_setemiting(void* privdata)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture parserwal rewinding state exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->decodingctx)
    {
        elog(RLOG_ERROR, "capture parserwal rewinding exception, decodingctx point is NULL");
    }

    inccapture->decodingctx->rewind->stat = RIPPLE_REWIND_EMITING;

    return;
}

/* 获取parser中的curtlid */
TimeLineID ripple_increment_capture_parserstat_curtlid_get(void* privdata)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture parserwal rewinding state exception, privdata point is NULL");
    }

    inccapture = (ripple_increment_capture*)privdata;

    if (NULL == inccapture->serialstate)
    {
        elog(RLOG_ERROR, "capture parserwal rewinding exception, decodingctx point is NULL");
    }

    return inccapture->decodingctx->base.curtlid;
}

/* 设置写入trail文件路径 */
void ripple_increment_capture_writestate_set_trail(void* privdata, char* trail)
{
    ripple_increment_capture* inccapture = NULL;
    if (NULL == privdata || NULL == trail)
    {
        elog(RLOG_ERROR, "capture writestate set trail exception, privdata or trail point is NULL");
    }

    if (NULL == inccapture->writestate)
    {
        elog(RLOG_ERROR, "capture writestate set trail exception, writestate point is NULL");
    }

    return;
}

/* 添加 onlinerefresh 节点 */
static void ripple_increment_capture_addonlinerefresh(void* privdata, void* rtables)
{
    ripple_increment_capture* inccapture                    = NULL;

    inccapture = (ripple_increment_capture*)privdata;

    ripple_thread_lock(&inccapture->olrefreshlock);
    inccapture->olrefreshtables = dlist_put(inccapture->olrefreshtables, rtables);
    ripple_thread_unlock(&inccapture->olrefreshlock);
}


ripple_increment_capture* ripple_increment_capture_init(void)
{
    ripple_increment_capture* inccapture = NULL;
    inccapture = (ripple_increment_capture*)rmalloc1(sizeof(ripple_increment_capture));
    if (NULL == inccapture)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(inccapture, 0, '\0', sizeof(ripple_increment_capture));

    inccapture->splitwalctx = ripple_splitwal_init();

    inccapture->recordsqueue = ripple_queue_init();
    ripple_queue_setmax(inccapture->recordsqueue, g_walcachemaxsize*1024*1024);

    inccapture->decodingctx = ripple_parserwork_walinitphase1();
    inccapture->parser2serialtxns = ripple_cache_txn_init();
    inccapture->serialstate = ripple_increment_captureserial_init();
    inccapture->txn2filebuffer = ripple_file_buffer_init();
    inccapture->writestate = ripple_increment_captureflush_init();
    inccapture->metric = ripple_metric_capture_init();
    inccapture->bigtxnserialstate = ripple_bigtxn_captureserial_init();
    inccapture->bigtxnwritestate = ripple_bigtxn_captureflush_init();
    inccapture->threads = ripple_threads_init();
    if(NULL == inccapture->threads)
    {
        elog(RLOG_WARNING, "increment capture init threads error");
        return NULL;
    }

    ripple_thread_mutex_init(&inccapture->olrefreshlock, NULL);
    inccapture->olrefreshing = NULL;

    inccapture->bigtxnserialstate->privdata = (void *)inccapture;
    inccapture->bigtxnserialstate->callback.bigtxn_parserstat_curtlid_get = ripple_increment_capture_parserstat_curtlid_get;

    inccapture->bigtxnwritestate->privdata = (void *)inccapture;
    inccapture->bigtxnwritestate->txn2filebuffer = inccapture->bigtxnserialstate->base.txn2filebuffer;
    inccapture->bigtxnwritestate->callback.setmetricflushlsn = NULL;

    inccapture->splitwalctx->privdata = (void *)inccapture;
    inccapture->splitwalctx->recordqueue = inccapture->recordsqueue;
    inccapture->splitwalctx->callback.parserwal_rewindstat_setemiting = ripple_increment_capture_parserwal_rewindingstat_setemiting;
    inccapture->splitwalctx->callback.capturestate_loadlsn_set = ripple_capturestate_loadlsn_set;

    inccapture->decodingctx->privdata = (void*)inccapture;
    inccapture->decodingctx->recordqueue = inccapture->recordsqueue;
    inccapture->decodingctx->parser2txns = inccapture->parser2serialtxns;
    inccapture->decodingctx->callback.setloadlsn = ripple_increment_capture_splitwal_lsn_set;
    inccapture->decodingctx->callback.setmetricparselsn = ripple_capturestate_parselsn_set;
    inccapture->decodingctx->parser2bigtxns = inccapture->bigtxnserialstate->bigtxn2serial;
    inccapture->decodingctx->callback.setparserlsn = ripple_capturestate_write_set_misc_callback;
    inccapture->decodingctx->callback.setmetricsynclsn = ripple_capturestate_walsynclsn_set;
    inccapture->decodingctx->callback.setmetricparsetimestamp = ripple_capturestate_parsetimestamp_set;

    inccapture->serialstate->privdata = (void*)inccapture;
    inccapture->serialstate->parser2serialtxns = inccapture->parser2serialtxns;
    inccapture->serialstate->base.txn2filebuffer = inccapture->txn2filebuffer;
    inccapture->serialstate->callback.parserstat_curtlid_get = ripple_increment_capture_parserstat_curtlid_get;

    inccapture->writestate->privdata = (void*)inccapture;
    inccapture->writestate->txn2filebuffer = inccapture->txn2filebuffer;
    inccapture->writestate->callback.setmetricflushlsn = ripple_capturestate_flushlsn_set;
    inccapture->writestate->callback.setmetrictrailno = ripple_capturestate_trailnoset;
    inccapture->writestate->callback.setmetrictrailstart = ripple_capturestate_trailstartset;
    inccapture->writestate->callback.setmetricflushtimestamp = ripple_capturestate_flushtimestamp_set;

    inccapture->metric->privdata = inccapture;
    inccapture->metric->addonlinerefresh = ripple_increment_capture_addonlinerefresh;
    return inccapture;
}

static void recordqueue_dlist_free(dlist* record_dlist)
{
    dlist_free(record_dlist, (dlistvaluefree)ripple_record_free);
}

void ripple_increment_capture_destroy(ripple_increment_capture* inccapture)
{
    if (NULL == inccapture)
    {
        return;
    }

    ripple_thread_mutex_destroy(&inccapture->olrefreshlock);

    ripple_splitwal_destroy(inccapture->splitwalctx);

    ripple_parserwork_wal_destroy(inccapture->decodingctx);

    ripple_increment_captureserial_destroy(inccapture->serialstate);

    ripple_increment_captureflush_destroy(inccapture->writestate);

    ripple_bigtxn_captureflush_destroy(inccapture->bigtxnwritestate);

    ripple_queue_destroy(inccapture->recordsqueue, (queuedatafree)recordqueue_dlist_free);

    ripple_file_buffer_destroy(inccapture->txn2filebuffer);

    ripple_cache_txn_destroy(inccapture->parser2serialtxns);

    ripple_metric_capture_destroy(inccapture->metric);

    ripple_bigtxn_captureserial_destroy(inccapture->bigtxnserialstate);

    ripple_threads_free(inccapture->threads);

    rfree(inccapture);
    inccapture = NULL;
}
