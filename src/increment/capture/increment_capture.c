#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "misc/misc_stat.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/uuid/uuid.h"
#include "utils/dlist/dlist.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "net/netpacket/netpacket.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/file_buffer.h"
#include "threads/threads.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "works/parserwork/wal/parserwork_wal.h"
#include "increment/capture/flush/increment_captureflush.h"
#include "serial/serial.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/bigtxn.h"
#include "bigtransaction/capture/serial/bigtxn_captureserial.h"
#include "bigtransaction/capture/flush/bigtxn_captureflush.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "increment/capture/serial/increment_captureserial.h"
#include "metric/capture/metric_capture.h"
#include "increment/capture/increment_capture.h"

static void capturestate_write_set_misc_callback(void*      state,
                                                 XLogRecPtr confirm,
                                                 XLogRecPtr restart,
                                                 XLogRecPtr redo)
{
    increment_capture* inccapture = state;
    inccapture->writestate->base.confirmedlsn = confirm;
    inccapture->writestate->base.restartlsn = restart;
    inccapture->writestate->base.redolsn = redo;
    misc_stat_decodewrite(&(inccapture->writestate->base), &inccapture->writestate->basefd);
}

/*------------metric info set   begin-------------------*/

/* Capture end set capturestate parsed lsn */
static void capturestate_parselsn_set(void* privdata, XLogRecPtr pareslsn)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric  pareslsn set exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric pareslsn set exception, capturestate point is NULL");
    }

    inccapture->metric->parselsn = pareslsn;

    return;
}

/* Capture end set capturestate split lsn */
static void capturestate_loadlsn_set(void* privdata, XLogRecPtr loadlasn)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric splitlsn set exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric splitlsn set exception, capturestate point is NULL");
    }

    inccapture->metric->loadlsn = loadlasn;

    return;
}

/* Capture end set capturestate flushed lsn */
static void capturestate_flushlsn_set(void* privdata, XLogRecPtr flushlsn)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric flushlsn set exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric flushlsn set exception, capturestate point is NULL");
    }

    inccapture->metric->flushlsn = flushlsn;

    return;
}

/* Capture end set capturestate trail file number */
static void capturestate_trailnoset(void* privdata, uint64 trailno)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric trailno set exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric trailno set exception, capturestate point is NULL");
    }

    inccapture->metric->trailno = trailno;

    return;
}

/* Capture end set capturestate trail file offset */
static void capturestate_trailstartset(void* privdata, uint64 trailstart)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric trailstart set exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR, "capture metric trailstart set exception, capturestate point is NULL");
    }

    inccapture->metric->trailstart = trailstart;

    return;
}

/* Capture end set capturestate confirmlsn */
static void capturestate_walsynclsn_set(void*      privdata,
                                        XLogRecPtr redolsn,
                                        XLogRecPtr restartlsn,
                                        XLogRecPtr confirmlsn)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR,
             "increment capture metric walsynclsn set exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

    if (NULL == inccapture->metric)
    {
        elog(RLOG_ERROR,
             "increment capture metric walsynclsn set exception, capturestate point is NULL");
    }

    inccapture->metric->redolsn = redolsn;
    inccapture->metric->restartlsn = restartlsn;
    inccapture->metric->confirmlsn = confirmlsn;

    return;
}

/* Capture end set capturestate parse timestamp */
static void capturestate_parsetimestamp_set(void* privdata, TimestampTz parsetimestamp)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric parsetimestamp set exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

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

/* Capture end set capturestate flush timestamp */
static void capturestate_flushtimestamp_set(void* privdata, TimestampTz flushtimestamp)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture metric flushtimestamp set exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

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

/*------------metric info set  end-------------------*/

/* capture end set splitwork split start and end points */
void increment_capture_splitwal_lsn_set(void* privdata, XLogRecPtr startlsn, XLogRecPtr endlsn)
{
    loadwalrecords*    loadrecords = NULL;
    increment_capture* inccapture = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture split lsn exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;
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
        inccapture->splitwalctx->status = SPLITWORK_WAL_STATUS_REWIND;
    }
    else if (startlsn && endlsn == InvalidXLogRecPtr)
    {
        /* If switching, cannot directly assign startptr yet, because records may still be splitting
         */
        if (inccapture->splitwalctx->status == SPLITWORK_WAL_STATUS_REWIND)
        {
            inccapture->splitwalctx->change = true;
            inccapture->splitwalctx->change_startptr = startlsn;
        }
        else
        {
            loadrecords->startptr = startlsn;
        }

        loadrecords->endptr = InvalidXLogRecPtr;
        inccapture->splitwalctx->status = SPLITWORK_WAL_STATUS_NORMAL;
    }
    else
    {
        elog(RLOG_ERROR, "error startlsn or endlsn in ");
    }

    return;
}

/* splitwork set parser thread state to emitting */
void increment_capture_parserwal_rewindingstat_setemiting(void* privdata)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture parserwal rewinding state exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

    if (NULL == inccapture->decodingctx)
    {
        elog(RLOG_ERROR, "capture parserwal rewinding exception, decodingctx point is NULL");
    }

    inccapture->decodingctx->rewind_ptr->stat = REWIND_EMITING;

    return;
}

/* Get curtlid from parser */
TimeLineID increment_capture_parserstat_curtlid_get(void* privdata)
{
    increment_capture* inccapture = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "capture parserwal rewinding state exception, privdata point is NULL");
    }

    inccapture = (increment_capture*)privdata;

    if (NULL == inccapture->serialstate)
    {
        elog(RLOG_ERROR, "capture parserwal rewinding exception, decodingctx point is NULL");
    }

    return inccapture->decodingctx->base.curtlid;
}

/* Set trail file path for writing */
void increment_capture_writestate_set_trail(void* privdata, char* trail)
{
    increment_capture* inccapture = NULL;
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

/* Add onlinerefresh node */
static void increment_capture_addonlinerefresh(void* privdata, void* rtables)
{
    increment_capture* inccapture = NULL;

    inccapture = (increment_capture*)privdata;

    osal_thread_lock(&inccapture->olrefreshlock);
    inccapture->olrefreshtables = dlist_put(inccapture->olrefreshtables, rtables);
    osal_thread_unlock(&inccapture->olrefreshlock);
}

increment_capture* increment_capture_init(void)
{
    increment_capture* inccapture = NULL;
    inccapture = (increment_capture*)rmalloc1(sizeof(increment_capture));
    if (NULL == inccapture)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(inccapture, 0, '\0', sizeof(increment_capture));

    inccapture->splitwalctx = splitwal_init();

    inccapture->recordsqueue = queue_init();
    queue_setmax(inccapture->recordsqueue, g_walcachemaxsize * 1024 * 1024);

    inccapture->decodingctx = parserwork_walinitphase1();
    inccapture->parser2serialtxns = cache_txn_init();
    inccapture->serialstate = increment_captureserial_init();
    inccapture->txn2filebuffer = file_buffer_init();
    inccapture->writestate = increment_captureflush_init();
    inccapture->metric = metric_capture_init();
    inccapture->bigtxnserialstate = bigtxn_captureserial_init();
    inccapture->bigtxnwritestate = bigtxn_captureflush_init();
    inccapture->threads = threads_init();
    if (NULL == inccapture->threads)
    {
        elog(RLOG_WARNING, "increment capture init threads error");
        return NULL;
    }

    osal_thread_mutex_init(&inccapture->olrefreshlock, NULL);
    inccapture->olrefreshing = NULL;

    inccapture->bigtxnserialstate->privdata = (void*)inccapture;
    inccapture->bigtxnserialstate->callback.bigtxn_parserstat_curtlid_get =
        increment_capture_parserstat_curtlid_get;

    inccapture->bigtxnwritestate->privdata = (void*)inccapture;
    inccapture->bigtxnwritestate->txn2filebuffer =
        inccapture->bigtxnserialstate->base.txn2filebuffer;
    inccapture->bigtxnwritestate->callback.setmetricflushlsn = NULL;

    inccapture->splitwalctx->privdata = (void*)inccapture;
    inccapture->splitwalctx->recordqueue = inccapture->recordsqueue;
    inccapture->splitwalctx->callback.parserwal_rewindstat_setemiting =
        increment_capture_parserwal_rewindingstat_setemiting;
    inccapture->splitwalctx->callback.capturestate_loadlsn_set = capturestate_loadlsn_set;

    inccapture->decodingctx->privdata = (void*)inccapture;
    inccapture->decodingctx->recordqueue = inccapture->recordsqueue;
    inccapture->decodingctx->parser2txns = inccapture->parser2serialtxns;
    inccapture->decodingctx->callback.setloadlsn = increment_capture_splitwal_lsn_set;
    inccapture->decodingctx->callback.setmetricparselsn = capturestate_parselsn_set;
    inccapture->decodingctx->parser2bigtxns = inccapture->bigtxnserialstate->bigtxn2serial;
    inccapture->decodingctx->callback.setparserlsn = capturestate_write_set_misc_callback;
    inccapture->decodingctx->callback.setmetricsynclsn = capturestate_walsynclsn_set;
    inccapture->decodingctx->callback.setmetricparsetimestamp = capturestate_parsetimestamp_set;

    inccapture->serialstate->privdata = (void*)inccapture;
    inccapture->serialstate->parser2serialtxns = inccapture->parser2serialtxns;
    inccapture->serialstate->base.txn2filebuffer = inccapture->txn2filebuffer;
    inccapture->serialstate->callback.parserstat_curtlid_get =
        increment_capture_parserstat_curtlid_get;

    inccapture->writestate->privdata = (void*)inccapture;
    inccapture->writestate->txn2filebuffer = inccapture->txn2filebuffer;
    inccapture->writestate->callback.setmetricflushlsn = capturestate_flushlsn_set;
    inccapture->writestate->callback.setmetrictrailno = capturestate_trailnoset;
    inccapture->writestate->callback.setmetrictrailstart = capturestate_trailstartset;
    inccapture->writestate->callback.setmetricflushtimestamp = capturestate_flushtimestamp_set;

    inccapture->metric->privdata = inccapture;
    inccapture->metric->addonlinerefresh = increment_capture_addonlinerefresh;
    return inccapture;
}

static void recordqueue_dlist_free(dlist* record_dlist)
{
    dlist_free(record_dlist, (dlistvaluefree)record_free);
}

void increment_capture_destroy(increment_capture* inccapture)
{
    if (NULL == inccapture)
    {
        return;
    }

    osal_thread_mutex_destroy(&inccapture->olrefreshlock);

    splitwal_destroy(inccapture->splitwalctx);

    parserwork_wal_destroy(inccapture->decodingctx);

    increment_captureserial_destroy(inccapture->serialstate);

    increment_captureflush_destroy(inccapture->writestate);

    bigtxn_captureflush_destroy(inccapture->bigtxnwritestate);

    queue_destroy(inccapture->recordsqueue, (queuedatafree)recordqueue_dlist_free);

    file_buffer_destroy(inccapture->txn2filebuffer);

    cache_txn_destroy(inccapture->parser2serialtxns);

    metric_capture_destroy(inccapture->metric);

    bigtxn_captureserial_destroy(inccapture->bigtxnserialstate);

    threads_free(inccapture->threads);

    rfree(inccapture);
    inccapture = NULL;
}
