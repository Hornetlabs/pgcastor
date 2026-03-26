#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "stmts/txnstmt.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "misc/misc_control.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "serial/serial.h"
#include "onlinerefresh/capture/serial/onlinerefresh_captureserial.h"

onlinerefresh_captureserial* onlinerefresh_captureserial_init(void)
{
    onlinerefresh_captureserial* result = NULL;

    result = rmalloc0(sizeof(onlinerefresh_captureserial));
    if (!result)
    {
        elog(RLOG_WARNING, "onlinerefresh capture init error, out of memory");
        return NULL;
    }
    result = rmemset0(result, 0, 0, sizeof(onlinerefresh_captureserial));
    result->serialstate = rmalloc0(sizeof(serialstate));
    if (!result->serialstate)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result->serialstate, 0, 0, sizeof(serialstate));
    serialstate_init(result->serialstate);

    result->dictcache = (transcache*)rmalloc0(sizeof(transcache));
    if (NULL == result->dictcache)
    {
        elog(RLOG_WARNING, "onlinerefresh capture init error, out of memory");
        return NULL;
    }
    rmemset0(result->dictcache, 0, '\0', sizeof(transcache));

    return result;
}

/* Load system dictionary from file */
static bool onlinerefresh_captureserial_loadsysdictsfromfile(transcache* dictcache)
{
    if (NULL == dictcache)
    {
        return false;
    }

    cache_sysdictsload((void**)&dictcache->sysdicts);

    return true;
}

/*
 * Put buffer into flush, need to set buffer's flag before putting
 */
static bool onlinerefresh_captureserial_buffer2waitflush(onlinerefresh_captureserial* serial_task,
                                                         txn*                         txn,
                                                         bool                         put_end)
{
    /*
     * 1、Get new cache
     * 2、Set new cache's flag information
     * 3、Set old cache's flag information based on lsn info in wstate
     */
    int          oldflag = 0;
    int          bufid = 0;
    int          timeout = 0;
    ff_fileinfo* finfo = NULL;
    file_buffer* fbuffer = NULL;
    file_buffer* foldbuffer = NULL;
    serialstate* serialstate = NULL;

    serialstate = serial_task->serialstate;

    foldbuffer =
        file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    if (0 == foldbuffer->start)
    {
        return true;
    }
    oldflag = foldbuffer->flag;

    /* Get new buffer cache */
    while (1)
    {
        bufid = file_buffer_get(serialstate->txn2filebuffer, &timeout);
        if (INVALID_BUFFERID == bufid)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }

    fbuffer = file_buffer_getbybufid(serialstate->txn2filebuffer, bufid);
    if (NULL == fbuffer->privdata)
    {
        finfo = (ff_fileinfo*)rmalloc0(sizeof(ff_fileinfo));
        if (NULL == finfo)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return false;
        }
        rmemset0(finfo, 0, '\0', sizeof(ff_fileinfo));
        fbuffer->privdata = (void*)finfo;
    }
    else
    {
        finfo = (ff_fileinfo*)fbuffer->privdata;
    }

    rmemcpy0(fbuffer->data, 0, foldbuffer->data, foldbuffer->start);
    fbuffer->start = foldbuffer->start;

    if (put_end)
    {
        foldbuffer->flag |= FILE_BUFFER_FLAG_ONLINREFRESHEND;
    }

    /* Set other information for new buffer */
    rmemcpy0(finfo, 0, (ff_fileinfo*)foldbuffer->privdata, sizeof(ff_fileinfo));

    /* Set oldbuffer's information */
    foldbuffer->extra.rewind.fileaddr.trail.fileid = finfo->fileid;
    foldbuffer->extra.rewind.fileaddr.trail.offset =
        (((finfo->blknum - 1) * FILE_BUFFER_SIZE) + fbuffer->start);
    if (false == serial_task->callback.parserstat_curtlid_get(serial_task->privdata,
                                                              &foldbuffer->extra.rewind.curtlid))
    {
        elog(RLOG_WARNING, "can not get timelineid");
        return false;
    }
    /* Put oldbuffer into flush waiting cache */
    rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(file_buffer_extra));
    file_buffer_waitflush_add(serialstate->txn2filebuffer, foldbuffer);

    fbuffer->flag = oldflag;
    serialstate->ffsmgrstate->bufid = bufid;

    return true;
}

/* Write entry data to disk */
static bool onlinerefresh_captureserial_txn2disk(serialstate* serialstate, txn* txn)
{
    bool first = true;
    bool txnformetadata =
        true; /* Used to indicate that current transaction only contains metadata */
    ListCell*  lc = NULL;
    ff_txndata txndata = {{0}};
    bool       find_end = false;

    /*
     * Assemble transaction information
     */
    if (NULL == txn->stmts)
    {
        return find_end;
    }

    /* Call formatting interface for formatting processing */
    /* When a transaction only contains metadata, then this transaction does not need to be written
     * to disk */
    foreach (lc, txn->stmts)
    {
        txnstmt* rstmt = (txnstmt*)lfirst(lc);
        rmemset1(&txndata, 0, '\0', sizeof(ff_txndata));
        txndata.data = rstmt;
        rstmt->database = serialstate->database;
        txndata.header.type = FF_DATA_TYPE_TXN;
        txndata.header.transid = txn->xid;

    trfwork_serial_txn2disk_reset:
        if (false == txnformetadata)
        {
            if (1 == list_length(txn->stmts))
            {
                /* Both start and end */
                txndata.header.transind = (FF_DATA_TRANSIND_START | FF_DATA_TRANSIND_IN);
            }
            else
            {
                if (true == first)
                {
                    first = false;
                    txndata.header.transind = FF_DATA_TRANSIND_START;
                }
                else
                {
                    txndata.header.transind = FF_DATA_TRANSIND_IN;
                }
            }
        }
        else
        {
            if (TXNSTMT_TYPE_METADATA == rstmt->type)
            {
                /* metadata flag as start, no commit will be generated later */
                txndata.header.transind = FF_DATA_TRANSIND_START;
            }
            else
            {
                txnformetadata = false;
                goto trfwork_serial_txn2disk_reset;
            }
        }
        serialstate->ffsmgrstate->ffsmgr->ffsmgr_serial(
            FFTRAIL_CXT_TYPE_DATA, (void*)&txndata, serialstate->ffsmgrstate);

        if (rstmt->type == TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END)
        {
            find_end = true;
        }
    }
    return find_end;
}

/* onlinerefresh serialization get buffers */
static file_buffers* onlinerefresh_captureserial_getfilebuffer(void* serial)
{
    onlinerefresh_captureserial* serialstate = NULL;

    if (NULL == serial)
    {
        elog(RLOG_ERROR,
             "onlinerefresh captureserial getfilebuffer exception, serial point is NULL");
    }

    serialstate = (onlinerefresh_captureserial*)serial;

    return serialstate->serialstate->txn2filebuffer;
}

/* Get dbname from system dictionary */
static char* onlinerefresh_captureserial_getdbname(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;

    return transcache_getdbname(oid, (void*)serialstate->dictcache);
}

/* Get dbid from system dictionary */
static Oid onlinerefresh_captureserial_getdboid(void* captureserial)
{
    return misc_controldata_database_get(captureserial);
}

/* Get namespace from system dictionary */
static void* onlinerefresh_captureserial_getnamespace(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    return transcache_getnamespace(oid, (void*)serialstate->dictcache);
}

/* Get class from system dictionary */
static void* onlinerefresh_captureserial_getclass(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    return transcache_getclass(oid, (void*)serialstate->dictcache);
}

/* Get index information by oid, return as linked list */
static void* onlinerefresh_captureserial_getindex(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    void*                        index = NULL;

    serialstate = (onlinerefresh_captureserial*)captureserial;

    index = transcache_getindex(oid, (void*)serialstate->dictcache);

    return index;
}

/* Get attributes from system dictionary */
static void* onlinerefresh_captureserial_getattributes(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    return transcache_getattributes(oid, (void*)serialstate->dictcache);
}

/* Get type from system dictionary */
static void* onlinerefresh_captureserial_gettype(void* captureserial, Oid oid)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    return transcache_gettype(oid, (void*)serialstate->dictcache);
}

/* System dictionary apply */
static void onlinerefresh_captureserial_transcatalog2transcache(void* captureserial, void* catalog)
{
    onlinerefresh_captureserial* serialstate = NULL;
    serialstate = (onlinerefresh_captureserial*)captureserial;
    cache_sysdicts_txnsysdicthisitem2cache(serialstate->dictcache->sysdicts, (ListCell*)catalog);
}

/* Set ffsmgrstate callback function */
static void onlinerefresh_captureserial_setffsmgrcallback(onlinerefresh_captureserial* wstate)
{
    wstate->serialstate->ffsmgrstate->callback.getdboid = onlinerefresh_captureserial_getdboid;
    wstate->serialstate->ffsmgrstate->callback.getdbname = onlinerefresh_captureserial_getdbname;
    wstate->serialstate->ffsmgrstate->callback.getfilebuffer =
        onlinerefresh_captureserial_getfilebuffer;
    wstate->serialstate->ffsmgrstate->callback.getclass = onlinerefresh_captureserial_getclass;
    wstate->serialstate->ffsmgrstate->callback.getindex = onlinerefresh_captureserial_getindex;
    wstate->serialstate->ffsmgrstate->callback.getnamespace =
        onlinerefresh_captureserial_getnamespace;
    wstate->serialstate->ffsmgrstate->callback.getattributes =
        onlinerefresh_captureserial_getattributes;
    wstate->serialstate->ffsmgrstate->callback.gettype = onlinerefresh_captureserial_gettype;
    wstate->serialstate->ffsmgrstate->callback.catalog2transcache =
        onlinerefresh_captureserial_transcatalog2transcache;
    wstate->serialstate->ffsmgrstate->callback.setonlinerefreshdataset = NULL;
    wstate->serialstate->ffsmgrstate->callback.setredosysdicts = NULL;
    wstate->serialstate->ffsmgrstate->callback.setdboid = NULL;
    wstate->serialstate->ffsmgrstate->callback.getrecords = NULL;
    wstate->serialstate->ffsmgrstate->callback.getparserstate = NULL;
    wstate->serialstate->ffsmgrstate->callback.freeattributes = NULL;
}

/* Set fdata content and privdata modification, not set uniformly */
static void onlinerefresh_captureserial_setffsmgr(onlinerefresh_captureserial* serial_task)
{
    serial_task->serialstate->ffsmgrstate->privdata = (void*)serial_task;
    serial_task->serialstate->ffsmgrstate->fdata->ffdata2 = serial_task->dictcache;
}

void* onlinerefresh_captureserial_main(void* args)
{
    /* When getting transactions from cache, additional output parameter */
    bool                         online_end = false;
    int                          timeout = 0;
    txn*                         entry = NULL;
    thrnode*                     thr_node = NULL;
    serialstate*                 serial = NULL;
    onlinerefresh_captureserial* cserial = NULL;

    thr_node = (thrnode*)args;
    cserial = (onlinerefresh_captureserial*)thr_node->data;
    serial = cserial->serialstate;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(
            RLOG_WARNING,
            "onlinerefresh capture serial stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    /* Get database information */
    serial->database = misc_controldata_database_get(NULL);

    /* Load dictionary table */
    onlinerefresh_captureserial_loadsysdictsfromfile(cserial->dictcache);

    /* onlinerefresh set to 0 */
    serialstate_fbuffer_set(serial, 0, 0, 0);

    /* Set ffsmgrstate callback functions */
    onlinerefresh_captureserial_setffsmgrcallback(cserial);

    /* Serialization content settings */
    serialstate_ffsmgr_set(serial, FFSMG_IF_TYPE_TRAIL);

    /* Set fdata content and privdata */
    onlinerefresh_captureserial_setffsmgr(cserial);

    while (1)
    {
        entry = NULL;
        /* First check if exit signal is received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data */
        entry = cache_txn_get(cserial->parser2serialtxns, &timeout);
        if (NULL == entry)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                /* Timeout, check if need to flush buffer to be written to disk */
                if (false == onlinerefresh_captureserial_buffer2waitflush(cserial, NULL, false))
                {
                    elog(RLOG_WARNING, "add buffer 2 wait flush error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
                continue;
            }

            /* Exception during execution, exit */
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* System table apply */
        /* Load first, then apply */
        /* Transaction type is commit */

        /* Write entry data to disk */
        online_end = onlinerefresh_captureserial_txn2disk(serial, entry);

        /* txn memory release */
        txn_free(entry);
        rfree(entry);
        entry = NULL;

        if (false == online_end)
        {
            continue;
        }

        if (false == onlinerefresh_captureserial_buffer2waitflush(cserial, NULL, true))
        {
            elog(RLOG_WARNING, "add buffer 2 wait flush error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        thr_node->stat = THRNODE_STAT_EXIT;
        break;
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_captureserial_free(void* args)
{
    onlinerefresh_captureserial* serial = NULL;

    serial = (onlinerefresh_captureserial*)args;
    if (serial->serialstate)
    {
        serialstate_destroy(serial->serialstate);
    }

    if (NULL != serial->dictcache)
    {
        transcache_free(serial->dictcache);
        rfree(serial->dictcache);
        serial->dictcache = NULL;
    }
    rfree(serial->serialstate);
    rfree(serial);
}
