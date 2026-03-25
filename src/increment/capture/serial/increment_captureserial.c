#include "app_incl.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/threads.h"
#include "misc/misc_control.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "catalog/control.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "cache/fpwcache.h"
#include "catalog/catalog.h"
#include "stmts/txnstmt.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "storage/trail/data/fftrail_data.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "serial/serial.h"
#include "increment/capture/serial/increment_captureserial.h"

/* Apply sysdict this system dictionary */
static void increment_captureserial_sysdicthis2sysdict(increment_captureserialstate* cserial,
                                                       List*                         his)
{
    if (NULL == his)
    {
        return;
    }

    /* Simply wrap a layer, internally mainly uses sysdict and relfilenode, and relfilenode has null
     * check */
    cache_sysdicts_txnsysdicthis2cache(cserial->dictcache->sysdicts, his);
}

/* Set timestamp */
static void increment_captureserial_settimestamp(serialstate* serialstate, txn* txn)
{
    file_buffer* fbuffer = NULL;

    if (NULL == txn || NULL == serialstate)
    {
        return;
    }

    fbuffer = file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);

    fbuffer->extra.timestamp = txn->endtimestamp;

    return;
}

/* Flush entry data to disk */
static void increment_captureserial_txn2disk(serialstate* serialstate, txn* txn)
{
    bool first = true;
    bool txnformetadata = true; /* Used to identify if current transaction only contains metadata */
    ListCell*  lc = NULL;
    ff_txndata txndata = {{0}};

    /*
     * Assemble transaction info
     */
    if (NULL == txn->stmts)
    {
        return;
    }

    /* Call format interface for formatting */
    /* When a transaction only contains metadata, then this transaction does not need to flush */
    foreach (lc, txn->stmts)
    {
        txnstmt* rstmt = (txnstmt*)lfirst(lc);

        if (TXNSTMT_TYPE_SYSDICTHIS == rstmt->type)
        {
            increment_captureserial_sysdicthis2sysdict((increment_captureserialstate*)serialstate,
                                                       txn->sysdictHis);
            continue;
        }

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
                /* metadata marked as start, later will not generate commit */
                txndata.header.transind = FF_DATA_TRANSIND_START;
            }
            else
            {
                txnformetadata = false;
                goto trfwork_serial_txn2disk_reset;
            }
        }
        serialstate->ffsmgrstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_DATA, (void*)&txndata,
                                                        serialstate->ffsmgrstate);
    }
    increment_captureserial_settimestamp(serialstate, txn);
}

/* Set serial parse node info */
static void increment_captureserial_lsn_set(increment_captureserialstate* captureserialstate,
                                            XLogRecPtr redolsn, XLogRecPtr restartlsn,
                                            XLogRecPtr confirmlsn)
{
    captureserialstate->redolsn = redolsn;
    captureserialstate->restartlsn = restartlsn;
    captureserialstate->confirmlsn = confirmlsn;
}

/* Set lsn and timeline */
static void increment_captureserial_fbuffer_lsnset(increment_captureserialstate* captureserialstate)
{
    file_buffer* fbuffer = NULL;

    fbuffer = file_buffer_getbybufid(captureserialstate->base.txn2filebuffer,
                                     captureserialstate->base.ffsmgrstate->bufid);

    fbuffer->extra.chkpoint.redolsn.wal.lsn = captureserialstate->redolsn;
    fbuffer->extra.rewind.restartlsn.wal.lsn = captureserialstate->restartlsn;
    fbuffer->extra.rewind.confirmlsn.wal.lsn = captureserialstate->confirmlsn;
    fbuffer->extra.rewind.flushlsn.wal.lsn = captureserialstate->confirmlsn;
    fbuffer->extra.rewind.curtlid = captureserialstate->curtlid;
}

/* Set timeline info */
static void increment_captureserial_timeline_set(increment_captureserialstate* captureserialstate,
                                                 TimeLineID                    curtlid)
{
    captureserialstate->curtlid = curtlid;
}

/*
 * Put buffer into flush, need to set buffer flag before putting
 */
static void increment_captureserial_buffer2waitflush(
    increment_captureserialstate* captureserialstate, txn* txn)
{
    /*
     * 1. Get new buffer cache
     * 2. Set new cache flag info
     * 3. Set old cache flag info based on lsn info from wstate
     */
    int  oldflag = 0;
    int  bufid = 0;
    bool flush = false;
    int  timeout = 0;

    ff_fileinfo* finfo = NULL;
    file_buffer* fbuffer = NULL;
    file_buffer* foldbuffer = NULL;
    serialstate* serial_state = NULL;

    serial_state = (serialstate*)captureserialstate;

    foldbuffer =
        file_buffer_getbybufid(serial_state->txn2filebuffer, serial_state->ffsmgrstate->bufid);
    if (0 == foldbuffer->start)
    {
        return;
    }

    oldflag = foldbuffer->flag;
    if (NULL != txn)
    {
        if (TXN_TYPE_TIMELINE == txn->type)
        {
            captureserialstate->curtlid = txn->curtlid;
            foldbuffer->flag |= FILE_BUFFER_FLAG_REWIND;
        }

        if (txn->restart.wal.lsn > captureserialstate->restartlsn)
        {
            captureserialstate->restartlsn = txn->restart.wal.lsn;
            foldbuffer->flag |= FILE_BUFFER_FLAG_REWIND;
            foldbuffer->extra.rewind.restartlsn.wal.lsn = captureserialstate->restartlsn;
        }

        if (txn->confirm.wal.lsn > captureserialstate->confirmlsn)
        {
            captureserialstate->confirmlsn = txn->confirm.wal.lsn;
            foldbuffer->flag |= FILE_BUFFER_FLAG_REWIND;
            foldbuffer->extra.rewind.confirmlsn.wal.lsn = captureserialstate->confirmlsn;
            if (NULL != txn->stmts)
            {
                foldbuffer->extra.rewind.flushlsn.wal.lsn = captureserialstate->confirmlsn;
            }
        }

        if (txn->redo.wal.lsn > captureserialstate->redolsn)
        {
            captureserialstate->redolsn = txn->redo.wal.lsn;
            foldbuffer->flag |= FILE_BUFFER_FLAG_REDO;
            foldbuffer->extra.chkpoint.redolsn.wal.lsn = captureserialstate->redolsn;
            /* System catalog changes between two checkpoints */
            foldbuffer->extra.chkpoint.sysdicts = catalog_sysdict_filterbylsn(
                &captureserialstate->redosysdicts, captureserialstate->redolsn);
            flush = true;
        }

        if (captureserialstate->onlinerefreshdataset)
        {
            foldbuffer->flag |= FILE_BUFFER_FLAG_ONLINREFRESH_DATASET;
            foldbuffer->extra.dataset.dataset = captureserialstate->onlinerefreshdataset;
            captureserialstate->onlinerefreshdataset = NULL;
            flush = true;
        }
    }
    else
    {
        /* Set oldbuffer info */
        foldbuffer->flag |= FILE_BUFFER_FLAG_REWIND;
        foldbuffer->extra.rewind.restartlsn.wal.lsn = captureserialstate->restartlsn;
        foldbuffer->extra.rewind.confirmlsn.wal.lsn = captureserialstate->confirmlsn;
        flush = true;
    }

    if (flush)
    {
        /* Get new buffer cache */
        while (1)
        {
            bufid = file_buffer_get(serial_state->txn2filebuffer, &timeout);
            if (INVALID_BUFFERID == bufid)
            {
                if (ERROR_TIMEOUT == timeout)
                {
                    usleep(10000);
                    continue;
                }
                elog(RLOG_WARNING, "get file buffer error");
                return;
            }
            break;
        }

        fbuffer = file_buffer_getbybufid(serial_state->txn2filebuffer, bufid);
        if (NULL == fbuffer->privdata)
        {
            finfo = (ff_fileinfo*)rmalloc0(sizeof(ff_fileinfo));
            if (NULL == finfo)
            {
                elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
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

        /* Set new buffer other info */
        rmemcpy0(finfo, 0, (ff_fileinfo*)foldbuffer->privdata, sizeof(ff_fileinfo));

        /* Set oldbuffer info */
        foldbuffer->extra.rewind.curtlid = captureserialstate->curtlid;

        /* Put oldbuffer into wait flush cache */
        rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(file_buffer_extra));
        file_buffer_waitflush_add(serial_state->txn2filebuffer, foldbuffer);

        /* Reset fbuffer content */
        fbuffer->flag = oldflag;
        fbuffer->extra.chkpoint.sysdicts = NULL;
        fbuffer->extra.dataset.dataset = NULL;
        serial_state->ffsmgrstate->bufid = bufid;
    }

    return;
}

/* Serialize fault recovery */
static bool increment_captureserial_recovery(increment_captureserialstate* captureserialstate,
                                             uint64                        fileoffset)
{
    bool   shiftfile = false;
    int    bufid = 0;
    int    maxbufid = 0;
    int    mbytes = 0;
    int    minsize = 0;
    int    timeout = 0;
    uint64 bytes = 0;
    uint64 freespc = 0;

    file_buffer* in_fbuffer = NULL;
    file_buffer* fbuffer = NULL;
    ff_fileinfo* finfo = NULL;
    ff_fileinfo* nfinfo = NULL;
    serialstate* serial_state = NULL;
    ff_tail      fftail = {0};

    if (NULL == captureserialstate)
    {
        return false;
    }

    serial_state = (serialstate*)captureserialstate;
    in_fbuffer =
        file_buffer_getbybufid(serial_state->txn2filebuffer, serial_state->ffsmgrstate->bufid);
    if (0 == fileoffset)
    {
        return false;
    }

    /* Calculate maxbufid */
    mbytes = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    bytes = MB2BYTE(mbytes);
    maxbufid = (bytes / FILE_BUFFER_SIZE);

    /* Need to consider insufficient space in block and file switching */
    finfo = (ff_fileinfo*)in_fbuffer->privdata;

    /* Get minsize */
    minsize = fftrail_data_tokenminsize(serial_state->ffsmgrstate->compatibility);
    if (maxbufid == finfo->blknum)
    {
        /* Append content after */
        minsize += fftrail_taillen(serial_state->ffsmgrstate->compatibility);
        shiftfile = true;
    }

    /* Check remaining space */
    freespc = (in_fbuffer->maxsize - in_fbuffer->start);

    elog(RLOG_INFO, "minsize:%u, freespc:%u, fileoffset:%lu", minsize, freespc, fileoffset);
    /* Compare if remaining space meets minimum requirement for data */
    if (minsize >= freespc)
    {
        if (false == shiftfile)
        {
            finfo->blknum++;
        }
        else
        {
            finfo->blknum = 1;
            finfo->fileid++;
        }

        in_fbuffer->start = 0;
        rmemset0(in_fbuffer->data, 0, '\0', in_fbuffer->maxsize);
    }

    fftail.nexttrailno = (finfo->fileid + 1);
    serial_state->ffsmgrstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_RESET, &fftail,
                                                     (void*)serial_state->ffsmgrstate);

    /* Re-fetch fbuffer */
    while (1)
    {
        bufid = file_buffer_get(serial_state->txn2filebuffer, &timeout);
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
    fbuffer = file_buffer_getbybufid(serial_state->txn2filebuffer, bufid);

    if (NULL != fbuffer->privdata)
    {
        nfinfo = (ff_fileinfo*)fbuffer->privdata;
    }
    else
    {
        nfinfo = (ff_fileinfo*)rmalloc0(sizeof(ff_fileinfo));
        if (NULL == nfinfo)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(nfinfo, 0, '\0', sizeof(ff_fileinfo));
        fbuffer->privdata = (void*)nfinfo;
    }
    /* Set new buffer other info */
    rmemcpy0(nfinfo, 0, finfo, sizeof(ff_fileinfo));
    nfinfo->fileid = finfo->fileid;
    nfinfo->fileid++;
    nfinfo->blknum = 1;

    /* Initialize header info */
    serial_state->ffsmgrstate->bufid = bufid;
    serial_state->ffsmgrstate->ffsmgr->ffsmgr_init(FFSMGR_IF_OPTYPE_SERIAL,
                                                   serial_state->ffsmgrstate);

    /*Stable after delete*/
    // serial_state->ffsmgrstate->fdata->extradata = (void*)captureserialstate->dictcache;

    /* Put buffer into wait flush cache */
    rmemcpy1(&fbuffer->extra, 0, &in_fbuffer->extra, sizeof(file_buffer_extra));
    file_buffer_waitflush_add(serial_state->txn2filebuffer, in_fbuffer);

    /* After switching file, add newly generated buffer to cache, used by write to update base file
     * with new file position info */
    increment_captureserial_buffer2waitflush(captureserialstate, NULL);

    return true;
}

/* Set ffsmgrstate privdata and fdata */
static void increment_captureserialstate_ffsmgr_set(increment_captureserialstate* serialstate)
{
    serialstate->base.ffsmgrstate->privdata = (void*)serialstate;
    serialstate->base.ffsmgrstate->fdata->ffdata2 = serialstate->dictcache;
}

/* Set redo saved system dictionary */
static void increment_captureserial_setredosysdicts(void* serial, void* catalogdata)
{
    increment_captureserialstate* captureserialstate = NULL;

    if (NULL == serial)
    {
        elog(RLOG_ERROR, "serialwork setredosysdicts exception, privdata point is NULL");
    }

    if (NULL == catalogdata)
    {
        elog(RLOG_ERROR, "serialwork setredosysdicts exception, catalogdata point is NULL");
    }

    captureserialstate = (increment_captureserialstate*)serial;
    captureserialstate->redosysdicts = lappend(captureserialstate->redosysdicts, catalogdata);

    return;
}

static void increment_captureserial_setonlinerefreshdataset(void* serial, void* dataset)
{
    increment_captureserialstate* captureserialstate = NULL;
    List*                         dataset_list = NULL;

    if (NULL == serial)
    {
        elog(RLOG_ERROR, "serialwork setredosysdicts exception, privdata point is NULL");
    }

    if (NULL == dataset)
    {
        elog(RLOG_ERROR, "serialwork setredosysdicts exception, catalogdata point is NULL");
    }

    dataset_list = (List*)dataset;
    captureserialstate = (increment_captureserialstate*)serial;

    /* If exists, merge list */
    if (captureserialstate->onlinerefreshdataset)
    {
        ListCell* cell = NULL;
        foreach (cell, dataset_list)
        {
            void* node = lfirst(cell);

            captureserialstate->onlinerefreshdataset =
                lappend(captureserialstate->onlinerefreshdataset, node);
        }
        list_free(dataset_list);
    }
    else
    {
        captureserialstate->onlinerefreshdataset = dataset_list;
    }

    return;
}

/* Get dbname from system dictionary */
char* increment_captureserial_getdbname(void* captureserial, Oid oid)
{
    increment_captureserialstate* serialstate = NULL;
    serialstate = (increment_captureserialstate*)captureserial;

    return transcache_getdbname(oid, (void*)serialstate->dictcache);
}

/* Get dboid from system dictionary */
Oid increment_captureserial_getdboid(void* captureserial)
{
    return misc_controldata_database_get(captureserial);
}

/* Get namespace from system dictionary */
void* increment_captureserial_getnamespace(void* captureserial, Oid oid)
{
    increment_captureserialstate* serialstate = NULL;
    serialstate = (increment_captureserialstate*)captureserial;
    return transcache_getnamespace(oid, (void*)serialstate->dictcache);
}

/* Get class from system dictionary */
void* increment_captureserial_getclass(void* captureserial, Oid oid)
{
    increment_captureserialstate* serialstate = NULL;
    serialstate = (increment_captureserialstate*)captureserial;
    return transcache_getclass(oid, (void*)serialstate->dictcache);
}

/* Get index info by oid, return as linked list */
static void* increment_captureserial_getindex(void* captureserial, Oid oid)
{
    increment_captureserialstate* serialstate = NULL;
    void*                         index = NULL;

    serialstate = (increment_captureserialstate*)captureserial;

    index = transcache_getindex(oid, (void*)serialstate->dictcache);

    return index;
}

/* Get attribute from system dictionary */
void* increment_captureserial_getattributes(void* captureserial, Oid oid)
{
    increment_captureserialstate* serialstate = NULL;
    serialstate = (increment_captureserialstate*)captureserial;
    return transcache_getattributes(oid, (void*)serialstate->dictcache);
}

/* Get type from system dictionary */
void* increment_captureserial_gettype(void* captureserial, Oid oid)
{
    increment_captureserialstate* serialstate = NULL;
    serialstate = (increment_captureserialstate*)captureserial;
    return transcache_gettype(oid, (void*)serialstate->dictcache);
}

/* System catalog apply */
void increment_captureserial_transcatalog2transcache(void* captureserial, void* catalog)
{
    increment_captureserialstate* serialstate = NULL;
    serialstate = (increment_captureserialstate*)captureserial;
    cache_sysdicts_txnsysdicthisitem2cache(serialstate->dictcache->sysdicts, (ListCell*)catalog);
}

/* Set ffsmgr callback function */
void increment_captureserial_ffsmgr_setcallback(increment_captureserialstate* wstate)
{
    wstate->base.ffsmgrstate->callback.getdboid = increment_captureserial_getdboid;
    wstate->base.ffsmgrstate->callback.getdbname = increment_captureserial_getdbname;
    wstate->base.ffsmgrstate->callback.getfilebuffer = serialstate_getfilebuffer;
    wstate->base.ffsmgrstate->callback.getclass = increment_captureserial_getclass;
    wstate->base.ffsmgrstate->callback.getindex = increment_captureserial_getindex;
    wstate->base.ffsmgrstate->callback.getnamespace = increment_captureserial_getnamespace;
    wstate->base.ffsmgrstate->callback.getattributes = increment_captureserial_getattributes;
    wstate->base.ffsmgrstate->callback.gettype = increment_captureserial_gettype;
    wstate->base.ffsmgrstate->callback.setredosysdicts = increment_captureserial_setredosysdicts;
    wstate->base.ffsmgrstate->callback.catalog2transcache =
        increment_captureserial_transcatalog2transcache;
    wstate->base.ffsmgrstate->callback.setonlinerefreshdataset =
        increment_captureserial_setonlinerefreshdataset;
    wstate->base.ffsmgrstate->callback.setdboid = NULL;
    wstate->base.ffsmgrstate->callback.getrecords = NULL;
    wstate->base.ffsmgrstate->callback.getparserstate = NULL;
    wstate->base.ffsmgrstate->callback.freeattributes = NULL;
}

/* Initialize capture_serialstate */
increment_captureserialstate* increment_captureserial_init(void)
{
    increment_captureserialstate* serialstate = NULL;

    serialstate = (increment_captureserialstate*)rmalloc0(sizeof(increment_captureserialstate));
    if (NULL == serialstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(serialstate, 0, '\0', sizeof(increment_captureserialstate));

    serialstate_init(&serialstate->base);

    serialstate->dictcache = (transcache*)rmalloc0(sizeof(transcache));
    if (NULL == serialstate->dictcache)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(serialstate->dictcache, 0, '\0', sizeof(transcache));

    serialstate->redolsn = InvalidXLogRecPtr;
    serialstate->restartlsn = InvalidXLogRecPtr;
    serialstate->confirmlsn = InvalidXLogRecPtr;
    serialstate->redosysdicts = NULL;
    serialstate->state = INCREMENT_CAPTURESERIAL_STATE_NOP;

    return serialstate;
}

static bool capture_serialstate_transcache_setfromfile(transcache* dictcache)
{
    if (NULL == dictcache)
    {
        return false;
    }

    cache_sysdictsload((void**)&dictcache->sysdicts);

    return true;
}

/*
 * Format main process
 */
void* increment_captureserial_main(void* args)
{
    int          iret = 0; /* Additional output parameter when getting transaction from cache */
    txn*         entry = NULL;
    thrnode*     thr_node = NULL;
    serialstate* serial_state = NULL;
    increment_captureserialstate* wstate = NULL;
    capturebase                   dbase = {0};

    thr_node = (thrnode*)args;

    wstate = (increment_captureserialstate*)thr_node->data;
    serial_state = (serialstate*)wstate;

    /* Check state */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "increment capture serial stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to work state */
    thr_node->stat = THRNODE_STAT_WORK;

    /* Get basic info */
    misc_stat_loaddecode(&dbase);

    /* database set */
    wstate->base.database = misc_controldata_database_get(NULL);

    /* Load dictionary table */
    capture_serialstate_transcache_setfromfile(wstate->dictcache);

    /* Set lsn info */
    increment_captureserial_lsn_set(wstate, dbase.redolsn, dbase.restartlsn, dbase.confirmedlsn);

    increment_captureserial_timeline_set(wstate, dbase.curtlid);

    serialstate_fbuffer_set(serial_state, dbase.fileid, dbase.fileoffset, 0);

    increment_captureserial_fbuffer_lsnset(wstate);

    /* Set ffsmgrstate callback function */
    increment_captureserial_ffsmgr_setcallback(wstate);

    /* Serialize content set */
    serialstate_ffsmgr_set(serial_state, FFSMG_IF_TYPE_TRAIL);

    /* Set fdata content and privdata */
    increment_captureserialstate_ffsmgr_set(wstate);

    increment_captureserial_recovery(wstate, dbase.fileoffset);

    while (1)
    {
        entry = NULL;
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialize/flush */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data */
        entry = cache_txn_get(wstate->parser2serialtxns, &iret);
        if (NULL == entry)
        {
            if (ERROR_TIMEOUT == iret)
            {
                /* Timeout, check if need to flush pending write buffer to disk */
                increment_captureserial_buffer2waitflush(wstate, NULL);
                continue;
            }
            /* Need to exit, wait for worknode->status to become WORK_STATUS_TERM then exit*/
            if (THRNODE_STAT_TERM != thr_node->stat)
            {
                /* Sleep 10 ms */
                usleep(10000);
                continue;
            }
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* System catalog apply */
        /* First load, then apply */
        /* Transaction type is commit */

        /* Flush entry data to disk */
        increment_captureserial_txn2disk(serial_state, entry);

        /* Update wstate->lsn info based on txn */
        increment_captureserial_buffer2waitflush(wstate, entry);

        /* txn memory release */
        txn_free(entry);

        rfree(entry);

        entry = NULL;
    }

    pthread_exit(NULL);
    return NULL;
}

/* Resource reclaim */
void increment_captureserial_destroy(increment_captureserialstate* captureserialstate)
{
    if (NULL == captureserialstate)
    {
        return;
    }

    serialstate_destroy((serialstate*)captureserialstate);

    if (NULL != captureserialstate->dictcache)
    {
        transcache_free(captureserialstate->dictcache);
        rfree(captureserialstate->dictcache);
        captureserialstate->dictcache = NULL;
    }

    cache_sysdicts_txnsysdicthisfree(captureserialstate->redosysdicts);
    list_free(captureserialstate->redosysdicts);
    captureserialstate->redosysdicts = NULL;

    captureserialstate->privdata = NULL;
    captureserialstate->parser2serialtxns = NULL;

    rfree(captureserialstate);
    captureserialstate = NULL;
}
