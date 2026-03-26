/*
 * Write cached data to file
 */
#include "app_incl.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/regex/regex.h"
#include "threads/threads.h"
#include "misc/misc_control.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "net/netpacket/netpacket.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "strategy/filter_dataset.h"
#include "metric/capture/metric_capture.h"
#include "increment/capture/flush/increment_captureflush.h"

static void increment_captureflush_reloadsyncdatasets(increment_captureflush* wstate);

static void writework_capture_reloadstate(increment_captureflush* wstate, int state)
{
    if (CAPTURERELOAD_STATUS_RELOADING_WRITE == state)
    {
        increment_captureflush_reloadsyncdatasets(wstate);
        g_gotsigreload = CAPTURERELOAD_STATUS_UNSET;
        elog(RLOG_INFO, "Ripple reload complete!");
    }
    return;
}

/* Initialize file */
static void increment_captureflush_initfile(increment_captureflush* wstate, ff_fileinfo* finfo)
{
    int         fd = -1;
    int         index = 0;
    int         blockcnt = 0;
    struct stat st;
    char        tmppath[MAXPATH] = {0};
    uint8       block[FILE_BUFFER_SIZE] = {0};
    if (-1 != wstate->fd)
    {
        close(wstate->fd);
        wstate->fd = -1;
    }

    /* Generate path */
    rmemset1(wstate->path, 0, '\0', MAXPATH);
    snprintf(wstate->path, MAXPATH, STORAGE_TRAIL_DIR "/%016lX", finfo->fileid);

    /* Check if file exists, open if exists */
    if (0 == stat(wstate->path, &st))
    {
        /* Open file */
        wstate->fd = osal_basic_open_file(wstate->path, O_RDWR | BINARY);
        if (wstate->fd < 0)
        {
            elog(RLOG_ERROR, "open file %s error %s", wstate->path, strerror(errno));
        }
        return;
    }

    /* Check if error is file not exists */
    if (ENOENT != errno)
    {
        elog(RLOG_ERROR, "stat %s error, %s", wstate->path, strerror(errno));
    }

    /* Create temp file */
    snprintf(tmppath, MAXPATH, STORAGE_TRAIL_DIR "/%016lX.tmp", finfo->fileid);
    unlink(tmppath);

    fd = osal_basic_open_file(tmppath, O_RDWR | O_CREAT | O_EXCL | BINARY);
    if (0 > fd)
    {
        elog(RLOG_ERROR, "open file %s error:%s", tmppath, strerror(errno));
    }
    blockcnt = (wstate->maxsize / FILE_BUFFER_SIZE);

    for (index = 0; index < blockcnt; index++)
    {
        if (write(fd, block, FILE_BUFFER_SIZE) != FILE_BUFFER_SIZE)
        {
            /* if write didn't set errno, assume no disk space */
            if (errno == ENOSPC)
            {
                elog(RLOG_WARNING, "The disk is full and there is no available space");
                sleep(10);
                index--;
                continue;
            }
            unlink(tmppath);
            osal_file_close(fd);
            elog(RLOG_ERROR, "can not write file %s, errno:%s", tmppath, strerror(errno));
        }
    }

    osal_file_sync(fd);

    osal_file_close(fd);

    /* Rename file */
    osal_durable_rename(tmppath, wstate->path, RLOG_DEBUG);

    /* Open file */
    wstate->fd = osal_basic_open_file(wstate->path, O_RDWR | BINARY);
    if (wstate->fd < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", wstate->path, strerror(errno));
    }
    return;
}

/*
 * Write main process
 */
void* increment_captureflush_main(void* args)
{
    int                     timeout = 0;
    ff_fileinfo*            finfo = NULL;
    file_buffer*            fbuffer = NULL;
    thrnode*                thr_node = NULL;
    increment_captureflush* cflush = NULL;

    thr_node = (thrnode*)args;
    cflush = (increment_captureflush*)thr_node->data;

    /* Check state */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "capture flush stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to work state */
    thr_node->stat = THRNODE_STAT_WORK;

    /* Load basic info */
    misc_stat_loaddecode((void*)&cflush->base);

    /* Load system catalog */
    cache_sysdictsload((void**)&cflush->txnsctx->sysdicts);

    /* Load addtablepattern */
    cflush->txnsctx->addtablepattern =
        filter_dataset_initaddtablepattern(cflush->txnsctx->addtablepattern);

    /* Load sync dataset */
    cflush->txnsctx->hsyncdataset = filter_dataset_load(cflush->txnsctx->sysdicts->by_namespace,
                                                        cflush->txnsctx->sysdicts->by_class);

    while (1)
    {
        /*
         * 1. Get data from cache
         * 2. Write data
         */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
        writework_capture_reloadstate(cflush, g_gotsigreload);

        fbuffer = file_buffer_waitflush_get(cflush->txn2filebuffer, &timeout);
        if (NULL == fbuffer)
        {
            if (ERROR_TIMEOUT != timeout)
            {
                /* Handle failure, exit */
                elog(RLOG_WARNING, "get file buffer error");
                break;
            }

            /* Timeout without obtaining data, continue waiting to obtain */
            continue;
        }

        /* Flush data to disk */
        if (FILE_BUFFER_FLAG_DATA == (FILE_BUFFER_FLAG_DATA & fbuffer->flag) && 0 != fbuffer->start)
        {
            /* Has data content, then flush to disk */
            finfo = (ff_fileinfo*)fbuffer->privdata;
            if (cflush->fileid != finfo->fileid || -1 == cflush->fd)
            {
                increment_captureflush_initfile(cflush, finfo);
                cflush->fileid = finfo->fileid;
            }

            /* Write data */
            osal_file_pwrite(cflush->fd,
                             (char*)fbuffer->data,
                             fbuffer->maxsize,
                             ((finfo->blknum - 1) * FILE_BUFFER_SIZE));
            osal_file_data_sync(cflush->fd);

            cflush->base.fileid = finfo->fileid;
            cflush->base.fileoffset = ((finfo->blknum - 1) * FILE_BUFFER_SIZE) + fbuffer->start;

            elog(RLOG_DEBUG,
                 "---------fileid:%lu:%lu",
                 cflush->base.fileid,
                 cflush->base.fileoffset);
            /* Set status thread, current parsed and persisted lsn */
            cflush->callback.setmetricflushlsn(cflush->privdata,
                                               fbuffer->extra.rewind.flushlsn.wal.lsn);
            cflush->callback.setmetrictrailno(cflush->privdata, cflush->base.fileid);
            cflush->callback.setmetrictrailstart(cflush->privdata, cflush->base.fileoffset);
            cflush->callback.setmetricflushtimestamp(cflush->privdata, fbuffer->extra.timestamp);
        }

        /* Update restartlsn and fileid and offset */
        if (FILE_BUFFER_FLAG_REWIND == (FILE_BUFFER_FLAG_REWIND & fbuffer->flag))
        {
            /* Write base file */
            cflush->base.confirmedlsn = fbuffer->extra.rewind.confirmlsn.wal.lsn;
            cflush->base.restartlsn = fbuffer->extra.rewind.restartlsn.wal.lsn;
            cflush->base.fileid = finfo->fileid;
            cflush->base.fileoffset = ((finfo->blknum - 1) * FILE_BUFFER_SIZE) + fbuffer->start;
            /* Update redolsn when valid */
            if (InvalidXLogRecPtr != fbuffer->extra.chkpoint.redolsn.wal.lsn)
            {
                cflush->base.redolsn = fbuffer->extra.chkpoint.redolsn.wal.lsn;
            }
            cflush->base.curtlid = fbuffer->extra.rewind.curtlid;

            elog(RLOG_DEBUG,
                 "************fileid:%lu:%lu",
                 cflush->base.fileid,
                 cflush->base.fileoffset);
            /* Flush data to disk, no lock needed */
            misc_stat_decodewrite(&cflush->base, &cflush->basefd);
        }

        /* Check if contains redolsn, if contains redolsn then write data */
        if (FILE_BUFFER_FLAG_REDO == (FILE_BUFFER_FLAG_REDO & fbuffer->flag))
        {
            /* Apply system catalog */
            if (NULL != fbuffer->extra.chkpoint.sysdicts)
            {
                cache_sysdicts_txnsysdicthis2cache(cflush->txnsctx->sysdicts,
                                                   fbuffer->extra.chkpoint.sysdicts);

                /* Update sync dataset */
                if (filter_dataset_updatedatasets(cflush->txnsctx->addtablepattern,
                                                  cflush->txnsctx->sysdicts->by_namespace,
                                                  fbuffer->extra.chkpoint.sysdicts,
                                                  cflush->txnsctx->hsyncdataset))
                {
                    filter_dataset_flush(cflush->txnsctx->hsyncdataset);
                }

                /* Free memory */
                cache_sysdicts_txnsysdicthisfree(fbuffer->extra.chkpoint.sysdicts);
                list_free(fbuffer->extra.chkpoint.sysdicts);
                fbuffer->extra.chkpoint.sysdicts = NULL;
                sysdictscache_write(cflush->txnsctx->sysdicts, cflush->base.redolsn);
            }
        }

        /* Exists dataset flag, force flush after update */
        if (FILE_BUFFER_FLAG_ONLINREFRESH_DATASET ==
            (FILE_BUFFER_FLAG_ONLINREFRESH_DATASET & fbuffer->flag))
        {
            if (fbuffer->extra.dataset.dataset)
            {
                ListCell* cell = NULL;
                filter_dataset_updatedatasets_onlinerefresh(cflush->txnsctx->hsyncdataset,
                                                            fbuffer->extra.dataset.dataset);

                foreach (cell, fbuffer->extra.dataset.dataset)
                {
                    refresh_table* table = (refresh_table*)lfirst(cell);
                    if (table->schema)
                    {
                        rfree(table->schema);
                    }
                    if (table->table)
                    {
                        rfree(table->table);
                    }
                }
                list_free_deep(fbuffer->extra.dataset.dataset);
                fbuffer->extra.dataset.dataset = NULL;
                filter_dataset_flush(cflush->txnsctx->hsyncdataset);
            }
        }

        /* Put into free queue */
        file_buffer_free(cflush->txn2filebuffer, fbuffer);
    }

    pthread_exit(NULL);
    return NULL;
}

increment_captureflush* increment_captureflush_init(void)
{
    increment_captureflush* writestate = NULL;
    writestate = (increment_captureflush*)rmalloc1(sizeof(increment_captureflush));
    if (NULL == writestate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(writestate, 0, '\0', sizeof(increment_captureflush));

    writestate->fd = -1;
    writestate->basefd = -1;
    writestate->fileid = -1;

    writestate->maxsize = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    writestate->maxsize = (writestate->maxsize * 1024 * 1024);

    writestate->txnsctx = (txnscontext*)rmalloc0(sizeof(txnscontext));
    if (NULL == writestate->txnsctx)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(writestate->txnsctx, 0, '\0', sizeof(txnscontext));

    return writestate;
}

/* Resource reclaim */
void increment_captureflush_destroy(increment_captureflush* wstate)
{
    if (NULL == wstate)
    {
        return;
    }

    if (NULL != wstate->txnsctx)
    {
        transcache_free(wstate->txnsctx);
        rfree(wstate->txnsctx);
        wstate->txnsctx = NULL;
    }

    wstate->privdata = NULL;
    wstate->txn2filebuffer = NULL;

    rfree(wstate);
    wstate = NULL;
}

static void increment_captureflush_reloadsyncdatasets(increment_captureflush* wstate)
{
    wstate->txnsctx->hsyncdataset = filter_dataset_reload(wstate->txnsctx->sysdicts->by_namespace,
                                                          wstate->txnsctx->sysdicts->by_class,
                                                          wstate->txnsctx->hsyncdataset);
}
