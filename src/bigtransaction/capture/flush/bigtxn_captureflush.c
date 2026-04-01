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
#include "increment/capture/flush/increment_captureflush.h"
#include "bigtransaction/capture/flush/bigtxn_captureflush.h"
#include "metric/capture/metric_capture.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "strategy/filter_dataset.h"

increment_captureflush* bigtxn_captureflush_init(void)
{
    increment_captureflush* cflush = NULL;

    cflush = increment_captureflush_init();
    if (NULL == cflush)
    {
        elog(RLOG_WARNING, "init big transaction capture flush error");
        return NULL;
    }
    return cflush;
}

static void bigtxn_captureflush_reloadsyncdatasets(increment_captureflush* wstate)
{
    wstate->txnsctx->hsyncdataset = filter_dataset_reload(wstate->txnsctx->sysdicts->by_namespace,
                                                          wstate->txnsctx->sysdicts->by_class,
                                                          wstate->txnsctx->hsyncdataset);
}

static void bigtxn_captureflush_reloadstate(increment_captureflush* wstate, int state)
{
    if (CAPTURERELOAD_STATUS_RELOADING_WRITE == state)
    {
        bigtxn_captureflush_reloadsyncdatasets(wstate);
        g_gotsigreload = CAPTURERELOAD_STATUS_UNSET;
        elog(RLOG_INFO, "castor reload complete!");
    }
    return;
}

/* Initialize file */
static bool bigtxn_captureflush_initfile(increment_captureflush* cflush, bigtxn_captureflush_file* cflushfile)
{
    struct stat st;
    uint8       block[FILE_BUFFER_SIZE] = {0};

    if (-1 != cflushfile->fd)
    {
        osal_file_close(cflushfile->fd);
        cflushfile->fd = -1;
    }

    /* Create directory */
    rmemset1(cflush->path, 0, '\0', MAXPATH);
    snprintf(cflush->path, MAXPATH, "%s/%lu", STORAGE_BIG_TRANSACTION_DIR, cflushfile->xid);
    osal_make_dir(cflush->path);

    /*
     * Open file
     *  If file exists, open directly
     *  If not exists, create new file
     */
    rmemset1(cflush->path, 0, '\0', MAXPATH);
    snprintf(cflush->path, MAXPATH, "%s/%lu/%016lX", STORAGE_BIG_TRANSACTION_DIR, cflushfile->xid, cflushfile->fileid);
    if (0 == stat(cflush->path, &st))
    {
        /* Open file */
        cflushfile->fd = osal_basic_open_file(cflush->path, O_RDWR | BINARY);
        if (cflushfile->fd < 0)
        {
            elog(RLOG_WARNING, "open file %s error %s", cflush->path, strerror(errno));
            return false;
        }
        return true;
    }

    if (false == osal_create_file_with_size(cflush->path,
                                            O_RDWR | O_CREAT | O_EXCL | BINARY,
                                            cflush->maxsize,
                                            FILE_BUFFER_SIZE,
                                            block))
    {
        elog(RLOG_WARNING, "create file error, %s", strerror(errno));
        return false;
    }

    /* Open file */
    cflushfile->fd = osal_basic_open_file(cflush->path, O_RDWR | BINARY);
    if (cflushfile->fd < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", cflush->path, strerror(errno));
        return false;
    }
    return true;
}

/*
 * Write main process
 */
void* bigtxn_captureflush_main(void* args)
{
    bool                      found = false;
    int                       timeout = 0;
    HTAB*                     htxns = NULL;
    thrnode*                  thr_node = NULL;
    ff_fileinfo*              finfo = NULL;
    file_buffer*              fbuffer = NULL;
    increment_captureflush*   cflush = NULL;
    bigtxn_captureflush_file* cflushfile = NULL;
    bigtxn_captureflush_file* cflushlastfile = NULL;
    HASHCTL                   hctl = {'\0'};

    thr_node = (thrnode*)args;
    cflush = (increment_captureflush*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "capture bigtxn flush stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        osal_thread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    /* Load basic information */
    misc_stat_loaddecode((void*)&cflush->base);

    /* Load system catalog */
    cache_sysdictsload((void**)&cflush->txnsctx->sysdicts);

    /* Load addtablepattern */
    cflush->txnsctx->addtablepattern = filter_dataset_initaddtablepattern(cflush->txnsctx->addtablepattern);

    /* Load synchronization dataset */
    cflush->txnsctx->hsyncdataset =
        filter_dataset_load(cflush->txnsctx->sysdicts->by_namespace, cflush->txnsctx->sysdicts->by_class);

    /* Initialize big transaction hash */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(FullTransactionId);
    hctl.entrysize = sizeof(bigtxn_captureflush_file);
    htxns = hash_create("bigtxncflushfile", 1024, &hctl, HASH_ELEM | HASH_BLOBS);

    while (1)
    {
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialization/flush to disk */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /*
         * 1. Get data from cache
         * 2. Write data
         */
        bigtxn_captureflush_reloadstate(cflush, g_gotsigreload);
        fbuffer = file_buffer_waitflush_get(cflush->txn2filebuffer, &timeout);
        if (NULL == fbuffer)
        {
            if (ERROR_TIMEOUT != timeout)
            {
                /* Processing failed, exit */
                elog(RLOG_WARNING, "get file buffer error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }

            /* Timeout without getting data, continue waiting */
            continue;
        }
        finfo = (ff_fileinfo*)fbuffer->privdata;

        if (InvalidFullTransactionId == finfo->xid)
        {
            elog(RLOG_WARNING, "big txn capture flush get InvalidFullTransactionId");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        cflushfile = hash_search(htxns, &finfo->xid, HASH_ENTER, &found);
        if (false == found)
        {
            cflushfile->xid = finfo->xid;
            cflushfile->fd = -1;
            cflushfile->fileid = finfo->fileid;

            if (0 != finfo->fileid)
            {
                elog(RLOG_WARNING,
                     "big txn capture got new big transaction %lu, but fileid not equal zero, %lu",
                     finfo->fileid);
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
        }

        if (NULL == cflushlastfile)
        {
            cflushlastfile = cflushfile;
        }
        else if (cflushfile->xid != cflushlastfile->xid)
        {
            /* Save content */
            cflushlastfile->fileid = cflush->fileid;
            osal_file_close(cflushlastfile->fd);
            cflushlastfile->fd = -1;
            cflushlastfile = cflushfile;
        }

        if (-1 == cflushlastfile->fd)
        {
            /* File not opened, then open file */
            if (false == bigtxn_captureflush_initfile(cflush, cflushlastfile))
            {
                elog(RLOG_WARNING, "big txn capture flush open file error, %s", cflush->path);
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
        }

        /* Reset flush-related content in cflush */
        if (finfo->fileid != cflushlastfile->fileid)
        {
            cflushlastfile->fileid = finfo->fileid;
            /* File not opened, then open file */
            if (false == bigtxn_captureflush_initfile(cflush, cflushlastfile))
            {
                elog(RLOG_WARNING, "big txn capture flush open file error, %s", cflush->path);
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
        }

        cflush->fd = cflushlastfile->fd;
        cflush->fileid = cflushlastfile->fileid;

        if (FILE_BUFFER_FLAG_DATA == (FILE_BUFFER_FLAG_DATA & fbuffer->flag))
        {
            /* Contains data, first flush data to disk */
            if (0 != fbuffer->start)
            {
                /* Write data */
                if (fbuffer->maxsize != osal_file_pwrite(cflush->fd,
                                                         (char*)fbuffer->data,
                                                         fbuffer->maxsize,
                                                         ((finfo->blknum - 1) * fbuffer->maxsize)))
                {
                    elog(RLOG_WARNING,
                         "big txn capture flush flush error, xid:%lu, fileid:%lu, %s",
                         cflushlastfile->xid,
                         cflushlastfile->fileid,
                         strerror(errno));
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
                osal_file_data_sync(cflush->fd);
            }
        }

        if (FILE_BUFFER_FLAG_BIGTXNEND == (FILE_BUFFER_FLAG_BIGTXNEND & fbuffer->flag))
        {
            /* Big transaction end */
            hash_search(htxns, &finfo->xid, HASH_REMOVE, NULL);
        }

        /* Put into free queue */
        file_buffer_free(cflush->txn2filebuffer, fbuffer);
    }

    hash_destroy(htxns);
    osal_thread_exit(NULL);
    return NULL;
}

/* Resource cleanup */
void bigtxn_captureflush_destroy(increment_captureflush* wstate)
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
