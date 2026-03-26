#include "app_incl.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "threads/threads.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "onlinerefresh/capture/flush/onlinerefresh_captureflush.h"

/* Initialize file */
static void onlinerefresh_captureflush_initfile(onlinerefresh_captureflush* cflush,
                                                ff_fileinfo*                finfo)
{
    int         fd = -1;
    int         index = 0;
    int         blockcnt = 0;
    struct stat st;
    char        path[MAXPATH] = {0};
    char        tmppath[MAXPATH] = {0};
    uint8       block[FILE_BUFFER_SIZE] = {0};

    /* Close already opened descriptor */
    if (-1 != cflush->fd)
    {
        close(cflush->fd);
        cflush->fd = -1;
    }

    /* Generate path */
    snprintf(path, MAXPATH, "%s/%016lX", cflush->trail, finfo->fileid);

    /* Check if file exists, if exists then open */
    if (0 == stat(path, &st))
    {
        /* Open file */
        cflush->fd = osal_basic_open_file(path, O_RDWR | BINARY);
        if (cflush->fd < 0)
        {
            elog(RLOG_ERROR, "open file %s error %s", path, strerror(errno));
        }
        return;
    }

    /* Check if error is file not exists */
    if (ENOENT != errno)
    {
        elog(RLOG_ERROR, "stat %s error, %s", path, strerror(errno));
    }

    /* Create temporary file */
    snprintf(tmppath, MAXPATH, "%s/%016lX.tmp", cflush->trail, finfo->fileid);
    unlink(tmppath);

    fd = osal_basic_open_file(tmppath, O_RDWR | O_CREAT | O_EXCL | BINARY);
    if (0 > fd)
    {
        elog(RLOG_ERROR, "open file %s error:%s", tmppath, strerror(errno));
    }
    blockcnt = (cflush->maxsize / FILE_BUFFER_SIZE);

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
    osal_durable_rename(tmppath, path, RLOG_DEBUG);

    /* Open file */
    cflush->fd = osal_basic_open_file(path, O_RDWR | BINARY);
    if (cflush->fd < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", path, strerror(errno));
    }
    return;
}

onlinerefresh_captureflush* onlinerefresh_captureflush_init(void)
{
    onlinerefresh_captureflush* result = NULL;

    result = rmalloc0(sizeof(onlinerefresh_captureflush));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    result = rmemset0(result, 0, 0, sizeof(onlinerefresh_captureflush));
    result->fd = -1;
    result->maxsize = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    result->maxsize = (result->maxsize * 1024 * 1024);
    return result;
}

void* onlinerefresh_captureflush_main(void* args)
{
    int                         timeout = 0;
    thrnode*                    thr_node = NULL;
    ff_fileinfo*                finfo = NULL;
    file_buffer*                fbuffer = NULL;
    onlinerefresh_captureflush* cflush = NULL;

    thr_node = (thrnode*)args;
    cflush = (onlinerefresh_captureflush*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "onlinerefresh capture flush stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (1)
    {
        /* First check if exit signal is received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /*
         * 1、Get data from cache
         * 2、Write data
         */
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

            /* Timeout without getting data, continue waiting to get */
            continue;
        }

        /* Write data to disk */
        if (FILE_BUFFER_FLAG_DATA == (FILE_BUFFER_FLAG_DATA & fbuffer->flag) && 0 != fbuffer->start)
        {
            /* Contains data content, write data to disk */
            finfo = (ff_fileinfo*)fbuffer->privdata;

            if (cflush->trailno != finfo->fileid || -1 == cflush->fd)
            {
                onlinerefresh_captureflush_initfile(cflush, finfo);
                cflush->trailno = finfo->fileid;
            }

            /* Write data */
            osal_file_pwrite(cflush->fd,
                             (char*)fbuffer->data,
                             fbuffer->maxsize,
                             ((finfo->blknum - 1) * FILE_BUFFER_SIZE));
            osal_file_data_sync(cflush->fd);
        }

        if (FILE_BUFFER_FLAG_ONLINREFRESHEND == (FILE_BUFFER_FLAG_ONLINREFRESHEND & fbuffer->flag))
        {
            /* Put into idle queue */
            file_buffer_free(cflush->txn2filebuffer, fbuffer);
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
        /* Put into idle queue */
        file_buffer_free(cflush->txn2filebuffer, fbuffer);
    }
    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_captureflush_free(void* args)
{
    onlinerefresh_captureflush* cflush = NULL;
    cflush = (onlinerefresh_captureflush*)args;

    if (NULL == cflush)
    {
        return;
    }

    if (cflush->trail)
    {
        rfree(cflush->trail);
    }
    rfree(cflush);
}
