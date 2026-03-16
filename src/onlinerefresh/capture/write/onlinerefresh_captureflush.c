#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "onlinerefresh/capture/flush/ripple_onlinerefresh_captureflush.h"

/* 初始化文件 */
static void ripple_onlinerefresh_captureflush_initfile(ripple_onlinerefresh_captureflush* cflush, ripple_ff_fileinfo* finfo)
{
    int fd = -1;
    int index = 0;
    int blockcnt = 0;
    struct stat st;
    char    path[RIPPLE_MAXPATH] = { 0 };
    char    tmppath[RIPPLE_MAXPATH] = { 0 };
    uint8   block[RIPPLE_FILE_BUFFER_SIZE] = { 0 };

    /* 关闭已经打开的描述符 */
    if(-1 != cflush->fd)
    {
        close(cflush->fd);
        cflush->fd = -1;
    }

    /* 生成路径 */
    snprintf(path, RIPPLE_MAXPATH, "%s/%016lX", cflush->trail, finfo->fileid);

    /* 校验文件是否存在，存在则打开 */
    if(0 == stat(path, &st))
    {
        /* 打开文件 */
        cflush->fd = BasicOpenFile(path, O_RDWR | RIPPLE_BINARY);
        if (cflush->fd  < 0)
        {
            elog(RLOG_ERROR, "open file %s error %s", path, strerror(errno));
        }
        return;
    }

    /* 查看错误是否为文件不存在 */
    if(ENOENT != errno)
    {
        elog(RLOG_ERROR, "stat %s error, %s", path, strerror(errno));
    }

    /* 创建临时文件 */
    snprintf(tmppath, RIPPLE_MAXPATH, "%s/%016lX.tmp", cflush->trail, finfo->fileid);
    unlink(tmppath);

    fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | RIPPLE_BINARY);
    if(0 > fd)
    {
        elog(RLOG_ERROR, "open file %s error:%s", tmppath, strerror(errno));
    }
    blockcnt = (cflush->maxsize / RIPPLE_FILE_BUFFER_SIZE);

//    elog(RLOG_WARNING, "blockcnt:%d", blockcnt);
    for(index = 0; index < blockcnt; index++)
    {
        if (write(fd, block, RIPPLE_FILE_BUFFER_SIZE) != RIPPLE_FILE_BUFFER_SIZE)
        {
            /* if write didn't set errno, assume no disk space */
            if(errno == ENOSPC)
            {
                elog(RLOG_WARNING, "The disk is full and there is no available space");
                sleep(10);
                index--;
                continue;
            }
            unlink(tmppath);
            FileClose(fd);
            elog(RLOG_ERROR, "can not write file %s, errno:%s", tmppath, strerror(errno));
        }
    }

    FileSync(fd);

    FileClose(fd);
    
    /* 重命名文件 */
    durable_rename(tmppath, path, RLOG_DEBUG);

    /* 打开文件 */
    cflush->fd = BasicOpenFile(path, O_RDWR | RIPPLE_BINARY);
    if (cflush->fd  < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", path, strerror(errno));
    }
    return;
}

ripple_onlinerefresh_captureflush *ripple_onlinerefresh_captureflush_init(void)
{
    ripple_onlinerefresh_captureflush *result = NULL;

    result = rmalloc0(sizeof(ripple_onlinerefresh_captureflush));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    result = rmemset0(result, 0, 0, sizeof(ripple_onlinerefresh_captureflush));
    result->fd = -1;
    result->maxsize = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    result->maxsize = (result->maxsize * 1024 * 1024);
    return result;
}

void *ripple_onlinerefresh_captureflush_main(void *args)
{
    int timeout                                 = 0;
    ripple_thrnode* thrnode                     = NULL;
    ripple_ff_fileinfo* finfo                   = NULL;
    ripple_file_buffer* fbuffer                 = NULL;
    ripple_onlinerefresh_captureflush *cflush   = NULL;

    thrnode = (ripple_thrnode*)args;
    cflush = (ripple_onlinerefresh_captureflush* )thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "onlinerefresh capture flush stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
        return NULL;
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while(1)
    {
        /* 首先判断是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /*
         * 1、在缓存中获取数据
         * 2、写数据
         */
        fbuffer = ripple_file_buffer_waitflush_get(cflush->txn2filebuffer, &timeout);
        if(NULL == fbuffer)
        {
            if (RIPPLE_ERROR_TIMEOUT != timeout)
            {
                /* 处理失败, 退出 */
                elog(RLOG_WARNING, "get file buffer error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
            
            /* 超时没有获取到数据,继续等待获取 */
            continue;
        }

        /* 将数据落盘 */
        if(RIPPLE_FILE_BUFFER_FLAG_DATA == (RIPPLE_FILE_BUFFER_FLAG_DATA&fbuffer->flag)
            && 0 != fbuffer->start)
        {
            /* 含有数据内容，那么将数据落盘 */
            finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

            if(cflush->trailno != finfo->fileid
                || -1 == cflush->fd)
            {
                ripple_onlinerefresh_captureflush_initfile(cflush, finfo);
                cflush->trailno = finfo->fileid;
            }

            /* 写数据 */
            FilePWrite(cflush->fd, (char*)fbuffer->data, fbuffer->maxsize, ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE));
            FileDataSync(cflush->fd);
        }

        if (RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESHEND == (RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESHEND&fbuffer->flag))
        {
            /* 放入到空闲队列中 */
            ripple_file_buffer_free(cflush->txn2filebuffer, fbuffer);
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
        /* 放入到空闲队列中 */
        ripple_file_buffer_free(cflush->txn2filebuffer, fbuffer);
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_onlinerefresh_captureflush_free(void *args)
{
    ripple_onlinerefresh_captureflush *cflush = NULL;
    cflush = (ripple_onlinerefresh_captureflush *)args;

    if(NULL == cflush)
    {
        return;
    }

    if (cflush->trail)
    {
        rfree(cflush->trail);
    }
    rfree(cflush);
}
