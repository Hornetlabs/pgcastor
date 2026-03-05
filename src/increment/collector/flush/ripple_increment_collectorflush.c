/*
 * 将缓存中的数据写入到文件中
*/
#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "misc/ripple_misc_control.h"
#include "misc/ripple_misc_stat.h"
#include "threads/ripple_threads.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "filetransfer/ripple_filetransfer.h"
#include "increment/collector/flush/ripple_increment_collectorflush.h"
#include "metric/collector/ripple_metric_collector.h"

static void ripple_increment_collectorflush_filetransfer_add(ripple_increment_collectorflush* cflush, uint64 fileid, bool partial)
{
    ripple_filetransfer_increment* filetransfer_inc = NULL;

    if (false == cflush->upload)
    {
        return;
    }

    filetransfer_inc = ripple_filetransfer_increment_init();
    filetransfer_inc->base.partial = partial;
    ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)filetransfer_inc, fileid);
    ripple_filetransfer_upload_path_set(filetransfer_inc, cflush->name);
    cflush->callback.collector_filetransfernode_add(cflush->privdata, (void*)filetransfer_inc);
    filetransfer_inc = NULL;

    return;
}


/* 初始化文件 */
static void ripple_increment_collectorflush_initfile(ripple_increment_collectorflush* cflush, ripple_ff_fileinfo* finfo)
{
    int fd = -1;
    int index = 0;
    int blockcnt = 0;
    struct stat st;
    char    tmppath[RIPPLE_MAXPATH] = { 0 };
    uint8   block[RIPPLE_FILE_BUFFER_SIZE] = { 0 };
    if(-1 != cflush->fd)
    {
        close(cflush->fd);
        cflush->fd = -1;
    }

    /* 生成路径 */
    rmemset1(cflush->path, 0, '\0', RIPPLE_MAXPATH);
    snprintf(cflush->path, RIPPLE_MAXPATH, "%s/%s/%016lX", cflush->name, RIPPLE_STORAGE_TRAIL_DIR, finfo->fileid);

    /* 校验文件是否存在，存在则打开 */
    if(0 == stat(cflush->path, &st))
    {
        /* 打开文件 */
        cflush->fd = BasicOpenFile(cflush->path, O_RDWR | RIPPLE_BINARY);
        if (cflush->fd  < 0)
        {
            elog(RLOG_ERROR, "open file %s error %s", cflush->path, strerror(errno));
        }
        return;
    }

    if (finfo->fileid > 0)
    {
        ripple_increment_collectorflush_filetransfer_add(cflush, cflush->fileid, false);
    }

    /* 查看错误是否为文件不存在 */
    if(ENOENT != errno)
    {
        elog(RLOG_ERROR, "stat %s error, %s", cflush->path, strerror(errno));
    }

    /* 创建临时文件 */
    snprintf(tmppath, RIPPLE_MAXPATH, "%s/%s/%016lX.tmp", cflush->name, RIPPLE_STORAGE_TRAIL_DIR, finfo->fileid);
    unlink(tmppath);

    fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | RIPPLE_BINARY);
    if(0 > fd)
    {
        elog(RLOG_ERROR, "open file %s error:%s", tmppath, strerror(errno));
    }
    blockcnt = (cflush->maxsize / RIPPLE_FILE_BUFFER_SIZE);

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
    durable_rename(tmppath, cflush->path, RLOG_DEBUG);

    /* 打开文件 */
    cflush->fd = BasicOpenFile(cflush->path, O_RDWR | RIPPLE_BINARY);
    if (cflush->fd  < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", cflush->path, strerror(errno));
    }
    return;
}

/*
 * 写主进程
*/
void* ripple_increment_collectorflush_main(void *args)
{
    int timeout                                     = 0;
    int timeinterval                                = 0;         /* 加入到网闸的时间间隔 */
    int index                                       = 0;
    ripple_thrnode* thrnode                         = NULL;
    ripple_ff_fileinfo* finfo                       = NULL;
    ripple_file_buffer* fbuffer                     = NULL;
    ripple_increment_collectorflush* cflush         = NULL;

    thrnode = (ripple_thrnode*)args;
 
    cflush = (ripple_increment_collectorflush* )thrnode->data;
 
     /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
     {
        elog(RLOG_WARNING, "increment collector netsvr exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
     }
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    ripple_misc_stat_loadcollector(&cflush->collectorbase, cflush->name);

    while(1)
    {
        usleep(50000);
         /*
          * 1、在缓存中获取数据
          * 2、写数据
          */

        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        fbuffer = ripple_file_buffer_waitflush_get(cflush->netdata2filebuffer, &timeout);
        if(NULL == fbuffer)
        {
            if (RIPPLE_ERROR_TIMEOUT != timeout)
            {
                /* 处理失败, 退出 */
                elog(RLOG_WARNING, "get file buffer error");
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }

            /* 检测是否需要加入到上传队列中 */
            timeinterval += timeout;
            if(RIPPLE_GAP_TIMEOUT > timeinterval)
            {
                continue;
            }
            ripple_increment_collectorflush_filetransfer_add(cflush, cflush->fileid, true);
            timeinterval = 0;
            continue;
        }

        /* 
         * 将数据落盘
         *  1、打开文件
         *  2、将数据落盘
         */
        /* 含有数据内容，那么将数据落盘 */
        finfo = (ripple_ff_fileinfo*)fbuffer->privdata;
        if(-1 == cflush->fd || cflush->fileid != finfo->fileid)
        {
            ripple_increment_collectorflush_initfile(cflush, finfo);
            cflush->fileid = finfo->fileid;
        }

        /* 写数据 */
        if(fbuffer->maxsize != FilePWrite(cflush->fd, (char*)fbuffer->data, fbuffer->maxsize, ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE)))
        {
            elog(RLOG_WARNING, "increment collector flush thread write error, %s", strerror(errno));
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        }

        if(RLOG_DEBUG == g_loglevel)
        {
            printf("\n");
            for(index = 0 ;index < fbuffer->start; index++)
            {
                printf("%02X", fbuffer->data[index]);
            }
            printf("\n");
        }

        FileDataSync(cflush->fd);

        elog(RLOG_DEBUG, "finfo->fileid:%lu, %lu, fbuffer->maxsize:%lu",
                            finfo->fileid, finfo->blknum,
                            fbuffer->maxsize);

        /* redolsn/restartlsn/confirmlsn 和 fileid/offset */
        cflush->collectorbase.redolsn = fbuffer->extra.chkpoint.redolsn.wal.lsn;
        cflush->collectorbase.restartlsn = fbuffer->extra.rewind.restartlsn.wal.lsn;
        cflush->collectorbase.confirmedlsn = fbuffer->extra.rewind.confirmlsn.wal.lsn;
        cflush->collectorbase.pfileid = fbuffer->extra.chkpoint.orgaddr.trail.fileid;
        cflush->collectorbase.cfileid = fbuffer->extra.rewind.fileaddr.trail.fileid;
        cflush->collectorbase.coffset = fbuffer->extra.rewind.fileaddr.trail.offset;

         elog(RLOG_DEBUG, "collectorbase: pfile:%lu, cfileid:%lu, coffset:%lu",
                            cflush->collectorbase.pfileid,
                            cflush->collectorbase.cfileid,
                            fbuffer->extra.rewind.fileaddr.trail.offset);

        cflush->callback.setmetricflushlsn(cflush->privdata, cflush->name, fbuffer->extra.rewind.confirmlsn.wal.lsn);
        cflush->callback.setmetricflushtrailno(cflush->privdata, cflush->name, fbuffer->extra.rewind.fileaddr.trail.fileid);
        cflush->callback.setmetricflushtrailstart(cflush->privdata, cflush->name, fbuffer->extra.rewind.fileaddr.trail.offset);
        cflush->callback.setmetricflushtimestamp(cflush->privdata, cflush->name, fbuffer->extra.timestamp);

        /* collector状态信息落盘 */
        ripple_misc_stat_collectorwrite(&cflush->collectorbase, cflush->name, &cflush->basefd);

        /* 放入到空闲队列中 */
        ripple_file_buffer_free(cflush->netdata2filebuffer, fbuffer);
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

/* 初始化 */
ripple_increment_collectorflush* ripple_increment_collectorflush_init(void)
{
    char* url = NULL;
    ripple_increment_collectorflush* cflush = NULL;
    cflush = (ripple_increment_collectorflush*)rmalloc0(sizeof(ripple_increment_collectorflush));
    if(NULL == cflush)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(cflush, 0, '\0', sizeof(ripple_increment_collectorflush));

    /* 设置初始值 */
    cflush->fd = -1;
    cflush->basefd = -1;
    cflush->fileid = 0;
    cflush->maxsize = ((uint64)guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE) * (uint64)1024 * (uint64)1024);
    url = guc_getConfigOption(RIPPLE_CFG_KEY_FTPURL);
    /* url不配置不做处理 */
    if (!(url == NULL || url[0] == '\0'))
    {
        cflush->upload = true;
    }
    return cflush;
}

/* 初始化 */
ripple_increment_collectorflushnode* ripple_increment_collectorflushnode_init(char* name)
{
    ripple_increment_collectorflushnode* flushnode = NULL;
    flushnode = (ripple_increment_collectorflushnode*)rmalloc0(sizeof(ripple_increment_collectorflushnode));
    if(NULL == flushnode)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(flushnode, 0, '\0', sizeof(ripple_increment_collectorflushnode));

    flushnode->flush = ripple_increment_collectorflush_init();
    flushnode->stat = RIPPLE_INCREMENT_COLLECTORFLUSHNODE_STAT_NOP;
    rmemset1(flushnode->name, 0, '\0', 128);
    snprintf(flushnode->name, 128, "%s", name);
    return flushnode;
}

/* 资源回收 */
void ripple_increment_collectorflush_destroy(ripple_increment_collectorflush* cflush)
{
    if(NULL == cflush)
    {
        return;
    }

    cflush->privdata = NULL;
    cflush->netdata2filebuffer = NULL;

    rfree(cflush);
    cflush = NULL;
}

/* flushnode资源回收 */
void ripple_increment_collectorflushnode_destroy(void* args)
{
    ripple_increment_collectorflushnode* flushnode = NULL;

    flushnode = (ripple_increment_collectorflushnode*)args;

    if(NULL == flushnode)
    {
        return;
    }

    ripple_increment_collectorflush_destroy(flushnode->flush);

    rfree(flushnode);
    flushnode = NULL;
}
