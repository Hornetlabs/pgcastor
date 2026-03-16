/*
 * 将缓存中的数据写入到文件中
*/
#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/regex/ripple_regex.h"
#include "threads/ripple_threads.h"
#include "misc/ripple_misc_control.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "net/netpacket/ripple_netpacket.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "strategy/ripple_filter_dataset.h"
#include "metric/capture/ripple_metric_capture.h"
#include "increment/capture/flush/ripple_increment_captureflush.h"


static void ripple_increment_captureflush_reloadsyncdatasets(ripple_increment_captureflush* wstate);

static void ripple_writework_capture_reloadstate(ripple_increment_captureflush* wstate, int state)
{
    if (RIPPLE_CAPTURERELOAD_STATUS_RELOADING_WRITE == state)
    {
        ripple_increment_captureflush_reloadsyncdatasets(wstate);
        g_gotsigreload = RIPPLE_CAPTURERELOAD_STATUS_UNSET;
        elog(RLOG_INFO,"Ripple reload complete!");
    }
  return;
}

/* 初始化文件 */
static void ripple_increment_captureflush_initfile(ripple_increment_captureflush* wstate, ripple_ff_fileinfo* finfo)
{
    int fd = -1;
    int index = 0;
    int blockcnt = 0;
    struct stat st;
    char    tmppath[RIPPLE_MAXPATH] = { 0 };
    uint8   block[RIPPLE_FILE_BUFFER_SIZE] = { 0 };
    if(-1 != wstate->fd)
    {
        close(wstate->fd);
        wstate->fd = -1;
    }

    /* 生成路径 */
    rmemset1(wstate->path, 0, '\0', RIPPLE_MAXPATH);
    snprintf(wstate->path, RIPPLE_MAXPATH, RIPPLE_STORAGE_TRAIL_DIR "/%016lX", finfo->fileid);

    /* 校验文件是否存在，存在则打开 */
    if(0 == stat(wstate->path, &st))
    {
        /* 打开文件 */
        wstate->fd = BasicOpenFile(wstate->path, O_RDWR | RIPPLE_BINARY);
        if (wstate->fd  < 0)
        {
            elog(RLOG_ERROR, "open file %s error %s", wstate->path, strerror(errno));
        }
        return;
    }

    /* 查看错误是否为文件不存在 */
    if(ENOENT != errno)
    {
        elog(RLOG_ERROR, "stat %s error, %s", wstate->path, strerror(errno));
    }

    /* 创建临时文件 */
    snprintf(tmppath, RIPPLE_MAXPATH, RIPPLE_STORAGE_TRAIL_DIR "/%016lX.tmp", finfo->fileid);
    unlink(tmppath);

    fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | RIPPLE_BINARY);
    if(0 > fd)
    {
        elog(RLOG_ERROR, "open file %s error:%s", tmppath, strerror(errno));
    }
    blockcnt = (wstate->maxsize / RIPPLE_FILE_BUFFER_SIZE);

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
    durable_rename(tmppath, wstate->path, RLOG_DEBUG);

    /* 打开文件 */
    wstate->fd = BasicOpenFile(wstate->path, O_RDWR | RIPPLE_BINARY);
    if (wstate->fd  < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", wstate->path, strerror(errno));
    }
    return;
}

/*
 * 写主进程
*/
void* ripple_increment_captureflush_main(void *args)
{
    int timeout                             = 0;
    ripple_ff_fileinfo* finfo               = NULL;
    ripple_file_buffer* fbuffer             = NULL;
    ripple_thrnode* thrnode                 = NULL;
    ripple_increment_captureflush* cflush   = NULL;

    thrnode = (ripple_thrnode*)args;
    cflush = (ripple_increment_captureflush* )thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "capture flush stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 加载基础信息 */
    ripple_misc_stat_loaddecode((void*)&cflush->base);

    /* 加载系统表 */
    ripple_cache_sysdictsload((void**)&cflush->txnsctx->sysdicts);

    /* 加载addtablepattern */
    cflush->txnsctx->addtablepattern = ripple_filter_dataset_initaddtablepattern(cflush->txnsctx->addtablepattern);

    /* 加载同步数据集 */
    cflush->txnsctx->hsyncdataset = ripple_filter_dataset_load(cflush->txnsctx->sysdicts->by_namespace,
                                                                cflush->txnsctx->sysdicts->by_class);

    while(1)
    {
        /*
         * 1、在缓存中获取数据
         * 2、写数据
         */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
        ripple_writework_capture_reloadstate(cflush, g_gotsigreload);

        fbuffer = ripple_file_buffer_waitflush_get(cflush->txn2filebuffer, &timeout);
        if(NULL == fbuffer)
        {
            if (RIPPLE_ERROR_TIMEOUT != timeout)
            {
                /* 处理失败, 退出 */
                elog(RLOG_WARNING, "get file buffer error");
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
            if(cflush->fileid != finfo->fileid
                || -1 == cflush->fd)
            {
                ripple_increment_captureflush_initfile(cflush, finfo);
                cflush->fileid = finfo->fileid;
            }

            /* 写数据 */
            FilePWrite(cflush->fd, (char*)fbuffer->data, fbuffer->maxsize, ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE));
            FileDataSync(cflush->fd);

            cflush->base.fileid = finfo->fileid;
            cflush->base.fileoffset = ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE) + fbuffer->start;

            elog(RLOG_DEBUG, "---------fileid:%lu:%lu", cflush->base.fileid, cflush->base.fileoffset);
            /* 设置状态线程,当前已经解析完成且持久化的 lsn */
            cflush->callback.setmetricflushlsn(cflush->privdata, fbuffer->extra.rewind.flushlsn.wal.lsn);
            cflush->callback.setmetrictrailno(cflush->privdata, cflush->base.fileid);
            cflush->callback.setmetrictrailstart(cflush->privdata, cflush->base.fileoffset);
            cflush->callback.setmetricflushtimestamp(cflush->privdata, fbuffer->extra.timestamp);
        }

        /* 更新 restartlsn 和 fileid 及 offset */
        if(RIPPLE_FILE_BUFFER_FLAG_REWIND == (RIPPLE_FILE_BUFFER_FLAG_REWIND&fbuffer->flag))
        {
            /* 写base文件 */
            cflush->base.confirmedlsn = fbuffer->extra.rewind.confirmlsn.wal.lsn;
            cflush->base.restartlsn = fbuffer->extra.rewind.restartlsn.wal.lsn;
            cflush->base.fileid = finfo->fileid;
            cflush->base.fileoffset = ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE) + fbuffer->start;
            /* 有效时更新 redolsn */
            if (InvalidXLogRecPtr != fbuffer->extra.chkpoint.redolsn.wal.lsn)
            {
                cflush->base.redolsn = fbuffer->extra.chkpoint.redolsn.wal.lsn;
            }
            cflush->base.curtlid = fbuffer->extra.rewind.curtlid;

            elog(RLOG_DEBUG, "************fileid:%lu:%lu", cflush->base.fileid, cflush->base.fileoffset);
            /* 数据落盘,不需要加锁 */
            ripple_misc_stat_decodewrite(&cflush->base, &cflush->basefd);
        }

        /* 查看是否含有 redolsn，含有 redolsn 那么写入数据 */
        if(RIPPLE_FILE_BUFFER_FLAG_REDO == (RIPPLE_FILE_BUFFER_FLAG_REDO&fbuffer->flag))
        {
            /* 应用系统表 */
            if(NULL != fbuffer->extra.chkpoint.sysdicts)
            {
                ripple_cache_sysdicts_txnsysdicthis2cache(cflush->txnsctx->sysdicts,
                                                            fbuffer->extra.chkpoint.sysdicts);

                /* 更新同步数据集 */
                if(ripple_filter_dataset_updatedatasets(cflush->txnsctx->addtablepattern,
                                                        cflush->txnsctx->sysdicts->by_namespace,
                                                        fbuffer->extra.chkpoint.sysdicts,
                                                        cflush->txnsctx->hsyncdataset))
                {
                    ripple_filter_dataset_flush(cflush->txnsctx->hsyncdataset);
                }

                /* 释放内存 */
                ripple_cache_sysdicts_txnsysdicthisfree(fbuffer->extra.chkpoint.sysdicts);
                list_free(fbuffer->extra.chkpoint.sysdicts);
                fbuffer->extra.chkpoint.sysdicts = NULL;
                ripple_sysdictscache_write(cflush->txnsctx->sysdicts, cflush->base.redolsn);
            }
        }

        /* 存在dataset标识, 更新后强制落盘 */
        if (RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESH_DATASET == (RIPPLE_FILE_BUFFER_FLAG_ONLINREFRESH_DATASET & fbuffer->flag))
        {
            if (fbuffer->extra.dataset.dataset)
            {
                ListCell *cell = NULL;
                ripple_filter_dataset_updatedatasets_onlinerefresh(cflush->txnsctx->hsyncdataset,
                                                                    fbuffer->extra.dataset.dataset);
                
                foreach(cell, fbuffer->extra.dataset.dataset)
                {
                    ripple_refresh_table *table = (ripple_refresh_table*) lfirst(cell);
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
                ripple_filter_dataset_flush(cflush->txnsctx->hsyncdataset);
            }
        }

        /* 放入到空闲队列中 */
        ripple_file_buffer_free(cflush->txn2filebuffer, fbuffer);
    }

    ripple_pthread_exit(NULL);
    return NULL;
}


ripple_increment_captureflush* ripple_increment_captureflush_init(void)
{
    ripple_increment_captureflush* writestate = NULL;
    writestate = (ripple_increment_captureflush*)rmalloc1(sizeof(ripple_increment_captureflush));
    if(NULL == writestate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(writestate, 0, '\0', sizeof(ripple_increment_captureflush));

    writestate->fd = -1;
    writestate->basefd = -1;
    writestate->fileid = -1;

    writestate->maxsize = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    writestate->maxsize = (writestate->maxsize * 1024 * 1024);

    writestate->txnsctx = (ripple_txnscontext*)rmalloc0(sizeof(ripple_txnscontext));
    if(NULL == writestate->txnsctx)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(writestate->txnsctx, 0, '\0', sizeof(ripple_txnscontext));

    return writestate;
}

/* 资源回收 */
void ripple_increment_captureflush_destroy(ripple_increment_captureflush* wstate)
{
    if(NULL == wstate)
    {
        return;
    }

    if(NULL != wstate->txnsctx)
    {
        ripple_transcache_free(wstate->txnsctx);
        rfree(wstate->txnsctx);
        wstate->txnsctx = NULL;
    }

    wstate->privdata = NULL;
    wstate->txn2filebuffer = NULL;

    rfree(wstate);
    wstate = NULL;
}

static void ripple_increment_captureflush_reloadsyncdatasets(ripple_increment_captureflush* wstate)
{
    wstate->txnsctx->hsyncdataset = ripple_filter_dataset_reload(wstate->txnsctx->sysdicts->by_namespace,
                                                                wstate->txnsctx->sysdicts->by_class,
                                                                wstate->txnsctx->hsyncdataset);
}
