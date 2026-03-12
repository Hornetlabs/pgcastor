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
#include "increment/capture/flush/ripple_increment_captureflush.h"
#include "bigtransaction/capture/flush/ripple_bigtxn_captureflush.h"
#include "metric/capture/ripple_metric_capture.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "strategy/ripple_filter_dataset.h"

ripple_increment_captureflush* ripple_bigtxn_captureflush_init(void)
{
    ripple_increment_captureflush* cflush = NULL;

    cflush = ripple_increment_captureflush_init();
    if(NULL == cflush)
    {
        elog(RLOG_WARNING, "init big transaction capture flush error");
        return NULL;
    }
    return cflush;
}


static void ripple_bigtxn_captureflush_reloadsyncdatasets(ripple_increment_captureflush* wstate)
{
    wstate->txnsctx->hsyncdataset = ripple_filter_dataset_reload(wstate->txnsctx->sysdicts->by_namespace,
                                                                wstate->txnsctx->sysdicts->by_class,
                                                                wstate->txnsctx->hsyncdataset);
}


static void ripple_bigtxn_captureflush_reloadstate(ripple_increment_captureflush* wstate, int state)
{
    if (RIPPLE_CAPTURERELOAD_STATUS_RELOADING_WRITE == state)
    {
        ripple_bigtxn_captureflush_reloadsyncdatasets(wstate);
        g_gotsigreload = RIPPLE_CAPTURERELOAD_STATUS_UNSET;
        elog(RLOG_INFO,"Ripple reload complete!");
    }
  return;
}

/* 初始化文件 */
static bool ripple_bigtxn_captureflush_initfile(ripple_increment_captureflush* cflush, ripple_bigtxn_captureflush_file* cflushfile)
{
    struct stat st;
    uint8 block[RIPPLE_FILE_BUFFER_SIZE] = { 0 };

    if(-1 != cflushfile->fd)
    {
        FileClose(cflushfile->fd);
        cflushfile->fd = -1;
    }

    /* 创建目录 */
    rmemset1(cflush->path, 0, '\0', RIPPLE_MAXPATH);
    snprintf(cflush->path, RIPPLE_MAXPATH, "%s/%lu", RIPPLE_STORAGE_BIG_TRANSACTION_DIR, cflushfile->xid);
    MakeDir(cflush->path);

    /*
     * 打开文件
     *  文件存在, 打开直接用
     *  不存在, 创建一个新文件
     */
    rmemset1(cflush->path, 0, '\0', RIPPLE_MAXPATH);
    snprintf(cflush->path, RIPPLE_MAXPATH, "%s/%lu/%016lX", RIPPLE_STORAGE_BIG_TRANSACTION_DIR, cflushfile->xid, cflushfile->fileid);
    if(0 == stat(cflush->path, &st))
    {
        /* 打开文件 */
        cflushfile->fd = BasicOpenFile(cflush->path, O_RDWR | RIPPLE_BINARY);
        if (cflushfile->fd  < 0)
        {
            elog(RLOG_WARNING, "open file %s error %s", cflush->path, strerror(errno));
            return false;
        }
        return true;
    }

    if(false == CreateFileWithSize(cflush->path,
                                    O_RDWR | O_CREAT | O_EXCL | RIPPLE_BINARY,
                                    cflush->maxsize,
                                    RIPPLE_FILE_BUFFER_SIZE,
                                    block))
    {
        elog(RLOG_WARNING, "create file error, %s", strerror(errno));
        return false;
    }

    /* 打开文件 */
    cflushfile->fd = BasicOpenFile(cflush->path, O_RDWR | RIPPLE_BINARY);
    if (cflushfile->fd  < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", cflush->path, strerror(errno));
        return false;
    }
    return true;
}

/*
 * 写主进程
*/
void* ripple_bigtxn_captureflush_main(void *args)
{
    bool found                                      = false;
    int timeout                                     = 0;
    HTAB* htxns                                     = NULL;
    ripple_thrnode* thrnode                         = NULL;
    ripple_ff_fileinfo* finfo                       = NULL;
    ripple_file_buffer* fbuffer                     = NULL;
    ripple_increment_captureflush* cflush           = NULL;
    ripple_bigtxn_captureflush_file* cflushfile     = NULL;
    ripple_bigtxn_captureflush_file* cflushlastfile = NULL;
    HASHCTL hctl = {'\0'};

    thrnode = (ripple_thrnode*)args;
    cflush = (ripple_increment_captureflush* )thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "capture bigtxn flush stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
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

    /* 初始化大事务hash */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(FullTransactionId);
    hctl.entrysize = sizeof(ripple_bigtxn_captureflush_file);
    htxns = hash_create("bigtxncflushfile", 1024, &hctl, HASH_ELEM | HASH_BLOBS);

    while(1)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /*
         * 1、在缓存中获取数据
         * 2、写数据
         */
        ripple_bigtxn_captureflush_reloadstate(cflush, g_gotsigreload);
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
        finfo = (ripple_ff_fileinfo*)fbuffer->privdata;

        if(InvalidFullTransactionId == finfo->xid)
        {
            elog(RLOG_WARNING, "big txn capture flush get InvalidFullTransactionId");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        cflushfile = hash_search(htxns, &finfo->xid, HASH_ENTER, &found);
        if(false == found)
        {
            cflushfile->xid = finfo->xid;
            cflushfile->fd = -1;
            cflushfile->fileid = finfo->fileid;

            if(0 != finfo->fileid)
            {
                elog(RLOG_WARNING, "big txn capture got new big transaction %lu, but fileid not equal zero, %lu", finfo->fileid);
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
        }

        if(NULL == cflushlastfile)
        {
            cflushlastfile = cflushfile;
        }
        else if(cflushfile->xid != cflushlastfile->xid)
        {
            /* 保存内容 */
            cflushlastfile->fileid = cflush->fileid;
            FileClose(cflushlastfile->fd);
            cflushlastfile->fd = -1;
            cflushlastfile = cflushfile;
        }

        if(-1 == cflushlastfile->fd)
        {
            /* 文件没有打开, 那么打开文件 */
            if(false == ripple_bigtxn_captureflush_initfile(cflush, cflushlastfile))
            {
                elog(RLOG_WARNING, "big txn capture flush open file error, %s", cflush->path);
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
        }

        /* 重置 cflush 中关于落盘的内容 */
        if(finfo->fileid != cflushlastfile->fileid)
        {
            cflushlastfile->fileid = finfo->fileid;
            /* 文件没有打开, 那么打开文件 */
            if(false == ripple_bigtxn_captureflush_initfile(cflush, cflushlastfile))
            {
                elog(RLOG_WARNING, "big txn capture flush open file error, %s", cflush->path);
                thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                break;
            }
        }

        cflush->fd = cflushlastfile->fd;
        cflush->fileid = cflushlastfile->fileid;

        if(RIPPLE_FILE_BUFFER_FLAG_DATA == (RIPPLE_FILE_BUFFER_FLAG_DATA&fbuffer->flag))
        {
            /* 含有数据, 先将数据落盘 */
            if(0 != fbuffer->start)
            {
                /* 写数据 */
                if(fbuffer->maxsize != FilePWrite(cflush->fd, (char*)fbuffer->data, fbuffer->maxsize, ((finfo->blknum - 1)*fbuffer->maxsize)))
                {
                    elog(RLOG_WARNING, "big txn capture flush flush error, xid:%lu, fileid:%lu, %s",
                                        cflushlastfile->xid,
                                        cflushlastfile->fileid,
                                        strerror(errno));
                    thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
                    break;
                }
                FileDataSync(cflush->fd);
            }
        }

        if(RIPPLE_FILE_BUFFER_FLAG_BIGTXNEND == (RIPPLE_FILE_BUFFER_FLAG_BIGTXNEND & fbuffer->flag))
        {
            /* 大事务结束 */
            hash_search(htxns, &finfo->xid, HASH_REMOVE, NULL);
        }

        /* 放入到空闲队列中 */
        ripple_file_buffer_free(cflush->txn2filebuffer, fbuffer);
    }

    hash_destroy(htxns);
    ripple_pthread_exit(NULL);
    return NULL;
}

/* 资源回收 */
void ripple_bigtxn_captureflush_destroy(ripple_increment_captureflush* wstate)
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
