/*
 * 将缓存中的数据写入到文件中
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
    if(NULL == cflush)
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
        elog(RLOG_INFO,"Ripple reload complete!");
    }
  return;
}

/* 初始化文件 */
static bool bigtxn_captureflush_initfile(increment_captureflush* cflush, bigtxn_captureflush_file* cflushfile)
{
    struct stat st;
    uint8 block[FILE_BUFFER_SIZE] = { 0 };

    if(-1 != cflushfile->fd)
    {
        osal_file_close(cflushfile->fd);
        cflushfile->fd = -1;
    }

    /* 创建目录 */
    rmemset1(cflush->path, 0, '\0', MAXPATH);
    snprintf(cflush->path, MAXPATH, "%s/%lu", STORAGE_BIG_TRANSACTION_DIR, cflushfile->xid);
    osal_make_dir(cflush->path);

    /*
     * 打开文件
     *  文件存在, 打开直接用
     *  不存在, 创建一个新文件
     */
    rmemset1(cflush->path, 0, '\0', MAXPATH);
    snprintf(cflush->path, MAXPATH, "%s/%lu/%016lX", STORAGE_BIG_TRANSACTION_DIR, cflushfile->xid, cflushfile->fileid);
    if(0 == stat(cflush->path, &st))
    {
        /* 打开文件 */
        cflushfile->fd = osal_basic_open_file(cflush->path, O_RDWR | BINARY);
        if (cflushfile->fd  < 0)
        {
            elog(RLOG_WARNING, "open file %s error %s", cflush->path, strerror(errno));
            return false;
        }
        return true;
    }

    if(false == osal_create_file_with_size(cflush->path,
                                    O_RDWR | O_CREAT | O_EXCL | BINARY,
                                    cflush->maxsize,
                                    FILE_BUFFER_SIZE,
                                    block))
    {
        elog(RLOG_WARNING, "create file error, %s", strerror(errno));
        return false;
    }

    /* 打开文件 */
    cflushfile->fd = osal_basic_open_file(cflush->path, O_RDWR | BINARY);
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
void* bigtxn_captureflush_main(void *args)
{
    bool found                                      = false;
    int timeout                                     = 0;
    HTAB* htxns                                     = NULL;
    thrnode* thr_node                         = NULL;
    ff_fileinfo* finfo                       = NULL;
    file_buffer* fbuffer                     = NULL;
    increment_captureflush* cflush           = NULL;
    bigtxn_captureflush_file* cflushfile     = NULL;
    bigtxn_captureflush_file* cflushlastfile = NULL;
    HASHCTL hctl = {'\0'};

    thr_node = (thrnode*)args;
    cflush = (increment_captureflush* )thr_node->data;

    /* 查看状态 */
    if(THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "capture bigtxn flush stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        osal_thread_exit(NULL);
    }

    /* 设置为工作状态 */
    thr_node->stat = THRNODE_STAT_WORK;

    /* 加载基础信息 */
    misc_stat_loaddecode((void*)&cflush->base);

    /* 加载系统表 */
    cache_sysdictsload((void**)&cflush->txnsctx->sysdicts);

    /* 加载addtablepattern */
    cflush->txnsctx->addtablepattern = filter_dataset_initaddtablepattern(cflush->txnsctx->addtablepattern);

    /* 加载同步数据集 */
    cflush->txnsctx->hsyncdataset = filter_dataset_load(cflush->txnsctx->sysdicts->by_namespace,
                                                                cflush->txnsctx->sysdicts->by_class);

    /* 初始化大事务hash */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(FullTransactionId);
    hctl.entrysize = sizeof(bigtxn_captureflush_file);
    htxns = hash_create("bigtxncflushfile", 1024, &hctl, HASH_ELEM | HASH_BLOBS);

    while(1)
    {
        if(THRNODE_STAT_TERM == thr_node->stat)
        {
            /* 序列化/落盘 */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /*
         * 1、在缓存中获取数据
         * 2、写数据
         */
        bigtxn_captureflush_reloadstate(cflush, g_gotsigreload);
        fbuffer = file_buffer_waitflush_get(cflush->txn2filebuffer, &timeout);
        if(NULL == fbuffer)
        {
            if (ERROR_TIMEOUT != timeout)
            {
                /* 处理失败, 退出 */
                elog(RLOG_WARNING, "get file buffer error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            
            /* 超时没有获取到数据,继续等待获取 */
            continue;
        }
        finfo = (ff_fileinfo*)fbuffer->privdata;

        if(InvalidFullTransactionId == finfo->xid)
        {
            elog(RLOG_WARNING, "big txn capture flush get InvalidFullTransactionId");
            thr_node->stat = THRNODE_STAT_ABORT;
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
                thr_node->stat = THRNODE_STAT_ABORT;
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
            osal_file_close(cflushlastfile->fd);
            cflushlastfile->fd = -1;
            cflushlastfile = cflushfile;
        }

        if(-1 == cflushlastfile->fd)
        {
            /* 文件没有打开, 那么打开文件 */
            if(false == bigtxn_captureflush_initfile(cflush, cflushlastfile))
            {
                elog(RLOG_WARNING, "big txn capture flush open file error, %s", cflush->path);
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
        }

        /* 重置 cflush 中关于落盘的内容 */
        if(finfo->fileid != cflushlastfile->fileid)
        {
            cflushlastfile->fileid = finfo->fileid;
            /* 文件没有打开, 那么打开文件 */
            if(false == bigtxn_captureflush_initfile(cflush, cflushlastfile))
            {
                elog(RLOG_WARNING, "big txn capture flush open file error, %s", cflush->path);
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
        }

        cflush->fd = cflushlastfile->fd;
        cflush->fileid = cflushlastfile->fileid;

        if(FILE_BUFFER_FLAG_DATA == (FILE_BUFFER_FLAG_DATA&fbuffer->flag))
        {
            /* 含有数据, 先将数据落盘 */
            if(0 != fbuffer->start)
            {
                /* 写数据 */
                if(fbuffer->maxsize != osal_file_pwrite(cflush->fd, (char*)fbuffer->data, fbuffer->maxsize, ((finfo->blknum - 1)*fbuffer->maxsize)))
                {
                    elog(RLOG_WARNING, "big txn capture flush flush error, xid:%lu, fileid:%lu, %s",
                                        cflushlastfile->xid,
                                        cflushlastfile->fileid,
                                        strerror(errno));
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
                osal_file_data_sync(cflush->fd);
            }
        }

        if(FILE_BUFFER_FLAG_BIGTXNEND == (FILE_BUFFER_FLAG_BIGTXNEND & fbuffer->flag))
        {
            /* 大事务结束 */
            hash_search(htxns, &finfo->xid, HASH_REMOVE, NULL);
        }

        /* 放入到空闲队列中 */
        file_buffer_free(cflush->txn2filebuffer, fbuffer);
    }

    hash_destroy(htxns);
    osal_thread_exit(NULL);
    return NULL;
}

/* 资源回收 */
void bigtxn_captureflush_destroy(increment_captureflush* wstate)
{
    if(NULL == wstate)
    {
        return;
    }

    if(NULL != wstate->txnsctx)
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
