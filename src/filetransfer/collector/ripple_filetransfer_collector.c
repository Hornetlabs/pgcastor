#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/md5/ripple_md5.h"
#include "utils/list/list_func.h"
#include "utils/string/stringinfo.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "filetransfer/collector/ripple_filetransfer_collector.h"

/* 删除上传完成的文件 */
static void ripple_filetransfer_collector_completefile_remove(ripple_filetransfernode* node)
{
    StringInfo path = NULL;

    if (true == node->partial)
    {
        return;
    }

    path = makeStringInfo();

    appendStringInfo(path, "%s", node->localpath);

    durable_unlink(path->data, RLOG_DEBUG);

    appendStringInfo(path, ".check");

    durable_unlink(path->data, RLOG_DEBUG);

    deleteStringInfo(path);

    return ;
}

/* 上传校验文件 */
static bool ripple_filetransfer_collector_checkfile_upload(ripple_filetransfer_ftp* ftp, ripple_filetransfernode* node)
{
    bool result = true;
    int fd = -1;
    struct stat st;
    uint8 md5[16];
    StringInfo path = NULL;

    path = makeStringInfo();
    /* 生成校验文件 */
    appendStringInfo(path, "%s", node->localpath);
    /* refresh文件不存在报错 */
    if(0 != stat(path->data, &st))
    {
        elog(RLOG_WARNING, "open file %s error %s", path->data, strerror(errno));
        deleteStringInfo(path);
        sleep(1);
        return false;
    }

    /* 计算文件内容md5值 */
    if(false == ripple_md5_filemd5_get(path->data, md5))
    {
        deleteStringInfo(path);
        return false;
    }

    /* 生成校验文件 */
    appendStringInfo(path, ".check");
    fd = BasicOpenFile(path->data, O_RDWR | O_CREAT | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", path->data, strerror(errno));
        deleteStringInfo(path);
        return false;
    }

    FileWrite(fd, (char *)md5, 16);

    if(0 != FileSync(fd))
    {
        elog(RLOG_WARNING, "could not fsync file %s", path->data);
        deleteStringInfo(path);
        return false;
    }

    if(FileClose(fd))
    {
        elog(RLOG_WARNING, "could not close file %s", path->data);
        deleteStringInfo(path);
        return false;
    }

    rmemset1(ftp->relativepath, 0, '\0', MAXPGPATH);
    snprintf(ftp->relativepath, MAXPGPATH, "%s/%s.check", ftp->base.ftpdata,
                                                        node->localpath);

    result = ripple_filetransfer_ftp_upload((ripple_filetransfer *)ftp, path->data);
    deleteStringInfo(path);

    return result;
}

ripple_filetransfer_collector* ripple_filetransfer_collector_init(void)
{
    char* ssl = NULL;
    ripple_filetransfer_collector* ftransfer = NULL;
    ftransfer = (ripple_filetransfer_collector*)rmalloc0(sizeof(ripple_filetransfer_collector));
    if(NULL == ftransfer)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ftransfer, 0, '\0', sizeof(ripple_filetransfer_collector));

    ftransfer->filetransfer = ripple_filetransfer_ftp_init();

    ssl = guc_getConfigOption(RIPPLE_CFG_KEY_FTPSSL);

    if (ssl && strlen(ssl) == strlen("on") && strcmp(ssl, "on") == 0)
    {
        ftransfer->filetransfer->ssl = true;
    }

    return ftransfer;
}

/*
 * 写主进程
*/
void* ripple_filetransfer_collector_main(void *args)
{
    int timeout                                     = 0;
    ripple_thrnode* thrnode                         = NULL;
    ripple_filetransfernode* node                   = NULL;
    ripple_filetransfer_collector* ftransfer        = NULL;

    thrnode = (ripple_thrnode*)args;

    ftransfer = (ripple_filetransfer_collector* )thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment collector filetransfer exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    ripple_filetransfer_ftp_global_init();

    while(1)
    {
        /*
         * 1、在缓存中获取数据
         * 2、写数据
         */
        /* 查看是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        node = (ripple_filetransfernode*)ripple_queue_get(ftransfer->filetransfernode, &timeout);
        if(NULL == node)
        {
            /* 需要退出，等待 worknode->status 变为 RIPPLE_WORK_STATUS_TERM 后退出*/
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }

            elog(RLOG_WARNING, "increment collector filetransfer get filetransfernode error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if (false == ripple_filetransfer_collector_checkfile_upload(ftransfer->filetransfer, node))
        {
            ripple_queue_put(ftransfer->filetransfernode, node);
            continue;
        }

        ripple_filetransfer_ftp_relativepath_set(ftransfer->filetransfer, node);

        if (false == ripple_filetransfer_ftp_upload((ripple_filetransfer *)ftransfer->filetransfer, node->localpath))
        {
            ripple_queue_put(ftransfer->filetransfernode, node);
            continue;
        }

        ripple_filetransfer_collector_completefile_remove(node);
        ripple_filetransfer_metadatafile_remove(node);

        rfree(node);
    }

    ripple_filetransfer_ftp_global_cleanup();
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_filetransfer_collector_free(void* args)
{
    ripple_filetransfer_collector* ftransfer = NULL;

    ftransfer = (ripple_filetransfer_collector*)args;

    if (NULL == ftransfer)
    {
        return;
    }
    
    if (ftransfer->filetransfer)
    {
        ripple_filetransfer_ftp_free(ftransfer->filetransfer);
    }

    ftransfer->filetransfernode = NULL;
    
    rfree(ftransfer);
}
