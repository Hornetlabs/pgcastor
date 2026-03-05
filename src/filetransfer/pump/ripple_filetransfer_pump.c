#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/md5/ripple_md5.h"
#include "utils/string/stringinfo.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "filetransfer/ripple_filetransfer.h"
#include "filetransfer/ripple_filetransfer_ftp.h"
#include "filetransfer/pump/ripple_filetransfer_pump.h"

/* refresh 添加每个分片下载任务 */
static void ripple_filetransfer_pump_delete_servefile(ripple_filetransfer_ftp* ftp, ripple_filetransfernode* node)
{
    StringInfo path = NULL;

    ripple_filetransfer_ftp_relativepath_set(ftp, node);

    path = makeStringInfo();
    /* 生成校验文件 */
    appendStringInfo(path, "%s", ftp->relativepath);

    ripple_filetransfer_ftp_removefile((ripple_filetransfer *)ftp, path->data);

    /* 生成校验文件 */
    appendStringInfo(path, ".check");

    ripple_filetransfer_ftp_removefile((ripple_filetransfer *)ftp, path->data);

    deleteStringInfo(path);

    return;
}

/* refresh 添加每个分片下载任务 */
static void ripple_filetransfer_pump_refreshdownload_add(ripple_filetransfer_pump* ftransfer, ripple_filetransfernode* node)
{
    int fd = -1;
    uint32 index = 0;
    struct stat st;
    StringInfo path = NULL;
    ripple_filetransfer_refresh *refresh = NULL;
    ripple_filetransfer_refreshshards *refreshshards = NULL;
    ripple_filetransfer_refreshinfo info = {{'\0'}};

    if(RIPPLE_FILETRANSFERNODE_TYPE_REFRESHSHARDS != node->type
       && RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESHSHARDS != node->type)
    {
        return;
    }

    refreshshards = (ripple_filetransfer_refreshshards *)node;

    path = makeStringInfo();
    /* 生成校验文件 */
    appendStringInfo(path, "%s", node->localpath);

    /* refresh文件不存在报错 */
    if(0 != stat(path->data, &st))
    {
        elog(RLOG_ERROR, " file %s not exit %s", path->data, strerror(errno));
    }

    fd = BasicOpenFile(path->data, O_RDWR | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR,"can not open file:%s  error %s", path->data, strerror(errno));
    }

     /* 输出内容 */
    FileRead(fd, (char*)&info, sizeof(ripple_filetransfer_refreshinfo));

    if (0 == info.shards)
    {
        refresh = ripple_filetransfer_refresh_init();
        ripple_filetransfer_refresh_set(refresh, info.schema, info.table, info.shards, 0);
        sprintf(refresh->base.localpath, "%s/%s/%s_%s_%u_%u",
                                          node->localdir,
                                          RIPPLE_REFRESH_PARTIAL,
                                          refresh->schema,
                                          refresh->table,
                                          refresh->shards,
                                          refresh->shardnum);
        sprintf(refresh->base.localdir, "%s", node->localdir);
        sprintf(refresh->prefixpath, "%s/%s", refreshshards->prefixpath,
                                                    RIPPLE_REFRESH_COMPLETE);
        ripple_filetransfer_node_add((void*)ftransfer->filetransfernode, (void*)refresh);
        refresh = NULL;
        deleteStringInfo(path);
        return;
    }

    for (index = 1; index <= info.shards; index++)
    {
        refresh = ripple_filetransfer_refresh_init();
        ripple_filetransfer_refresh_set(refresh, info.schema, info.table, info.shards, index);
        sprintf(refresh->base.localpath, "%s/%s/%s_%s_%u_%u", node->localdir,
                                                              RIPPLE_REFRESH_PARTIAL,
                                                              refresh->schema,
                                                              refresh->table,
                                                              refresh->shards,
                                                              refresh->shardnum);
        sprintf(refresh->base.localdir, "%s", node->localdir);
        sprintf(refresh->prefixpath, "%s/%s", refreshshards->prefixpath,
                                                    RIPPLE_REFRESH_COMPLETE);
        ripple_filetransfer_node_add((void*)ftransfer->filetransfernode, (void*)refresh);
        refresh = NULL;
    }

    FileClose(fd);

    deleteStringInfo(path);

    return;
}

/* 校验文件内容 */
static bool ripple_filetransfer_pump_checkrefreshfile(ripple_filetransfer_ftp* ftp, ripple_filetransfernode* node)
{
    int fd = -1;
    uint8 md5[16] = {'\0'};
    uint8 filemd5[16] = {'\0'};
    StringInfo path = NULL;
    ripple_filetransfer_refresh* refresh = NULL;
    refresh = (ripple_filetransfer_refresh*)node;

    /* 计算文件MD5值 */
    if(false == ripple_md5_filemd5_get(node->localpath, md5))
    {
        return false;
    }

    /* 获取文件内校验值 */
    path = makeStringInfo();
    appendStringInfo(path, "%s.check", ftp->relativepath);
    rmemset1(ftp->relativepath, 0, '\0', MAXPGPATH);
    snprintf(ftp->relativepath, MAXPGPATH, "%s", path->data);

    resetStringInfo(path);
    appendStringInfo(path, "%s.check", node->localpath);

    if(false == ripple_filetransfer_ftp_download((ripple_filetransfer *)ftp, path->data))
    {
        deleteStringInfo(path);
        return false;
    }

    fd = BasicOpenFile(path->data, O_RDWR | RIPPLE_BINARY);
    if(-1 == fd)
    {
        elog(RLOG_WARNING,"collector state open file %s error %s", path->data, strerror(errno));
        deleteStringInfo(path);
        return false;
    }
    FileRead(fd, (char*)filemd5, 16);

    FileClose(fd);

    /* 判断文件内容是否正确 */
    if (memcmp(md5, filemd5, 16) != 0)
    {
        elog(RLOG_WARNING,"file md5 check error:%s", path->data);
        deleteStringInfo(path);
        return false;
    }
    elog(RLOG_DEBUG,"file md5 check:%s", path->data);

    /* refresh数据移动到正确路径 */
    if(node->type == RIPPLE_FILETRANSFERNODE_TYPE_REFRESH)
    {
        resetStringInfo(path);

        appendStringInfo(path, "%s/%s", node->localdir, RIPPLE_REFRESH_COMPLETE);
        while(!DirExist(path->data))
        {
            if(true == g_gotsigterm)
            {
                deleteStringInfo(path);
                return true;
            }
            /* 创建目录 */
            MakeDir(path->data);
        }

        appendStringInfo(path, "/%s_%s_%u_%u", refresh->schema, refresh->table, refresh->shards, refresh->shardnum);

        if (durable_rename(node->localpath, path->data, RLOG_DEBUG) != 0) 
        {
            elog(RLOG_WARNING, "Error renaming file %s", node->localpath);
            deleteStringInfo(path);
            return false;
        }
    }
    resetStringInfo(path);
    appendStringInfo(path, "%s.check", node->localpath);
    durable_unlink(path->data, RLOG_DEBUG);

    deleteStringInfo(path);

    return true;
}

/* 创建本地文件夹 */
static void ripple_filetransfer_pump_createlocaldir(ripple_filetransfernode* node)
{
    if (NULL == node->localdir)
    {
        return;
    }

    while(!DirExist(node->localdir))
    {
        if(true == g_gotsigterm)
        {
            return;
        }
        /* 创建目录 */
        MakeDir(node->localdir);
    }

    return;
}

/* 创建增量空文件，防止写入错误 */
static bool ripple_filetransfer_pump_increment_createfile(ripple_filetransfernode* node)
{
    int index = 0;
    int fd = 0;
    int blockcnt = 0;
    uint64 maxsize = 0;
    struct stat st;
    char* localpath = NULL;
    char tmppath[RIPPLE_MAXPATH] = { 0 };
    uint8   block[RIPPLE_FILE_BUFFER_SIZE] = { 0 };

    if (node->type > RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_INC)
    {
        return true;
    }

    while(!DirExist(node->localdir))
    {
        if(true == g_gotsigterm)
        {
            return true;
        }
        /* 创建目录 */
        MakeDir(node->localdir);
    }

    localpath = node->localpath;

    /* 创建临时文件 */
    snprintf(tmppath, RIPPLE_MAXPATH, "%s.tmp", localpath);
    unlink(tmppath);

    /* 校验文件是否存在，不存在创建文件 */
    if(0 != stat(localpath, &st))
    {
        fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | RIPPLE_BINARY);
        if (fd < 0)
        {
            elog(RLOG_WARNING, "can not open file %s", tmppath);
            return false;
        }

        maxsize = ((uint64)guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE) * (uint64)1024 * (uint64)1024);

        blockcnt = (maxsize / RIPPLE_FILE_BUFFER_SIZE);

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
        if (durable_rename(tmppath, localpath, RLOG_WARNING) != 0)
        {
            elog(RLOG_WARNING, "Error renaming file %s", strerror(errno));
            return false;
        }
    }

    return true;
}

ripple_filetransfer_pump* ripple_filetransfer_pump_init(void)
{
    char* ssl = NULL;
    ripple_filetransfer_pump* ftransfer = NULL;
    ftransfer = (ripple_filetransfer_pump*)rmalloc0(sizeof(ripple_filetransfer_pump));
    if(NULL == ftransfer)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(ftransfer, 0, '\0', sizeof(ripple_filetransfer_pump));

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
void* ripple_filetransfer_pump_main(void *args)
{
    int timeout = 0;
    ripple_thrnode* thrnode = NULL;
    ripple_filetransfernode* node = NULL;
    ripple_filetransfer_pump* ftransfer = NULL;

    thrnode = (ripple_thrnode*)args;

    ftransfer = (ripple_filetransfer_pump* )thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump gap exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    ripple_filetransfer_ftp_global_init();

    while(1)
    {
        /* 
         * 首先判断是否接收到退出信号
         *  对于子管理线程，收到 TERM 信号有两种场景:
         *  1、子管理线程的上级常驻线程退出
         *  2、接收到了退出标识
         * 
         * 上述两种场景, 都不需要子管理线程设置工作线程为 FREE 状态
         */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        node = (ripple_filetransfernode*)ripple_queue_get(ftransfer->filetransfernode, &timeout);
        if(NULL == node)
        {
            /* 需要退出，等待 worknode->status 变为 RIPPLE_WORK_STATUS_TERM 后退出*/
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            /* 设置为退出状态 */
            elog(RLOG_WARNING, "pump gap thread get task from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        ripple_filetransfer_pump_createlocaldir(node);

        ripple_filetransfer_ftp_relativepath_set(ftransfer->filetransfer, node);

        if (false == ripple_filetransfer_pump_increment_createfile(node))
        {
            ripple_filetransfer_node_add((void*)ftransfer->filetransfernode, node);
            continue;
        }

        if (RIPPLE_FILETRANSFERNODE_TYPE_DELETEFILE == node->type)
        {
            ripple_filetransfer_pump_delete_servefile(ftransfer->filetransfer, node);
            rfree(node);
            continue;
        }

        if (RIPPLE_FILETRANSFERNODE_TYPE_DELETEDIR == node->type)
        {
            ripple_filetransfer_ftp_removedir((ripple_filetransfer *)ftransfer->filetransfer, ftransfer->filetransfer->relativepath);
            rfree(node);
            continue;
        }

        if (false == ripple_filetransfer_ftp_download((ripple_filetransfer *)ftransfer->filetransfer, node->localpath))
        {
            ripple_filetransfer_node_add((void*)ftransfer->filetransfernode, node);
            continue;
        }

        if (false == ripple_filetransfer_pump_checkrefreshfile(ftransfer->filetransfer, node))
        {
            ripple_filetransfer_node_add((void*)ftransfer->filetransfernode, node);
            continue;
        }

        /* refresh 添加每个分片下载任务 */
        ripple_filetransfer_pump_refreshdownload_add(ftransfer, node);

        rfree(node);
    }

    ripple_filetransfer_ftp_global_cleanup();
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_filetransfer_pump_free(ripple_filetransfer_pump* ftransfer)
{
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
