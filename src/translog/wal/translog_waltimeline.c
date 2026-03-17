#include "app_incl.h"
#include "port/file/fd.h"
#include "translog/wal/translog_waltimeline.h"

/*
 * 查看时间线文件是否存在
 *  存在返回 true
 *  不存在返回 false
 */
bool translog_waltimeline_exist(char* walpath, TimeLineID tli)
{
    char tlipath[ABSPATH]                = { 0 };

    snprintf(tlipath, ABSPATH, "%s/%08X.history", walpath, tli);
    if(true == osal_file_exist(tlipath))
    {
        return true;
    }

    return false;
}

/* 写时间线文件 */
bool translog_waltimeline_flush(char* walpath, char* filename, char* content)
{
    int fd = -1;
    int size = 0;
    char tlipath[ABSPATH]                = { 0 };
    char tmptlipath[ABSPATH]             = { 0 };

    size = strlen(content);
    snprintf(tlipath, ABSPATH, "%s/%s", walpath, filename);
    snprintf(tmptlipath, ABSPATH, "%s/%s.tmp", walpath, filename);

    /* 清理临时文件 */
    osal_durable_unlink(tmptlipath, RLOG_DEBUG);

    /* 打开临时文件 */
    fd = osal_basic_open_file(tmptlipath, O_RDWR | O_CREAT| BINARY);
    if (fd  < 0)
    {
        elog(RLOG_WARNING, "write timeline open file %s error %s", tmptlipath, strerror(errno));
        return false;
    }

    if (0 > osal_file_pwrite(fd, content, size, 0))
    {
        osal_file_close(fd);
        return false;
    }

    osal_file_close(fd);

    /* 重命名 */
    osal_durable_rename(tmptlipath, tlipath, RLOG_INFO);
    return true;
}
