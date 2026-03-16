#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "translog/wal/ripple_translog_waltimeline.h"

/*
 * 查看时间线文件是否存在
 *  存在返回 true
 *  不存在返回 false
 */
bool ripple_translog_waltimeline_exist(char* walpath, TimeLineID tli)
{
    char tlipath[RIPPLE_ABSPATH]                = { 0 };

    snprintf(tlipath, RIPPLE_ABSPATH, "%s/%08X.history", walpath, tli);
    if(true == FileExist(tlipath))
    {
        return true;
    }

    return false;
}

/* 写时间线文件 */
bool ripple_translog_waltimeline_flush(char* walpath, char* filename, char* content)
{
    int fd = -1;
    int size = 0;
    char tlipath[RIPPLE_ABSPATH]                = { 0 };
    char tmptlipath[RIPPLE_ABSPATH]             = { 0 };

    size = strlen(content);
    snprintf(tlipath, RIPPLE_ABSPATH, "%s/%s", walpath, filename);
    snprintf(tmptlipath, RIPPLE_ABSPATH, "%s/%s.tmp", walpath, filename);

    /* 清理临时文件 */
    durable_unlink(tmptlipath, RLOG_DEBUG);

    /* 打开临时文件 */
    fd = BasicOpenFile(tmptlipath, O_RDWR | O_CREAT| RIPPLE_BINARY);
    if (fd  < 0)
    {
        elog(RLOG_WARNING, "write timeline open file %s error %s", tmptlipath, strerror(errno));
        return false;
    }

    if (0 > FilePWrite(fd, content, size, 0))
    {
        FileClose(fd);
        return false;
    }

    FileClose(fd);

    /* 重命名 */
    durable_rename(tmptlipath, tlipath, RLOG_INFO);
    return true;
}
