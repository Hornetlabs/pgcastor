#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "port/thread/ripple_thread.h"
#include "misc/ripple_misc_stat.h"

/* 加载 decode.stat 文件 */
void ripple_misc_stat_loaddecode(ripple_capturebase* decodebase)
{
    int     r;
    int     fd;
    char    path[RIPPLE_MAXPATH] = { 0 };

    if(NULL == decodebase)
    {
        elog(RLOG_ERROR, "argv error");
    }

    snprintf(path, RIPPLE_MAXPATH, "%s/%s", RIPPLE_STAT, RIPPLE_STAT_DECODE);
    rmemset1(decodebase, 0, '\0', sizeof(ripple_capturebase));

    /* 在磁盘中加载数据 */
    fd = BasicOpenFile(path, O_RDWR | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", path);
    }

    /* 读取文件 */
    r = FileRead(fd, (char*)decodebase, sizeof(ripple_capturebase));
    if(r != sizeof(ripple_capturebase))
    {
        if (r < 0)
        {
            elog(RLOG_ERROR, "could not read file %s", path);
        }
        else
        {
            elog(RLOG_ERROR, "could not read file %s : %d of %zu",
                                path,
                                r,
                                sizeof(ripple_capturebase));
        }
    }

    FileClose(fd);
    return;
}

/*
 * 将 decode 文件落盘
 * 参数:
 *  decodebase              待落盘的数据
 *  *pfd                    即是入参也是出参，当没有打开过，那么打开并将打开的描述符返回
 *  lock                    是否需要加锁
 *                          true 需要加锁
 *                          false 不需要加锁
 *                          当前此函数会在三处调用 解析事务日志 格式化事务 trail文件落盘
*/
void ripple_misc_stat_decodewrite(ripple_capturebase* decodebase, int* pfd)
{
    int     fd;
    char    path[RIPPLE_MAXPATH] = {'\0'};
    char buffer[RIPPLE_DECODE_STAT] = { 0 };    /* need not be aligned */

    if(NULL == decodebase)
    {
        return;
    }

    if(NULL == pfd || -1 == *pfd)
    {
        snprintf(path, RIPPLE_MAXPATH, "%s/%s", RIPPLE_STAT, RIPPLE_STAT_DECODE);
        rmemset1(buffer, 0, 0, RIPPLE_DECODE_STAT);
    }
    else
    {
        fd = *pfd;
    }
    
    rmemcpy1(buffer, 0, decodebase, sizeof(ripple_capturebase));
    if(NULL == pfd || -1 == *pfd)
    {
        fd = BasicOpenFile(path,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);
        if (fd < 0)
        {
            elog(RLOG_ERROR, "could not create file %s", path);
        }

        if(NULL != pfd && -1 == *pfd)
        {
            *pfd = fd;
        }
    }

    if (RIPPLE_CONTROL_FILE_SIZE != FilePWrite(fd, buffer, RIPPLE_CONTROL_FILE_SIZE, 0))
    {
        elog(RLOG_ERROR, "could not write to file %s", path);
    }

    if(0 != FileSync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", path);
    }

    if(NULL == pfd)
    {
        if(FileClose(fd))
        {
            elog(RLOG_ERROR, "could not close file %s", path);
        }
    }
}

/* 初始化 */
static ripple_capturebase* ripple_misc_stat_decodeinit(void)
{
    ripple_capturebase*     decodebase = NULL;

    decodebase = (ripple_capturebase *)rmalloc1(sizeof(ripple_capturebase));
    if(NULL == decodebase)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset1(decodebase, 0, 0, sizeof(ripple_capturebase));

    decodebase->confirmedlsn = InvalidXLogRecPtr;
    decodebase->restartlsn = InvalidXLogRecPtr;
    decodebase->fileid = 0;
    decodebase->fileoffset = 0;

    return decodebase;
}

/* 加载 collector.stat 文件 */
void ripple_misc_stat_loadcollector(ripple_collectorbase* collectorbase, char* name)
{
    int     r;
    int     fd;
    char    path[RIPPLE_MAXPATH] = { 0 };

    if(NULL == collectorbase)
    {
        elog(RLOG_ERROR, "argv error");
    }

    snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s", name, RIPPLE_STAT, RIPPLE_STAT_COLLECTOR);
    rmemset1(collectorbase, 0, '\0', sizeof(ripple_collectorbase));

    /* 在磁盘中加载数据 */
    fd = BasicOpenFile(path, O_RDWR | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", path);
    }

    /* 读取文件 */
    r = FileRead(fd, (char*)collectorbase, sizeof(ripple_collectorbase));
    if(r != sizeof(ripple_collectorbase))
    {
        if (r < 0)
        {
            elog(RLOG_ERROR, "could not read file %s", path);
        }
        else
        {
            elog(RLOG_ERROR, "could not read file %s : %d of %zu",
                                path,
                                r,
                                sizeof(ripple_collectorbase));
        }
    }

    FileClose(fd);
    return;
}

void ripple_misc_stat_collectorwrite(ripple_collectorbase* collectorbase, char* name, int* pfd)
{
    int     fd;
    char    path[RIPPLE_MAXPATH] = {'\0'};
    char buffer[RIPPLE_DECODE_STAT] = { 0 };

    if(NULL == collectorbase)
    {
        return;
    }

    if(NULL == pfd || -1 == *pfd)
    {
        snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s", name, RIPPLE_STAT, RIPPLE_STAT_COLLECTOR);
        rmemset1(buffer, 0, 0, RIPPLE_DECODE_STAT);
    }
    else
    {
        fd = *pfd;
    }
    
    rmemcpy1(buffer, 0, collectorbase, sizeof(ripple_collectorbase));

    if(NULL == pfd || -1 == *pfd)
    {
        fd = BasicOpenFile(path,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);
        if (fd < 0)
        {
            elog(RLOG_ERROR, "could not create file %s", path);
        }

        if(NULL != pfd && -1 == *pfd)
        {
            *pfd = fd;
        }
    }

    if (RIPPLE_CONTROL_FILE_SIZE != FilePWrite(fd, buffer, RIPPLE_CONTROL_FILE_SIZE, 0))
    {
        elog(RLOG_ERROR, "could not write to file %s", path);
    }

    if(0 != FileSync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", path);
    }

    if(NULL == pfd)
    {
        if(FileClose(fd))
        {
            elog(RLOG_ERROR, "could not close file %s", path);
        }
    }
}

/* collector初始化 */
static ripple_collectorbase* ripple_misc_stat_collectorinit(void)
{
    ripple_collectorbase*   collectorbase = NULL;

    collectorbase = (ripple_collectorbase *)rmalloc1(sizeof(ripple_collectorbase));
    if(NULL == collectorbase)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset1(collectorbase, 0, 0, sizeof(ripple_collectorbase));

    collectorbase->redolsn = 0;
    collectorbase->restartlsn = 0;
    collectorbase->confirmedlsn = 0;
    collectorbase->pfileid = 0;
    collectorbase->cfileid = 0;
    collectorbase->coffset = 0;

    return collectorbase;
}


/* capture状态信息初始化 */
void ripple_misc_capturestat_init(void)
{
    ripple_capturebase* decodebase = NULL;

    /* 获取复制槽信息 */
    decodebase = ripple_misc_stat_decodeinit();

    ripple_misc_stat_decodewrite(decodebase, NULL);
    rfree(decodebase);
}

/* collector状态信息初始化 */
void ripple_misc_collectorstat_init(char* name)
{
    ripple_collectorbase* collectorbase = NULL;

    /* 初始化collector状态信息 */
    collectorbase = ripple_misc_stat_collectorinit();

    ripple_misc_stat_collectorwrite(collectorbase, name, NULL);
    rfree(collectorbase);
}
