#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "port/thread/thread.h"
#include "misc/misc_stat.h"

/* 加载 decode.stat 文件 */
void misc_stat_loaddecode(capturebase* decodebase)
{
    int     r;
    int     fd;
    char    path[MAXPATH] = { 0 };

    if(NULL == decodebase)
    {
        elog(RLOG_ERROR, "argv error");
    }

    snprintf(path, MAXPATH, "%s/%s", STAT, STAT_DECODE);
    rmemset1(decodebase, 0, '\0', sizeof(capturebase));

    /* 在磁盘中加载数据 */
    fd = osal_basic_open_file(path, O_RDWR | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", path);
    }

    /* 读取文件 */
    r = osal_file_read(fd, (char*)decodebase, sizeof(capturebase));
    if(r != sizeof(capturebase))
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
                                sizeof(capturebase));
        }
    }

    osal_file_close(fd);
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
void misc_stat_decodewrite(capturebase* decodebase, int* pfd)
{
    int     fd = -1;
    char    path[MAXPATH] = {'\0'};
    char buffer[DECODE_STAT] = { 0 };    /* need not be aligned */

    if(NULL == decodebase)
    {
        return;
    }

    if(NULL == pfd || -1 == *pfd)
    {
        snprintf(path, MAXPATH, "%s/%s", STAT, STAT_DECODE);
        rmemset1(buffer, 0, 0, DECODE_STAT);
    }
    else
    {
        fd = *pfd;
    }
    
    rmemcpy1(buffer, 0, decodebase, sizeof(capturebase));
    if(NULL == pfd || -1 == *pfd)
    {
        fd = osal_basic_open_file(path,
                        O_RDWR | O_CREAT | BINARY);
        if (fd < 0)
        {
            elog(RLOG_ERROR, "could not create file %s", path);
        }

        if(NULL != pfd && -1 == *pfd)
        {
            *pfd = fd;
        }
    }

    if (CONTROL_FILE_SIZE != osal_file_pwrite(fd, buffer, CONTROL_FILE_SIZE, 0))
    {
        elog(RLOG_ERROR, "could not write to file %s", path);
    }

    if(0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", path);
    }

    if(NULL == pfd)
    {
        if(osal_file_close(fd))
        {
            elog(RLOG_ERROR, "could not close file %s", path);
        }
    }
}

/* 初始化 */
static capturebase* misc_stat_decodeinit(void)
{
    capturebase*     decodebase = NULL;

    decodebase = (capturebase *)rmalloc1(sizeof(capturebase));
    if(NULL == decodebase)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset1(decodebase, 0, 0, sizeof(capturebase));

    decodebase->confirmedlsn = InvalidXLogRecPtr;
    decodebase->restartlsn = InvalidXLogRecPtr;
    decodebase->fileid = 0;
    decodebase->fileoffset = 0;

    return decodebase;
}

/* capture状态信息初始化 */
void misc_capturestat_init(void)
{
    capturebase* decodebase = NULL;

    /* 获取复制槽信息 */
    decodebase = misc_stat_decodeinit();

    misc_stat_decodewrite(decodebase, NULL);
    rfree(decodebase);
}
