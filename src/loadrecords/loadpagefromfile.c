#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/mpage/mpage.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"

/* 初始化 */
loadpage* loadpagefromfile_init(void)
{
    loadpagefromfile* lpff = NULL;

    lpff = rmalloc0(sizeof(loadpagefromfile));
    if(NULL == lpff)
    {
        elog(RLOG_WARNING, "load page from file init error");
        return NULL;
    }
    rmemset0(lpff, 0, '\0', sizeof(loadpagefromfile));
    lpff->fd = -1;
    lpff->loadpage.blksize = 0;
    lpff->loadpage.filesize = 0;
    lpff->loadpage.type = LOADPAGE_TYPE_FILE;
    rmemset1(lpff->fdir, 0, '\0', MAXPATH);
    lpff->fileno = 0;
    lpff->foffset = 0;
    rmemset1(lpff->fpath, 0, '\0', MAXPATH);
    return (loadpage*)lpff;
}

/* 设置文件的存储路径 */
bool loadpagefromfile_setfdir(loadpage* loadpage, char* fsource)
{
    loadpagefromfile* lpff = NULL;

    lpff = (loadpagefromfile*)loadpage;
    rmemcpy1(lpff->fdir, 0, fsource, strlen(fsource));
    return true;
}

void loadpagefromfile_settype(loadpage* loadpage, int type)
{
    loadpagefromfile* lpff = NULL;

    lpff = (loadpagefromfile*)loadpage;

    lpff->filetype = type;
}

/* 设置解析的起点 */
void loadpagefromfile_setstartpos(loadpage* loadpage, recpos pos)
{
    loadpagefromfile* lpff = NULL;

    lpff = (loadpagefromfile*)loadpage;

    if (RECPOS_TYPE_WAL == pos.wal.type)
    {
        /* foffset存放lsn */
        lpff->foffset = pos.wal.lsn;

        /* fileno存放timeline */
        lpff->fileno = pos.wal.timeline;
    }
    else
    {
        /* 目前很多地方的type没改, 所以NOP和TRAIL都认为是TRAIL */
        lpff->foffset = pos.trail.offset;
        if(pos.trail.fileid == lpff->fileno)
        {
            return;
        }
        lpff->fileno = pos.trail.fileid;
    }

    if(-1 != lpff->fd)
    {
        osal_file_close(lpff->fd);
        lpff->fd = -1;
    }
}

/* 加载页 */
bool loadpagefromfile_loadpage(loadpage* loadpage, mpage* mp)
{
    int     rlen = 0;
    uint32  blkoffset = 0;
    uint64  foffset = 0;
    loadpagefromfile* lpff = NULL;

    lpff = (loadpagefromfile*)loadpage;
    lpff->loadpage.error = ERROR_SUCCESS;
    if (-1 == lpff->fd)
    {
        rmemset1(lpff->fpath, 0, '\0', MAXPATH);

        if (LOADPAGEFROMFILE_TYPE_WAL == lpff->filetype)
        {
            uint64_t sendSegNo = (lpff->foffset) / (lpff->loadpage.filesize);
            snprintf(lpff->fpath, ABSPATH, "%s/%08X%08X%08X", lpff->fdir,
                                                                     (uint32) lpff->fileno,
                                                                     (uint32) ((sendSegNo) / ((0x100000000UL) / (lpff->loadpage.filesize))),
                                                                     (uint32) ((sendSegNo) % ((0x100000000UL) / (lpff->loadpage.filesize))));
        }
        else
        {
            snprintf(lpff->fpath, ABSPATH, "%s/%016lX",
                                                        lpff->fdir,
                                                        lpff->fileno);
        }

        /* 在磁盘中加载数据 */
        lpff->fd = osal_basic_open_file(lpff->fpath, O_RDONLY | BINARY);
        if (lpff->fd < 0)
        {
            if(ENOENT == errno)
            {
                lpff->loadpage.error = ERROR_NOENT;
                return false;
            }
            elog(RLOG_WARNING, "open file error, %s", strerror(errno));
            lpff->loadpage.error = ERROR_OPENFILEERROR;
            return false;
        }
    }

    if(NULL == mp->data)
    {
        mp->data = rmalloc0(lpff->loadpage.blksize);
        if(NULL == mp->data)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            lpff->loadpage.error = ERROR_OOM;
            return false;
        }
        mp->size = lpff->loadpage.blksize;
    }
    rmemset0(mp->data, 0, '\0', mp->size);
    mp->doffset = 0;

    /* 读取数据 */
    rlen = lpff->loadpage.blksize ;
    blkoffset = 0;

    /* 每次读取一个块, 在块的开头读取 */
    foffset = lpff->foffset;

    if (LOADPAGEFROMFILE_TYPE_WAL == lpff->filetype)
    {
        /* 读取wal先单独计算lsn到文件开始的偏移 */
        foffset = (foffset & LOADPAGEBLKSIZEMASK(lpff->loadpage.filesize));
    }

    /* 计算块开始的偏移 */
    foffset -= (foffset & LOADPAGEBLKSIZEMASK(lpff->loadpage.blksize));

    while (0 != rlen)
    {
        rlen = osal_file_pread(lpff->fd, (char*)(mp->data + blkoffset), rlen, foffset);
        if (0 > rlen)
        {
            elog(RLOG_WARNING, "pread file:%s error:%s", lpff->fpath, strerror(errno));
            lpff->loadpage.error = ERROR_READFILEERROR;
            return false;
        }
        foffset += rlen;
        blkoffset += rlen;
        rlen = (lpff->loadpage.blksize - rlen);
    }

    return true;
}

void loadpagefromfile_close(loadpage* loadpage)
{
    loadpagefromfile* lpff = NULL;

    if(NULL == loadpage)
    {
        return;
    }

    lpff = (loadpagefromfile*)loadpage;
    if(-1 != lpff->fd)
    {
        osal_file_close(lpff->fd);
        lpff->fd = -1;
    }

    return;
}

/* 释放 */
void loadpagefromfile_free(loadpage* loadpage)
{
    loadpagefromfile* lpff = NULL;

    if(NULL == loadpage)
    {
        return;
    }

    lpff = (loadpagefromfile*)loadpage;
    if(-1 != lpff->fd)
    {
        osal_file_close(lpff->fd);
        lpff->fd = -1;
    }

    rfree(lpff);
}


