#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/mpage/mpage.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"

/* 初始化 */
ripple_loadpage* ripple_loadpagefromfile_init(void)
{
    ripple_loadpagefromfile* loadpagefromfile = NULL;

    loadpagefromfile = rmalloc0(sizeof(ripple_loadpagefromfile));
    if(NULL == loadpagefromfile)
    {
        elog(RLOG_WARNING, "load page from file init error");
        return NULL;
    }
    rmemset0(loadpagefromfile, 0, '\0', sizeof(ripple_loadpagefromfile));
    loadpagefromfile->fd = -1;
    loadpagefromfile->loadpage.blksize = 0;
    loadpagefromfile->loadpage.filesize = 0;
    loadpagefromfile->loadpage.type = RIPPLE_LOADPAGE_TYPE_FILE;
    rmemset1(loadpagefromfile->fdir, 0, '\0', RIPPLE_MAXPATH);
    loadpagefromfile->fileno = 0;
    loadpagefromfile->foffset = 0;
    rmemset1(loadpagefromfile->fpath, 0, '\0', RIPPLE_MAXPATH);
    return (ripple_loadpage*)loadpagefromfile;
}

/* 设置文件的存储路径 */
bool ripple_loadpagefromfile_setfdir(ripple_loadpage* loadpage, char* fsource)
{
    ripple_loadpagefromfile* loadpagefromfile = NULL;

    loadpagefromfile = (ripple_loadpagefromfile*)loadpage;
    rmemcpy1(loadpagefromfile->fdir, 0, fsource, strlen(fsource));
    return true;
}

void ripple_loadpagefromfile_settype(ripple_loadpage* loadpage, int type)
{
    ripple_loadpagefromfile* loadpagefromfile = NULL;

    loadpagefromfile = (ripple_loadpagefromfile*)loadpage;

    loadpagefromfile->filetype = type;
}

/* 设置解析的起点 */
void ripple_loadpagefromfile_setstartpos(ripple_loadpage* loadpage, ripple_recpos pos)
{
    ripple_loadpagefromfile* loadpagefromfile = NULL;

    loadpagefromfile = (ripple_loadpagefromfile*)loadpage;

    if (RIPPLE_RECPOS_TYPE_WAL == pos.wal.type)
    {
        /* foffset存放lsn */
        loadpagefromfile->foffset = pos.wal.lsn;

        /* fileno存放timeline */
        loadpagefromfile->fileno = pos.wal.timeline;
    }
    else
    {
        /* 目前很多地方的type没改, 所以NOP和TRAIL都认为是TRAIL */
        loadpagefromfile->foffset = pos.trail.offset;
        if(pos.trail.fileid == loadpagefromfile->fileno)
        {
            return;
        }
        loadpagefromfile->fileno = pos.trail.fileid;
    }

    if(-1 != loadpagefromfile->fd)
    {
        FileClose(loadpagefromfile->fd);
        loadpagefromfile->fd = -1;
    }
}

/* 加载页 */
bool ripple_loadpagefromfile_loadpage(ripple_loadpage* loadpage, mpage* mp)
{
    int     rlen = 0;
    uint32  blkoffset = 0;
    uint64  foffset = 0;
    ripple_loadpagefromfile* loadpagefromfile = NULL;

    loadpagefromfile = (ripple_loadpagefromfile*)loadpage;
    loadpagefromfile->loadpage.error = RIPPLE_ERROR_SUCCESS;
    if (-1 == loadpagefromfile->fd)
    {
        rmemset1(loadpagefromfile->fpath, 0, '\0', RIPPLE_MAXPATH);

        if (RIPPLE_LOADPAGEFROMFILE_TYPE_WAL == loadpagefromfile->filetype)
        {
            uint64_t sendSegNo = (loadpagefromfile->foffset) / (loadpagefromfile->loadpage.filesize);
            snprintf(loadpagefromfile->fpath, RIPPLE_ABSPATH, "%s/%08X%08X%08X", loadpagefromfile->fdir,
                                                                     (uint32) loadpagefromfile->fileno,
                                                                     (uint32) ((sendSegNo) / ((0x100000000UL) / (loadpagefromfile->loadpage.filesize))),
                                                                     (uint32) ((sendSegNo) % ((0x100000000UL) / (loadpagefromfile->loadpage.filesize))));
        }
        else
        {
            snprintf(loadpagefromfile->fpath, RIPPLE_ABSPATH, "%s/%016lX",
                                                        loadpagefromfile->fdir,
                                                        loadpagefromfile->fileno);
        }

        /* 在磁盘中加载数据 */
        loadpagefromfile->fd = BasicOpenFile(loadpagefromfile->fpath, O_RDONLY | RIPPLE_BINARY);
        if (loadpagefromfile->fd < 0)
        {
            if(ENOENT == errno)
            {
                loadpagefromfile->loadpage.error = RIPPLE_ERROR_NOENT;
                return false;
            }
            elog(RLOG_WARNING, "open file error, %s", strerror(errno));
            loadpagefromfile->loadpage.error = RIPPLE_ERROR_OPENFILEERROR;
            return false;
        }
    }

    if(NULL == mp->data)
    {
        mp->data = rmalloc0(loadpagefromfile->loadpage.blksize);
        if(NULL == mp->data)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            loadpagefromfile->loadpage.error = RIPPLE_ERROR_OOM;
            return false;
        }
        mp->size = loadpagefromfile->loadpage.blksize;
    }
    rmemset0(mp->data, 0, '\0', mp->size);
    mp->doffset = 0;

    /* 读取数据 */
    rlen = loadpagefromfile->loadpage.blksize ;
    blkoffset = 0;

    /* 每次读取一个块, 在块的开头读取 */
    foffset = loadpagefromfile->foffset;

    if (RIPPLE_LOADPAGEFROMFILE_TYPE_WAL == loadpagefromfile->filetype)
    {
        /* 读取wal先单独计算lsn到文件开始的偏移 */
        foffset = (foffset & LOADPAGEBLKSIZEMASK(loadpagefromfile->loadpage.filesize));
    }

    /* 计算块开始的偏移 */
    foffset -= (foffset & LOADPAGEBLKSIZEMASK(loadpagefromfile->loadpage.blksize));

    while (0 != rlen)
    {
        rlen = FilePRead(loadpagefromfile->fd, (char*)(mp->data + blkoffset), rlen, foffset);
        if (0 > rlen)
        {
            elog(RLOG_WARNING, "pread file:%s error:%s", loadpagefromfile->fpath, strerror(errno));
            loadpagefromfile->loadpage.error = RIPPLE_ERROR_READFILEERROR;
            return false;
        }
        foffset += rlen;
        blkoffset += rlen;
        rlen = (loadpagefromfile->loadpage.blksize - rlen);
    }

    return true;
}

void ripple_loadpagefromfile_close(ripple_loadpage* loadpage)
{
    ripple_loadpagefromfile* loadpagefromfile = NULL;

    if(NULL == loadpage)
    {
        return;
    }

    loadpagefromfile = (ripple_loadpagefromfile*)loadpage;
    if(-1 != loadpagefromfile->fd)
    {
        FileClose(loadpagefromfile->fd);
        loadpagefromfile->fd = -1;
    }

    return;
}

/* 释放 */
void ripple_loadpagefromfile_free(ripple_loadpage* loadpage)
{
    ripple_loadpagefromfile* loadpagefromfile = NULL;

    if(NULL == loadpage)
    {
        return;
    }

    loadpagefromfile = (ripple_loadpagefromfile*)loadpage;
    if(-1 != loadpagefromfile->fd)
    {
        FileClose(loadpagefromfile->fd);
        loadpagefromfile->fd = -1;
    }

    rfree(loadpagefromfile);
}


