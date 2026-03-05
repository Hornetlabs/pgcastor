#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "serial/ripple_serial.h"

/*
 * 填充 buffer 数据
*/
static void ripple_serial_readdatafromfile(ripple_file_buffer* fbuffer)
{
    /*
     * 在重启时，需要查看当前的 buffer 是否需要在磁盘中读取出来
     *  start 为 0 时， 不需要读取
     */
    int fd = 0;
    int rlen = 0;
    uint32  blkoffset = 0;
    uint64 foffset = 0;
    ripple_ff_fileinfo* finfo = NULL;

    struct stat st;
    char    path[RIPPLE_MAXPATH] = { 0 };

    if(0 == fbuffer->start)
    {
        return;
    }

    finfo = (ripple_ff_fileinfo*)fbuffer->privdata;
    /* 生成路径 */
    snprintf(path, RIPPLE_MAXPATH, RIPPLE_STORAGE_TRAIL_DIR "/%016lX", finfo->fileid);

    /* 换算偏移量 */
    foffset = ((finfo->blknum - 1) * RIPPLE_FILE_BUFFER_SIZE);

    /* 打开文件读取文件 */
    /* 校验文件是否存在，存在则打开 */
    if(0 != stat(path, &st))
    {
        elog(RLOG_ERROR, "file not exist, please recheck %s, fbuffer->start:%lu",
                            path,
                            fbuffer->start);
    }

    /* 打开文件 */
    fd = BasicOpenFile(path, O_RDWR | RIPPLE_BINARY);
    if (fd  < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", path, strerror(errno));
    }

    /* 读取数据 */
    blkoffset = 0;
    rlen = RIPPLE_FILE_BUFFER_SIZE;
    while(0 != rlen)
    {
        rlen = FilePRead(fd, (char*)(fbuffer->data + blkoffset), rlen, foffset);
        if(0 > rlen)
        {
            if(true == g_gotsigterm)
            {
                return;
            }
            elog(RLOG_ERROR, "pread file:%s error:%s", path, strerror(errno));
        }
        foffset += rlen;
        blkoffset += rlen;
        rlen = (RIPPLE_FILE_BUFFER_SIZE - rlen);
    }

    FileClose(fd);
    return;
}


void ripple_serialstate_init(ripple_serialstate* serialstate)
{
    /* 初始化落盘文件接口 */
    serialstate->ffsmgrstate = (ripple_ffsmgr_state*)rmalloc0(sizeof(ripple_ffsmgr_state));
    if(NULL == serialstate->ffsmgrstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(serialstate->ffsmgrstate, 0, '\0', sizeof(ripple_ffsmgr_state));
    return;
}

ripple_file_buffers* ripple_serialstate_getfilebuffer(void* privdata)
{
    ripple_serialstate* serialstate = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "serialwork getfilebuffer exception, privdata point is NULL");
    }

    serialstate = (ripple_serialstate*)privdata;

    return serialstate->txn2filebuffer;
}

void ripple_serialstate_fbuffer_set(ripple_serialstate* serialstate, uint64 fileid, uint64 fileoffset, FullTransactionId xid)
{
    int             bufid = 0;
    int timeout = 0;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;

    /* 获取 bufid */
    while(1)
    {
        bufid = ripple_file_buffer_get(serialstate->txn2filebuffer, &timeout);
        if(RIPPLE_INVALID_BUFFERID == bufid)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return;
        }
        break;
    }

    fbuffer = ripple_file_buffer_getbybufid(serialstate->txn2filebuffer, bufid);
    if(NULL == fbuffer->privdata)
    {
        finfo = (ripple_ff_fileinfo*)rmalloc0(sizeof(ripple_ff_fileinfo));
        if(NULL == finfo)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(finfo, 0, '\0', sizeof(ripple_ff_fileinfo));
        fbuffer->privdata = (void*)finfo;
    }
    else
    {
        finfo = (ripple_ff_fileinfo*)fbuffer->privdata;
    }

    finfo->fileid = fileid;
    finfo->blknum = (fileoffset / RIPPLE_FILE_BUFFER_SIZE);
    finfo->blknum++;
    finfo->xid = xid;
    fbuffer->start = (fileoffset%RIPPLE_FILE_BUFFER_SIZE);

    elog(RLOG_DEBUG, "ffsmgr_set fileid:%lu, fileoffset:%lu, %lu", fileid, fileoffset, fbuffer->start);

    /* 读取时以 fbuffer->start为准 */
    if(0 != fbuffer->start)
    {
        /* 填充 fbuffer 数据 */
        ripple_serial_readdatafromfile(fbuffer);
    }
    serialstate->ffsmgrstate->bufid = bufid;
}

/* ffsmgrstate信息填充 */
void ripple_serialstate_ffsmgr_set(ripple_serialstate* serialstate, int serialtype)
{
    int             mbytes = 0;
    uint64          bytes = 0;

    serialstate->ffsmgrstate->status = RIPPLE_FFSMGR_STATUS_NOP;
    serialstate->ffsmgrstate->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);

    /* 换算文件的大小 */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    bytes = RIPPLE_MB2BYTE(mbytes);
    serialstate->ffsmgrstate->maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);

    ripple_ffsmgr_init(serialtype, serialstate->ffsmgrstate);

    /* 调用初始化接口 */
    serialstate->ffsmgrstate->ffsmgr->ffsmgr_init(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL,
                                                serialstate->ffsmgrstate);
}


/* 资源回收 */
void ripple_serialstate_destroy(ripple_serialstate* serialstate)
{
    if(NULL == serialstate)
    {
        return;
    }

    /* smgrstate 管理单元释放 */
    if (serialstate->ffsmgrstate)
    {
        if (serialstate->ffsmgrstate->ffsmgr)
        {
            serialstate->ffsmgrstate->ffsmgr->ffsmgr_free(RIPPLE_FFSMGR_IF_OPTYPE_SERIAL, serialstate->ffsmgrstate);
        }
        rfree(serialstate->ffsmgrstate);
    }

    serialstate->txn2filebuffer = NULL;
}
