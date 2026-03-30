#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "serial/serial.h"

/*
 * fill buffer data
 */
static void serial_readdatafromfile(file_buffer* fbuffer)
{
    /*
     * on restart, need to check if current buffer needs to be read from disk
     * when start is 0, no need to read
     */
    int          fd = 0;
    int          rlen = 0;
    uint32       blkoffset = 0;
    uint64       foffset = 0;
    ff_fileinfo* finfo = NULL;

    struct stat  st;
    char         path[MAXPATH] = {0};

    if (0 == fbuffer->start)
    {
        return;
    }

    finfo = (ff_fileinfo*)fbuffer->privdata;
    /* generate path */
    snprintf(path, MAXPATH, STORAGE_TRAIL_DIR "/%016lX", finfo->fileid);

    /* calculate offset */
    foffset = ((finfo->blknum - 1) * FILE_BUFFER_SIZE);

    /* open and read file */
    /* verify file exists, then open */
    if (0 != stat(path, &st))
    {
        elog(RLOG_ERROR, "file not exist, please recheck %s, fbuffer->start:%lu", path, fbuffer->start);
    }

    /* open file */
    fd = osal_basic_open_file(path, O_RDWR | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", path, strerror(errno));
    }

    /* read data */
    blkoffset = 0;
    rlen = FILE_BUFFER_SIZE;
    while (0 != rlen)
    {
        rlen = osal_file_pread(fd, (char*)(fbuffer->data + blkoffset), rlen, foffset);
        if (0 > rlen)
        {
            if (true == g_gotsigterm)
            {
                return;
            }
            elog(RLOG_ERROR, "pread file:%s error:%s", path, strerror(errno));
        }
        foffset += rlen;
        blkoffset += rlen;
        rlen = (FILE_BUFFER_SIZE - rlen);
    }

    osal_file_close(fd);
    return;
}

void serialstate_init(serialstate* serialstate)
{
    /* initialize flush file interface */
    serialstate->ffsmgrstate = (ffsmgr_state*)rmalloc0(sizeof(ffsmgr_state));
    if (NULL == serialstate->ffsmgrstate)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(serialstate->ffsmgrstate, 0, '\0', sizeof(ffsmgr_state));
    return;
}

file_buffers* serialstate_getfilebuffer(void* privdata)
{
    serialstate* serial_state = NULL;

    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "serialwork getfilebuffer exception, privdata point is NULL");
    }

    serial_state = (serialstate*)privdata;

    return serial_state->txn2filebuffer;
}

void serialstate_fbuffer_set(serialstate* serial_state, uint64 fileid, uint64 fileoffset, FullTransactionId xid)
{
    int          bufid = 0;
    int          timeout = 0;
    ff_fileinfo* finfo = NULL;
    file_buffer* fbuffer = NULL;

    /* get bufid */
    while (1)
    {
        bufid = file_buffer_get(serial_state->txn2filebuffer, &timeout);
        if (INVALID_BUFFERID == bufid)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return;
        }
        break;
    }

    fbuffer = file_buffer_getbybufid(serial_state->txn2filebuffer, bufid);
    if (NULL == fbuffer->privdata)
    {
        finfo = (ff_fileinfo*)rmalloc0(sizeof(ff_fileinfo));
        if (NULL == finfo)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(finfo, 0, '\0', sizeof(ff_fileinfo));
        fbuffer->privdata = (void*)finfo;
    }
    else
    {
        finfo = (ff_fileinfo*)fbuffer->privdata;
    }

    finfo->fileid = fileid;
    finfo->blknum = (fileoffset / FILE_BUFFER_SIZE);
    finfo->blknum++;
    finfo->xid = xid;
    fbuffer->start = (fileoffset % FILE_BUFFER_SIZE);

    elog(RLOG_DEBUG, "ffsmgr_set fileid:%lu, fileoffset:%lu, %lu", fileid, fileoffset, fbuffer->start);

    /* when reading, use fbuffer->start as reference */
    if (0 != fbuffer->start)
    {
        /* fill fbuffer data */
        serial_readdatafromfile(fbuffer);
    }
    serial_state->ffsmgrstate->bufid = bufid;
}

/* ffsmgrstate information fill */
void serialstate_ffsmgr_set(serialstate* serial_state, int serialtype)
{
    int    mbytes = 0;
    uint64 bytes = 0;

    serial_state->ffsmgrstate->status = FFSMGR_STATUS_NOP;
    serial_state->ffsmgrstate->compatibility = guc_getConfigOptionInt(CFG_KEY_COMPATIBILITY);

    /* calculate file size */
    mbytes = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    bytes = MB2BYTE(mbytes);
    serial_state->ffsmgrstate->maxbufid = (bytes / FILE_BUFFER_SIZE);

    ffsmgr_init(serialtype, serial_state->ffsmgrstate);

    /* call initialization interface */
    serial_state->ffsmgrstate->ffsmgr->ffsmgr_init(FFSMGR_IF_OPTYPE_SERIAL, serial_state->ffsmgrstate);
}

/* resource cleanup */
void serialstate_destroy(serialstate* serial_state)
{
    if (NULL == serial_state)
    {
        return;
    }

    /* free smgrstate management unit */
    if (serial_state->ffsmgrstate)
    {
        if (serial_state->ffsmgrstate->ffsmgr)
        {
            serial_state->ffsmgrstate->ffsmgr->ffsmgr_free(FFSMGR_IF_OPTYPE_SERIAL, serial_state->ffsmgrstate);
        }
        rfree(serial_state->ffsmgrstate);
    }

    serial_state->txn2filebuffer = NULL;
}
