#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_p2cdata.h"
#include "misc/ripple_misc_stat.h"
#include "filetransfer/ripple_filetransfer.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"

static void ripple_netmsg_onlinerefresh_p2cdata_filetransfer_add(ripple_increment_collectornetclient_state* nodesvrstate, uint64 fileid)
{
    char* uuid = NULL;
    ripple_filetransfer_onlinerefreshinc* filetransfer_inc = NULL;
    ripple_collectorincrementstate_privdatacallback* callback = NULL;
    ripple_collectornetclient_onlinerefreshinc* increamentstate = NULL;

    increamentstate = (ripple_collectornetclient_onlinerefreshinc*)nodesvrstate->data;

    /* 使用回调函数获取 collector中的filebuffers */
    callback = (ripple_collectorincrementstate_privdatacallback*)nodesvrstate->callback;

    if (false == increamentstate->upload)
    {
        return;
    }

    uuid = uuid2string(&increamentstate->onlinerefreshno);

    filetransfer_inc = ripple_filetransfer_onlinerefreshinc_init();
    ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)filetransfer_inc, fileid);
    ripple_filetransfer_upload_olincpath_set(filetransfer_inc, uuid, nodesvrstate->clientname);
    callback->collector_filetransfernode_add(nodesvrstate->privdata, (void*)filetransfer_inc);
    rfree(uuid);
    filetransfer_inc = NULL;

    return;
}


static void ripple_netmsg_onlinerefresh_p2cdata_initfile(ripple_collectornetclient_onlinerefreshinc* increamentstate, 
                                                          ripple_increment_collectornetclient_state* nodesvrstate,
                                                          ripple_ff_fileinfo* finfo)
{
    int fd = -1;
    int index = 0;
    int blockcnt = 0;
    struct stat st;
    char* uuid = NULL;
    char    tablepath[RIPPLE_MAXPATH] = { 0 };
    char    path[RIPPLE_MAXPATH] = { 0 };
    char    tmppath[RIPPLE_MAXPATH] = { 0 };
    uint8   block[RIPPLE_FILE_BUFFER_SIZE] = { 0 };

    if(-1 != increamentstate->fd)
    {
        FileClose(increamentstate->fd);
        increamentstate->fd = -1;
    }
    
    uuid = uuid2string(&increamentstate->onlinerefreshno);

    snprintf(tablepath, RIPPLE_MAXPATH, "%s/%s/%s/%s",  nodesvrstate->clientname,
                                                        RIPPLE_REFRESH_ONLINEREFRESH,
                                                        uuid,
                                                        RIPPLE_STORAGE_TRAIL_DIR);

    while(false == DirExist(tablepath))
    {
        if(true == g_gotsigterm)
        {
            rfree(uuid);
            return;
        }
        /* 创建目录 */
        MakeDir(tablepath);
    }

    /* 生成路径 */
    rmemset1(path, 0, '\0', RIPPLE_MAXPATH);
    snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s/%s/%016lX", nodesvrstate->clientname,
                                                         RIPPLE_REFRESH_ONLINEREFRESH,
                                                         uuid,
                                                         RIPPLE_STORAGE_TRAIL_DIR,
                                                         finfo->fileid);

    /* 校验文件是否存在，存在则打开 */
    if(0 == stat(path, &st))
    {
        /* 打开文件 */
        increamentstate->fd = BasicOpenFile(path, O_RDWR | RIPPLE_BINARY);
        if (increamentstate->fd  < 0)
        {
            elog(RLOG_ERROR, "open file %s error %s", path, strerror(errno));
        }
        rfree(uuid);
        return;
    }

    if (finfo->fileid > 0)
    {
        ripple_netmsg_onlinerefresh_p2cdata_filetransfer_add(nodesvrstate, increamentstate->fileid);
    }

    /* 查看错误是否为文件不存在 */
    if(ENOENT != errno)
    {
        elog(RLOG_ERROR, "stat %s error, %s", path, strerror(errno));
    }

    /* 创建临时文件 */
    rmemset1(tmppath, 0, '\0', RIPPLE_MAXPATH);
    snprintf(tmppath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%016lX.tmp", nodesvrstate->clientname,
                                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                                uuid,
                                                                RIPPLE_STORAGE_TRAIL_DIR,
                                                                finfo->fileid);
    unlink(tmppath);

    fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | RIPPLE_BINARY);
    if(0 > fd)
    {
        elog(RLOG_ERROR, "open file %s error:%s", tmppath, strerror(errno));
    }
    blockcnt = ((RIPPLE_MB2BYTE(guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE))) / RIPPLE_FILE_BUFFER_SIZE);

    for(index = 0; index < blockcnt; index++)
    {
        if (FileWrite(fd, (char*)block, RIPPLE_FILE_BUFFER_SIZE) != RIPPLE_FILE_BUFFER_SIZE)
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

    rfree(uuid);
    
    /* 重命名文件 */
    durable_rename(tmppath, path, RLOG_DEBUG);

    /* 打开文件 */
    increamentstate->fd = BasicOpenFile(path, O_RDWR | RIPPLE_BINARY);
    if (increamentstate->fd  < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", path, strerror(errno));
    }
    return;
}

/* 
 * 接收来自pump的data请求
 *  collector 处理
 */
bool ripple_netmsg_onlinerefresh_p2cdata(void* privdata, uint8* msg)
{
    bool end_onlinerefresh = false;
    int bufid = 0;
    int timeout = 0;
    uint32 msglen = 0;

    uint64  pfileid = 0;                                                /* pump 端的 Trail 文件编号 */
    uint64  cblknum = 0;
    uint64  cfileid = 0;                                                /* 生成的 collector Trail 文件的编号 */
    uint64  coffset = 0;                                                /* 生成的 collector Trail 文件的偏移 */
    XLogRecPtr  redolsn = InvalidXLogRecPtr;                            /* 源端 redolsn    */
    XLogRecPtr  restartlsn = InvalidXLogRecPtr;                         /* 源端 restartlsn */
    XLogRecPtr  confirmlsn = InvalidXLogRecPtr;                         /* 源端 confirmlsn */

    uint8* uptr = NULL;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_ff_fileinfo* nfinfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_file_buffer* nfbuffer = NULL;
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;
    ripple_collectornetclient_onlinerefreshinc* increamentstate = NULL;
    ripple_collectorincrementstate_privdatacallback* callback = NULL;

    RIPPLE_UNUSED(pfileid);

    uptr = msg;
    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;
    increamentstate = (ripple_collectornetclient_onlinerefreshinc*)nodesvrstate->data;

    /* 使用回调函数获取 collector中的filebuffers */
    callback = (ripple_collectorincrementstate_privdatacallback*)nodesvrstate->callback;
    increamentstate->netdata2filebuffer = callback->netbuffer_get(nodesvrstate->privdata,
                                                                    nodesvrstate->clientname);

    /* 获取 msglen */
    uptr += 4;
    msglen = RIPPLE_CONCAT(get, 32bit)(&uptr);

    /* 获取固定的附加信息 */
    redolsn = RIPPLE_CONCAT(get, 64bit)(&uptr);
    restartlsn = RIPPLE_CONCAT(get, 64bit)(&uptr);
    confirmlsn = RIPPLE_CONCAT(get, 64bit)(&uptr);

    pfileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
    cfileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
    cblknum = RIPPLE_CONCAT(get, 64bit)(&uptr);
    coffset = RIPPLE_CONCAT(get, 64bit)(&uptr);             /* 开始地址 */
    end_onlinerefresh = RIPPLE_CONCAT(get, 8bit)(&uptr);

    msglen -= RIPPLE_NETMSG_TYPE_P2C_ONLINEREFRESH_INC_DATA_SIZE;

    /* 
     * 读取数据并将数据放入到filebuffer中
     *  1、获取bufid
     *  2、写入数据
     *  3、将数据放入到缓存中
     */
    if(0 == increamentstate->bufid)
    {
        /* 证明是首次接收到此请求 */
        while(1)
        {
            increamentstate->bufid = ripple_file_buffer_get(increamentstate->netdata2filebuffer, &timeout);
            if(RIPPLE_INVALID_BUFFERID == increamentstate->bufid)
            {
                if(RIPPLE_ERROR_TIMEOUT == timeout)
                {
                    usleep(10000);
                    continue;
                }
                elog(RLOG_WARNING, "get file buffer error");
                return false;
            }
            break;
        }
        fbuffer = ripple_file_buffer_getbybufid(increamentstate->netdata2filebuffer, increamentstate->bufid);
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
    }
    else
    {
        /* 获取 buffer 信息 */
        fbuffer = ripple_file_buffer_getbybufid(increamentstate->netdata2filebuffer, increamentstate->bufid);

        /* 如果 offset 相同,那么不做处理 */
        if(cfileid != fbuffer->extra.rewind.fileaddr.trail.fileid
            && cfileid != (fbuffer->extra.rewind.fileaddr.trail.fileid + 1))
        {
            /* Trail 文件编码错误 */
            /* 将 buffer 放入到 free 中 */
            /* 判断是否为新的 fileid */
            elog(RLOG_WARNING, "pump 2 collector data error, collector recv fileid:%lu, collector hold fileid:%lu, expect fileid:%lu",
                                cfileid,
                                fbuffer->extra.rewind.fileaddr.trail.fileid,
                                fbuffer->extra.rewind.fileaddr.trail.fileid + 1);

            ripple_file_buffer_free(increamentstate->netdata2filebuffer, fbuffer);

            return false;
        }
        else if(cfileid == fbuffer->extra.rewind.fileaddr.trail.fileid)
        {
            /* 小于当前的 offset, 那么说明同步出现了问题 */
            if(coffset < fbuffer->extra.rewind.fileaddr.trail.offset)
            {
                elog(RLOG_WARNING, "pump 2 collector data error, collector recv offset:%lu, collector hold offset:%lu",
                                    coffset,
                                    fbuffer->extra.rewind.fileaddr.trail.offset);
                ripple_file_buffer_free(increamentstate->netdata2filebuffer, fbuffer);
                return false;
            }
        }
        finfo = (ripple_ff_fileinfo*)fbuffer->privdata;
    }

    /*
     * 1、设置文件的地址信息
     * 2、设置 redolsn/confirmlsn/redolsn
     */
    finfo->fileid = cfileid;
    finfo->blknum = ((coffset/RIPPLE_FILE_BUFFER_SIZE));
    if(0 == coffset
        || 0 != (coffset%RIPPLE_FILE_BUFFER_SIZE))
    {
        finfo->blknum += 1;
    }

    /* 开启onlinerefresh trail文件 */
    if(increamentstate->fileid != finfo->fileid
        || -1 == increamentstate->fd)
    {
        ripple_netmsg_onlinerefresh_p2cdata_initfile(increamentstate, nodesvrstate, finfo);
    }

    if(cfileid == increamentstate->fileid)
    {
        if(finfo->blknum != increamentstate->blknum
            && finfo->blknum != (increamentstate->blknum + 1))
        {
            elog(RLOG_WARNING, "cfileid:%lu.%lu. %lu.%lu, %lu",
                                finfo->fileid,
                                finfo->blknum,
                                increamentstate->fileid,
                                increamentstate->blknum,
                                cblknum);
            g_gotsigterm = true;
            return false;
        }
        else if(finfo->blknum != increamentstate->blknum)
        {
            increamentstate->blknum++;
        }
    }
    else if(cfileid < increamentstate->fileid)
    {
        elog(RLOG_ERROR, "cfileid:%lu, nodesvrstate->fileid:%lu",
                        cfileid,increamentstate->fileid);
    }
    else
    {
        increamentstate->fileid = finfo->fileid;
        increamentstate->blknum = finfo->blknum;
    }

    fbuffer->extra.chkpoint.redolsn.wal.lsn = redolsn;
    fbuffer->extra.chkpoint.orgaddr.trail.fileid = pfileid;
    fbuffer->extra.rewind.restartlsn.wal.lsn = restartlsn;
    fbuffer->extra.rewind.confirmlsn.wal.lsn = confirmlsn;
    fbuffer->extra.rewind.fileaddr.trail.fileid = cfileid;
    fbuffer->extra.rewind.fileaddr.trail.offset = coffset;

    elog(RLOG_DEBUG, "finfo->fileid:%lu, offset:%lu, blknum:%lu.%lu",
                        finfo->fileid,
                        coffset,
                        finfo->blknum,
                        cblknum);

    /* 将网络中的数据复制到缓存中 */
    rmemcpy0(fbuffer->data, 0, uptr, msglen);
    fbuffer->start = msglen;

    /* 用于于后面的判断 */
    while(1)
    {
        bufid = ripple_file_buffer_get(increamentstate->netdata2filebuffer, &timeout);
        if(RIPPLE_INVALID_BUFFERID == bufid)
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "get file buffer error");
            return false;
        }
        break;
    }
    nfbuffer = ripple_file_buffer_getbybufid(increamentstate->netdata2filebuffer, bufid);
    if(NULL == nfbuffer->privdata)
    {
        nfinfo = (ripple_ff_fileinfo*)rmalloc0(sizeof(ripple_ff_fileinfo));
        if(NULL == nfinfo)
        {
            /* 将 buffer 放入到 free 中 */
            ripple_file_buffer_free(increamentstate->netdata2filebuffer, fbuffer);
            ripple_file_buffer_free(increamentstate->netdata2filebuffer, nfbuffer);
            FileClose(increamentstate->fd);
            increamentstate->fd = -1;
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return false;
        }
        rmemset0(nfinfo, 0, '\0', sizeof(ripple_ff_fileinfo));
        nfbuffer->privdata = (void*)nfinfo;
    }
    else
    {
        nfinfo = (ripple_ff_fileinfo*)nfbuffer->privdata;
    }
    rmemcpy0(nfinfo, 0, finfo, sizeof(ripple_ff_fileinfo));
    rmemcpy1(&nfbuffer->extra, 0, &fbuffer->extra, sizeof(ripple_file_buffer_extra));

    /* 写数据 */
    FilePWrite(increamentstate->fd, (char*)fbuffer->data, fbuffer->maxsize, ((finfo->blknum - 1)*RIPPLE_FILE_BUFFER_SIZE));

    if (end_onlinerefresh)
    {
        ripple_netmsg_onlinerefresh_p2cdata_filetransfer_add(nodesvrstate, increamentstate->fileid);
    }
    

    FileDataSync(increamentstate->fd);
    ripple_file_buffer_free(increamentstate->netdata2filebuffer, fbuffer);

    increamentstate->bufid = bufid;
    return true;
}
