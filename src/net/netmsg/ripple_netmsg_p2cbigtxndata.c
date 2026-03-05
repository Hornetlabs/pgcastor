#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/string/stringinfo.h"
#include "utils/uuid/ripple_uuid.h"
#include "misc/ripple_misc_stat.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netmsg/ripple_netmsg_p2cbigtxndata.h"
#include "filetransfer/ripple_filetransfer.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"

/* 添加网闸上传任务 */
static void ripple_netmsg_p2cbigtxndata_filegap_add(ripple_increment_collectornetclient_state* nodesvrstate, uint64 fileid)
{
    ripple_filetransfer_bigtxninc* filetransfer_inc = NULL;
    ripple_collectorincrementstate_privdatacallback* callback = NULL;
    ripple_collectornetclient_bigtxn* bigtxninc = NULL;

    bigtxninc = (ripple_collectornetclient_bigtxn*)nodesvrstate->data;

    /* 使用回调函数获取 collector中的filebuffers */
    callback = (ripple_collectorincrementstate_privdatacallback*)nodesvrstate->callback;

    if (false == bigtxninc->upload)
    {
        return;
    }
    filetransfer_inc = ripple_filetransfer_bigtxninc_init();
    filetransfer_inc->xid = bigtxninc->xid;
    ripple_filetransfer_increment_trail_set((ripple_filetransfernode *)filetransfer_inc, fileid);
    ripple_filetransfer_upload_bigtxnincpath_set(filetransfer_inc, bigtxninc->xid, nodesvrstate->clientname);
    callback->collector_filetransfernode_add(nodesvrstate->privdata, (void*)filetransfer_inc);
    filetransfer_inc = NULL;

    return;
}

static void ripple_netmsg_p2cbigtxndata_initfile(ripple_increment_collectornetclient_state* nodesvrstate, uint64 fileid)
{
    int fd = -1;
    int index = 0;
    int blockcnt = 0;
    struct stat st;
    StringInfo path = NULL;
    StringInfo tmppath = NULL;
    ripple_collectornetclient_bigtxn* bigtxnstate = NULL;
    uint8   block[RIPPLE_FILE_BUFFER_SIZE] = { 0 };

    bigtxnstate = (ripple_collectornetclient_bigtxn*)nodesvrstate->data;

    if(-1 != bigtxnstate->fd)
    {
        close(bigtxnstate->fd);
        bigtxnstate->fd = -1;
    }

    path = makeStringInfo();
    /* 生成路径 */
    appendStringInfo(path, "%s/%s/%lu/%016lX",
                            bigtxnstate->trailpath,
                            RIPPLE_STORAGE_BIG_TRANSACTION_DIR,
                            bigtxnstate->xid,
                            fileid);

    /* 校验文件是否存在，存在则打开 */
    if(0 == stat(path->data, &st))
    {
        /* 打开文件 */
        bigtxnstate->fd = BasicOpenFile(path->data, O_RDWR | RIPPLE_BINARY);
        if (bigtxnstate->fd  < 0)
        {
            elog(RLOG_ERROR, "open file %s error %s", path->data, strerror(errno));
        }
        deleteStringInfo(path);
        return;
    }

    if (fileid > 0)
    {
        ripple_netmsg_p2cbigtxndata_filegap_add(nodesvrstate, bigtxnstate->fileid);
    }

    /* 查看错误是否为文件不存在 */
    if(ENOENT != errno)
    {
        elog(RLOG_ERROR, "stat %s error, %s", path->data, strerror(errno));
    }

    /* 创建临时文件 */
    tmppath = makeStringInfo();
    appendStringInfo(tmppath, "%s/%s/%lu/%016lX.tmp",
                                bigtxnstate->trailpath,
                                RIPPLE_STORAGE_BIG_TRANSACTION_DIR,
                                bigtxnstate->xid,
                                fileid);
    unlink(tmppath->data);

    fd = BasicOpenFile(tmppath->data, O_RDWR | O_CREAT | O_EXCL | RIPPLE_BINARY);
    if(0 > fd)
    {
        elog(RLOG_ERROR, "open file %s error:%s", tmppath->data, strerror(errno));
    }
    blockcnt = ((RIPPLE_MB2BYTE(guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE))) / RIPPLE_FILE_BUFFER_SIZE);

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
            unlink(tmppath->data);
            FileClose(fd);
            elog(RLOG_ERROR, "can not write file %s, errno:%s", tmppath->data, strerror(errno));
        }
    }

    FileSync(fd);

    FileClose(fd);

    
    /* 重命名文件 */
    durable_rename(tmppath->data, path->data, RLOG_DEBUG);

    /* 打开文件 */
    bigtxnstate->fd = BasicOpenFile(path->data, O_RDWR | RIPPLE_BINARY);
    if (bigtxnstate->fd  < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", path->data, strerror(errno));
    }
    bigtxnstate->fileid = fileid;
    bigtxnstate->blknum = 1;

    deleteStringInfo(path);
    deleteStringInfo(tmppath);
    return;
}

/* 
 * 接收来自pump的data请求
 *  collector 处理
 */
bool ripple_netmsg_p2cbigtxndata(void* privdata, uint8* msg)
{
    uint32 msglen = 0;

    uint64  fileid = 0;                                                /* pump 端的 Trail 文件编号 */
    uint64  offset = 0;                                                /* 生成的 collector Trail 文件的偏移 */
    uint8* uptr = NULL;

    ripple_increment_collectornetclient_state* nodesvrstate = NULL;
    ripple_collectornetclient_bigtxn* bigtxnstate = NULL;

    uptr = msg;
    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;
    bigtxnstate = (ripple_collectornetclient_bigtxn*)nodesvrstate->data;

    /* 获取 msglen */
    uptr += 4;
    msglen = RIPPLE_CONCAT(get, 32bit)(&uptr);

    /* 获取固定的附加信息 */
    fileid = RIPPLE_CONCAT(get, 64bit)(&uptr);
    offset = RIPPLE_CONCAT(get, 64bit)(&uptr);

    elog(RLOG_DEBUG, "p2c bigtxn data, fileid:%lu, offset:%lu", fileid, offset);

    bigtxnstate->blknum = (offset/RIPPLE_FILE_BUFFER_SIZE);

    msglen -= RIPPLE_NETMSG_TYPE_P2C_BIGTXN_DATA_SIZE;

    if (0 == msglen)
    {
        return true;
    }

    /* 开启onlinerefresh trail文件 */
    if(bigtxnstate->fileid != fileid
        || -1 == bigtxnstate->fd)
    {
        ripple_netmsg_p2cbigtxndata_initfile(nodesvrstate, fileid);
    }

    /* 写数据 */
    FilePWrite(bigtxnstate->fd, (char*)uptr, msglen, offset);

    FileDataSync(bigtxnstate->fd);

    return true;
}
