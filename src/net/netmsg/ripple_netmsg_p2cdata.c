#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "port/net/ripple_net.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netmsg/ripple_netmsg_p2cdata.h"
#include "net/netiomp/ripple_netiomp.h"
#include "misc/ripple_misc_stat.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"
#include "metric/collector/ripple_metric_collector.h"

/* 
 * 接收来自pump的data请求
 *  collector 处理
 */
bool ripple_netmsg_p2cdata(void* privdata, uint8* msg)
{
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
    TimestampTz  timestamp = 0;

    uint8* uptr = NULL;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_ff_fileinfo* nfinfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_file_buffer* nfbuffer = NULL;
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;
    ripple_collectornetclient_increment* increamentstate = NULL;
    ripple_collectorincrementstate_privdatacallback* callback = NULL;

    RIPPLE_UNUSED(pfileid);

    uptr = msg;
    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;
    increamentstate = (ripple_collectornetclient_increment*)nodesvrstate->data;

    /* 使用回调函数获取 collector中的filebuffers */
    callback = (ripple_collectorincrementstate_privdatacallback*)nodesvrstate->callback;
    /* 使用回调函数获取 collector中的filebuffers */
    if(NULL == increamentstate->netdata2filebuffer)
    {
        elog(RLOG_WARNING, "p2c data, collector need pump identity");
        return false;
    }

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
    timestamp = RIPPLE_CONCAT(get, 64bit)(&uptr);

    msglen -= RIPPLE_NETMSG_TYPE_P2C_DATA_FIXSIZE;

    elog(RLOG_DEBUG, "p2cdata:pfileid:%lu, cfileid:%lu", pfileid, cfileid);
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
    fbuffer->extra.timestamp = timestamp;

    callback->client_setmetricrecvlsn(nodesvrstate->privdata, nodesvrstate->clientname, confirmlsn);
    callback->client_setmetricrecvtrailno(nodesvrstate->privdata, nodesvrstate->clientname, cfileid);
    callback->client_setmetricrecvtrailstart(nodesvrstate->privdata, nodesvrstate->clientname, coffset);
    callback->client_setmetricrecvtimestamp(nodesvrstate->privdata, nodesvrstate->clientname, timestamp);

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

    /* 将 fbuffer 放入到待刷新缓存中 */
    elog(RLOG_DEBUG, "ADD 2 WAITFLUSH");
    ripple_file_buffer_waitflush_add(increamentstate->netdata2filebuffer, fbuffer);

    /* 更新waitflush状态值 */

    increamentstate->bufid = bufid;
    return true;
}
