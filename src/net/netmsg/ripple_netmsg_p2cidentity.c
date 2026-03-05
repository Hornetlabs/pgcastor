#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/init/ripple_datainit.h"
#include "port/net/ripple_net.h"
#include "net/netmsg/ripple_netmsg.h"
#include "threads/ripple_threads.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "storage/trail/data/ripple_fftrail_data.h"
#include "net/netmsg/ripple_netmsg_p2cidentity.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/ripple_netserver.h"
#include "misc/ripple_misc_stat.h"
#include "filetransfer/ripple_filetransfer.h"
#include "increment/collector/net/ripple_increment_collectornetsvr.h"
#include "refresh/ripple_refresh_tables.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"
#include "metric/collector/ripple_metric_collector.h"

/* 检查jobname下refresh目录是否存在 */
static bool ripple_netmsg_refreshdir_isexist(char* jobname)
{
    char path[512];

    snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA),
                                                jobname,
                                                RIPPLE_REFRESH_REFRESH);
    return DirExist(path);
}

/* 检查jobname下onlinerefresh目录是否存在 */
static bool ripple_netmsg_onlinerefreshdir_isexist(char* jobname)
{
    char path[512];

    snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA),
                                                jobname,
                                                RIPPLE_REFRESH_ONLINEREFRESH);
    return DirExist(path);
}

/* 检查状态文件是否存在，不存在创建子目录 */
static void ripple_netmsg_check_increment_stat(char* jobname)
{
    char path[512] = {'\0'};
    char datadir[512] = {'\0'};
    struct stat statbuf;

    snprintf(path, RIPPLE_MAXPATH, "%s/%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA),
                                                   jobname,
                                                   "stat",
                                                   RIPPLE_STAT_COLLECTOR);

    /* 检测文件是否存在 */
    if (0 == stat(path, &statbuf))
    {
        return;
    }

    snprintf(datadir, RIPPLE_MAXPATH, "%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA),
                                               jobname);
    ripple_datainit_init_jobnamesubdir(datadir);
    ripple_misc_collectorstat_init(jobname);
}


/*
 * 填充 buffer 数据
*/
static void ripple_netmsg_serial_readdatafromfile(ripple_file_buffer* fbuffer, char* name)
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
    snprintf(path, RIPPLE_MAXPATH, "%s/%s/%016lX", name, RIPPLE_STORAGE_TRAIL_DIR, finfo->fileid);

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

/*
 * 切换文件
*/
static bool ripple_netmsg_serial_shiftfile(ripple_collectornetclient_increment* increamentstate,
                                            ripple_file_buffer* in_fbuffer)
{
    /*
     * 切换文件
     */
    bool shiftfile = false;
    int timeout = 0;
    int bufid = 0;
    int maxbufid = 0;
    int mbytes = 0;
    int minsize = 0;
    int compatibility = 0;
    uint64 bytes = 0;
    uint64 freespc = 0;
    ripple_file_buffer* fbuffer = NULL;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_ff_fileinfo* nfinfo = NULL;

    uint32  reclen = 0;
    uint8*  uptr = NULL;
    ripple_ff_tail fftail = { 0 };                  /* tail 信息 */

    elog(RLOG_DEBUG,"shiftfile:offset:%lu, fileid:%lu",
                        increamentstate->collectorbase.coffset,
                        increamentstate->collectorbase.cfileid);

    /* 获取兼容版本 */
    compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);

    /* 计算 maxbufid */
    mbytes = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TRAIL_MAX_SIZE);
    bytes = RIPPLE_MB2BYTE(mbytes);
    maxbufid = (bytes/RIPPLE_FILE_BUFFER_SIZE);

    finfo = (ripple_ff_fileinfo*)in_fbuffer->privdata;

    /* 获取 minsize */
    minsize = ripple_fftrail_data_tokenminsize(compatibility);

    if(maxbufid == finfo->blknum)
    {
        minsize += ripple_fftrail_taillen(compatibility);
        shiftfile = true;
    }

    /* 查看剩余空间 */
    freespc = (in_fbuffer->maxsize - in_fbuffer->start);

    /* 比较剩余空间是否满足放入数据的最小要求 */
    if(minsize >= freespc)
    {
        if(false == shiftfile)
        {
            finfo->blknum++;
        }
        else
        {
            finfo->blknum = 1;
            finfo->fileid++;
        }
        
        in_fbuffer->start = 0;
        in_fbuffer->extra.rewind.fileaddr.trail.offset = (finfo->blknum-1)*RIPPLE_FILE_BUFFER_SIZE;
        rmemset0(in_fbuffer->data, 0, '\0', in_fbuffer->maxsize);
    }

    /* 获取 buffer */
    uptr = in_fbuffer->data + in_fbuffer->start;

    /* 添加RESET数据 */
    /* 偏移出头部信息 */
    uptr += RIPPLE_TOKENHDRSIZE;

    /* 增加 token 内容 */
    fftail.nexttrailno = (finfo->fileid + 1);
    uptr = ripple_fftrail_token2buffer( 0,
                                        RIPPLE_FFTRAIL_INFOTYPE_TOKEN,
                                        RIPPLE_FTRAIL_TOKENDATATYPE_BIGINT,
                                        8,
                                        (uint8*)&fftail.nexttrailno,
                                        &reclen,
                                        uptr);

    /* 增加 rectail */
    uptr = in_fbuffer->data + in_fbuffer->start;
    reclen += RIPPLE_TOKENHDRSIZE;

    /* reclen 为RESET数据的总长度 */
    reclen = RIPPLE_MAXALIGN(reclen);
    RIPPLE_FTRAIL_GROUP2BUFFER(put,
                                RIPPLE_FFTRAIL_CXT_TYPE_RESET,
                                RIPPLE_FFTRAIL_INFOTYPE_GROUP,
                                reclen,
                                uptr)
    in_fbuffer->extra.rewind.fileaddr.trail.offset += reclen;

    /* 重新获取 fbuffer */
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

    fbuffer = ripple_file_buffer_getbybufid(increamentstate->netdata2filebuffer, bufid);

    if(NULL != fbuffer->privdata)
    {
        nfinfo = (ripple_ff_fileinfo*)fbuffer->privdata;
    }
    else
    {
        nfinfo = (ripple_ff_fileinfo*)rmalloc0(sizeof(ripple_ff_fileinfo));
        if(NULL == nfinfo)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(nfinfo, 0, '\0', sizeof(ripple_ff_fileinfo));
        fbuffer->privdata = (void*)nfinfo;
    }
    nfinfo->fileid = finfo->fileid;
    nfinfo->fileid++;
    nfinfo->blknum = 1;

    /* 初始化头部信息 */
    increamentstate->bufid = bufid;

    /* 设置新缓存的出始值 */
    rmemcpy1(&fbuffer->extra, 0, &in_fbuffer->extra, sizeof(ripple_file_buffer_extra));
    fbuffer->extra.rewind.fileaddr.trail.fileid = nfinfo->fileid;
    fbuffer->extra.rewind.fileaddr.trail.offset = 0;
    increamentstate->collectorbase.cfileid = nfinfo->fileid;
    increamentstate->collectorbase.coffset = 0;

    /* 将 in_buffer 放入到待刷新缓存中 */
    elog(RLOG_DEBUG, "in_buffer info:%lu.%lu", 
                        in_fbuffer->extra.rewind.fileaddr.trail.fileid,
                        in_fbuffer->extra.rewind.fileaddr.trail.offset);
    ripple_file_buffer_waitflush_add(increamentstate->netdata2filebuffer, in_fbuffer);

    return true;
}

/* 
 * 将 buffer 放入到 flush 中, 在放入前需要设置 buffer 的 flag
*/
static bool ripple_netmsg_buffer2waitflush(ripple_collectornetclient_increment* increamentstate)
{
    /*
     * 1、获取新的缓存
     * 2、设置新缓存的标识信息
     * 3、根据 nodesvrstate 中的 lsn 信息设置旧缓存的标识信息
     */

    int             bufid = 0;
    int timeout = 0;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_file_buffer* foldbuffer = NULL;

    foldbuffer = ripple_file_buffer_getbybufid(increamentstate->netdata2filebuffer, increamentstate->bufid);
    if(0 == foldbuffer->start)
    {
        return true;
    }

    /* 获取新的 buffer 缓存 */
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

    fbuffer = ripple_file_buffer_getbybufid(increamentstate->netdata2filebuffer, bufid);
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

    rmemcpy0(fbuffer->data, 0, foldbuffer->data, foldbuffer->start);
    fbuffer->start = foldbuffer->start;

    /* 设置新 buffer 的其它信息 */
    rmemcpy0(finfo, 0, (ripple_ff_fileinfo*)foldbuffer->privdata, sizeof(ripple_ff_fileinfo));

    /* 设置 oldbuffer 的信息 */
    foldbuffer->flag |= RIPPLE_FILE_BUFFER_FLAG_REWIND;
    foldbuffer->extra.rewind.restartlsn.wal.lsn = increamentstate->collectorbase.restartlsn;
    foldbuffer->extra.rewind.confirmlsn.wal.lsn = increamentstate->collectorbase.confirmedlsn;
    foldbuffer->extra.rewind.fileaddr.trail.fileid = finfo->fileid;
    foldbuffer->extra.rewind.fileaddr.trail.offset = (((finfo->blknum - 1) * RIPPLE_FILE_BUFFER_SIZE) + fbuffer->start);

    /* 将 oldbuffer 放入到待刷新缓存中 */
    rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(ripple_file_buffer_extra));
    ripple_file_buffer_waitflush_add(increamentstate->netdata2filebuffer, foldbuffer);

    increamentstate->bufid = bufid;
    return true;
}

static bool ripple_netmsg_p2cidentity_faultrecovery(ripple_increment_collectornetclient_state* nodesvrstate)
{
    bool bshiftfile = false;
    int bufid = 0;
    int timeout = 0;
    ripple_ff_fileinfo* finfo = NULL;
    ripple_file_buffer* fbuffer = NULL;
    ripple_collectornetclient_increment* increamentstate = NULL;
    ripple_collectorincrementstate_privdatacallback* callback = NULL;

    increamentstate = (ripple_collectornetclient_increment*)nodesvrstate->data;

    /* 使用回调函数获取 collector中的filebuffers */
    callback = (ripple_collectorincrementstate_privdatacallback*)nodesvrstate->callback;
    increamentstate->netdata2filebuffer = callback->netbuffer_get(nodesvrstate->privdata,
                                                                    nodesvrstate->clientname);

    if(NULL == increamentstate->netdata2filebuffer)
    {
        return false;
    }

    /* 获取文件编号数据信息 */
    ripple_misc_stat_loadcollector((void*)&increamentstate->collectorbase,
                                    nodesvrstate->clientname);

    elog(RLOG_INFO, "p2cidentity_faultrecovery jobname:%s, pfileid:%lu, cfileid:%lu, coffset:%lu",
                        nodesvrstate->clientname,
                        increamentstate->collectorbase.pfileid,
                        increamentstate->collectorbase.cfileid,
                        increamentstate->collectorbase.coffset);

    /* 获取 bufid */
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

    fbuffer = ripple_file_buffer_getbybufid(increamentstate->netdata2filebuffer, bufid);

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

    fbuffer->extra.chkpoint.orgaddr.trail.fileid = increamentstate->collectorbase.pfileid;
    fbuffer->extra.rewind.fileaddr.trail.fileid = increamentstate->collectorbase.cfileid;
    fbuffer->extra.rewind.fileaddr.trail.offset = increamentstate->collectorbase.coffset;

    finfo->fileid = increamentstate->collectorbase.cfileid;
    finfo->blknum = (increamentstate->collectorbase.coffset / RIPPLE_FILE_BUFFER_SIZE);
    finfo->blknum++;
    fbuffer->start = (increamentstate->collectorbase.coffset%RIPPLE_FILE_BUFFER_SIZE);

    increamentstate->bufid = bufid;

    /* 填充 fbuffer 数据 */
    ripple_netmsg_serial_readdatafromfile(fbuffer, nodesvrstate->clientname);

    /* 查看是否含有数据, 含有数据则证明，已经写入部分数据了，那么进行切换 */
    if(0 != increamentstate->collectorbase.coffset)
    {
        bshiftfile = true;
        if(false == ripple_netmsg_serial_shiftfile(increamentstate, fbuffer))
        {
            return false;
        }
    }

    /* 产生了文件切换，那么将新的文件点信息写入到 base 文件中 */
    if(true == bshiftfile)
    {
        if(false == ripple_netmsg_buffer2waitflush(increamentstate))
        {
            return false;
        }
        bshiftfile = false;
    }

    return true;
}

/* 遍历filetransfer目录将未完成的任务再次加入队列*/
static bool ripple_netmsg_p2cidentity_filetransfer_faultrecovery(ripple_increment_collectornetclient_state* nodesvrstate)
{
    void* node = NULL;
    DIR* compdir = NULL;
    struct dirent *entry = NULL;
    char  path[RIPPLE_MAXPATH] = {'\0'};
    ripple_collectorincrementstate_privdatacallback* callback = NULL;

    /* 使用回调函数获取 collector中的filebuffers */
    callback = (ripple_collectorincrementstate_privdatacallback*)nodesvrstate->callback;

    snprintf(path, RIPPLE_MAXPATH, "%s/%s", nodesvrstate->clientname,
                                            RIPPLE_FILETRANSFER_DIR);
    compdir = OpenDir(path);

    while (NULL != (entry = ReadDir(compdir, path)))
    {
        if (0 == strcmp(".", entry->d_name)
        || 0 == strcmp("..", entry->d_name))
        {
            continue;
        }

        node = ripple_filetransfer_makenode_fromfile(nodesvrstate->clientname, entry->d_name);

        callback->collector_filetransfernode_add(nodesvrstate->privdata, node);
    }
    FreeDir(compdir);

    return true;
}

/* 
 * 接收来自pump的addr请求
 *  collector 处理
 */
bool ripple_netmsg_p2cidentity(void* privdata, uint8* msg)
{
    /*
     * 向 pump 发送当前的位置信息
     */
    int iret = 0;
    int event = 0;
    int8 type = 0;
    uint32 jobnamelen = 0;
    uint8* uptr = NULL;
    ripple_uuid_t onlinerefreshno = {{'\0'}};
    uint8   c2pidentity[RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE] = { 0 };
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;

    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;

    /* 获取 type */
    uptr = msg;
    uptr += RIPPLE_NETMSG_TYPE_HDR_SIZE;
    type = RIPPLE_CONCAT(get, 8bit)(&uptr);
    jobnamelen = RIPPLE_CONCAT(get, 32bit)(&uptr);

    if (jobnamelen == 0 || jobnamelen > 127 )
    {
        elog(RLOG_WARNING, "Invalid jobname, length does not match");
        return false;
    }

    rmemset1(nodesvrstate->clientname, 0, '\0', 128);
    rmemcpy1(nodesvrstate->clientname, 0, uptr, jobnamelen);

    nodesvrstate->type = type;

    /* 根据类型申请nodesvrstate->data 空间 */
    if (RIPPLE_NETIDENTITY_TYPE_PUMPINCREAMENT == nodesvrstate->type)
    {
        ripple_collectorincrementstate_privdatacallback* increament_callback = NULL;
        nodesvrstate->data = ripple_collectornetclient_pumpincreament_alloc();
        increament_callback = (ripple_collectorincrementstate_privdatacallback*)rmalloc0(sizeof(ripple_collectorincrementstate_privdatacallback));
        if (NULL == increament_callback)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(increament_callback, 0, '\0', sizeof(ripple_collectorincrementstate_privdatacallback));
        increament_callback->netbuffer_get = ripple_increment_collectornetsvr_netbuffer_get;
        increament_callback->client_setmetricrecvlsn = ripple_increment_collectornetsvr_collectorstate_recvlsn_set;
        increament_callback->client_setmetricrecvtrailno = ripple_increment_collectornetsvr_collectorstate_recvtrailno_set;
        increament_callback->client_setmetricrecvtrailstart = ripple_increment_collectornetsvr_collectorstate_recvtrailstart_set;
        increament_callback->client_setmetricrecvtimestamp = ripple_increment_collectornetsvr_collectorstate_recvtimestamp_set;
        increament_callback->writestate_fileid_get = ripple_increment_collectornetsvr_writestate_fileid_get;
        increament_callback->increment_addflush = ripple_increment_collectornetsvr_collectorstate_addflush;
        increament_callback->collector_filetransfernode_add = ripple_increment_collectornetsvr_filetransfernode_add;
        nodesvrstate->callback = (void*)increament_callback;

        ripple_netmsg_check_increment_stat(nodesvrstate->clientname);

        if(false == increament_callback->increment_addflush(nodesvrstate->privdata, nodesvrstate->clientname))
        {
            return false;
        }

        if(false == ripple_netmsg_p2cidentity_filetransfer_faultrecovery(nodesvrstate))
        {
            return false;
        }

        if(false == ripple_netmsg_p2cidentity_faultrecovery(nodesvrstate))
        {
            return false;
        }
    }
    else if (RIPPLE_NETIDENTITY_TYPE_PUMPREFRESHARDING == nodesvrstate->type)
    {
        ripple_collectornetclient_refreshsharding* refreshstate = NULL;
        ripple_collectorrefreshshardingstate_privdatacallback* refresh_callback = NULL;
        refreshstate = ripple_collectornetclient_pumprefreshsharding_alloc();
        refresh_callback = (ripple_collectorrefreshshardingstate_privdatacallback*)rmalloc0(sizeof(ripple_collectorrefreshshardingstate_privdatacallback));
        if (NULL == refresh_callback)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(refresh_callback, 0, '\0', sizeof(ripple_collectorrefreshshardingstate_privdatacallback));
        refresh_callback->collector_filetransfernode_add = ripple_increment_collectornetsvr_filetransfernode_add;
        nodesvrstate->callback = (void*)refresh_callback;

        sprintf(refreshstate->refresh_path, "%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA), nodesvrstate->clientname, RIPPLE_REFRESH_REFRESH);
        nodesvrstate->data = (void*)refreshstate;
        while (false == ripple_netmsg_refreshdir_isexist(nodesvrstate->clientname))
        {
            if (true == g_gotsigterm)
            {
                return false;
            }
            
            usleep(50000);
        }
    }
    else if (RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_INC == nodesvrstate->type)
    {
        ripple_collectornetclient_onlinerefreshinc* increament = NULL;
        ripple_collectorincrementstate_privdatacallback* increament_callback = NULL;

        uptr += jobnamelen;
        rmemcpy1(onlinerefreshno.data, 0, uptr, RIPPLE_UUID_LEN);
        increament = ripple_collectornetclient_onlinerefreshinc_alloc();
        rmemcpy1(increament->onlinerefreshno.data, 0, onlinerefreshno.data, RIPPLE_UUID_LEN);
        nodesvrstate->data = (void*)increament;
        increament_callback = (ripple_collectorincrementstate_privdatacallback*)rmalloc0(sizeof(ripple_collectorincrementstate_privdatacallback));
        if (NULL == increament_callback)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(increament_callback, 0, '\0', sizeof(ripple_collectorincrementstate_privdatacallback));
        increament_callback->netbuffer_get = ripple_increment_collectornetsvr_netbuffer_get;
        increament_callback->collector_filetransfernode_add = ripple_increment_collectornetsvr_filetransfernode_add;
        increament_callback->client_setmetricrecvlsn = NULL;
        increament_callback->client_setmetricrecvtrailno = NULL;
        increament_callback->client_setmetricrecvtrailstart = NULL;
        increament_callback->client_setmetricrecvtimestamp = NULL;
        increament_callback->writestate_fileid_get = NULL;
        nodesvrstate->callback = (void*)increament_callback;
        while (false == ripple_netmsg_onlinerefreshdir_isexist(nodesvrstate->clientname))
        {
            if (true == g_gotsigterm)
            {
                return false;
            }
            usleep(50000);
        }
    }
    else if (RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_SHARDING == nodesvrstate->type)
    {
        char* uuid = NULL;
        ripple_collectornetclient_onlinerefreshsharding* refresh = NULL;
        ripple_collectoronlinerefreshshardingstate_privdatacallback* refresh_callback = NULL;

        uptr += jobnamelen;
        rmemcpy1(onlinerefreshno.data, 0, uptr, RIPPLE_UUID_LEN);
        refresh = ripple_collectornetclient_onlinerefreshsharding_alloc();
        rmemcpy1(refresh->onlinerefreshno.data, 0, onlinerefreshno.data, RIPPLE_UUID_LEN);
        uuid = uuid2string(&refresh->onlinerefreshno);
        sprintf(refresh->refresh_path, "%s/%s/%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA),
                                                      nodesvrstate->clientname,
                                                      RIPPLE_REFRESH_ONLINEREFRESH,
                                                      uuid);
        rfree(uuid);

        refresh_callback = (ripple_collectoronlinerefreshshardingstate_privdatacallback*)rmalloc0(sizeof(ripple_collectorrefreshshardingstate_privdatacallback));
        if (NULL == refresh_callback)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(refresh_callback, 0, '\0', sizeof(ripple_collectoronlinerefreshshardingstate_privdatacallback));
        refresh_callback->collector_filetransfernode_add = ripple_increment_collectornetsvr_filetransfernode_add;
        nodesvrstate->callback = (void*)refresh_callback;

        nodesvrstate->data = (void*)refresh;

        while (false == ripple_netmsg_onlinerefreshdir_isexist(nodesvrstate->clientname))
        {
            if (true == g_gotsigterm)
            {
                return false;
            }
            usleep(50000);
        }
    }
    else if (RIPPLE_NETIDENTITY_TYPE_BIGTRANSACTION == nodesvrstate->type)
    {
        ripple_collectornetclient_bigtxn* bigtxn = NULL;
        ripple_collectorincrementstate_privdatacallback* increament_callback = NULL;

        uptr += jobnamelen;
        bigtxn = ripple_collectornetclient_bigtxn_alloc();
        sprintf(bigtxn->trailpath, "%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA), nodesvrstate->clientname);
        nodesvrstate->data = (void*)bigtxn;

        increament_callback = (ripple_collectorincrementstate_privdatacallback*)rmalloc0(sizeof(ripple_collectorincrementstate_privdatacallback));
        if (NULL == increament_callback)
        {
            elog(RLOG_ERROR, "out of memory");
        }
        rmemset0(increament_callback, 0, '\0', sizeof(ripple_collectorincrementstate_privdatacallback));
        increament_callback->netbuffer_get = ripple_increment_collectornetsvr_netbuffer_get;
        increament_callback->collector_filetransfernode_add = ripple_increment_collectornetsvr_filetransfernode_add;
        increament_callback->client_setmetricrecvlsn = NULL;
        increament_callback->client_setmetricrecvtrailno = NULL;
        increament_callback->client_setmetricrecvtrailstart = NULL;
        increament_callback->client_setmetricrecvtimestamp = NULL;
        increament_callback->writestate_fileid_get = NULL;
        nodesvrstate->callback = (void*)increament_callback;
    }

    uptr = c2pidentity;
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_IDENTITY);
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE);
    RIPPLE_CONCAT(put, 8bit)(&uptr, type);

    /* 设置值信息 */
    while(1)
    {
        /*
         * 1、获取数据，并根据数据的类型做处理
         * 2、查看是否需要发送 hb 包,并检测是否超时
         */
        /* 查看是否接收到退出信号 */
        if(true == g_gotsigterm)
        {
            /* 接收到退出信号, 退出处理 */
            break;
        }

        /* 获取数据 */
        /* 
         * 1、创建监听事件
         * 2、检测监听事件是否触发
         * 3、根据不同的协议类型走不同的处理逻辑
         */
        /* 重置事件监听 */
        nodesvrstate->ops->reset(nodesvrstate->base);
        event |= POLLOUT;

        /* 添加监听事件 */
        nodesvrstate->pos = nodesvrstate->ops->add(nodesvrstate->base, nodesvrstate->fd, event);

        /* 调用iomp端口 */
        iret = nodesvrstate->ops->iomp(nodesvrstate->base);
        if(-1 == iret)
        {
            /* 查看错误是否为信号引起的，若为信号引起那么继续监测 */
            if(errno == EINTR)
            {
                continue;
            }
            elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
            return false;
        }

        if(0 == iret)
        {
            /* 超时了, 那么继续 */
            continue;
        }

        /* 有消息触发，那么看看触发的事件类型 */
        event = nodesvrstate->ops->getevent(nodesvrstate->base, nodesvrstate->pos);

        /*
         * 检测事件类型，当为 POLLUP 或者 POLLERROR 时，那么说明出现了错误，退出
         */
        if(POLLHUP == (event&POLLHUP)
            || POLLERR == (event&POLLERR))
        {
            elog(RLOG_WARNING, "iomp error, %s", strerror(errno));
            return false;
        }

        /* 查看是否有数据需要写 */
        if(POLLOUT == (POLLOUT&event))
        {
            /* 根据类型写入数据 */
            if (RIPPLE_NETIDENTITY_TYPE_PUMPINCREAMENT == nodesvrstate->type)
            {
                ripple_collectornetclient_increment* increamentstate = NULL;
                increamentstate = (ripple_collectornetclient_increment*)nodesvrstate->data;
                /* 写数据 */
                /* TODO 组装位置信息 */

                /* pump 端开始读取的 Trail 文件的编号 */
                RIPPLE_CONCAT(put, 64bit)(&uptr, increamentstate->collectorbase.pfileid);

                /* collector 写入的 Trail 文件编号 */
                RIPPLE_CONCAT(put, 64bit)(&uptr, increamentstate->collectorbase.cfileid);

                elog(RLOG_DEBUG, "collector side, send c2pidentity pfileid: %lu, cfileid: %lu",
                                increamentstate->collectorbase.pfileid,
                                increamentstate->collectorbase.cfileid);

                if(false == ripple_net_write(nodesvrstate->fd, c2pidentity, RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE))
                {
                    /* 发送数据失败，关闭连接 */
                    elog(RLOG_WARNING, "write RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE  error, %s", strerror(errno));
                    return true;
                }

                nodesvrstate->hbtimeout = 0;
            }
            else if (RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_INC == nodesvrstate->type
                    || RIPPLE_NETIDENTITY_TYPE_BIGTRANSACTION == nodesvrstate->type)
            {
                /* 写数据 */
                /* TODO 组装位置信息 */

                /* pump 端开始读取的 Trail 文件的编号 */
                RIPPLE_CONCAT(put, 64bit)(&uptr, 0);

                /* collector 写入的 Trail 文件编号 */
                RIPPLE_CONCAT(put, 64bit)(&uptr, 0);

                if(false == ripple_net_write(nodesvrstate->fd, c2pidentity, RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE))
                {
                    /* 发送数据失败，关闭连接 */
                    elog(RLOG_WARNING, "write RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE  error, %s", strerror(errno));
                    return true;
                }

                nodesvrstate->hbtimeout = 0;
            }
            else if(RIPPLE_NETIDENTITY_TYPE_PUMPREFRESHARDING == nodesvrstate->type
                    || RIPPLE_NETIDENTITY_TYPE_ONLINEREFRESH_SHARDING == nodesvrstate->type)
            {
                if(false == ripple_net_write(nodesvrstate->fd, c2pidentity, RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE))
                {
                    /* 发送数据失败，关闭连接 */
                    elog(RLOG_WARNING, "write RIPPLE_NETMSG_TYPE_C2P_IDENTITY_SIZE  error, %s", strerror(errno));
                    return true;
                }
            }

            elog(RLOG_DEBUG, "collector 2 pump identity");
            break;
        }
    }

    return true;
}
