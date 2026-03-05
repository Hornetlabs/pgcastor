#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/guc/guc.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/list/list_func.h"
#include "utils/string/stringinfo.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_p2cshardingbegin.h"
#include "misc/ripple_misc_stat.h"
#include "filetransfer/ripple_filetransfer.h"
#include "works/dyworks/ripple_dyworks.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"

/* 生成refresh信息文件和上传任务，加入到任务中 */
static bool ripple_netmsg_onlinerefresh_p2cshardingbegin_filetransfer_add(ripple_increment_collectornetclient_state* nodesvrstate)
{
    int fd = -1;
    struct stat st;
    char* uuid = NULL;
    ripple_filetransfer_refreshinfo buffer = {{'\0'}};
    StringInfo path = NULL;
    ripple_filetransfer_refreshshards *file = NULL;
    ripple_collectornetclient_onlinerefreshsharding* refreshstate = NULL;
    ripple_collectoronlinerefreshshardingstate_privdatacallback* refresh_callback = NULL;

    refreshstate = (ripple_collectornetclient_onlinerefreshsharding*)nodesvrstate->data;
    refresh_callback = (ripple_collectoronlinerefreshshardingstate_privdatacallback*)nodesvrstate->callback;

    /* 不配置ftpurl不上传文件 */
    if (false == refreshstate->upload)
    {
        return true;
    }

    uuid = uuid2string(&refreshstate->refreshtablebase.onlinerefreshno);

    path = makeStringInfo();
    appendStringInfo(path, "%s/%s/%s/%s/%s_%s/%s_%s", nodesvrstate->clientname,
                                                      RIPPLE_REFRESH_ONLINEREFRESH,
                                                      uuid,
                                                      RIPPLE_REFRESH_REFRESH,
                                                      refreshstate->refreshtablebase.schema,
                                                      refreshstate->refreshtablebase.table,
                                                      refreshstate->refreshtablebase.schema,
                                                      refreshstate->refreshtablebase.table);

    /* 文件存在不处理 */
    if(0 == stat(path->data, &st))
    {
        deleteStringInfo(path);
        rfree(uuid);
        return true;
    }

    /* 创建refresh信息文件包含模式名，表名，分片总数 */
    fd = BasicOpenFile(path->data, O_RDWR | O_CREAT | RIPPLE_BINARY);
    if (fd < 0)
    {
        /* 文件存在正常进行 */
        if (EEXIST == errno)
        {
            deleteStringInfo(path);
            rfree(uuid);
            return true;
        }
        elog(RLOG_WARNING, "open file %s error %s", path->data, strerror(errno));
        rfree(uuid);
        deleteStringInfo(path);
        return false;
    }

    rmemset1(&buffer, 0, '\0', sizeof(ripple_filetransfer_refreshinfo));
    rmemcpy1(buffer.schema, 0, refreshstate->refreshtablebase.schema, NAMEDATALEN);
    rmemcpy1(buffer.table, 0, refreshstate->refreshtablebase.table, NAMEDATALEN);
    buffer.shards = refreshstate->refreshtablebase.shards;

    FileWrite(fd, (char*)&buffer, sizeof(ripple_filetransfer_refreshinfo));

    if(0 != FileSync(fd))
    {
        elog(RLOG_WARNING, "could not fsync file %s", path->data);
        deleteStringInfo(path);
        rfree(uuid);
        return false;
    }

    if(FileClose(fd))
    {
        elog(RLOG_WARNING, "could not close file %s", path->data);
        deleteStringInfo(path);
        rfree(uuid);
        return false;
    }

    /* 添加 filetransfer节点 */
    file = ripple_filetransfer_refreshshards_init();
    file->base.type = RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESHSHARDS;
    snprintf(file->base.jobname, 128, "%s", nodesvrstate->clientname);
    snprintf(file->uuid, 37, "%s", uuid);
    snprintf(file->base.localpath, RIPPLE_MAXPATH, "%s", path->data);

    snprintf(file->prefixpath, RIPPLE_MAXPATH, "%s/%s/%s/%s/%s_%s",
                                                nodesvrstate->clientname,
                                                RIPPLE_REFRESH_ONLINEREFRESH,
                                                uuid,
                                                RIPPLE_REFRESH_REFRESH,
                                                refreshstate->refreshtablebase.schema,
                                                refreshstate->refreshtablebase.table);
    snprintf(file->schema, NAMEDATALEN, "%s", refreshstate->refreshtablebase.schema);
    snprintf(file->table, NAMEDATALEN, "%s", refreshstate->refreshtablebase.table);
    refresh_callback->collector_filetransfernode_add(nodesvrstate->privdata, (void*)file);

    deleteStringInfo(path);
    rfree(uuid);

    return true;
}


/* 
 * 接收来自pump的data请求
 *  collector 处理
 */
bool ripple_netmsg_onlinerefresh_p2cshardingbegin(void* privdata, uint8* msg)
{
    struct stat st;
    int iret = 0;
    int event = 0;
    uint32 shards = 0;
    uint32 shardnum = 0;

    uint16  tblen = 0;                                                /* table长度 */
    uint16  schlen = 0;                                               /* schema长度 */
    char table[NAMEDATALEN] = {'\0'};
    char schema[NAMEDATALEN] = {'\0'};
    StringInfo path = NULL;
    StringInfo tablepath = NULL;
    ripple_uuid_t onlinerefreshno = {{'\0'}};

    uint8* uptr = NULL;
    uint8   c2pbeginrefresh[RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_BEGIN_SIZE] = { 0 };
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;
    ripple_collectornetclient_onlinerefreshsharding* refreshstate = NULL;

    uptr = msg;
    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;
    refreshstate = (ripple_collectornetclient_onlinerefreshsharding*)nodesvrstate->data;
    path = makeStringInfo();
    tablepath = makeStringInfo();

    /* 获取 msglen */
    uptr += RIPPLE_NETMSG_TYPE_HDR_SIZE;

    /* 获取固定的附加信息 */
    rmemcpy1(onlinerefreshno.data, 0, uptr, RIPPLE_UUID_LEN);
    uptr += RIPPLE_UUID_LEN;
    schlen = RIPPLE_CONCAT(get, 16bit)(&uptr);
    rmemset1(schema, 0, '\0', NAMEDATALEN);
    rmemcpy1(schema, 0, uptr, schlen);
    uptr += schlen;
    tblen = RIPPLE_CONCAT(get, 16bit)(&uptr);
    rmemset1(table, 0, '\0', NAMEDATALEN);
    rmemcpy1(table, 0, uptr, tblen);
    uptr += tblen;
    shards = RIPPLE_CONCAT(get, 32bit)(&uptr);
    shardnum = RIPPLE_CONCAT(get, 32bit)(&uptr);

    /*
     * 生成partial文件
     */

    appendStringInfo(tablepath, "%s/%s/%s_%s/%s", refreshstate->refresh_path,
                                                  RIPPLE_REFRESH_REFRESH,
                                                  schema,
                                                  table,
                                                  RIPPLE_REFRESH_PARTIAL);

    while(false == DirExist(tablepath->data))
    {
        if(true == g_gotsigterm)
        {
            return true;
        }
        /* 创建目录 */
        MakeDir(tablepath->data);
    }

    appendStringInfo(path, "%s/%s_%s_%u_%u.%s",
                            tablepath->data,
                            schema,
                            table,
                            shards,
                            shardnum,
                            RIPPLE_REFRESH_PARTIAL);
    if(0 == stat(path->data, &st))
    {
        unlink(path->data);
    }
    // elog(RLOG_WARNING, "open file %s schema:%s table:%s shards:%lu shardnum:%lu ", path->data, schema, table, shards, shardnum);

    refreshstate->fd = BasicOpenFile(path->data,
                                     O_RDWR | O_CREAT | O_EXCL | RIPPLE_BINARY);
    if (refreshstate->fd < 0)
    {
        elog(RLOG_ERROR, "open file %s error %s", path->data, strerror(errno));
    }
    deleteStringInfo(path);
    deleteStringInfo(tablepath);

    rmemset1(refreshstate->refreshtablebase.onlinerefreshno.data, 0, '\0', RIPPLE_UUID_LEN);
    rmemcpy1(refreshstate->refreshtablebase.onlinerefreshno.data, 0, onlinerefreshno.data, RIPPLE_UUID_LEN);
    rmemset1(refreshstate->refreshtablebase.schema, 0, '\0', NAMEDATALEN);
    rmemcpy1(refreshstate->refreshtablebase.schema, 0, schema, schlen);
    rmemset1(refreshstate->refreshtablebase.table, 0, '\0', NAMEDATALEN);
    rmemcpy1(refreshstate->refreshtablebase.table, 0, table, tblen);
    refreshstate->refreshtablebase.shards = shards;
    refreshstate->refreshtablebase.shardnum = shardnum;

    ripple_netmsg_onlinerefresh_p2cshardingbegin_filetransfer_add(nodesvrstate);

    uptr = c2pbeginrefresh;
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_BEGIN);
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_BEGIN_SIZE);

    /* 设置值信息 */
    while(1)
    {
        /*
         * 1、获取数据，并根据数据的类型做处理
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
            /* 写数据 */
            /* TODO 组装位置信息 */

            if(false == ripple_net_write(nodesvrstate->fd, c2pbeginrefresh, RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_BEGIN_SIZE))
            {
                /* 发送数据失败，关闭连接 */
                elog(RLOG_WARNING, "write RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_BEGIN_SIZE error, %s", strerror(errno));
                return true;
            }

            nodesvrstate->hbtimeout = 0;
            elog(RLOG_INFO, "collector 2 pump onlinerefresh beginrefresh");
            break;
        }
    }

    return true;
}
