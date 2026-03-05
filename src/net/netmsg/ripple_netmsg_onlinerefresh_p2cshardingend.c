#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/string/stringinfo.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netmsg/ripple_netmsg_onlinerefresh_p2cshardingend.h"
#include "misc/ripple_misc_stat.h"
#include "filetransfer/ripple_filetransfer.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"

/* 生成filetransfer节点，并添加到任务中 */
static void ripple_netmsg_onlinerefresh_p2cshardingend_filetransfer_add(ripple_increment_collectornetclient_state* nodesvrstate)
{
    char* uuid = NULL;
    ripple_filetransfer_refresh *filetransfer_refresh = NULL;
    ripple_collectornetclient_onlinerefreshsharding* refreshstate = NULL;
    ripple_collectorrefreshshardingstate_privdatacallback* refresh_callback = NULL;

    refreshstate = (ripple_collectornetclient_onlinerefreshsharding*)nodesvrstate->data;

    refresh_callback = (ripple_collectorrefreshshardingstate_privdatacallback*)nodesvrstate->callback;

    /* 不配置url不做处理 */
    if (false == refreshstate->upload)
    {
        return;
    }

    /* 加入到任务中 */
    filetransfer_refresh = ripple_filetransfer_refresh_init();
    filetransfer_refresh->base.type = RIPPLE_FILETRANSFERNODE_TYPE_ONLINEREFRESH_SHARDING;
    ripple_filetransfer_refresh_set(filetransfer_refresh,
                                    refreshstate->refreshtablebase.schema,
                                    refreshstate->refreshtablebase.table,
                                    refreshstate->refreshtablebase.shards,
                                    refreshstate->refreshtablebase.shardnum);
    uuid = uuid2string(&refreshstate->refreshtablebase.onlinerefreshno);
    ripple_filetransfer_upload_olrefreshshardspath_set(filetransfer_refresh, uuid, nodesvrstate->clientname);
    rfree(uuid);
    refresh_callback->collector_filetransfernode_add(nodesvrstate->privdata, (void*)filetransfer_refresh);
    filetransfer_refresh = NULL;

    return;
}

static bool ripple_netmsg_onlinerefresh_p2cshardingend_mvfile(ripple_increment_collectornetclient_state* nodesvrstate)
{
    StringInfo  partialpath = NULL;
    StringInfo  completepath = NULL;
    ripple_refresh_tablebase* tablebase = NULL;
    ripple_collectornetclient_onlinerefreshsharding* refreshstate = NULL;

    refreshstate = (ripple_collectornetclient_onlinerefreshsharding*)nodesvrstate->data;
    tablebase = (ripple_refresh_tablebase*)&refreshstate->refreshtablebase;
    partialpath = makeStringInfo();
    completepath = makeStringInfo();

    appendStringInfo(completepath, "%s/%s/%s_%s/%s",
                                    refreshstate->refresh_path,
                                    RIPPLE_REFRESH_REFRESH,
                                    tablebase->schema,
                                    tablebase->table,
                                    RIPPLE_REFRESH_COMPLETE);

    while(!DirExist(completepath->data))
    {
        if(true == g_gotsigterm)
        {
            return true;
        }
        /* 创建目录 */
        if(0 != MakeDir(completepath->data))
        {
            elog(RLOG_ERROR, "could not create directory:%s", completepath->data);
        }
    }

    appendStringInfo(partialpath, "%s/%s/%s_%s/%s/%s_%s_%u_%u.%s",
                                    refreshstate->refresh_path,
                                    RIPPLE_REFRESH_REFRESH,
                                    tablebase->schema,
                                    tablebase->table,
                                    RIPPLE_REFRESH_PARTIAL,
                                    tablebase->schema,
                                    tablebase->table,
                                    tablebase->shards,
                                    tablebase->shardnum,
                                    RIPPLE_REFRESH_PARTIAL);

    if(FileClose(refreshstate->fd))
    {
        deleteStringInfo(partialpath);
        deleteStringInfo(completepath);
        elog(RLOG_WARNING, "could not close file %s", partialpath->data);
        return false;
    }

    appendStringInfo(completepath, "/%s_%s_%u_%u",
                                    tablebase->schema,
                                    tablebase->table,
                                    tablebase->shards,
                                    tablebase->shardnum);

    durable_rename(partialpath->data, completepath->data, RLOG_WARNING);

    ripple_netmsg_onlinerefresh_p2cshardingend_filetransfer_add(nodesvrstate);

    deleteStringInfo(partialpath);
    deleteStringInfo(completepath);

    return true;
}

bool ripple_netmsg_onlinerefresh_p2cshardingend(void* privdata, uint8* msg)
{
    /*
     * 向 pump 发送结束信息
     */
    int iret = 0;
    int event = 0;
    uint8* uptr = NULL;
    uint8   c2pendrefresh[RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_END_SIZE] = { 0 };
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;

    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;

    if (!ripple_netmsg_onlinerefresh_p2cshardingend_mvfile(nodesvrstate))
    {
        return false;
    }

    uptr = c2pendrefresh;
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_END);
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_END_SIZE);

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
            /* 写数据 */
            if(false == ripple_net_write(nodesvrstate->fd, c2pendrefresh, RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_END_SIZE))
            {
                /* 发送数据失败，关闭连接 */
                elog(RLOG_WARNING, "write RIPPLE_NETMSG_TYPE_C2P_ONLINEREFRESH_SHARDING_END_SIZE error, %s", strerror(errno));
                return true;
            }

            nodesvrstate->hbtimeout = 0;
            elog(RLOG_INFO, "collector 2 pump onlinerefresh endrefresh");
            break;
        }
    }

    return true;
}

