#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "misc/ripple_misc_stat.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netmsg/ripple_netmsg_p2cbigtxnend.h"
#include "filetransfer/ripple_filetransfer.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"

/* 添加网闸上传任务 */
static void ripple_netmsg_p2cbigtxnend_filegap_add(ripple_increment_collectornetclient_state* nodesvrstate, uint64 fileid)
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

/* 
 * 接收来自pump的heartbeat
 *  collector 处理
 */
bool ripple_netmsg_p2cbigtxnend(void* privdata, uint8* msg)
{
    /*
     * 向 pump 发送结束信息
     */
    int iret = 0;
    int event = 0;
    uint8* uptr = NULL;
    uint8   c2pendbigtxn[RIPPLE_NETMSG_TYPE_C2P_ENDBIGTXN_SIZE] = { 0 };
    ripple_increment_collectornetclient_state* nodesvrstate = NULL;
    ripple_collectornetclient_bigtxn* bigtxn = NULL;

    uptr = msg;
    nodesvrstate = (ripple_increment_collectornetclient_state*)privdata;

    bigtxn = (ripple_collectornetclient_bigtxn*)nodesvrstate->data;

    if(FileClose(bigtxn->fd))
    {
        elog(RLOG_ERROR, "could not close file %s/%lu", bigtxn->trailpath, bigtxn->xid);
    }
    
    elog(RLOG_INFO, "recv pump 2 collector bigtxn end:%lu", bigtxn->xid);

    /* 网闸下上传最后一个文件 */
    ripple_netmsg_p2cbigtxnend_filegap_add(nodesvrstate, bigtxn->fileid);

    uptr = c2pendbigtxn;
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_BIGTXN_END);
    RIPPLE_CONCAT(put, 32bit)(&uptr, RIPPLE_NETMSG_TYPE_C2P_ENDBIGTXN_SIZE);

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
            if(false == ripple_net_write(nodesvrstate->fd, c2pendbigtxn, RIPPLE_NETMSG_TYPE_C2P_ENDREFRESH_SIZE))
            {
                /* 发送数据失败，关闭连接 */
                elog(RLOG_WARNING, "write RIPPLE_NETMSG_TYPE_C2P_ENDBIGTXN_SIZE error, %s", strerror(errno));
                return true;
            }

            nodesvrstate->hbtimeout = 0;
            elog(RLOG_INFO, "collector 2 pump bigtxn end");
            break;
        }
    }
    return true;
}
