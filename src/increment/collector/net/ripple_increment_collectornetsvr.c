#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/uuid/ripple_uuid.h"
#include "misc/ripple_misc_stat.h"
#include "threads/ripple_threads.h"
#include "storage/ripple_file_buffer.h"
#include "net/netmsg/ripple_netmsg.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netserver.h"
#include "increment/collector/net/ripple_increment_collectornetsvr.h"
#include "increment/collector/netclient/ripple_increment_collectornetclient.h"

static bool ripple_increment_collectornetsvr_newconnhandler(void* in_netsvr, rsocket nsock)
{
    ripple_increment_collectornetclient* nodesvr = NULL;
    ripple_increment_collectornetsvr* netsvr = NULL;

    netsvr = (ripple_increment_collectornetsvr*)in_netsvr;

    /* 注册启动新线程 */
    nodesvr = (ripple_increment_collectornetclient*)rmalloc0(sizeof(ripple_increment_collectornetclient));
    if(NULL == nodesvr)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(nodesvr, 0, '\0', sizeof(ripple_increment_collectornetclient));

    /* 设置新连接 */
    nodesvr->sock = nsock;

    /* 传递privdata 子线程使用 */
    nodesvr->privdata = (void*)netsvr;

    return netsvr->callback.collectornetsvr_netclient_add(netsvr->privdata, nodesvr);
}

/* 初始化操作 */
ripple_increment_collectornetsvr* ripple_increment_collectornetsvr_init(void)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    netsvr = (ripple_increment_collectornetsvr*)rmalloc1(sizeof(ripple_increment_collectornetsvr));
    if(NULL == netsvr)
    {
        elog(RLOG_WARNING, "collector net server out of memory");
        return NULL;
    }
    rmemset0(netsvr, 0, '\0', sizeof(ripple_increment_collectornetsvr));

    netsvr->thrsmgr = NULL;

    /* 初始化网络信息 */
    if (false == ripple_netserver_reset(&netsvr->base))
    {
        elog(RLOG_WARNING, "collector net server out of memory");
        return NULL;
    }

    /* 获取监听主机 */
    ripple_netserver_host_set(&netsvr->base, guc_getConfigOption(RIPPLE_CFG_KEY_HOST), RIPPLE_NETSERVER_HOSTTYPE_IP);

    /* 获取监听端口 */
    ripple_netserver_port_set(&netsvr->base, guc_getConfigOptionInt(RIPPLE_CFG_KEY_PORT));

    /* 设置回调用于处理接收到新的连接 */
    netsvr->base.callback = ripple_increment_collectornetsvr_newconnhandler;

    return netsvr;
}

/* 通过 netsvr 中 privdata获取netbuffer */
ripple_file_buffers* ripple_increment_collectornetsvr_netbuffer_get(void* privdata, char * pumpname)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "networksvr netbuffer get exception, privdata point is NULL");
    }
    
    netsvr = (ripple_increment_collectornetsvr* )privdata;

    /* 使用 privdata中的callback获取collectorstate的 file_buffers*/
    return netsvr->callback.netsvr_netbuffer_get(netsvr->privdata, pumpname);
}

/* 通过 netsvr 中 privdata获取 writestate, pfileid, cfileid */
void ripple_increment_collectornetsvr_writestate_fileid_get(void* privdata, char* name, uint64* pfileid, uint64* cfileid)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "networksvr netbuffer get exception, privdata point is NULL");
    }
    
    netsvr = (ripple_increment_collectornetsvr* )privdata;

    /* 使用 privdata中的callback获取collectorstate的 writestate, pfileid, cfileid */
    return netsvr->callback.writestate_fileid_get(netsvr->privdata, name, pfileid, cfileid);
}

/* 通过 netsvr 中 privdata 添加filetransfernode */
void ripple_increment_collectornetsvr_filetransfernode_add(void* privdata, void* filetransfernode)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "networksvr netbuffer get exception, privdata point is NULL");
    }
    
    netsvr = (ripple_increment_collectornetsvr* )privdata;

    /* 使用 privdata中的callback获取collectorstate的 writestate, pfileid, cfileid */
    return netsvr->callback.collectornetsvr_filetransfernode_add(netsvr->privdata, filetransfernode);
}

/* 通过 networksvrstate中 privdata获设置recvlsn*/
void ripple_increment_collectornetsvr_collectorstate_recvlsn_set(void* privdata, char* name, XLogRecPtr recvlsn)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "networksvr netbuffer get exception, privdata point is NULL");
    }
    
    netsvr = (ripple_increment_collectornetsvr* )privdata;

    /* 使用 privdata中的callback设置collectorstate的 recvlsn*/
    return netsvr->callback.setmetricrecvlsn(netsvr->privdata, name, recvlsn);
}

/* 通过 networksvrstate中 privdata设置接收到 trail 文件编号*/
void ripple_increment_collectornetsvr_collectorstate_recvtrailno_set(void* privdata, char* name, uint64 recvtrailno)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "networksvr netbuffer get exception, privdata point is NULL");
    }
    
    netsvr = (ripple_increment_collectornetsvr* )privdata;

    /* 使用 privdata中的callback设置collectorstate的 接收到 trail 文件编号*/
    return netsvr->callback.setmetricrecvtrailno(netsvr->privdata, name, recvtrailno);
}

/* 通过 networksvrstate中 privdata设置trail 文件内的偏移*/
void ripple_increment_collectornetsvr_collectorstate_recvtrailstart_set(void* privdata, char* name, uint64 recvtrailstart)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "networksvr netbuffer get exception, privdata point is NULL");
    }
    
    netsvr = (ripple_increment_collectornetsvr* )privdata;

    /* 使用 privdata中的callback设置collectorstate的 trail 文件内的偏移*/
    return netsvr->callback.setmetricrecvtrailstart(netsvr->privdata, name, recvtrailstart);
}

/* 通过 networksvrstate中 privdata获设置pump发送的时间戳*/
void ripple_increment_collectornetsvr_collectorstate_recvtimestamp_set(void* privdata, char* name, TimestampTz recvtimestamp)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "networksvr netbuffer get exception, privdata point is NULL");
    }
    
    netsvr = (ripple_increment_collectornetsvr* )privdata;

    /* 使用 privdata中的callback设置pump发送的时间戳*/
    return netsvr->callback.setmetricrecvtimestamp(netsvr->privdata, name, recvtimestamp);
}

/* 向 privdata中添加flash */
bool ripple_increment_collectornetsvr_collectorstate_addflush(void* privdata, char* name)
{
    ripple_increment_collectornetsvr* netsvr = NULL;
    if (NULL == privdata)
    {
        elog(RLOG_ERROR, "networksvr netbuffer get exception, privdata point is NULL");
    }
    
    netsvr = (ripple_increment_collectornetsvr* )privdata;
    /* 向 privdata中添加flash */
    return netsvr->callback.collector_increment_addflush(netsvr->privdata, name);
}


/* 网络服务端 */
void* ripple_increment_collectornetsvr_main(void *args)
{
    ripple_thrnode* thrnode                         = NULL;
    ripple_increment_collectornetsvr* netsvr        = NULL;


    thrnode = (ripple_thrnode*)args;

    netsvr = (ripple_increment_collectornetsvr*)thrnode->data;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "increment collector netsvr exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    /* 创建监听 */
    if(false == ripple_netserver_create(&netsvr->base))
    {
        elog(RLOG_WARNING, "collector net server create listen error");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    while(1)
    {
        /* 查看是否接收到退出信号 */
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 查看是否有新连接，若有新连接，那么注册新的接收线程处理接收 */
        if(false == ripple_netserver_desc(&netsvr->base))
        {
            elog(RLOG_WARNING, "collector net server desc error");

            /* 出错了，那么等待一秒后在处理 */
            sleep(1);
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}


/* 资源回收 */
void ripple_increment_collectornetsvr_destroy(void* args)
{
    ripple_increment_collectornetsvr* netsvr        = NULL;

    netsvr = (ripple_increment_collectornetsvr*)args;

    if(NULL == netsvr)
    {
        return;
    }

    ripple_netserver_free(&netsvr->base);
    netsvr->privdata = NULL;

    rfree(netsvr);
    netsvr = NULL;
}
