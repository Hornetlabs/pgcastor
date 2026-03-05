#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netserver.h"
#include "xmanager/ripple_xmanager_listen.h"

/* 接收新连接 */
static bool ripple_xmanager_listen_netconn(void* netserver, rsocket  sock)
{
    ripple_xmanager_listen* xmgrlisten = NULL;

    xmgrlisten = (ripple_xmanager_listen*)netserver;

    /* 将新连接加入到队列中 */
    return ripple_queue_put(xmgrlisten->authqueue, (void*)((uintptr_t)sock));
}

/* 初始化 */
ripple_xmanager_listen* ripple_xmanager_listen_init(void)
{
    char* unixdomain                    = NULL;
    ripple_xmanager_listen* xmgrlisten  = NULL;
    char unixdomainpath[512]            = { 0 };

    xmgrlisten = (ripple_xmanager_listen*)rmalloc0(sizeof(ripple_xmanager_listen));
    if (NULL == xmgrlisten)
    {
        elog(RLOG_WARNING, "xmanager listen module init error");
        return NULL;
    }
    rmemset0(xmgrlisten, 0, '\0', sizeof(ripple_xmanager_listen));
    
    /* netserver 重置 */
    if (false == ripple_netserver_reset(&xmgrlisten->base))
    {
        elog(RLOG_WARNING, "xmanager listen server init error");
        goto ripple_xmanager_listen_init_error;
    }

    /* 设置 IP 地址监听 */
    ripple_netserver_host_set(&xmgrlisten->base, guc_getConfigOption(RIPPLE_CFG_KEY_HOST), RIPPLE_NETSERVER_HOSTTYPE_IP);

    /* 设置 unixdomain */
    unixdomain = guc_getConfigOption(RIPPLE_CFG_KEY_UNIXDOMAINDIR);
    if (NULL == unixdomain || '\0' == unixdomain[0])
    {
        unixdomain = RMANAGER_UNIXDOMAINDIR;
    }
    snprintf(unixdomainpath, 512, "%s/%s", unixdomain, RMANAGER_UNIXDOMAINPREFIX);

    /* 设置 unixdomain 监听 */
    ripple_netserver_host_set(&xmgrlisten->base, unixdomainpath, RIPPLE_NETSERVER_HOSTTYPE_UNIXDOMAIN);

    /* 设置 监听端口 */
    ripple_netserver_port_set(&xmgrlisten->base, guc_getConfigOptionInt(RIPPLE_CFG_KEY_PORT));

    /* 设置类型 */
    ripple_netserver_type_set(&xmgrlisten->base, RIPPLE_NETSERVER_TYPE_XMANAGER);

    xmgrlisten->base.callback = ripple_xmanager_listen_netconn;

    /* 启动监听 */
    if (false == ripple_netserver_create(&xmgrlisten->base))
    {
        elog(RLOG_WARNING, "xmanager create listen error");
        goto ripple_xmanager_listen_init_error;
    }

    return xmgrlisten;

ripple_xmanager_listen_init_error:
    if (NULL != xmgrlisten)
    {
        rfree(xmgrlisten);
    }
    return NULL;
}

/* 主流程 */
void* ripple_xmanager_listen_main(void *args)
{
    ripple_thrnode* thrnode             = NULL;
    ripple_xmanager_listen* xmgrlisten  = NULL;

    thrnode = (ripple_thrnode *)args;
    xmgrlisten = (ripple_xmanager_listen*)thrnode->data;

    /* 查看状态 */
    if (RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "xmanager listen stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while (1)
    {
        if (RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 监听描述符 */
        if (false == ripple_netserver_desc(&xmgrlisten->base))
        {
            elog(RLOG_WARNING, "xmanager listen desc error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

/* 资源回收 */
void ripple_xmanager_listen_destroy(void* args)
{
    ripple_xmanager_listen* xmgrlisten = NULL;

    xmgrlisten = (ripple_xmanager_listen*)args;
    if (NULL == xmgrlisten)
    {
        return;
    }

    ripple_netserver_free(&xmgrlisten->base);
    rfree(xmgrlisten);
}
