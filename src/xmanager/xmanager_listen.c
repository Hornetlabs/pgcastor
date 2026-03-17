#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netserver.h"
#include "xmanager/xmanager_listen.h"

/* 接收新连接 */
static bool xmanager_listen_netconn(void* netserver, rsocket  sock)
{
    xmanager_listen* xmgrlisten = NULL;

    xmgrlisten = (xmanager_listen*)netserver;

    /* 将新连接加入到队列中 */
    return queue_put(xmgrlisten->authqueue, (void*)((uintptr_t)sock));
}

/* 初始化 */
xmanager_listen* xmanager_listen_init(void)
{
    char* unixdomain                    = NULL;
    xmanager_listen* xmgrlisten  = NULL;
    char unixdomainpath[512]            = { 0 };

    xmgrlisten = (xmanager_listen*)rmalloc0(sizeof(xmanager_listen));
    if (NULL == xmgrlisten)
    {
        elog(RLOG_WARNING, "xmanager listen module init error");
        return NULL;
    }
    rmemset0(xmgrlisten, 0, '\0', sizeof(xmanager_listen));
    
    /* netserver 重置 */
    if (false == netserver_reset(&xmgrlisten->base))
    {
        elog(RLOG_WARNING, "xmanager listen server init error");
        goto xmanager_listen_init_error;
    }

    /* 设置 IP 地址监听 */
    netserver_host_set(&xmgrlisten->base, guc_getConfigOption(CFG_KEY_HOST), NETSERVER_HOSTTYPE_IP);

    /* 设置 unixdomain */
    unixdomain = guc_getConfigOption(CFG_KEY_UNIXDOMAINDIR);
    if (NULL == unixdomain || '\0' == unixdomain[0])
    {
        unixdomain = RMANAGER_UNIXDOMAINDIR;
    }
    snprintf(unixdomainpath, 512, "%s/%s", unixdomain, RMANAGER_UNIXDOMAINPREFIX);

    /* 设置 unixdomain 监听 */
    netserver_host_set(&xmgrlisten->base, unixdomainpath, NETSERVER_HOSTTYPE_UNIXDOMAIN);

    /* 设置 监听端口 */
    netserver_port_set(&xmgrlisten->base, guc_getConfigOptionInt(CFG_KEY_PORT));

    /* 设置类型 */
    netserver_type_set(&xmgrlisten->base, NETSERVER_TYPE_XMANAGER);

    xmgrlisten->base.callback = xmanager_listen_netconn;

    /* 启动监听 */
    if (false == netserver_create(&xmgrlisten->base))
    {
        elog(RLOG_WARNING, "xmanager create listen error");
        goto xmanager_listen_init_error;
    }

    return xmgrlisten;

xmanager_listen_init_error:
    if (NULL != xmgrlisten)
    {
        rfree(xmgrlisten);
    }
    return NULL;
}

/* 主流程 */
void* xmanager_listen_main(void *args)
{
    thrnode* thrnode_ptr             = NULL;
    xmanager_listen* xmgrlisten  = NULL;

    thrnode_ptr = (thrnode *)args;
    xmgrlisten = (xmanager_listen*)thrnode_ptr->data;

    /* 查看状态 */
    if (THRNODE_STAT_STARTING != thrnode_ptr->stat)
    {
        elog(RLOG_WARNING, "xmanager listen stat exception, expected state is THRNODE_STAT_STARTING");
        thrnode_ptr->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode_ptr->stat = THRNODE_STAT_WORK;

    while (1)
    {
        if (THRNODE_STAT_TERM == thrnode_ptr->stat)
        {
            thrnode_ptr->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* 监听描述符 */
        if (false == netserver_desc(&xmgrlisten->base))
        {
            elog(RLOG_WARNING, "xmanager listen desc error");
            thrnode_ptr->stat = THRNODE_STAT_ABORT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

/* 资源回收 */
void xmanager_listen_destroy(void* args)
{
    xmanager_listen* xmgrlisten = NULL;

    xmgrlisten = (xmanager_listen*)args;
    if (NULL == xmgrlisten)
    {
        return;
    }

    netserver_free(&xmgrlisten->base);
    rfree(xmgrlisten);
}
