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

/* Accept new connection */
static bool xmanager_listen_netconn(void* netserver, rsocket sock)
{
    xmanager_listen* xmgrlisten = NULL;

    xmgrlisten = (xmanager_listen*)netserver;

    /* Add new connection to queue */
    return queue_put(xmgrlisten->authqueue, (void*)((uintptr_t)sock));
}

/* Initialize */
xmanager_listen* xmanager_listen_init(void)
{
    char*            unixdomain = NULL;
    xmanager_listen* xmgrlisten = NULL;
    char             unixdomainpath[512] = {0};

    xmgrlisten = (xmanager_listen*)rmalloc0(sizeof(xmanager_listen));
    if (NULL == xmgrlisten)
    {
        elog(RLOG_WARNING, "xmanager listen module init error");
        return NULL;
    }
    rmemset0(xmgrlisten, 0, '\0', sizeof(xmanager_listen));

    /* Reset netserver */
    if (false == netserver_reset(&xmgrlisten->base))
    {
        elog(RLOG_WARNING, "xmanager listen server init error");
        goto xmanager_listen_init_error;
    }

    /* Set IP address to listen */
    netserver_host_set(&xmgrlisten->base, guc_getConfigOption(CFG_KEY_HOST), NETSERVER_HOSTTYPE_IP);

    /* Set unixdomain */
    unixdomain = guc_getConfigOption(CFG_KEY_UNIXDOMAINDIR);
    if (NULL == unixdomain || '\0' == unixdomain[0])
    {
        unixdomain = RMANAGER_UNIXDOMAINDIR;
    }
    snprintf(unixdomainpath, 512, "%s/%s", unixdomain, RMANAGER_UNIXDOMAINPREFIX);

    /* Set unixdomain listener */
    netserver_host_set(&xmgrlisten->base, unixdomainpath, NETSERVER_HOSTTYPE_UNIXDOMAIN);

    /* Set listen port */
    netserver_port_set(&xmgrlisten->base, guc_getConfigOptionInt(CFG_KEY_PORT));

    /* Set type */
    netserver_type_set(&xmgrlisten->base, NETSERVER_TYPE_XMANAGER);

    xmgrlisten->base.callback = xmanager_listen_netconn;

    /* Start listening */
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

/* Main loop */
void* xmanager_listen_main(void* args)
{
    thrnode*         thrnode_ptr = NULL;
    xmanager_listen* xmgrlisten = NULL;

    thrnode_ptr = (thrnode*)args;
    xmgrlisten = (xmanager_listen*)thrnode_ptr->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thrnode_ptr->stat)
    {
        elog(RLOG_WARNING,
             "xmanager listen stat exception, expected state is THRNODE_STAT_STARTING");
        thrnode_ptr->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thrnode_ptr->stat = THRNODE_STAT_WORK;

    while (1)
    {
        if (THRNODE_STAT_TERM == thrnode_ptr->stat)
        {
            thrnode_ptr->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Listen on descriptors */
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

/* Resource cleanup */
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
