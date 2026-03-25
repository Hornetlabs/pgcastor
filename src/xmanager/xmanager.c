#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/path/path.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netserver.h"
#include "net/netpool.h"
#include "xmanager/xmanager_listen.h"
#include "xmanager/xmanager_auth.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager.h"

void xmanager_destroy(xmanager* xmgr)
{
    if (NULL == xmgr)
    {
        return;
    }

    queue_destroy(xmgr->authqueue, NULL);

    queue_destroy(xmgr->metricqueue, NULL);

    xmanager_listen_destroy((void*)xmgr->listens);

    xmanager_auth_destroy(xmgr->auth);
    xmanager_metric_destroy(xmgr->metric);

    threads_free(xmgr->threads);

    rfree(xmgr);
}

static void xmanager_destroyvoid(void* args)
{
    xmanager_destroy((xmanager*)args);
}

xmanager* xmanager_init(void)
{
    xmanager* xmgr = NULL;

    /*
     * Manager initialization
     *1. Structure initialization
     *2. Start daemon thread
     */
    xmgr = rmalloc0(sizeof(xmanager));
    if (NULL == xmgr)
    {
        elog(RLOG_WARNING, "xmgr init, out of memory");
        return NULL;
    }
    rmemset0(xmgr, 0, '\0', sizeof(xmanager));

    xmgr->xsynchpath = getenv("XSYNCH");
    if (NULL == xmgr->xsynchpath)
    {
        elog(RLOG_WARNING, "please config XSYNCH, like: export XSYNCH=PATH");
        return NULL;
    }

    xmgr->threads = threads_init();
    if (NULL == xmgr->threads)
    {
        elog(RLOG_WARNING, "xmanager init threads error");
        xmanager_destroy(xmgr);
        return NULL;
    }

    /* Queue initialization */
    xmgr->authqueue = queue_init();
    if (NULL == xmgr->authqueue)
    {
        elog(RLOG_WARNING, "xmanager init auth queue error");
        xmanager_destroy(xmgr);
        return NULL;
    }

    xmgr->metricqueue = queue_init();
    if (NULL == xmgr->metricqueue)
    {
        elog(RLOG_WARNING, "xmanager init metric queue error");
        xmanager_destroy(xmgr);
        return NULL;
    }

    /* Daemon thread */
    xmgr->listens = xmanager_listen_init();
    if (NULL == xmgr->listens)
    {
        elog(RLOG_WARNING, "xmanager listen init error");
        xmanager_destroy(xmgr);
        return NULL;
    }
    xmgr->listens->authqueue = xmgr->authqueue;

    /* Auth initialization */
    xmgr->auth = xmanager_auth_init();
    if (NULL == xmgr->auth)
    {
        elog(RLOG_WARNING, "xmanager auth init error");
        xmanager_destroy(xmgr);
        return NULL;
    }
    xmgr->auth->authqueue = xmgr->authqueue;
    xmgr->auth->metricqueue = xmgr->metricqueue;

    /* Metric initialization */
    xmgr->metric = xmanager_metric_init();
    if (NULL == xmgr->metric)
    {
        elog(RLOG_WARNING, "xmanager metric init error");
        xmanager_destroy(xmgr);
        return NULL;
    }
    if (false == xmanager_metric_setxsynchpath(xmgr->metric, xmgr->xsynchpath))
    {
        elog(RLOG_WARNING, "xmanager metric set config path error");
        xmanager_destroy(xmgr);
        return NULL;
    }
    xmgr->metric->metricqueue = xmgr->metricqueue;
    xmgr->metric->privdata = xmgr;
    xmgr->metric->privdatadestroy = xmanager_destroyvoid;

    return xmgr;
}
