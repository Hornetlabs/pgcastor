#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/path/ripple_path.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "threads/ripple_threads.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/ripple_netserver.h"
#include "net/ripple_netpool.h"
#include "xmanager/ripple_xmanager_listen.h"
#include "xmanager/ripple_xmanager_auth.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager.h"

void ripple_xmanager_destroy(ripple_xmanager* xmgr)
{
    if (NULL == xmgr)
    {
        return;
    }

    ripple_queue_destroy(xmgr->authqueue, NULL);

    ripple_queue_destroy(xmgr->metricqueue, NULL);

    ripple_xmanager_listen_destroy((void*)xmgr->listens);

    ripple_xmanager_auth_destroy(xmgr->auth);
    ripple_xmanager_metric_destroy(xmgr->metric);

    ripple_threads_free(xmgr->threads);

    rfree(xmgr);
}

static void ripple_xmanager_destroyvoid(void* args)
{
    ripple_xmanager_destroy((ripple_xmanager*)args);
}

ripple_xmanager* ripple_xmanager_init(void)
{
    ripple_xmanager* xmgr = NULL;

    /*
     * 管理初始化
     *  1、结构初始化
     *  2、启动常驻线程
     */
    xmgr = rmalloc0(sizeof(ripple_xmanager));
    if (NULL == xmgr)
    {
        elog(RLOG_WARNING, "xmgr init, out of memory");
        return NULL;
    }
    rmemset0(xmgr, 0, '\0', sizeof(ripple_xmanager));

    xmgr->xsynchpath = getenv("XSYNCH");
    if (NULL == xmgr->xsynchpath)
    {
        elog(RLOG_WARNING, "please config XSYNCH, like: export XSYNCH=PATH");
        return NULL;
    }

    xmgr->threads = ripple_threads_init();
    if(NULL == xmgr->threads)
    {
        elog(RLOG_WARNING, "xmanager init threads error");
        ripple_xmanager_destroy(xmgr);
        return NULL;
    }

    /* 队列初始化 */
    xmgr->authqueue = ripple_queue_init();
    if (NULL == xmgr->authqueue)
    {
        elog(RLOG_WARNING, "xmanager init auth queue error");
        ripple_xmanager_destroy(xmgr);
        return NULL;
    }

    xmgr->metricqueue = ripple_queue_init();
    if (NULL == xmgr->metricqueue)
    {
        elog(RLOG_WARNING, "xmanager init metric queue error");
        ripple_xmanager_destroy(xmgr);
        return NULL;
    }

    /* 常驻线程 */
    xmgr->listens = ripple_xmanager_listen_init();
    if (NULL == xmgr->listens)
    {
        elog(RLOG_WARNING, "xmanager listen init error");
        ripple_xmanager_destroy(xmgr);
        return NULL;
    }
    xmgr->listens->authqueue = xmgr->authqueue;

    /* auth 初始化 */
    xmgr->auth = ripple_xmanager_auth_init();
    if (NULL == xmgr->auth)
    {
        elog(RLOG_WARNING, "xmanager auth init error");
        ripple_xmanager_destroy(xmgr);
        return NULL;
    }
    xmgr->auth->authqueue = xmgr->authqueue;
    xmgr->auth->metricqueue = xmgr->metricqueue;

    /* metric 初始化 */
    xmgr->metric = ripple_xmanager_metric_init();
    if (NULL == xmgr->metric)
    {
        elog(RLOG_WARNING, "xmanager metric init error");
        ripple_xmanager_destroy(xmgr);
        return NULL;
    }
    if (false == ripple_xmanager_metric_setxsynchpath(xmgr->metric, xmgr->xsynchpath))
    {
        elog(RLOG_WARNING, "xmanager metric set config path error");
        ripple_xmanager_destroy(xmgr);
        return NULL;
    }
    xmgr->metric->metricqueue = xmgr->metricqueue;
    xmgr->metric->privdata = xmgr;
    xmgr->metric->privdatadestroy = ripple_xmanager_destroyvoid;

    return xmgr;
}

