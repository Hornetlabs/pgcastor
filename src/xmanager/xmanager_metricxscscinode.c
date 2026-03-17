#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/dttime/dttimestamp.h"
#include "net/netpacket/netpacket.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricasyncmsg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metricxscscinode.h"

void xmanager_metricxscscinode_destroy(xmanager_metricnode* metricnode)
{
    xmanager_metricxscscinode* xmetricxscscinode = NULL;

    xmetricxscscinode = (xmanager_metricxscscinode*)metricnode;
    if (NULL == xmetricxscscinode)
    {
        return;
    }

    xmanager_metricasyncmsgs_destroy(xmetricxscscinode->asyncmsgs);
    rfree(xmetricxscscinode);
}

xmanager_metricnode* xmanager_metricxscscinode_init(void)
{
    xmanager_metricxscscinode* xscscinode = NULL;

    xscscinode = rmalloc0(sizeof(xmanager_metricxscscinode));
    if (NULL == xscscinode)
    {
        elog(RLOG_WARNING, "metric xscsci node init error");
        return NULL;
    }

    xmanager_metricnode_reset(&xscscinode->base);
    xscscinode->base.type = XMANAGER_METRICNODETYPE_XSCSCI;
    xscscinode->number = 0;
    xscscinode->asyncmsgs = xmanager_metricasyncmsgs_init();
    if (NULL == xscscinode->asyncmsgs)
    {
        elog(RLOG_WARNING, "metric xscsci node init async msg error");
        xmanager_metricxscscinode_destroy((xmanager_metricnode*)xscscinode);
        return NULL;
    }
    return (xmanager_metricnode*)xscscinode;
}

int xmanager_metricxscscinode_cmp(void* s1, void* s2)
{
    xmanager_metricxscscinode* xscscinode1 = NULL;
    xmanager_metricxscscinode* xscscinode2 = NULL;

    xscscinode1 = (xmanager_metricxscscinode*)s1;
    xscscinode2 = (xmanager_metricxscscinode*)s2;
    if (xscscinode1->number != xscscinode2->number)
    {
        return 1;
    }

    return 0;
}
