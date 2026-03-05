#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/dttime/dttimestamp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricasyncmsg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metricxscscinode.h"

void ripple_xmanager_metricxscscinode_destroy(ripple_xmanager_metricnode* metricnode)
{
    ripple_xmanager_metricxscscinode* xmetricxscscinode = NULL;

    xmetricxscscinode = (ripple_xmanager_metricxscscinode*)metricnode;
    if (NULL == xmetricxscscinode)
    {
        return;
    }

    ripple_xmanager_metricasyncmsgs_destroy(xmetricxscscinode->asyncmsgs);
    rfree(xmetricxscscinode);
}

ripple_xmanager_metricnode* ripple_xmanager_metricxscscinode_init(void)
{
    ripple_xmanager_metricxscscinode* xscscinode = NULL;

    xscscinode = rmalloc0(sizeof(ripple_xmanager_metricxscscinode));
    if (NULL == xscscinode)
    {
        elog(RLOG_WARNING, "metric xscsci node init error");
        return NULL;
    }

    ripple_xmanager_metricnode_reset(&xscscinode->base);
    xscscinode->base.type = RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI;
    xscscinode->number = 0;
    xscscinode->asyncmsgs = ripple_xmanager_metricasyncmsgs_init();
    if (NULL == xscscinode->asyncmsgs)
    {
        elog(RLOG_WARNING, "metric xscsci node init async msg error");
        ripple_xmanager_metricxscscinode_destroy((ripple_xmanager_metricnode*)xscscinode);
        return NULL;
    }
    return (ripple_xmanager_metricnode*)xscscinode;
}

int ripple_xmanager_metricxscscinode_cmp(void* s1, void* s2)
{
    ripple_xmanager_metricxscscinode* xscscinode1 = NULL;
    ripple_xmanager_metricxscscinode* xscscinode2 = NULL;

    xscscinode1 = (ripple_xmanager_metricxscscinode*)s1;
    xscscinode2 = (ripple_xmanager_metricxscscinode*)s2;
    if (xscscinode1->number != xscscinode2->number)
    {
        return 1;
    }

    return 0;
}
