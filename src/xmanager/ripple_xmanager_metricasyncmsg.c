#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/dttime/dttimestamp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricasyncmsg.h"


ripple_xmanager_metricasyncmsg* ripple_xmanager_metricasyncmsg_init(void)
{
    ripple_xmanager_metricasyncmsg* xmetricasyncmsg = NULL;

    xmetricasyncmsg = rmalloc0(sizeof(ripple_xmanager_metricasyncmsg));
    if (NULL == xmetricasyncmsg)
    {
        elog(RLOG_WARNING, "xmanager metric async msg error, out of memory");
        return NULL;
    }
    rmemset0(xmetricasyncmsg, 0, '\0', sizeof(ripple_xmanager_metricasyncmsg));
    xmetricasyncmsg->errcode = 0;
    xmetricasyncmsg->errormsg = NULL;
    xmetricasyncmsg->name = NULL;
    xmetricasyncmsg->msgtype = RIPPLE_XMANAGER_MSG_NOP;
    xmetricasyncmsg->result = 0;
    xmetricasyncmsg->type = RIPPLE_XMANAGER_METRICNODETYPE_NOP;
    return xmetricasyncmsg;
}

/* 删除 metricasyncmsg */
void ripple_xmanager_metricasyncmsg_destroy(ripple_xmanager_metricasyncmsg* xmetricasyncmsg)
{
    if (NULL == xmetricasyncmsg)
    {
        return;
    }

    if (NULL != xmetricasyncmsg->name)
    {
        rfree(xmetricasyncmsg->name);
        xmetricasyncmsg->name = NULL;
    }

    if (NULL == xmetricasyncmsg->errormsg)
    {
        rfree(xmetricasyncmsg->errormsg);
        xmetricasyncmsg->errormsg = NULL;
    }
    rfree(xmetricasyncmsg);
}

/* 删除 metricasyncmsg */
void ripple_xmanager_metricasyncmsg_destroyvoid(void* args)
{
    ripple_xmanager_metricasyncmsg_destroy((ripple_xmanager_metricasyncmsg*)args);
}

ripple_xmanager_metricasyncmsgs* ripple_xmanager_metricasyncmsgs_init(void)
{
    ripple_xmanager_metricasyncmsgs* xmetricasyncmsgs = NULL;

    xmetricasyncmsgs = rmalloc0(sizeof(ripple_xmanager_metricasyncmsgs));
    if (NULL == xmetricasyncmsgs)
    {
        elog(RLOG_WARNING, "xmanager metric asyncmsg init error, out of memory");
        return NULL;
    }
    rmemset0(xmetricasyncmsgs, 0, '\0', sizeof(ripple_xmanager_metricasyncmsgs));
    xmetricasyncmsgs->timeout = 0;
    xmetricasyncmsgs->msgs = NULL;
    xmetricasyncmsgs->results = NULL;
    return xmetricasyncmsgs;
}

void ripple_xmanager_metricasyncmsgs_destroy(ripple_xmanager_metricasyncmsgs* xmetricasyncmsgs)
{
    if (NULL == xmetricasyncmsgs)
    {
        return;
    }

    dlist_free(xmetricasyncmsgs->results, ripple_xmanager_metricasyncmsg_destroyvoid);
    dlist_free(xmetricasyncmsgs->msgs, ripple_xmanager_metricasyncmsg_destroyvoid);
    rfree(xmetricasyncmsgs);
}
