#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/dttime/dttimestamp.h"
#include "net/netpacket/netpacket.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricasyncmsg.h"


xmanager_metricasyncmsg* xmanager_metricasyncmsg_init(void)
{
    xmanager_metricasyncmsg* xmetricasyncmsg = NULL;

    xmetricasyncmsg = rmalloc0(sizeof(xmanager_metricasyncmsg));
    if (NULL == xmetricasyncmsg)
    {
        elog(RLOG_WARNING, "xmanager metric async msg error, out of memory");
        return NULL;
    }
    rmemset0(xmetricasyncmsg, 0, '\0', sizeof(xmanager_metricasyncmsg));
    xmetricasyncmsg->errcode = 0;
    xmetricasyncmsg->errormsg = NULL;
    xmetricasyncmsg->name = NULL;
    xmetricasyncmsg->msgtype = XMANAGER_MSG_NOP;
    xmetricasyncmsg->result = 0;
    xmetricasyncmsg->type = XMANAGER_METRICNODETYPE_NOP;
    return xmetricasyncmsg;
}

/* 删除 metricasyncmsg */
void xmanager_metricasyncmsg_destroy(xmanager_metricasyncmsg* xmetricasyncmsg)
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
void xmanager_metricasyncmsg_destroyvoid(void* args)
{
    xmanager_metricasyncmsg_destroy((xmanager_metricasyncmsg*)args);
}

xmanager_metricasyncmsgs* xmanager_metricasyncmsgs_init(void)
{
    xmanager_metricasyncmsgs* xmetricasyncmsgs = NULL;

    xmetricasyncmsgs = rmalloc0(sizeof(xmanager_metricasyncmsgs));
    if (NULL == xmetricasyncmsgs)
    {
        elog(RLOG_WARNING, "xmanager metric asyncmsg init error, out of memory");
        return NULL;
    }
    rmemset0(xmetricasyncmsgs, 0, '\0', sizeof(xmanager_metricasyncmsgs));
    xmetricasyncmsgs->timeout = 0;
    xmetricasyncmsgs->msgs = NULL;
    xmetricasyncmsgs->results = NULL;
    return xmetricasyncmsgs;
}

void xmanager_metricasyncmsgs_destroy(xmanager_metricasyncmsgs* xmetricasyncmsgs)
{
    if (NULL == xmetricasyncmsgs)
    {
        return;
    }

    dlist_free(xmetricasyncmsgs->results, xmanager_metricasyncmsg_destroyvoid);
    dlist_free(xmetricasyncmsgs->msgs, xmanager_metricasyncmsg_destroyvoid);
    rfree(xmetricasyncmsgs);
}
