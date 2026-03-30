#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/daemon/process.h"
#include "command/cmd.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricasyncmsg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metricxscscinode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsgrefresh.h"
#include "xmanager/xmanager_metricmsg.h"

/* Assemble capture refresh message */
static bool xmanager_metricmsg_assemblerefreshforcapture(xmanager_metric*     xmetric,
                                                         xmanager_metricnode* xmetricnode,
                                                         int                  tableslen,
                                                         uint8*               tables)
{
    int                     ivalue = 0;
    int                     msglen = 0;
    uint8*                  uptr = NULL;
    netpacket*              npacket = NULL;
    netpoolentry*           npoolentry = NULL;
    xmanager_metricfd2node* fd2node = NULL;

    /* Build onlinerefresh message */
    fd2node = dlist_get(xmetric->fd2metricnodes, xmetricnode, xmanager_metricfd2node_cmp2);

    /* Get capture node netpoolentry */
    npoolentry = NULL;
    npoolentry = netpool_getentrybyfd(xmetric->npool, fd2node->fd);
    if (NULL == npoolentry)
    {
        elog(RLOG_WARNING, "can not get capture node");
        return false;
    }

    /*
     * Length calculation
     *  4 + 4 + 4
     * msglen + crc32 + cmdtype
     */
    msglen = 12;
    msglen += tableslen;

    /* Build message and mount to npoolentry */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble refresh to capture out of memory");
        return false;
    }

    msglen += 1;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble refresh msg data, out of memory");
        netpacket_destroy(npacket);
        return false;
    }
    msglen -= 1;
    npacket->used = msglen;

    /* Assemble data */
    uptr = npacket->data;

    /* Total data length */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* Type */
    ivalue = XMANAGER_MSG_CAPTUREREFRESH;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    rmemcpy1(uptr, 0, tables, tableslen);

    /* Mount netpacket to send queue */
    if (false == queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble refresh msg to capture add message to queue error");
        netpacket_destroy(npacket);
        return false;
    }

    return true;
}

/*
 * Handle refresh command
 *1. Verify job exists, error if not
 *2. Forward refresh message to capture
 *3. Create async message and mount to xscsci node
 */
bool xmanager_metricmsg_parserefresh(xmanager_metric* xmetric, netpoolentry* npoolentry, netpacket* npacket)
{
    int                        errcode = 0;
    int                        ivalue = 0;
    int                        msglen = 0;

    uint8*                     uptr = NULL;
    char*                      jobname = NULL;
    xmanager_metricnode*       pxmetricnode = NULL;
    xmanager_metricfd2node*    fd2node = NULL;
    xmanager_metricasyncmsg*   asyncmsg = NULL;
    xmanager_metricxscscinode* xmetricxscscinode = NULL;
    char                       errormsg[2048] = {0};
    xmanager_metricnode        xmetricnode = {0};

    /* Get job type */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    rmemcpy1(&msglen, 0, uptr, 4);
    msglen = r_ntoh32(msglen);
    uptr += 12;
    msglen -= 12;

    /* There is no jobtype in refresh */

    /* jobname */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    ivalue += 1;
    msglen -= 4;

    jobname = rmalloc0(ivalue);
    if (NULL == jobname)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: xmanager parse refresh command, out of memory.");
        goto xmanager_metricmsg_parserefresh_error;
    }
    rmemset0(jobname, 0, '\0', ivalue);
    ivalue -= 1;
    rmemcpy0(jobname, 0, uptr, ivalue);
    msglen -= ivalue;
    uptr += ivalue;

    /* Check if node exists */
    xmetricnode.type = XMANAGER_METRICNODETYPE_CAPTURE;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto xmanager_metricmsg_parserefresh_error;
    }

    if (XMANAGER_METRICNODESTAT_ONLINE != pxmetricnode->stat)
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: start %s first.", jobname);
        goto xmanager_metricmsg_parserefresh_error;
    }

    if (false == xmanager_metricmsg_assemblerefreshforcapture(xmetric, pxmetricnode, msglen, uptr))
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: assemble refresh message to capture error.");
        goto xmanager_metricmsg_parserefresh_error;
    }

    /*
     * Create async message
     *  1、Get xscsci node
     *  2、Create async wait message
     */
    /* Get xscsci node */
    fd2node = dlist_get(xmetric->fd2metricnodes, (void*)((uintptr_t)npoolentry->fd), xmanager_metricfd2node_cmp);
    xmetricxscscinode = (xmanager_metricxscscinode*)fd2node->metricnode;

    /* Create async wait message */
    asyncmsg = xmanager_metricasyncmsg_init();
    if (NULL == asyncmsg)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s", "xmanager refresh command, out of memory");
        goto xmanager_metricmsg_parserefresh_error;
    }

    asyncmsg->errormsg = NULL;
    asyncmsg->msgtype = XMANAGER_MSG_REFRESHCMD;
    asyncmsg->type = XMANAGER_METRICNODETYPE_CAPTURE;
    asyncmsg->name = jobname;
    jobname = NULL;
    asyncmsg->result = 0;

    xmetricxscscinode->asyncmsgs->msgs = dlist_put(xmetricxscscinode->asyncmsgs->msgs, asyncmsg);
    return true;

xmanager_metricmsg_parserefresh_error:
    if (NULL != jobname)
    {
        rfree(jobname);
    }

    if (NULL != asyncmsg)
    {
        xmanager_metricasyncmsg_destroy(asyncmsg);
    }

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(xmetric,
                                               npoolentry->wpackets,
                                               XMANAGER_MSG_REFRESHCMD,
                                               errcode,
                                               errormsg);
    return false;
}

/*
 * Assemble refresh response message
 */
bool xmanager_metricmsg_assemblerefresh(xmanager_metric* xmetric, netpoolentry* npoolentry, dlist* dlmsgs)
{
    xmanager_metricasyncmsg* xmetricasyncmsg = NULL;
    char                     errmsg[2048] = {0};

    if (true == dlist_isnull(dlmsgs) || 1 < dlist_getcount(dlmsgs))
    {
        /* Assemble error message */
        elog(RLOG_WARNING, "metric msg assemble refresh too many async msgs.");
        snprintf(errmsg, 2048, "metric msg assemble refresh too many async msgs.");
        return xmanager_metricmsg_assembleerrormsg(xmetric,
                                                   npoolentry->wpackets,
                                                   XMANAGER_MSG_REFRESHCMD,
                                                   ERROR_MSGCOMMAND,
                                                   errmsg);
    }

    xmetricasyncmsg = (xmanager_metricasyncmsg*)dlmsgs->head->value;
    return xmanager_metricmsg_assembleerrormsg(xmetric,
                                               npoolentry->wpackets,
                                               XMANAGER_MSG_REFRESHCMD,
                                               xmetricasyncmsg->errcode,
                                               xmetricasyncmsg->errormsg);
}
