#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/dlist/dlist.h"
#include "utils/daemon/process.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricasyncmsg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metricxscscinode.h"
#include "xmanager/xmanager_metriccapturenode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsgcapturerefresh.h"
#include "xmanager/xmanager_metricmsg.h"

/*
 * Handle capture onlinerefresh command
 */
bool xmanager_metricmsg_parsecapturerefresh(xmanager_metric* xmetric, netpoolentry* npoolentry, netpacket* npacket)
{
    /*
     * 1. Get capture metric node
     * 2. Traverse all nodes, get correct xscsci node
     * 3. Assemble data to send to xscsci node
     */
    bool                       found = false;
    int                        errcode = 0;
    int                        resultlen = 0;
    uint8*                     uptr = NULL;
    dlistnode*                 dlnode = NULL;
    char*                      errmsg = NULL;
    dlistnode*                 dlnodemsg = NULL;
    dlistnode*                 dlnodemsgtmp = NULL;
    xmanager_metricnode*       xmetricnode = NULL;
    xmanager_metricfd2node*    fd2node = NULL;
    xmanager_metricasyncmsg*   asyncmsg = NULL;
    xmanager_metricxscscinode* xmetricxscscinode = NULL;

    /* Get last content */
    uptr = npacket->data;

    uptr += 4 + 4 + 4 + 1;

    /* Total length */
    rmemcpy1(&resultlen, 0, uptr, 4);
    resultlen = r_ntoh32(resultlen);
    uptr += 4;
    resultlen -= 8;

    /* Get error code */
    rmemcpy1(&errcode, 0, uptr, 4);
    errcode = r_ntoh32(errcode);
    uptr += 4;

    resultlen += 1;
    errmsg = rmalloc0(resultlen);
    if (NULL == errmsg)
    {
        elog(RLOG_WARNING, "parse capture onlinerefresh msg out of memory");
        return false;
    }
    rmemset0(errmsg, 0, '\0', resultlen);
    resultlen -= 1;
    rmemcpy0(errmsg, 0, uptr, resultlen);

    /* Get capture node */
    fd2node = dlist_get(xmetric->fd2metricnodes, (void*)((uintptr_t)npoolentry->fd), xmanager_metricfd2node_cmp);
    xmetricnode = fd2node->metricnode;

    /* Get xscsci node */
    for (dlnode = xmetric->fd2metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        fd2node = (xmanager_metricfd2node*)dlnode->value;
        if (XMANAGER_METRICNODETYPE_XSCSCI != fd2node->metricnode->type)
        {
            continue;
        }

        xmetricxscscinode = (xmanager_metricxscscinode*)fd2node->metricnode;
        if (NULL == xmetricxscscinode->asyncmsgs || true == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
        {
            continue;
        }

        dlnodemsg = xmetricxscscinode->asyncmsgs->msgs->head;
        while (NULL != dlnodemsg)
        {
            asyncmsg = (xmanager_metricasyncmsg*)dlnodemsg->value;
            dlnodemsgtmp = dlnodemsg->next;

            if (XMANAGER_MSG_REFRESHCMD != asyncmsg->msgtype)
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            if (xmetricnode->type != asyncmsg->type)
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            if (0 != strcmp(xmetricnode->name, asyncmsg->name))
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            /* Found node */
            npoolentry = netpool_getentrybyfd(xmetric->npool, fd2node->fd);
            if (NULL == npoolentry)
            {
                elog(RLOG_WARNING, "can not get pool entry by fd:%d", fd2node->fd);
                rfree(errmsg);
                return true;
            }

            /* Build message */
            found = true;

            /* Move asyncmsg to results */
            asyncmsg->result = 1;
            asyncmsg->errcode = ERROR_APPENDMSG;
            asyncmsg->errormsg = errmsg;
            errmsg = NULL;

            /* Delete dlnodemsg from messages */
            xmetricxscscinode->asyncmsgs->msgs = dlist_delete(xmetricxscscinode->asyncmsgs->msgs, dlnodemsg, NULL);

            /* Add asyncmsg to results */
            xmetricxscscinode->asyncmsgs->results = dlist_put(xmetricxscscinode->asyncmsgs->results, asyncmsg);
            break;
        }
    }

    if (false == found)
    {
        /* Not processed, return */
        rfree(errmsg);
        return true;
    }

    /* Check if there are async messages waiting for feedback */
    if (false == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
    {
        elog(RLOG_WARNING, "un done");
        return true;
    }

    if (false == xmanager_metricmsg_assembleresponse(xmetric,
                                                     npoolentry,
                                                     XMANAGER_MSG_REFRESHCMD,
                                                     xmetricxscscinode->asyncmsgs->results))
    {
        elog(RLOG_WARNING, "assemble response to xscsci error, close xscsci connect");

        /* Remove node from netpool */
        netpool_del(xmetric->npool, npoolentry->fd);

        /* Remove metricnode from list */
        xmetric->metricnodes = dlist_deletebyvaluefirstmatch(xmetric->metricnodes,
                                                             xmetricxscscinode,
                                                             xmanager_metricnode_cmp,
                                                             xmanager_metricnode_destroyvoid);

        /* Remove data from fd mapping list */
        xmetric->fd2metricnodes = dlist_delete(xmetric->fd2metricnodes, dlnode, xmanager_metricfd2node_destroyvoid);
        return true;
    }

    /* Clean up results */
    dlist_free(xmetricxscscinode->asyncmsgs->results, xmanager_metricasyncmsg_destroyvoid);
    xmetricxscscinode->asyncmsgs->results = NULL;
    xmetricxscscinode->asyncmsgs->timeout = 0;
    return true;
}
