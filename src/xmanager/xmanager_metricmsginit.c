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
#include "xmanager/xmanager_metricmsginit.h"
#include "xmanager/xmanager_metricmsg.h"

/*
 * Handle init command
 *1. jobtype must be less than PROCESS
 *2. Verify job exists, error if not
 *3. Create async message and mount to xscsci node
 *4. Execute init command
 */
bool xmanager_metricmsg_parseinit(xmanager_metric* xmetric, netpoolentry* npoolentry, netpacket* npacket)
{
    int                        errcode = 0;
    int                        len = 0;
    int                        jobtype = 0;
    uint8*                     uptr = NULL;
    char*                      jobname = NULL;
    xmanager_metricnode*       pxmetricnode = NULL;
    xmanager_metricxscscinode* xmetricxscscinode = NULL;
    xmanager_metricfd2node*    fd2node = NULL;
    xmanager_metricasyncmsg*   asyncmsg = NULL;
    xmanager_metricnode        xmetricnode = {0};
    char                       errormsg[2048] = {0};
    char                       execcmd[1024] = {0};

    /* Get job type */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (XMANAGER_METRICNODETYPE_PROCESS <= jobtype)
    {
        errcode = ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg,
                 512,
                 "ERROR: xmanager recv init command, unsupport %s",
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parseinit_error;
    }

    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s", "xmanager recv init command, oom");
        goto xmanager_metricmsg_parseinit_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);

    /* Check if node exists */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto xmanager_metricmsg_parseinit_error;
    }

    if (XMANAGER_METRICNODESTAT_ONLINE == pxmetricnode->stat)
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: %s already start.", jobname);
        goto xmanager_metricmsg_parseinit_error;
    }
    else if (XMANAGER_METRICNODESTAT_OFFLINE == pxmetricnode->stat)
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg,
                 2048,
                 "ERROR: %s already init, use start command start %s node.",
                 jobname,
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parseinit_error;
    }

    /*
     * Create async message
     *  1、Get xscsci node
     *  2、Create async wait message
     */
    fd2node = dlist_get(xmetric->fd2metricnodes, (void*)((uintptr_t)npoolentry->fd), xmanager_metricfd2node_cmp);

    xmetricxscscinode = (xmanager_metricxscscinode*)fd2node->metricnode;
    asyncmsg = xmanager_metricasyncmsg_init();
    if (NULL == asyncmsg)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s", "xmanager recv init command, out of memory");
        goto xmanager_metricmsg_parseinit_error;
    }

    asyncmsg->errormsg = NULL;
    asyncmsg->msgtype = XMANAGER_MSG_INITCMD;
    asyncmsg->type = jobtype;
    asyncmsg->name = jobname;
    jobname = NULL;
    asyncmsg->result = 0;

    /* Execute init command execcmd */
    if (XMANAGER_METRICNODETYPE_PGRECEIVELOG == jobtype)
    {
        snprintf(execcmd, 1024, "%s/bin/pgreceivelog/receivelog -f %s init", xmetric->pgcastorpath, pxmetricnode->conf);
    }
    else
    {
        snprintf(execcmd,
                 1024,
                 "%s/bin/%s -f %s init",
                 xmetric->pgcastorpath,
                 xmanager_metricnode_getname(jobtype),
                 pxmetricnode->conf);
    }

    /* Execute execcmd command */
    if (false == execcommand(execcmd, xmetric->privdata, xmetric->privdatadestroy))
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: can not init %s", xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parseinit_error;
    }

    /* Mount message to async message queue */
    xmetricxscscinode->asyncmsgs->msgs = dlist_put(xmetricxscscinode->asyncmsgs->msgs, asyncmsg);
    return true;

xmanager_metricmsg_parseinit_error:
    if (NULL != jobname)
    {
        rfree(jobname);
    }

    if (NULL != asyncmsg)
    {
        xmanager_metricasyncmsg_destroy(asyncmsg);
    }
    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(xmetric, npoolentry->wpackets, XMANAGER_MSG_INITCMD, errcode, errormsg);
}

/*
 * Assemble init response message
 */
bool xmanager_metricmsg_assembleinit(xmanager_metric* xmetric, netpoolentry* npoolentry, dlist* dlmsgs)
{
    int                      ivalue = 0;
    int                      msglen = 0;
    int                      errmsglen = 0;
    uint8*                   uptr = NULL;
    netpacket*               npacket = NULL;
    xmanager_metricasyncmsg* xmetricasyncmsg = NULL;
    char                     errormsg[2048] = {0};

    if (true == dlist_isnull(dlmsgs) || 1 < dlist_getcount(dlmsgs))
    {
        /* Assemble error message */
        elog(RLOG_WARNING, "metric msg assemble init too many async msgs.");
        snprintf(errormsg, 2048, "metric msg assemble init too many async msgs.");
        return xmanager_metricmsg_assembleerrormsg(xmetric,
                                                   npoolentry->wpackets,
                                                   XMANAGER_MSG_INITCMD,
                                                   ERROR_MSGCOMMAND,
                                                   errormsg);
    }

    xmetricasyncmsg = (xmanager_metricasyncmsg*)dlmsgs->head->value;

    /* Total length + crc32 + type + flag */
    msglen = 4 + 4 + 4 + 1;

    if (0 != xmetricasyncmsg->result)
    {
        /* 4 length + 4 Error code + Error message */
        errmsglen = 4 + 4;
        errmsglen += strlen(xmetricasyncmsg->errormsg);
    }
    msglen += errmsglen;
    msglen += 1;

    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble init msg out of memory");
        return false;
    }

    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble init msg data, out of memory");
        netpacket_destroy(npacket);
        return false;
    }
    msglen -= 1;

    npacket->used = msglen;

    /* Assemble data */
    uptr = npacket->data;

    /* Length */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* Type */
    ivalue = XMANAGER_MSG_INITCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    rmemcpy1(uptr, 0, &xmetricasyncmsg->result, 1);
    uptr += 1;

    if (1 == xmetricasyncmsg->result)
    {
        /* Total length */
        ivalue = errmsglen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;

        /* Error code */
        ivalue = xmetricasyncmsg->errcode;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;

        /* Error message */
        errmsglen -= 8;
        rmemcpy1(uptr, 0, xmetricasyncmsg->errormsg, errmsglen);
    }

    /* Mount netpacket to send queue */
    if (false == queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble init msg add message to queue error");
        netpacket_destroy(npacket);
        return false;
    }
    return true;
}
