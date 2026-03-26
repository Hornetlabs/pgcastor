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
#include "xmanager/xmanager_metricmsgstop.h"
#include "xmanager/xmanager_metricmsg.h"

bool xmanager_metricmsg_parsestop(xmanager_metric* xmetric,
                                  netpoolentry*    npoolentry,
                                  netpacket*       npacket)
{
    int                        len = 0;
    int                        jobtype = 0;
    int                        errcode = 0;
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
                 2048,
                 "ERROR: xmanager parse stop command, unsupport %s",
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parsestop_error;
    }

    /* Get jobname */
    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: xmanager parse stop command, out of memory.");
        goto xmanager_metricmsg_parsestop_error;
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
        snprintf(errormsg, 2048, "%s does not exist.", jobname);
        goto xmanager_metricmsg_parsestop_error;
    }

    if (XMANAGER_METRICNODESTAT_OFFLINE == pxmetricnode->stat)
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "%s already stoped.", jobname);
        goto xmanager_metricmsg_parsestop_error;
    }
    else if (XMANAGER_METRICNODESTAT_INIT == pxmetricnode->stat)
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg,
                 2048,
                 " %s not init, use init command init %s node.",
                 jobname,
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parsestop_error;
    }

    /*
     * Create async message
     *  1、Get xscsci node
     *  2、Create async wait message
     */
    /* Get xscsci node */
    fd2node = dlist_get(
        xmetric->fd2metricnodes, (void*)((uintptr_t)npoolentry->fd), xmanager_metricfd2node_cmp);
    xmetricxscscinode = (xmanager_metricxscscinode*)fd2node->metricnode;

    /* Create async wait message */
    asyncmsg = xmanager_metricasyncmsg_init();
    if (NULL == asyncmsg)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s", "xmanager recv stop command, out of memory");
        goto xmanager_metricmsg_parsestop_error;
    }

    asyncmsg->errormsg = NULL;
    asyncmsg->msgtype = XMANAGER_MSG_STOPCMD;
    asyncmsg->type = jobtype;
    asyncmsg->name = jobname;
    jobname = NULL;
    asyncmsg->result = 0;

    /* Execute start command execcmd */
    if (XMANAGER_METRICNODETYPE_PGRECEIVELOG == jobtype)
    {
        snprintf(execcmd,
                 1024,
                 "%s/bin/pgreceivelog/receivelog -f %s stop",
                 xmetric->xsynchpath,
                 pxmetricnode->conf);
    }
    else
    {
        snprintf(execcmd,
                 1024,
                 "%s/bin/%s -f %s stop",
                 xmetric->xsynchpath,
                 xmanager_metricnode_getname(jobtype),
                 pxmetricnode->conf);
    }

    /* Execute execcmd command */
    if (false == execcommand(execcmd, xmetric->privdata, xmetric->privdatadestroy))
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "can not stop %s", xmanager_metricnode_getname(jobtype));

        goto xmanager_metricmsg_parsestop_error;
    }

    /* Mount message to async message queue */
    xmetricxscscinode->asyncmsgs->msgs = dlist_put(xmetricxscscinode->asyncmsgs->msgs, asyncmsg);
    return true;

xmanager_metricmsg_parsestop_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    if (NULL != asyncmsg)
    {
        xmanager_metricasyncmsg_destroy(asyncmsg);
    }

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(
        xmetric, npoolentry->wpackets, XMANAGER_MSG_STOPCMD, errcode, errormsg);
}

/*
 * Assemble start response message
 */
bool xmanager_metricmsg_assemblestop(xmanager_metric* xmetric,
                                     netpoolentry*    npoolentry,
                                     dlist*           dlmsgs)
{
    uint8                    u8value = 0;
    uint16                   u16value = 0;
    int                      rowlen = 0;
    int                      ivalue = 0;
    int                      msglen = 0;
    uint8*                   uptr = NULL;
    uint8*                   rowuptr = NULL;
    dlistnode*               dlnode = NULL;
    netpacket*               npacket = NULL;
    xmanager_metricasyncmsg* xmetricasyncmsg = NULL;
    char                     errormsg[2048] = {0};
    if (true == dlist_isnull(dlmsgs) || 1 < dlist_getcount(dlmsgs))
    {
        /* Assemble error message */
        elog(RLOG_WARNING, "metric msg assemble stop too many async msgs.");
        snprintf(errormsg, 2048, "metric msg assemble stop too many async msgs.");
        return xmanager_metricmsg_assembleerrormsg(
            xmetric, npoolentry->wpackets, XMANAGER_MSG_STOPCMD, ERROR_MSGCOMMAND, errormsg);
    }

    /* Total length + crc32 + type + flag + rowcnt */
    msglen = 4 + 4 + 4 + 1 + 4;

    /*
     * First row content
     *  jobtypelen     jobtype      jobnamelen     jobname     resultlen    result
     *      4        jobtypelen         4        jobnamelen      4        resultlen
     */
    /* Row length, total column length */
    msglen += 4;

    /* jobtypelen */
    msglen += 4;

    /* jobtype */
    msglen += strlen("jobtype");

    /* jobnamelen */
    msglen += 4;

    /* jobname */
    msglen += strlen("jobname");

    /* resultlen */
    msglen += 4;

    /* result */
    msglen += strlen("result");

    /* Get length */
    for (dlnode = dlmsgs->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricasyncmsg = (xmanager_metricasyncmsg*)dlnode->value;

        rowlen = 0;
        /* jobtype + jobnamelen + jobname */
        /* rowlen Total length */
        rowlen = 4;

        /* nullmap count */
        rowlen += 2;

        /* nullmap */
        rowlen += 1;

        /*-----------Column 1------------*/
        /* jobtypelen */
        rowlen += 4;

        /* jobtype */
        rowlen += strlen(xmanager_metricnode_getname(xmetricasyncmsg->type));

        /*-----------Column 2------------*/
        /* jobnamelen */
        rowlen += 4;

        /* jobname */
        rowlen += strlen(xmetricasyncmsg->name);

        /*-----------Column 3------------*/
        /* resultlen */
        rowlen += 4;

        /* result */
        if (0 == xmetricasyncmsg->result)
        {
            rowlen += strlen("SUCCESS");
        }
        else
        {
            rowlen += strlen(xmetricasyncmsg->errormsg);
        }

        msglen += rowlen;
    }

    /* Allocate space */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble stop msg out of memory");
        return false;
    }
    msglen += 1;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble stop msg data, out of memory");
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
    ivalue = XMANAGER_MSG_STOPCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* Type success flag */
    u8value = 0;
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;

    /* rowcnt */
    ivalue = 1;
    ivalue += dlist_getcount(dlmsgs);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /*
     * First row content is column description, all strings
     *  jobtype
     *  jobname
     *  result
     */
    rowlen = 0;
    rowuptr = uptr;

    /* Offset Row length length first */
    rowlen = 4;
    uptr += 4;

    /* jobtypelen */
    ivalue = strlen("jobtype");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* jobtype */
    ivalue = strlen("jobtype");
    rmemcpy1(uptr, 0, "jobtype", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* jobnamelen */
    ivalue = strlen("jobname");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* jobname */
    ivalue = strlen("jobname");
    rmemcpy1(uptr, 0, "jobname", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* resultlen */
    ivalue = strlen("result");
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;

    /* result */
    ivalue = strlen("result");
    rmemcpy1(uptr, 0, "result", ivalue);
    uptr += ivalue;
    rowlen += ivalue;

    /* Total row length */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    /* Assemble data */
    for (dlnode = dlmsgs->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricasyncmsg = (xmanager_metricasyncmsg*)dlnode->value;
        rowuptr = uptr;
        rowlen = 0;
        /* Skip total length first */
        uptr += 4;
        rowlen = 4;

        /* Null column map count */
        u16value = 1;
        u16value = r_hton16(u16value);
        rmemcpy1(uptr, 0, &u16value, 2);
        uptr += 2;
        rowlen += 2;

        /* Null column map */
        uptr += 1;
        rowlen += 1;

        /*-----------Column 1-----------------*/
        /* jobtype length */
        ivalue = strlen(xmanager_metricnode_getname(xmetricasyncmsg->type));
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;

        /* jobtype */
        ivalue = strlen(xmanager_metricnode_getname(xmetricasyncmsg->type));
        rmemcpy1(uptr, 0, xmanager_metricnode_getname(xmetricasyncmsg->type), ivalue);
        uptr += ivalue;
        rowlen += ivalue;

        /*-----------Column 2-----------------*/
        /* jobname length */
        ivalue = strlen(xmetricasyncmsg->name);
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;

        /* jobname */
        ivalue = strlen(xmetricasyncmsg->name);
        rmemcpy1(uptr, 0, xmetricasyncmsg->name, ivalue);
        uptr += ivalue;
        rowlen += ivalue;

        /*-----------Column 3-----------------*/
        /* result length */
        if (0 == xmetricasyncmsg->result)
        {
            ivalue = strlen("SUCCESS");
        }
        else
        {
            ivalue = strlen(xmetricasyncmsg->errormsg);
        }
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;

        /* result */
        if (0 == xmetricasyncmsg->result)
        {
            ivalue = strlen("SUCCESS");
            rmemcpy1(uptr, 0, "SUCCESS", ivalue);
        }
        else
        {
            ivalue = strlen(xmetricasyncmsg->errormsg);
            rmemcpy1(uptr, 0, xmetricasyncmsg->errormsg, ivalue);
        }
        uptr += ivalue;
        rowlen += ivalue;

        /* Fill row length */
        rowlen = r_hton32(rowlen);
        rmemcpy1(rowuptr, 0, &rowlen, 4);
    }

    /* Mount netpacket to send queue */
    if (false == queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble start msg add message to queue error");
        netpacket_destroy(npacket);
        return false;
    }
    return true;
}
