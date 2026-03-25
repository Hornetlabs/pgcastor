#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/daemon/process.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsgdrop.h"
#include "xmanager/xmanager_metricmsg.h"
#include "xmanager/xmanager_metricprogressnode.h"

/*
 * Handle drop command
 *1. jobtype must be less than PROCESS
 *2. Verify job already exists
 *  3、Check if running
 *4. Delete data and conf files
 *5. Return success message
 */
bool xmanager_metricmsg_parsedrop(xmanager_metric* xmetric, netpoolentry* npoolentry,
                                  netpacket* npacket)
{
    int                          len = 0;
    int                          jobtype = 0;
    int                          errcode = 0;
    uint8*                       uptr = NULL;
    char*                        jobname = NULL;
    dlistnode*                   dlnode = NULL;
    xmanager_metricnode*         pxmetricnode = NULL;
    xmanager_metricnode*         tmpxmetricnode = NULL;
    xmanager_metricprogressnode* xmetricprogressnode = NULL;
    xmanager_metricnode          xmetricnode = {0};
    char                         errormsg[2048] = {0};
    char                         execcmd[1024] = {0};

    /* Get job type */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (XMANAGER_METRICNODETYPE_PROCESS < jobtype)
    {
        errcode = ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg, 2048, "ERROR: xmanager parse drop command, unsupport %s",
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parsedrop_error;
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
        snprintf(errormsg, 2048, "ERROR: xmanager parse drop command, out of memory.");
        goto xmanager_metricmsg_parsedrop_error;
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
        goto xmanager_metricmsg_parsedrop_error;
    }

    if (XMANAGER_METRICNODETYPE_PROCESS > jobtype)
    {
        /* Check if running, return error to stop */
        if (XMANAGER_METRICNODESTAT_ONLINE == pxmetricnode->stat)
        {
            errcode = ERROR_MSGCOMMAND;
            snprintf(errormsg, 2048, "ERROR: %s is running, Please execute the stop command.",
                     jobname);
            goto xmanager_metricmsg_parsedrop_error;
        }

        for (dlnode = xmetric->metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
        {
            tmpxmetricnode = (xmanager_metricnode*)dlnode->value;
            /* Check if metricnode to delete is in progress */
            if (XMANAGER_METRICNODETYPE_PROCESS == tmpxmetricnode->type)
            {
                xmetricprogressnode = (xmanager_metricprogressnode*)tmpxmetricnode;
                if (false == dlist_isnull(xmetricprogressnode->progressjop))
                {
                    if (NULL != dlist_isexist(xmetricprogressnode->progressjop, pxmetricnode,
                                              xmanager_metricnode_cmp))
                    {
                        errcode = ERROR_MSGEXIST;
                        snprintf(errormsg, 2048,
                                 "ERROR: progress job %s is running, Please execute the altre "
                                 "remove command",
                                 pxmetricnode->name);
                        goto xmanager_metricmsg_parsedrop_error;
                    }
                }
            }
        }
        /* Delete file */
        if (NULL != pxmetricnode->data && NULL != pxmetricnode->conf)
        {
            snprintf(execcmd, 1024, "rm -rf %s/* ; rm -rf %s ;", pxmetricnode->data,
                     pxmetricnode->conf);
        }
        else if (NULL == pxmetricnode->data && NULL != pxmetricnode->conf)
        {
            snprintf(execcmd, 1024, "rm -rf %s ;", pxmetricnode->conf);
        }
        else if (NULL != pxmetricnode->data && NULL == pxmetricnode->conf)
        {
            snprintf(execcmd, 1024, "rm -rf %s/* ;", pxmetricnode->data);
        }
        else
        {
            errcode = ERROR_NOENT;
            snprintf(errormsg, 2048, "%s",
                     "ERROR: xmanager recv drop command, data and conf is null");
            goto xmanager_metricmsg_parsedrop_error;
        }

        /* Execute execcmd command */
        if (false == execcommand(execcmd, xmetric->privdata, xmetric->privdatadestroy))
        {
            errcode = ERROR_MSGCOMMAND;
            snprintf(errormsg, 2048, "ERROR: can not drop %s", jobname);
            goto xmanager_metricmsg_parsedrop_error;
        }
    }

    /* Remove metricnode from list */
    xmetric->metricnodes =
        dlist_deletebyvalue(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp,
                            xmanager_metricnode_destroyvoid);

    xmanager_metricnode_flush(xmetric->metricnodes);

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, XMANAGER_MSG_DROPCMD);

xmanager_metricmsg_parsedrop_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(xmetric, npoolentry->wpackets, XMANAGER_MSG_DROPCMD,
                                               errcode, errormsg);
}
