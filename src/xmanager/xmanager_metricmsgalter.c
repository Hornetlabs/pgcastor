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
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsgalter.h"
#include "xmanager/xmanager_metricmsg.h"
#include "xmanager/xmanager_metricprogressnode.h"

/*
 * Handle alter command
 *1. jobtype equals PROCESS
 *2. If capture, check for same type job
 *  3、add/remove job
 *4. Assemble return message
 */
bool xmanager_metricmsg_parsealter(xmanager_metric* xmetric, netpoolentry* npoolentry, netpacket* npacket)
{
    uint8_t                      action = 0;
    int                          len = 0;
    int                          jobcnt = 0;
    int                          jobtype = 0;
    int                          idx_jobcnt = 0;
    int                          errcode = 0;
    uint8*                       uptr = NULL;
    char*                        jobname = NULL;
    dlistnode*                   dlnode = NULL;
    xmanager_metricnode*         pxmetricnode = NULL;
    xmanager_metricnode*         jobxmetricnode = NULL;
    xmanager_metricnode*         tmpxmetricnode = NULL;
    xmanager_metricprogressnode* xmetricprogressnode = NULL;
    xmanager_metricnode          xmetricnode = {0};
    char                         errormsg[2048] = {0};

    /* Get job type */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (XMANAGER_METRICNODETYPE_PROCESS != jobtype)
    {
        errcode = ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg,
                 2048,
                 "ERROR: xmanager parse alter command, unsupport %s",
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parsealter_error;
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
        snprintf(errormsg, 2048, "ERROR: xmanager parse alter command, out of memory.");
        goto xmanager_metricmsg_parsealter_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);
    uptr += len;

    /* Check if node exists */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto xmanager_metricmsg_parsealter_error;
    }

    xmetricprogressnode = (xmanager_metricprogressnode*)pxmetricnode;
    pxmetricnode = NULL;
    if (NULL != jobname)
    {
        rfree(jobname);
        jobname = NULL;
    }

    /* Get action */
    rmemcpy1(&action, 0, uptr, 1);
    uptr += 1;

    /* Get jobcnt */
    rmemcpy1(&jobcnt, 0, uptr, 4);
    jobcnt = r_ntoh32(jobcnt);
    uptr += 4;

    for (idx_jobcnt = 0; idx_jobcnt < jobcnt; idx_jobcnt++)
    {
        if (NULL != jobname)
        {
            rfree(jobname);
            jobname = NULL;
        }
        /* jobtype */
        rmemcpy1(&jobtype, 0, uptr, 4);
        jobtype = r_ntoh32(jobtype);
        uptr += 4;

        if (XMANAGER_METRICNODETYPE_INTEGRATE < jobtype)
        {
            errcode = ERROR_MSGUNSPPORT;
            snprintf(errormsg,
                     2048,
                     "ERROR: xmanager recv alter progress command, need jobtype less then HGRECEIVELOG");
            goto xmanager_metricmsg_parsealter_error;
        }

        /* name length */
        rmemcpy1(&len, 0, uptr, 4);
        len = r_ntoh32(len);
        uptr += 4;
        len += 1;

        jobname = rmalloc0(len);
        if (NULL == jobname)
        {
            errcode = ERROR_OOM;
            snprintf(errormsg, 2048, "ERROR: xmanager recv alter progress command, oom");
            goto xmanager_metricmsg_parsealter_error;
        }
        rmemset0(jobname, 0, '\0', len);
        len -= 1;
        rmemcpy0(jobname, 0, uptr, len);
        uptr += len;

        /* Check if exists */
        xmetricnode.type = jobtype;
        xmetricnode.name = jobname;

        /* Remove directly delete */
        if (XMANAGER_METRICNODEACTION_REMOVE == action)
        {
            xmetricprogressnode->progressjop = dlist_deletebyvalue(xmetricprogressnode->progressjop,
                                                                   &xmetricnode,
                                                                   xmanager_metricnode_cmp,
                                                                   xmanager_metricnode_destroyvoid);
            continue;
        }

        /* Check if exists in progressjop */
        pxmetricnode = dlist_get(xmetricprogressnode->progressjop, &xmetricnode, xmanager_metricnode_cmp);
        if (NULL != pxmetricnode)
        {
            elog(RLOG_WARNING, "xmanager recv alter progress command, %s already in progressjob ", xmetricnode.name);
            continue;
        }

        /* If capture already exited in progressjop */
        if (XMANAGER_METRICNODETYPE_CAPTURE == jobtype)
        {
            if (false == dlist_isnull(xmetricprogressnode->progressjop))
            {
                for (dlnode = xmetricprogressnode->progressjop->head; NULL != dlnode; dlnode = dlnode->next)
                {
                    tmpxmetricnode = (xmanager_metricnode*)dlnode->value;
                    if (jobtype == tmpxmetricnode->type)
                    {
                        errcode = ERROR_MSGEXIST;
                        snprintf(errormsg,
                                 2048,
                                 "ERROR: xmanager recv alter progress command, capture %s already "
                                 "exists",
                                 jobname);
                        goto xmanager_metricmsg_parsealter_error;
                    }
                }
            }
        }

        /* Check if metricnode exists */
        pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
        if (NULL == pxmetricnode)
        {
            errcode = ERROR_NOENT;
            snprintf(errormsg, 2048, "ERROR: xmanager recv alter progress command, not find %s", jobname);
            goto xmanager_metricmsg_parsealter_error;
        }

        /* Create new metricnode */
        jobxmetricnode = xmanager_metricnode_init(jobtype);
        if (NULL == jobxmetricnode)
        {
            errcode = ERROR_OOM;
            snprintf(errormsg, 2048, "ERROR: xmanager alter command, oom");
            goto xmanager_metricmsg_parsealter_error;
        }

        jobxmetricnode->name = rstrdup(jobname);
        jobxmetricnode->conf = rstrdup(pxmetricnode->conf);
        pxmetricnode = NULL;

        /* Add to progress */
        xmetricprogressnode->progressjop = dlist_put(xmetricprogressnode->progressjop, jobxmetricnode);
        jobxmetricnode = NULL;
    }

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, XMANAGER_MSG_ALTERCMD);
    ;

xmanager_metricmsg_parsealter_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(xmetric, npoolentry->wpackets, XMANAGER_MSG_ALTERCMD, errcode, errormsg);
}
