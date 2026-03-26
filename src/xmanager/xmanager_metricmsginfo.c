#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metriccapturenode.h"
#include "xmanager/xmanager_metricintegratenode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsginfo.h"
#include "xmanager/xmanager_metricmsg.h"
#include "xmanager/xmanager_metricprogressnode.h"

/*  Assemble info result */
static bool xmanager_metricmsg_assembleinforesult(xmanager_metric*     xmetric,
                                                  netpoolentry*        npoolentry,
                                                  xmanager_metricnode* pxmetricnode)
{
    void* result = NULL;

    switch (pxmetricnode->type)
    {
        case XMANAGER_METRICNODETYPE_CAPTURE:
            result = xmanager_metricmsg_assemblecapture(pxmetricnode);
            break;
        case XMANAGER_METRICNODETYPE_INTEGRATE:
            result = xmanager_metricmsg_assembleintegrate(pxmetricnode);
            break;
        case XMANAGER_METRICNODETYPE_PROCESS:
            result = xmanager_metricmsg_assembleprogress(xmetric, pxmetricnode);
            break;
        default:
            elog(RLOG_WARNING,
                 "xmanager metric assemble info  msg data, unsupported metricnode type :%d",
                 pxmetricnode->type);
            return false;
            break;
    }

    if (NULL == result)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info msg npacket is null ");
        netpacket_destroyvoid(result);
        return false;
    }

    /* Mount netpacket to send queue */
    if (false == queue_put(npoolentry->wpackets, result))
    {
        elog(RLOG_WARNING, "xmanager metric assemble info msg add message to queue error");
        netpacket_destroyvoid(result);
        return false;
    }
    return true;
}

/*
 * Handle info command
 *1. jobtype less than PROCESS
 *2. Verify job already exists
 *3. Get info and return
 */
bool xmanager_metricmsg_parseinfo(xmanager_metric* xmetric,
                                  netpoolentry*    npoolentry,
                                  netpacket*       npacket)
{
    /* Error code */
    int                  errcode = 0;
    int                  len = 0;
    int                  jobtype = 0;
    uint8*               uptr = NULL;
    char*                jobname = NULL;
    xmanager_metricnode* pxmetricnode = NULL;
    xmanager_metricnode  xmetricnode = {0};
    char                 errormsg[512] = {0};

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
        snprintf(errormsg,
                 512,
                 "ERROR: xmanager info command, temporarily unsupport %s",
                 xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parseinfo_error;
    }

    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        snprintf(errormsg, 512, "%s", "ERROR: xmanager recv info command, oom");
        errcode = ERROR_OOM;
        goto xmanager_metricmsg_parseinfo_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);

    /* Check if exists */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    /* Get metricnode */
    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto xmanager_metricmsg_parseinfo_error;
    }

    /* Assemble return info */
    if (false == xmanager_metricmsg_assembleinforesult(xmetric, npoolentry, pxmetricnode))
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s does not assemble info result.", jobname);
        goto xmanager_metricmsg_parseinfo_error;
    }

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return true;

xmanager_metricmsg_parseinfo_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(
        xmetric, npoolentry->wpackets, XMANAGER_MSG_INFOCMD, errcode, errormsg);
}
