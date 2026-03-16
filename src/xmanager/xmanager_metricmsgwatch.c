#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netpool.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metriccapturenode.h"
#include "xmanager/ripple_xmanager_metricintegratenode.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager_metricmsgwatch.h"
#include "xmanager/ripple_xmanager_metricmsg.h"
#include "xmanager/ripple_xmanager_metricprogressnode.h"


/*  watch 结果 组装 */
static bool ripple_xmanager_metricmsg_assemblewatchresult(ripple_xmanager_metric* xmetric,
                                                         ripple_netpoolentry* npoolentry,
                                                         ripple_xmanager_metricnode* pxmetricnode)
{
    void* result = NULL;

    switch (pxmetricnode->type)
    {
    case RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE:
        result = ripple_xmanager_metricmsg_assemblecapture(pxmetricnode);
        break;
    case RIPPLE_XMANAGER_METRICNODETYPE_INTEGRATE:
        result = ripple_xmanager_metricmsg_assembleintegrate(pxmetricnode);
        break;
    case RIPPLE_XMANAGER_METRICNODETYPE_PROCESS:
        result = ripple_xmanager_metricmsg_assembleprogress(xmetric, pxmetricnode);
        break;
    default:
        elog(RLOG_WARNING, "xmanager metric assemble watch msg data, unsupported metricnode type :%d", pxmetricnode->type);
        return false;
        break;
    }

    if (NULL == result)
    {
        elog(RLOG_WARNING, "xmanager metric assemble watch msg npacket is null ");
        ripple_netpacket_destroyvoid(result);
        return false;
    }

    /* 将 netpacket 挂载到待发送队列中 */
    if (false == ripple_queue_put(npoolentry->wpackets, result))
    {
        elog(RLOG_WARNING, "xmanager metric assemble watch msg add message to queue error");
        ripple_netpacket_destroyvoid(result);
        return false;
    }
    return true;
}

/*
 * 处理 watch 命令
 *  1、jobtype 暂时小于 PROCESS
 *  2、校验 job 是否已经存在
 *  3、获取信息并返回
*/
bool ripple_xmanager_metricmsg_parsewatch(ripple_xmanager_metric* xmetric,
                                         ripple_netpoolentry* npoolentry,
                                         ripple_netpacket* npacket)
{
    /* 错误码 */
    int errcode                                         = 0;
    int len                                             = 0;
    int jobtype                                         = 0;
    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    ripple_xmanager_metricnode* pxmetricnode            = NULL;
    ripple_xmanager_metricnode xmetricnode              = { 0 };
    char errormsg[512]                                  = { 0 };

    /* 获取作业类型 */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (RIPPLE_XMANAGER_METRICNODETYPE_PROCESS < jobtype)
    {
        errcode = RIPPLE_ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg, 512, "ERROR: xmanager watch command, temporarily unsupport %s", ripple_xmanager_metricnode_getname(jobtype));
        goto ripple_xmanager_metricmsg_parsewatch_error;
    }

    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        snprintf(errormsg, 512, "%s", "ERROR: xmanager recv watch command, oom");
        errcode = RIPPLE_ERROR_OOM;
        goto ripple_xmanager_metricmsg_parsewatch_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);

    /* 查看是否存在 */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    /* 获取 metricnode */
    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, ripple_xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = RIPPLE_ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto ripple_xmanager_metricmsg_parsewatch_error;
    }

    /* 组装返回信息 */
    if(false == ripple_xmanager_metricmsg_assemblewatchresult(xmetric,
                                                             npoolentry,
                                                             pxmetricnode))
    {
        errcode = RIPPLE_ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s does not assemble watch result.", jobname);
        goto ripple_xmanager_metricmsg_parsewatch_error;
    }

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return true;

ripple_xmanager_metricmsg_parsewatch_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      RIPPLE_XMANAGER_MSG_WATCHCMD,
                                                      errcode,
                                                      errormsg);
}
