#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/dlist/dlist.h"
#include "utils/daemon/ripple_process.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netpool.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager_metricmsgalter.h"
#include "xmanager/ripple_xmanager_metricmsg.h"
#include "xmanager/ripple_xmanager_metricprogressnode.h"


/*
 * 处理 alter 命令
 *  1、jobtype 等于 PROCESS
 *  2、如果是capture/collector, 检查是否有同类型job
 *  3、add/remove job
 *  4、组装返回消息
*/
bool ripple_xmanager_metricmsg_parsealter(ripple_xmanager_metric* xmetric,
                                          ripple_netpoolentry* npoolentry,
                                          ripple_netpacket* npacket)
{
    uint8_t action                                          = 0;
    int len                                                 = 0;
    int jobcnt                                              = 0;
    int jobtype                                             = 0;
    int idx_jobcnt                                          = 0;
    int errcode                                             = 0;
    uint8* uptr                                             = NULL;
    char* jobname                                           = NULL;
    dlistnode* dlnode                                       = NULL;
    ripple_xmanager_metricnode* pxmetricnode                = NULL;
    ripple_xmanager_metricnode* jobxmetricnode              = NULL;
    ripple_xmanager_metricnode* tmpxmetricnode              = NULL;
    ripple_xmanager_metricprogressnode* xmetricprogressnode = NULL;
    ripple_xmanager_metricnode xmetricnode                  = { 0 };
    char errormsg[2048]                                     = { 0 };

    /* 获取作业类型 */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (RIPPLE_XMANAGER_METRICNODETYPE_PROCESS != jobtype)
    {
        errcode = RIPPLE_ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg, 2048, 
                 "ERROR: xmanager parse alter command, unsupport %s",
                 ripple_xmanager_metricnode_getname(jobtype));
        goto ripple_xmanager_metricmsg_parsealter_error;
    }

    /* 获取 jobname */
    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        errcode = RIPPLE_ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: xmanager parse alter command, out of memory.");
        goto ripple_xmanager_metricmsg_parsealter_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);
    uptr += len;

    /* 查看节点是否存在 */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, ripple_xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = RIPPLE_ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto ripple_xmanager_metricmsg_parsealter_error;
    }

    xmetricprogressnode = (ripple_xmanager_metricprogressnode*)pxmetricnode;
    pxmetricnode = NULL;
    if (NULL != jobname)
    {
        rfree(jobname);
        jobname = NULL;
    }

    /* 获取 action */
    rmemcpy1(&action, 0, uptr, 1);
    uptr += 1;

    /* 获取 jobcnt */
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

        if (RIPPLE_XMANAGER_METRICNODETYPE_HGRECEIVELOG < jobtype)
        {
            errcode = RIPPLE_ERROR_MSGUNSPPORT;
            snprintf(errormsg, 2048, "ERROR: xmanager recv alter progress command, need jobtype less then HGRECEIVELOG");
            goto ripple_xmanager_metricmsg_parsealter_error;
        }

        /* name len */
        rmemcpy1(&len, 0, uptr, 4);
        len = r_ntoh32(len);
        uptr += 4;
        len += 1;

        jobname = rmalloc0(len);
        if (NULL == jobname)
        {
            errcode = RIPPLE_ERROR_OOM;
            snprintf(errormsg, 2048, "ERROR: xmanager recv alter progress command, oom");
            goto ripple_xmanager_metricmsg_parsealter_error;
        }
        rmemset0(jobname, 0, '\0', len);
        len -= 1;
        rmemcpy0(jobname, 0, uptr, len);
        uptr += len;

        /* 查看是否存在 */
        xmetricnode.type = jobtype;
        xmetricnode.name = jobname;

        /* remove 直接删除 */
        if (RIPPLE_XMANAGER_METRICNODEACTION_REMOVE == action)
        {
            xmetricprogressnode->progressjop = dlist_deletebyvalue(xmetricprogressnode->progressjop,
                                                                   &xmetricnode,
                                                                   ripple_xmanager_metricnode_cmp,
                                                                   ripple_xmanager_metricnode_destroyvoid);
            continue;
        }

        /* 查看是否在progressjop存在 */
        pxmetricnode = dlist_get(xmetricprogressnode->progressjop, &xmetricnode, ripple_xmanager_metricnode_cmp);
        if (NULL != pxmetricnode)
        {
            elog(RLOG_WARNING, "xmanager recv alter progress command, %s already in progressjob ", xmetricnode.name);
            continue;
        }

        /* 如果是capture/collector已经在progressjop退出 */
        if(RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE == jobtype
           || RIPPLE_XMANAGER_METRICNODETYPE_COLLECTOR == jobtype)
        {
            if (false == dlist_isnull(xmetricprogressnode->progressjop))
            {
                for (dlnode = xmetricprogressnode->progressjop->head; NULL != dlnode; dlnode = dlnode->next)
                {
                    tmpxmetricnode = (ripple_xmanager_metricnode*)dlnode->value;
                    if (jobtype == tmpxmetricnode->type)
                    {
                        errcode = RIPPLE_ERROR_MSGEXIST;
                        snprintf(errormsg, 2048, "ERROR: xmanager recv alter progress command, capture/collector %s already exists", jobname);
                        goto ripple_xmanager_metricmsg_parsealter_error;
                    }
                }
            }
        }

        /* metricnode是否存在 */
        pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, ripple_xmanager_metricnode_cmp);
        if (NULL == pxmetricnode)
        {
            errcode = RIPPLE_ERROR_NOENT;
            snprintf(errormsg, 2048, "ERROR: xmanager recv alter progress command, not find %s", jobname);
            goto ripple_xmanager_metricmsg_parsealter_error;
        }

        /* 创建新的 metricnode */
        jobxmetricnode = ripple_xmanager_metricnode_init(jobtype);
        if (NULL == jobxmetricnode)
        {
            errcode = RIPPLE_ERROR_OOM;
            snprintf(errormsg, 2048, "ERROR: xmanager alter command, oom");
            goto ripple_xmanager_metricmsg_parsealter_error;
        }

        jobxmetricnode->name = rstrdup(jobname);
        jobxmetricnode->conf = rstrdup(pxmetricnode->conf);
        pxmetricnode = NULL;

        /* 加入progress */
        xmetricprogressnode->progressjop = dlist_put(xmetricprogressnode->progressjop, jobxmetricnode);
        jobxmetricnode = NULL;
    }

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return ripple_xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, RIPPLE_XMANAGER_MSG_ALTERCMD);;

ripple_xmanager_metricmsg_parsealter_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      RIPPLE_XMANAGER_MSG_ALTERCMD,
                                                      errcode,
                                                      errormsg);

}
