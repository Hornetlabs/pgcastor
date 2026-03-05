#include "ripple_app_incl.h"
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
#include "xmanager/ripple_xmanager_metricmsgremove.h"
#include "xmanager/ripple_xmanager_metricmsg.h"

/*
 * 处理 remove 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否已经存在
 *  3、检验是否在运行中
 *  4、删除data和conf文件
 *  5、返回成功消息
*/
bool ripple_xmanager_metricmsg_parseremove(ripple_xmanager_metric* xmetric,
                                          ripple_netpoolentry* npoolentry,
                                          ripple_netpacket* npacket)
{
    int len                                             = 0;
    int jobtype                                         = 0;
    int errcode                                         = 0;
    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    ripple_xmanager_metricnode* pxmetricnode            = NULL;
    ripple_xmanager_metricnode xmetricnode              = { 0 };
    char errormsg[2048]                                 = { 0 };
    char execcmd[1024]                                  = { 0 };

    /* 获取作业类型 */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    uptr += 12;

    /* jobtype */
    rmemcpy1(&jobtype, 0, uptr, 4);
    jobtype = r_ntoh32(jobtype);
    uptr += 4;

    if (RIPPLE_XMANAGER_METRICNODETYPE_PROCESS <= jobtype)
    {
        errcode = RIPPLE_ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg, 2048, 
                 "ERROR: xmanager parse remove command, unsupport %s",
                 ripple_xmanager_metricnode_getname(jobtype));
        goto ripple_xmanager_metricmsg_parseremove_error;
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
        snprintf(errormsg, 2048, "ERROR: xmanager parse remove command, out of memory.");
        goto ripple_xmanager_metricmsg_parseremove_error;
    }
    rmemset0(jobname, 0, '\0', len);
    len -= 1;
    rmemcpy0(jobname, 0, uptr, len);

    /* 查看节点是否存在 */
    xmetricnode.type = jobtype;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, ripple_xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = RIPPLE_ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto ripple_xmanager_metricmsg_parseremove_error;
    }

    /* 检验是否在运行中，在运行中返回错误信息提示停止 */
    if (RIPPLE_XMANAGER_METRICNODESTAT_INIT < pxmetricnode->stat)
    {
        errcode = RIPPLE_ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: %s already started, Please use drop command.", jobname);
        goto ripple_xmanager_metricmsg_parseremove_error;
    }

    /* 删除文件 */
    if (NULL == pxmetricnode->conf)
    {
        errcode = RIPPLE_ERROR_NOENT;
        snprintf(errormsg, 2048, "%s", "ERROR: xmanager recv remove command, conf is null");
        goto ripple_xmanager_metricmsg_parseremove_error;
    }

    snprintf(execcmd, 1024, "rm -rf %s ;", pxmetricnode->conf);

    /* 执行 execcmd 命令 */
    if (false == ripple_execcommand(execcmd, xmetric->privdata, xmetric->privdatadestroy))
    {
        errcode = RIPPLE_ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: can not remove %s", jobname);
        goto ripple_xmanager_metricmsg_parseremove_error;
    }

    /* metricnode 节点在链表中移除 */
    xmetric->metricnodes = dlist_deletebyvalue(xmetric->metricnodes,
                                               &xmetricnode,
                                               ripple_xmanager_metricnode_cmp,
                                               ripple_xmanager_metricnode_destroyvoid);

    ripple_xmanager_metricnode_flush(xmetric->metricnodes);

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    return ripple_xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, RIPPLE_XMANAGER_MSG_REMOVECMD);

ripple_xmanager_metricmsg_parseremove_error:

    if (NULL != jobname)
    {
        rfree(jobname);
    }

    elog(RLOG_WARNING, errormsg);
    return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      RIPPLE_XMANAGER_MSG_REMOVECMD,
                                                      errcode,
                                                      errormsg);

}
