#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/daemon/ripple_process.h"
#include "command/ripple_cmd.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netpool.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricasyncmsg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metricxscscinode.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager_metricmsginit.h"
#include "xmanager/ripple_xmanager_metricmsg.h"


/*
 * 处理 init 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否存在, 不存在报错
 *  3、创建异步消息挂载到 xscsci 节点上
 *  4、执行初始化命令
*/
bool ripple_xmanager_metricmsg_parseinit(ripple_xmanager_metric* xmetric,
                                         ripple_netpoolentry* npoolentry,
                                         ripple_netpacket* npacket)
{
    int errcode                                         = 0;
    int len                                             = 0;
    int jobtype                                         = 0;
    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    ripple_xmanager_metricnode* pxmetricnode            = NULL;
    ripple_xmanager_metricxscscinode* xmetricxscscinode = NULL;
    ripple_xmanager_metricfd2node* fd2node              = NULL;
    ripple_xmanager_metricasyncmsg* asyncmsg            = NULL;
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
        snprintf(errormsg, 512, "ERROR: xmanager recv init command, unsupport %s", ripple_xmanager_metricnode_getname(jobtype));
        goto ripple_xmanager_metricmsg_parseinit_error;
    }

    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;
    len += 1;

    jobname = rmalloc0(len);
    if (NULL == jobname)
    {
        errcode = RIPPLE_ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s", "xmanager recv init command, oom");
        goto ripple_xmanager_metricmsg_parseinit_error;
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
        goto ripple_xmanager_metricmsg_parseinit_error;
    }

    if (RIPPLE_XMANAGER_METRICNODESTAT_ONLINE == pxmetricnode->stat)
    {
        errcode = RIPPLE_ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: %s already start.", jobname);
        goto ripple_xmanager_metricmsg_parseinit_error;
    }
    else if (RIPPLE_XMANAGER_METRICNODESTAT_OFFLINE == pxmetricnode->stat)
    {
        errcode = RIPPLE_ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048,
                 "ERROR: %s already init, use start command start %s node.",
                 jobname, ripple_xmanager_metricnode_getname(jobtype));
        goto ripple_xmanager_metricmsg_parseinit_error;
    }

    /* 
     * 创建异步消息
     *  1、获取 xscsci 节点
     *  2、创建异步等待消息
     */
    fd2node = dlist_get(xmetric->fd2metricnodes,
                        (void*)((uintptr_t)npoolentry->fd),
                        ripple_xmanager_metricfd2node_cmp);

    xmetricxscscinode = (ripple_xmanager_metricxscscinode*)fd2node->metricnode;
    asyncmsg = ripple_xmanager_metricasyncmsg_init();
    if (NULL == asyncmsg)
    {
        errcode = RIPPLE_ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s", "xmanager recv init command, out of memory");
        goto ripple_xmanager_metricmsg_parseinit_error;
    }

    asyncmsg->errormsg = NULL;
    asyncmsg->msgtype = RIPPLE_XMANAGER_MSG_INITCMD;
    asyncmsg->type = jobtype;
    asyncmsg->name =jobname;
    jobname = NULL;
    asyncmsg->result = 0;

    /* 执行 init 命令 execcmd */
    if (RIPPLE_XMANAGER_METRICNODETYPE_HGRECEIVELOG == jobtype)
    {
        snprintf(execcmd,
                 1024,
                 "%s/bin/hgreceivelog/receivelog -f %s init",
                 xmetric->xsynchpath,
                 xmetric->configpath);
    }
    else if (RIPPLE_XMANAGER_METRICNODETYPE_PGRECEIVELOG == jobtype)
    {
        snprintf(execcmd,
                 1024,
                 "%s/bin/pgreceivelog/receivelog -f %s init",
                 xmetric->xsynchpath,
                 pxmetricnode->conf);
    }
    else
    {
        snprintf(execcmd,
                 1024,
                 "%s/bin/%s -f %s init",
                 xmetric->xsynchpath,
                 ripple_xmanager_metricnode_getname(jobtype),
                 pxmetricnode->conf);
    }

    /* 执行 execcmd 命令 */
    if (false == ripple_execcommand(execcmd, xmetric->privdata, xmetric->privdatadestroy))
    {
        errcode = RIPPLE_ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: can not init %s", ripple_xmanager_metricnode_getname(jobtype));
        goto ripple_xmanager_metricmsg_parseinit_error;
    }

    /* 将消息挂载到异步消息队列中 */
    xmetricxscscinode->asyncmsgs->msgs =  dlist_put(xmetricxscscinode->asyncmsgs->msgs, asyncmsg);
    return true;

ripple_xmanager_metricmsg_parseinit_error:
    if (NULL != jobname)
    {
        rfree(jobname);
    }

    if (NULL != asyncmsg)
    {
        ripple_xmanager_metricasyncmsg_destroy(asyncmsg);
    }
    elog(RLOG_WARNING, errormsg);
    return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      RIPPLE_XMANAGER_MSG_INITCMD,
                                                      errcode,
                                                      errormsg);
}


/*
 * 组装 init 返回消息
*/
bool ripple_xmanager_metricmsg_assembleinit(ripple_xmanager_metric* xmetric,
                                            ripple_netpoolentry* npoolentry,
                                            dlist* dlmsgs)
{
    int ivalue                                          = 0;
    int msglen                                          = 0;
    int errmsglen                                       = 0;
    uint8* uptr                                         = NULL;
    ripple_netpacket* npacket                           = NULL;
    ripple_xmanager_metricasyncmsg* xmetricasyncmsg     = NULL;
    char errormsg[2048]                                 = { 0 };

    if (true == dlist_isnull(dlmsgs) || 1 < dlist_getcount(dlmsgs))
    {
        /* 组装错误消息 */
        elog(RLOG_WARNING, "metric msg assemble init too many async msgs.");
        snprintf(errormsg, 2048, "metric msg assemble init too many async msgs.");
        return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                          npoolentry->wpackets,
                                                          RIPPLE_XMANAGER_MSG_INITCMD,
                                                          RIPPLE_ERROR_MSGCOMMAND,
                                                          errormsg);
    }

    xmetricasyncmsg = (ripple_xmanager_metricasyncmsg*)dlmsgs->head->value;

    /* 总长度 + crc32 + type + flag */
    msglen = 4 + 4 + 4 + 1;

    if (0 != xmetricasyncmsg->result)
    {
        /* 4 长度 + 4 错误码 + 错误信息 */
        errmsglen = 4 + 4;
        errmsglen += strlen(xmetricasyncmsg->errormsg);
    }
    msglen += errmsglen;
    msglen += 1;

    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble init msg out of memory");
        return false;
    }

    npacket->data = ripple_netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble init msg data, out of memory");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    msglen -= 1;

    npacket->used = msglen;

    /* 组装数据 */
    uptr = npacket->data;

    /* 长度 */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* 类型 */
    ivalue = RIPPLE_XMANAGER_MSG_INITCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    rmemcpy1(uptr, 0, &xmetricasyncmsg->result, 1);
    uptr += 1;

    if (1 == xmetricasyncmsg->result)
    {
        /* 总长度 */
        ivalue = errmsglen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;

        /* 错误码 */
        ivalue = xmetricasyncmsg->errcode;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;

        /* 错误信息 */
        errmsglen -= 8;
        rmemcpy1(uptr, 0, xmetricasyncmsg->errormsg, errmsglen);
    }

    /* 将 netpacket 挂载到待发送队列中 */
    if (false == ripple_queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble init msg add message to queue error");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    return true;
}
