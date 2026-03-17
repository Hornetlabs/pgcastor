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
 * 处理 init 命令
 *  1、jobtype 需要小于 PROCESS
 *  2、校验 job 是否存在, 不存在报错
 *  3、创建异步消息挂载到 xscsci 节点上
 *  4、执行初始化命令
*/
bool xmanager_metricmsg_parseinit(xmanager_metric* xmetric,
                                         netpoolentry* npoolentry,
                                         netpacket* npacket)
{
    int errcode                                         = 0;
    int len                                             = 0;
    int jobtype                                         = 0;
    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    xmanager_metricnode* pxmetricnode            = NULL;
    xmanager_metricxscscinode* xmetricxscscinode = NULL;
    xmanager_metricfd2node* fd2node              = NULL;
    xmanager_metricasyncmsg* asyncmsg            = NULL;
    xmanager_metricnode xmetricnode              = { 0 };
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

    if (XMANAGER_METRICNODETYPE_PROCESS <= jobtype)
    {
        errcode = ERROR_MSGCOMMANDUNVALID;
        snprintf(errormsg, 512, "ERROR: xmanager recv init command, unsupport %s", xmanager_metricnode_getname(jobtype));
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

    /* 查看节点是否存在 */
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
        snprintf(errormsg, 2048,
                 "ERROR: %s already init, use start command start %s node.",
                 jobname, xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parseinit_error;
    }

    /* 
     * 创建异步消息
     *  1、获取 xscsci 节点
     *  2、创建异步等待消息
     */
    fd2node = dlist_get(xmetric->fd2metricnodes,
                        (void*)((uintptr_t)npoolentry->fd),
                        xmanager_metricfd2node_cmp);

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
    asyncmsg->name =jobname;
    jobname = NULL;
    asyncmsg->result = 0;

    /* 执行 init 命令 execcmd */
    if (XMANAGER_METRICNODETYPE_PGRECEIVELOG == jobtype)
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
                 xmanager_metricnode_getname(jobtype),
                 pxmetricnode->conf);
    }

    /* 执行 execcmd 命令 */
    if (false == execcommand(execcmd, xmetric->privdata, xmetric->privdatadestroy))
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: can not init %s", xmanager_metricnode_getname(jobtype));
        goto xmanager_metricmsg_parseinit_error;
    }

    /* 将消息挂载到异步消息队列中 */
    xmetricxscscinode->asyncmsgs->msgs =  dlist_put(xmetricxscscinode->asyncmsgs->msgs, asyncmsg);
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
    return xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      XMANAGER_MSG_INITCMD,
                                                      errcode,
                                                      errormsg);
}


/*
 * 组装 init 返回消息
*/
bool xmanager_metricmsg_assembleinit(xmanager_metric* xmetric,
                                            netpoolentry* npoolentry,
                                            dlist* dlmsgs)
{
    int ivalue                                          = 0;
    int msglen                                          = 0;
    int errmsglen                                       = 0;
    uint8* uptr                                         = NULL;
    netpacket* npacket                           = NULL;
    xmanager_metricasyncmsg* xmetricasyncmsg     = NULL;
    char errormsg[2048]                                 = { 0 };

    if (true == dlist_isnull(dlmsgs) || 1 < dlist_getcount(dlmsgs))
    {
        /* 组装错误消息 */
        elog(RLOG_WARNING, "metric msg assemble init too many async msgs.");
        snprintf(errormsg, 2048, "metric msg assemble init too many async msgs.");
        return xmanager_metricmsg_assembleerrormsg(xmetric,
                                                          npoolentry->wpackets,
                                                          XMANAGER_MSG_INITCMD,
                                                          ERROR_MSGCOMMAND,
                                                          errormsg);
    }

    xmetricasyncmsg = (xmanager_metricasyncmsg*)dlmsgs->head->value;

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
    ivalue = XMANAGER_MSG_INITCMD;
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
    if (false == queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble init msg add message to queue error");
        netpacket_destroy(npacket);
        return false;
    }
    return true;
}
