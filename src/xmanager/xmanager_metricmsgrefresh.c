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
#include "xmanager/xmanager_metricmsgrefresh.h"
#include "xmanager/xmanager_metricmsg.h"


/* 组装 capture 的 refresh 消息 */
static bool xmanager_metricmsg_assemblerefreshforcapture(xmanager_metric* xmetric,
                                                                xmanager_metricnode* xmetricnode,
                                                                int tableslen,
                                                                uint8* tables)
{
    int ivalue                                          = 0;
    int msglen                                          = 0;
    uint8* uptr                                         = NULL;
    netpacket* npacket                           = NULL;
    netpoolentry* npoolentry                     = NULL;
    xmanager_metricfd2node* fd2node              = NULL;

    /* 构建 onlinerefresh 消息 */
    fd2node = dlist_get(xmetric->fd2metricnodes,
                        xmetricnode,
                        xmanager_metricfd2node_cmp2);

    /* 获取 capture 节点的 netpoolentry */
    npoolentry = NULL;
    npoolentry = netpool_getentrybyfd(xmetric->npool, fd2node->fd);
    if (NULL == npoolentry)
    {
        elog(RLOG_WARNING, "can not get capture node");
        return false;
    }

    /* 
     * 长度计算
     *  4 + 4 + 4
     * msglen + crc32 + cmdtype
    */
    msglen = 12;
    msglen += tableslen;

    /* 构建消息挂载到 npoolentry 上 */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble refresh to capture out of memory");
        return false;
    }

    msglen += 1;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble refresh msg data, out of memory");
        netpacket_destroy(npacket);
        return false;
    }
    msglen -= 1;
    npacket->used = msglen;

    /* 组装数据 */
    uptr = npacket->data;

    /* 数据总长度 */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* 类型 */
    ivalue = XMANAGER_MSG_CAPTUREREFRESH;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    rmemcpy1(uptr, 0, tables, tableslen);

    /* 将 netpacket 挂载到待发送队列中 */
    if (false == queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble refresh msg to capture add message to queue error");
        netpacket_destroy(npacket);
        return false;
    }

    return true;
}

/*
 * 处理 refresh 命令
 *  1、校验 job 是否存在, 不存在报错
 *  2、将 refresh 消息转发到 capture
 *  3、创建异步消息挂载到 xscsci 节点上
*/
bool xmanager_metricmsg_parserefresh(xmanager_metric* xmetric,
                                            netpoolentry* npoolentry,
                                            netpacket* npacket)
{
    int errcode = 0;
    int ivalue = 0;
    int msglen = 0;

    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    xmanager_metricnode* pxmetricnode            = NULL;
    xmanager_metricfd2node* fd2node              = NULL;
    xmanager_metricasyncmsg* asyncmsg            = NULL;
    xmanager_metricxscscinode* xmetricxscscinode = NULL;
    char errormsg[2048]                                 = { 0 };
    xmanager_metricnode xmetricnode              = { 0 };

    /* 获取作业类型 */
    uptr = npacket->data;

    /* msglen + crc32 + commandtype */
    rmemcpy1(&msglen, 0, uptr, 4);
    msglen = r_ntoh32(msglen);
    uptr += 12;
    msglen -= 12;

    /* 在 refresh 中是没有 jobtype 的 */

    /* jobname */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    ivalue += 1;
    msglen -= 4;

    jobname = rmalloc0(ivalue);
    if (NULL == jobname)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: xmanager parse refresh command, out of memory.");
        goto xmanager_metricmsg_parserefresh_error;
    }
    rmemset0(jobname, 0, '\0', ivalue);
    ivalue -= 1;
    rmemcpy0(jobname, 0, uptr, ivalue);
    msglen -= ivalue;
    uptr += ivalue;

    /* 查看 node 是否存在 */
    xmetricnode.type = XMANAGER_METRICNODETYPE_CAPTURE;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto xmanager_metricmsg_parserefresh_error;
    }

    if (XMANAGER_METRICNODESTAT_ONLINE != pxmetricnode->stat)
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: start %s first.", jobname);
        goto xmanager_metricmsg_parserefresh_error;
    }

    if (false == xmanager_metricmsg_assemblerefreshforcapture(xmetric,
                                                                     pxmetricnode,
                                                                     msglen,
                                                                     uptr))
    {
        errcode = ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: assemble refresh message to capture error.");
        goto xmanager_metricmsg_parserefresh_error;
    }

    /* 
     * 创建异步消息
     *  1、获取 xscsci 节点
     *  2、创建异步等待消息
     */
    /* 获取 xscsci 节点 */
    fd2node = dlist_get(xmetric->fd2metricnodes,
                        (void*)((uintptr_t)npoolentry->fd),
                        xmanager_metricfd2node_cmp);
    xmetricxscscinode = (xmanager_metricxscscinode*)fd2node->metricnode;

    /* 创建异步等待消息 */
    asyncmsg = xmanager_metricasyncmsg_init();
    if (NULL == asyncmsg)
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s", "xmanager refresh command, out of memory");
        goto xmanager_metricmsg_parserefresh_error;
    }

    asyncmsg->errormsg = NULL;
    asyncmsg->msgtype = XMANAGER_MSG_REFRESHCMD;
    asyncmsg->type = XMANAGER_METRICNODETYPE_CAPTURE;
    asyncmsg->name =jobname;
    jobname = NULL;
    asyncmsg->result = 0;

    xmetricxscscinode->asyncmsgs->msgs =  dlist_put(xmetricxscscinode->asyncmsgs->msgs, asyncmsg);
    return true;

xmanager_metricmsg_parserefresh_error:
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
                                                      XMANAGER_MSG_REFRESHCMD,
                                                      errcode,
                                                      errormsg);
    return false;
}


/*
 * 组装 refresh 返回消息
*/
bool xmanager_metricmsg_assemblerefresh(xmanager_metric* xmetric,
                                               netpoolentry* npoolentry,
                                               dlist* dlmsgs)
{
    xmanager_metricasyncmsg* xmetricasyncmsg     = NULL;
    char errmsg[2048]                                 = { 0 };

    if (true == dlist_isnull(dlmsgs) || 1 < dlist_getcount(dlmsgs))
    {
        /* 组装错误消息 */
        elog(RLOG_WARNING, "metric msg assemble refresh too many async msgs.");
        snprintf(errmsg, 2048, "metric msg assemble refresh too many async msgs.");
        return xmanager_metricmsg_assembleerrormsg(xmetric,
                                                          npoolentry->wpackets,
                                                          XMANAGER_MSG_REFRESHCMD,
                                                          ERROR_MSGCOMMAND,
                                                          errmsg);
    }

    xmetricasyncmsg = (xmanager_metricasyncmsg*)dlmsgs->head->value;
    return xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      XMANAGER_MSG_REFRESHCMD,
                                                      xmetricasyncmsg->errcode,
                                                      xmetricasyncmsg->errormsg);
}
