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
#include "xmanager/ripple_xmanager_metricmsgrefresh.h"
#include "xmanager/ripple_xmanager_metricmsg.h"


/* 组装 capture 的 refresh 消息 */
static bool ripple_xmanager_metricmsg_assemblerefreshforcapture(ripple_xmanager_metric* xmetric,
                                                                ripple_xmanager_metricnode* xmetricnode,
                                                                int tableslen,
                                                                uint8* tables)
{
    int ivalue                                          = 0;
    int msglen                                          = 0;
    uint8* uptr                                         = NULL;
    ripple_netpacket* npacket                           = NULL;
    ripple_netpoolentry* npoolentry                     = NULL;
    ripple_xmanager_metricfd2node* fd2node              = NULL;

    /* 构建 onlinerefresh 消息 */
    fd2node = dlist_get(xmetric->fd2metricnodes,
                        xmetricnode,
                        ripple_xmanager_metricfd2node_cmp2);

    /* 获取 capture 节点的 netpoolentry */
    npoolentry = NULL;
    npoolentry = ripple_netpool_getentrybyfd(xmetric->npool, fd2node->fd);
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
    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble refresh to capture out of memory");
        return false;
    }

    msglen += 1;
    npacket->data = ripple_netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble refresh msg data, out of memory");
        ripple_netpacket_destroy(npacket);
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
    ivalue = RIPPLE_XMANAGER_MSG_CAPTUREREFRESH;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    rmemcpy1(uptr, 0, tables, tableslen);

    /* 将 netpacket 挂载到待发送队列中 */
    if (false == ripple_queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble refresh msg to capture add message to queue error");
        ripple_netpacket_destroy(npacket);
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
bool ripple_xmanager_metricmsg_parserefresh(ripple_xmanager_metric* xmetric,
                                            ripple_netpoolentry* npoolentry,
                                            ripple_netpacket* npacket)
{
    int errcode = 0;
    int ivalue = 0;
    int msglen = 0;

    uint8* uptr                                         = NULL;
    char* jobname                                       = NULL;
    ripple_xmanager_metricnode* pxmetricnode            = NULL;
    ripple_xmanager_metricfd2node* fd2node              = NULL;
    ripple_xmanager_metricasyncmsg* asyncmsg            = NULL;
    ripple_xmanager_metricxscscinode* xmetricxscscinode = NULL;
    char errormsg[2048]                                 = { 0 };
    ripple_xmanager_metricnode xmetricnode              = { 0 };

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
        errcode = RIPPLE_ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: xmanager parse refresh command, out of memory.");
        goto ripple_xmanager_metricmsg_parserefresh_error;
    }
    rmemset0(jobname, 0, '\0', ivalue);
    ivalue -= 1;
    rmemcpy0(jobname, 0, uptr, ivalue);
    msglen -= ivalue;
    uptr += ivalue;

    /* 查看 node 是否存在 */
    xmetricnode.type = RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE;
    xmetricnode.name = jobname;

    pxmetricnode = dlist_get(xmetric->metricnodes, &xmetricnode, ripple_xmanager_metricnode_cmp);
    if (NULL == pxmetricnode)
    {
        errcode = RIPPLE_ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: %s does not exist.", jobname);
        goto ripple_xmanager_metricmsg_parserefresh_error;
    }

    if (RIPPLE_XMANAGER_METRICNODESTAT_ONLINE != pxmetricnode->stat)
    {
        errcode = RIPPLE_ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: start %s first.", jobname);
        goto ripple_xmanager_metricmsg_parserefresh_error;
    }

    if (false == ripple_xmanager_metricmsg_assemblerefreshforcapture(xmetric,
                                                                     pxmetricnode,
                                                                     msglen,
                                                                     uptr))
    {
        errcode = RIPPLE_ERROR_MSGCOMMAND;
        snprintf(errormsg, 2048, "ERROR: assemble refresh message to capture error.");
        goto ripple_xmanager_metricmsg_parserefresh_error;
    }

    /* 
     * 创建异步消息
     *  1、获取 xscsci 节点
     *  2、创建异步等待消息
     */
    /* 获取 xscsci 节点 */
    fd2node = dlist_get(xmetric->fd2metricnodes,
                        (void*)((uintptr_t)npoolentry->fd),
                        ripple_xmanager_metricfd2node_cmp);
    xmetricxscscinode = (ripple_xmanager_metricxscscinode*)fd2node->metricnode;

    /* 创建异步等待消息 */
    asyncmsg = ripple_xmanager_metricasyncmsg_init();
    if (NULL == asyncmsg)
    {
        errcode = RIPPLE_ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: %s", "xmanager refresh command, out of memory");
        goto ripple_xmanager_metricmsg_parserefresh_error;
    }

    asyncmsg->errormsg = NULL;
    asyncmsg->msgtype = RIPPLE_XMANAGER_MSG_REFRESHCMD;
    asyncmsg->type = RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE;
    asyncmsg->name =jobname;
    jobname = NULL;
    asyncmsg->result = 0;

    xmetricxscscinode->asyncmsgs->msgs =  dlist_put(xmetricxscscinode->asyncmsgs->msgs, asyncmsg);
    return true;

ripple_xmanager_metricmsg_parserefresh_error:
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
                                                      RIPPLE_XMANAGER_MSG_REFRESHCMD,
                                                      errcode,
                                                      errormsg);
    return false;
}


/*
 * 组装 refresh 返回消息
*/
bool ripple_xmanager_metricmsg_assemblerefresh(ripple_xmanager_metric* xmetric,
                                               ripple_netpoolentry* npoolentry,
                                               dlist* dlmsgs)
{
    ripple_xmanager_metricasyncmsg* xmetricasyncmsg     = NULL;
    char errmsg[2048]                                 = { 0 };

    if (true == dlist_isnull(dlmsgs) || 1 < dlist_getcount(dlmsgs))
    {
        /* 组装错误消息 */
        elog(RLOG_WARNING, "metric msg assemble refresh too many async msgs.");
        snprintf(errmsg, 2048, "metric msg assemble refresh too many async msgs.");
        return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                          npoolentry->wpackets,
                                                          RIPPLE_XMANAGER_MSG_REFRESHCMD,
                                                          RIPPLE_ERROR_MSGCOMMAND,
                                                          errmsg);
    }

    xmetricasyncmsg = (ripple_xmanager_metricasyncmsg*)dlmsgs->head->value;
    return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      RIPPLE_XMANAGER_MSG_REFRESHCMD,
                                                      xmetricasyncmsg->errcode,
                                                      xmetricasyncmsg->errormsg);
}
