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
#include "xmanager/ripple_xmanager_metricasyncmsg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metricxscscinode.h"
#include "xmanager/ripple_xmanager_metriccapturenode.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager_metricmsgcapturerefresh.h"
#include "xmanager/ripple_xmanager_metricmsg.h"


/*
 * 处理 capture onlinerefresh 命令
*/
bool ripple_xmanager_metricmsg_parsecapturerefresh(ripple_xmanager_metric* xmetric,
                                                   ripple_netpoolentry* npoolentry,
                                                   ripple_netpacket* npacket)
{
    /*
     * 1、获取 capture 的 metric node 节点
     * 2、遍历所有节点, 获取正确的 xscsci 节点
     * 3、组装发送到 xscsci 节点的数据
     */
    bool found                                          = false;
    int errcode                                         = 0;
    int resultlen                                       = 0;
    uint8* uptr                                         = NULL;
    dlistnode* dlnode                                   = NULL;
    char* errmsg                                       = NULL;
    dlistnode* dlnodemsg                                = NULL;
    dlistnode* dlnodemsgtmp                             = NULL;
    ripple_xmanager_metricnode* xmetricnode             = NULL;
    ripple_xmanager_metricfd2node* fd2node              = NULL;
    ripple_xmanager_metricasyncmsg* asyncmsg            = NULL;
    ripple_xmanager_metricxscscinode* xmetricxscscinode = NULL;

    /* 获取最后的内容 */
    uptr = npacket->data;

    uptr += 4 + 4 + 4 + 1;

    /* 总长度 */
    rmemcpy1(&resultlen, 0, uptr, 4);
    resultlen = r_ntoh32(resultlen);
    uptr += 4;
    resultlen -= 8;

    /* 获取错误码 */
    rmemcpy1(&errcode, 0, uptr, 4);
    errcode = r_ntoh32(errcode);
    uptr += 4;

    resultlen += 1;
    errmsg = rmalloc0(resultlen);
    if (NULL == errmsg)
    {
        elog(RLOG_WARNING, "parse capture onlinerefresh msg out of memory");
        return false;
    }
    rmemset0(errmsg, 0, '\0',resultlen);
    resultlen -= 1;
    rmemcpy0(errmsg, 0, uptr, resultlen);

    /* 获取 capture 节点 */
    fd2node = dlist_get(xmetric->fd2metricnodes,
                        (void*)((uintptr_t)npoolentry->fd),
                        ripple_xmanager_metricfd2node_cmp);
    xmetricnode = fd2node->metricnode;

    /* 获取 xscsci 节点 */
    for (dlnode = xmetric->fd2metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        fd2node = (ripple_xmanager_metricfd2node*)dlnode->value;
        if (RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI != fd2node->metricnode->type)
        {
            continue;
        }

        xmetricxscscinode = (ripple_xmanager_metricxscscinode*)fd2node->metricnode;
        if (NULL == xmetricxscscinode->asyncmsgs
            || true == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
        {
            continue;
        }

        dlnodemsg = xmetricxscscinode->asyncmsgs->msgs->head;
        while(NULL != dlnodemsg)
        {
            asyncmsg = (ripple_xmanager_metricasyncmsg*)dlnodemsg->value;
            dlnodemsgtmp = dlnodemsg->next;

            if (RIPPLE_XMANAGER_MSG_REFRESHCMD != asyncmsg->msgtype)
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            if (xmetricnode->type != asyncmsg->type)
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            if (0 != strcmp(xmetricnode->name, asyncmsg->name))
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            /* 找到了节点 */
            npoolentry = ripple_netpool_getentrybyfd(xmetric->npool, fd2node->fd);
            if (NULL == npoolentry)
            {
                elog(RLOG_WARNING, "can not get pool entry by fd:%d", fd2node->fd);
                rfree(errmsg);
                return true;
            }

            /* 构建消息 */
            found = true;

            /* 将 asyncmsg 转移到 results 中 */
            asyncmsg->result = 1;
            asyncmsg->errcode = RIPPLE_ERROR_APPENDMSG;
            asyncmsg->errormsg = errmsg;
            errmsg = NULL;

            /* 在消息中删除dlnodemsg */
            xmetricxscscinode->asyncmsgs->msgs = dlist_delete(xmetricxscscinode->asyncmsgs->msgs,
                                                              dlnodemsg,
                                                              NULL);

            /* 将 asyncmsg 加入到 result 中 */
            xmetricxscscinode->asyncmsgs->results = dlist_put(xmetricxscscinode->asyncmsgs->results, asyncmsg);
            break;
        }
    }

    if (false == found)
    {
        /* 没有处理, 返回 */
        rfree(errmsg);
        return true;
    }

     /* 检测是否还有异步消息待反馈 */
    if (false == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
    {
        elog(RLOG_WARNING, "un done");
        return true;
    }

    if (false == ripple_xmanager_metricmsg_assembleresponse(xmetric,
                                                            npoolentry,
                                                            RIPPLE_XMANAGER_MSG_REFRESHCMD,
                                                            xmetricxscscinode->asyncmsgs->results))
    {
        elog(RLOG_WARNING, "assemble response to xscsci error, close xscsci connect");

        /* netpool 中的节点移除 */
        ripple_netpool_del(xmetric->npool, npoolentry->fd);

        /* metricnode 节点在链表中移除 */
        xmetric->metricnodes = dlist_deletebyvaluefirstmatch(xmetric->metricnodes,
                                                             xmetricxscscinode,
                                                             ripple_xmanager_metricnode_cmp,
                                                             ripple_xmanager_metricnode_destroyvoid);

        /* 在描述符映射链表中将数据移除 */
        xmetric->fd2metricnodes = dlist_delete(xmetric->fd2metricnodes,
                                               dlnode,
                                               ripple_xmanager_metricfd2node_destroyvoid);
        return true;
    }

    /* 清理 results */
    dlist_free(xmetricxscscinode->asyncmsgs->results, ripple_xmanager_metricasyncmsg_destroyvoid);
    xmetricxscscinode->asyncmsgs->results = NULL;
    xmetricxscscinode->asyncmsgs->timeout = 0;
    return true;
}
