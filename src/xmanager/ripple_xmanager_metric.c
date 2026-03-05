#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "command/ripple_cmd.h"
#include "threads/ripple_threads.h"
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
#include "xmanager/ripple_xmanager_metricmsg.h"

ripple_xmanager_metric* ripple_xmanager_metric_init(void)
{
    ripple_xmanager_metric* xmetric = NULL;

    xmetric = rmalloc0(sizeof(ripple_xmanager_metric));
    if (NULL == xmetric)
    {
        elog(RLOG_WARNING, "xmanager metric init error");
        return NULL;
    }
    rmemset0(xmetric, 0, '\0', sizeof(ripple_xmanager_metric));
    xmetric->fd2metricnodes = NULL;
    xmetric->metricnodes = NULL;
    xmetric->npool = ripple_netpool_init();
    if (NULL == xmetric->npool)
    {
        rfree(xmetric);
        elog(RLOG_WARNING, "xmanager metric net pool init error");
        return NULL;
    }
    xmetric->xsynchpath = NULL;
    xmetric->configpath = NULL;
    xmetric->metricqueue = NULL;
    xmetric->privdata = NULL;

    /* 加载 metricnode */
    if (false == ripple_xmanager_metricnode_load(&xmetric->metricnodes))
    {
        elog(RLOG_WARNING, "xmanager metricnode init error, load metric node error");
        return NULL;
    }

    return xmetric;
}

/* 设置 configpath */
bool ripple_xmanager_metric_setxsynchpath(ripple_xmanager_metric* xmetric, char* xsynchpath)
{
    int len = 0;
    if (NULL == xsynchpath || '\0' == xsynchpath[0])
    {
        return true;
    }

    /* xsynchpath/config */
    len = strlen(xsynchpath);
    len += 1;
    xmetric->xsynchpath = rmalloc0(len);
    if (NULL == xmetric->xsynchpath)
    {
        elog(RLOG_WARNING, "xmanager set xsynch path error, out of memory");
        return false;
    }
    rmemset0(xmetric->xsynchpath, 0, '\0', len);
    snprintf(xmetric->xsynchpath, len, "%s", xsynchpath);

    len += 7;
    xmetric->configpath = rmalloc0(len);
    if (NULL == xmetric->configpath)
    {
        elog(RLOG_WARNING, "xmanager set config path error, out of memory");
        return false;
    }
    rmemset0(xmetric->configpath, 0, '\0', len);
    snprintf(xmetric->configpath, len, "%s/config", xsynchpath);
    return true;
}

/*
 * 读网络包处理
 *  返回 false 时, 需要在外围释放 index 对应的 npoolentry
*/
static bool ripple_xmanager_metric_parsepacket(ripple_xmanager_metric* xmetric, int index)
{
    int msgtype                     = 0;
    uint8* uptr                     = NULL;
    ripple_netpacket* npacket       = NULL;
    ripple_netpoolentry* npoolentry = NULL;
    char errormsg[512]              = { 0 };

    npoolentry = xmetric->npool->fds[index];
    while(1)
    {
        npacket = ripple_queue_tryget(npoolentry->rpackets);
        if (NULL == npacket)
        {
            return true;
        }

        if (npacket->offset != npacket->used)
        {
            if (false == ripple_queue_puthead(npoolentry->rpackets, npacket))
            {
                /* 组装一个 error packet 包 */
                uptr = npacket->data;
                rmemcpy1(&msgtype, 0, uptr, 4);
                msgtype = r_ntoh32(msgtype);
                snprintf(errormsg, 512, "unknown error happend");
                if (false == ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                                        npoolentry->wpackets,
                                                                        msgtype,
                                                                        RIPPLE_ERROR_OOM,
                                                                        errormsg))
                {
                    elog(RLOG_WARNING, "metric parse packet error");
                    return false;
                }
                return true;
            }
            return true;
        }

        /* 解析数据 */
        if (false == ripple_xmanager_metricmsg_parsenetpacket(xmetric, npoolentry, npacket))
        {
            ripple_netpacket_destroy(npacket);
            return false;
        }

        ripple_netpacket_destroy(npacket);
    }
    return true;
}

/*
 * 接收到新节点后的预处理
 *  1、xscsci 节点无需处理
 * 
 *  2、根据连接的节点 在 metricnodes 中找到对应的 xscsci 节点
 *    2.1 在 metricnodes 中遍历查找 xscsci 节点
 *    2.2 匹配 xscsci->asyncmsgs->msg 中的消息，匹配原则: 操作类型/作业类型/名称一致
 * 
 *  3、匹配到 msg 后, 根据 regnode 的成功还是失败组装成功还是失败信息
 *  4、在 asyncmsgs->msg 删除匹配的 msg
 *  5、检测 asyncmsg 是否为空，若为空 重组 packets 包
*/
static bool ripple_xmanager_metric_newnodepre(ripple_xmanager_metric* xmetric,
                                              ripple_xmanager_metricregnode* xmetricregnode)
{
    bool found                                          = false;
    dlistnode* dlnodemsg                                = NULL;
    dlistnode* dlnodemsgtmp                             = NULL;
    dlistnode* dlnode                                   = NULL;
    ripple_netpoolentry* npoolentry                     = NULL;
    ripple_xmanager_metricfd2node* xmetricfd2node       = NULL;
    ripple_xmanager_metricxscscinode* xmetricxscscinode = NULL;
    ripple_xmanager_metricasyncmsg* asyncmsg            = NULL;

    if (true == dlist_isnull(xmetric->fd2metricnodes))
    {
        return true;
    }

    /* xscsci 节点不处理 */
    if (RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI == xmetricregnode->nodetype)
    {
        return true;
    }

    /* 
     * 通过 xscsci 发送的消息中, 其中 init/stat/stop 消息需要异步返回
     *  可立即返回的有:
     *    RIPPLE_XMANAGER_MSG_CREATECMD
     *    RIPPLE_XMANAGER_MSG_ALTERCMD
     *    RIPPLE_XMANAGER_MSG_REMOVECMD
     *    RIPPLE_XMANAGER_MSG_DROPCMD
     *    RIPPLE_XMANAGER_MSG_EDITCMD
     *    RIPPLE_XMANAGER_MSG_RELOADCMD
     *    RIPPLE_XMANAGER_MSG_INFOCMD
     *    RIPPLE_XMANAGER_MSG_WATCHCMD
     *    RIPPLE_XMANAGER_MSG_LISTCMD
     * 
     *  需要等待反馈后的异步消息有:
     *    RIPPLE_XMANAGER_MSG_INITCMD
     *    RIPPLE_XMANAGER_MSG_STARTCMD
     *    RIPPLE_XMANAGER_MSG_STOPCMD
     *    onlinerefresh
    */
    if (RIPPLE_XMANAGER_MSG_INITCMD != xmetricregnode->msgtype
        && RIPPLE_XMANAGER_MSG_STARTCMD != xmetricregnode->msgtype
        && RIPPLE_XMANAGER_MSG_STOPCMD != xmetricregnode->msgtype)
    {
        return true;
    }

    for (dlnode = xmetric->fd2metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricfd2node = (ripple_xmanager_metricfd2node*)dlnode->value;
        if (RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI != xmetricfd2node->metricnode->type)
        {
            continue;
        }

        xmetricxscscinode = (ripple_xmanager_metricxscscinode*)xmetricfd2node->metricnode;
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
            if (xmetricregnode->msgtype != asyncmsg->msgtype)
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            if (xmetricregnode->nodetype != asyncmsg->type)
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            if (0 != strcmp(xmetricregnode->metricfd2node->metricnode->name, asyncmsg->name))
            {
                dlnodemsg = dlnodemsgtmp;
                continue;
            }

            npoolentry = ripple_netpool_getentrybyfd(xmetric->npool, xmetricfd2node->fd);
            if (NULL == npoolentry)
            {
                elog(RLOG_WARNING, "can not get pool entry by fd:%d", xmetricfd2node->fd);
                return true;
            }

            /* 构建消息 */
            found = true;

            /* 将 asyncmsg 转移到 results 中 */
            asyncmsg->result = xmetricregnode->result;
            if (1 == xmetricregnode->result)
            {
                asyncmsg->errcode = xmetricregnode->errcode;
                asyncmsg->errormsg = xmetricregnode->msg;
                xmetricregnode->msg = NULL;
            }

            /* 在消息中删除dlnodemsg */
            xmetricxscscinode->asyncmsgs->msgs = dlist_delete(xmetricxscscinode->asyncmsgs->msgs,
                                                              dlnodemsg,
                                                              NULL);

            /* 将 asyncmsg 加入到 result 中 */
            xmetricxscscinode->asyncmsgs->results = dlist_put(xmetricxscscinode->asyncmsgs->results, asyncmsg);
            break;
        }
    }

    /* 设置 metricnode 的状态 */
    if (0 == xmetricregnode->result)
    {
        if (RIPPLE_XMANAGER_MSG_INITCMD == xmetricregnode->msgtype)
        {
            xmetricregnode->metricfd2node->metricnode->stat = RIPPLE_XMANAGER_METRICNODESTAT_OFFLINE;
        }
        else if (RIPPLE_XMANAGER_MSG_STARTCMD == xmetricregnode->msgtype)
        {
            xmetricregnode->metricfd2node->metricnode->stat = RIPPLE_XMANAGER_METRICNODESTAT_ONLINE;
        }
        else if (RIPPLE_XMANAGER_MSG_STOPCMD == xmetricregnode->msgtype)
        {
            xmetricregnode->metricfd2node->metricnode->stat = RIPPLE_XMANAGER_METRICNODESTAT_OFFLINE;
        }

        /* 将 metricnode 落盘 */
        ripple_xmanager_metricnode_flush(xmetric->metricnodes);
    }

    if (false == found)
    {
        /* 没有处理, 返回 */
        return true;
    }

    /* 检测是否还有异步消息待反馈 */
    if (false == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
    {
        elog(RLOG_WARNING, "un done");
        return true;
    }

    /* 消息重组, init/start错误/stop 消息需要重组返回内容 */
    if (false == ripple_xmanager_metricmsg_assembleresponse(xmetric,
                                                            npoolentry,
                                                            xmetricregnode->msgtype,
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

/* 
 * 清理掉动态节点
 *  xscsci
 *  xmanager
*/
static void ripple_xmanager_metric_removenode(ripple_xmanager_metric* xmetric,
                                              ripple_xmanager_metricfd2node* xmetricfd2node)
{
    ripple_xmanager_metricnode* xmetricnode = NULL;

    xmetricnode = xmetricfd2node->metricnode;
    xmetricfd2node->metricnode->stat = RIPPLE_XMANAGER_METRICNODESTAT_OFFLINE;
    xmetric->fd2metricnodes = dlist_deletebyvalue(xmetric->fd2metricnodes,
                                                 (void*)((uintptr_t)xmetricfd2node->fd),
                                                 ripple_xmanager_metricfd2node_cmp,
                                                 ripple_xmanager_metricfd2node_destroyvoid);

    /* xscsci 和 xmanager 都需要在 xmanager 中移除 */
    if (RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI != xmetricnode->type
        && RIPPLE_XMANAGER_METRICNODETYPE_MANAGER != xmetricnode->type)
    {
        return;
    }

    xmetric->metricnodes = dlist_deletebyvaluefirstmatch(xmetric->metricnodes,
                                                        (void*)xmetricnode,
                                                        ripple_xmanager_metricnode_cmp,
                                                        ripple_xmanager_metricnode_destroyvoid);
}


/* 异步消息超时检测 */
static void ripple_xmanager_metric_asyncmsgtimeout(ripple_xmanager_metric* xmetric)
{
    int ilen                                            = 0;
    dlist* dlmsgs                                       = NULL;
    dlistnode* dlnode                                   = NULL;
    dlistnode* dlnodemsg                                = NULL;
    ripple_netpoolentry* npoolentry                     = NULL;
    ripple_xmanager_metricfd2node* xmetricfd2node       = NULL;
    ripple_xmanager_metricasyncmsg* xmetricasyncmsg     = NULL;
    ripple_xmanager_metricxscscinode* xmetricxscscinode = NULL;

    if (true == dlist_isnull(xmetric->fd2metricnodes))
    {
        return;
    }

    for (dlnode = xmetric->fd2metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricfd2node = (ripple_xmanager_metricfd2node*)dlnode->value;
        if (RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI != xmetricfd2node->metricnode->type)
        {
            continue;
        }
        xmetricxscscinode = (ripple_xmanager_metricxscscinode*)xmetricfd2node->metricnode;

        if (true == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
        {
            xmetricxscscinode->asyncmsgs->timeout = 0;
            continue;
        }

        if (xmetricxscscinode->asyncmsgs->timeout < RIPPLE_XMANAGER_METRICASYNCHMSG_TIMEOUT)
        {
            xmetricxscscinode->asyncmsgs->timeout += xmetric->npool->base->timeout;
            continue;
        }

        /* 获取描述符信息 */
        npoolentry = ripple_netpool_getentrybyfd(xmetric->npool, xmetricfd2node->fd);

        /* 消息超时，将 asyncmsgs->msgs 转移到 asyncmsgs->result 中并设置为超时 */
        dlmsgs = xmetricxscscinode->asyncmsgs->msgs;
        for (dlnodemsg = dlmsgs->head; NULL != dlnodemsg; dlnodemsg = dlnodemsg->next)
        {
            xmetricasyncmsg = (ripple_xmanager_metricasyncmsg*)dlnodemsg->value;
            xmetricasyncmsg->result = 1;
            xmetricasyncmsg->errcode = RIPPLE_ERROR_TIMEOUT;
            ilen = strlen("timeout");
            ilen += 1;
            xmetricasyncmsg->errormsg = rmalloc0(ilen);
            if (NULL == xmetricasyncmsg->errormsg)
            {
                elog(RLOG_WARNING, "async msg timeout, out of memory");
                break;
            }
            rmemset0(xmetricasyncmsg->errormsg, 0, '\0', ilen);
            ilen -= 1;
            rmemcpy0(xmetricasyncmsg->errormsg, 0, "timeout", ilen);
        }

        /* 合并 */
        xmetricxscscinode->asyncmsgs->results = dlist_concat(xmetricxscscinode->asyncmsgs->results,
                                                             xmetricxscscinode->asyncmsgs->msgs);
        xmetricxscscinode->asyncmsgs->msgs = NULL;

        /* 组装信息 */
        ripple_xmanager_metricmsg_assembleresponse(xmetric,
                                                   npoolentry,
                                                   xmetricasyncmsg->msgtype,
                                                   xmetricxscscinode->asyncmsgs->results);

        /* 清理 results */
        dlist_free(xmetricxscscinode->asyncmsgs->results, ripple_xmanager_metricasyncmsg_destroyvoid);
        xmetricxscscinode->asyncmsgs->results = NULL;
        xmetricxscscinode->asyncmsgs->timeout = 0;
    }
    return;
}

/* 主流程 */
void* ripple_xmanager_metric_main(void *args)
{
    struct timespec last_flush_ts                       = { 0, 0 };
    struct timespec now_ts                              = { 0, 0 };
    int index                                           = 0;
    int errorfdscnt                                     = 0;
    int* errorfds                                       = NULL;
    ripple_queueitem* item                              = NULL;
    ripple_queueitem* items                             = NULL;
    ripple_thrnode* thrnode                             = NULL;
    ripple_netpoolentry* npoolentry                     = NULL;
    ripple_xmanager_metric* xmetric                     = NULL;
    ripple_xmanager_metricnode* metricnode              = NULL;
    ripple_xmanager_metricfd2node* fd2node              = NULL;
    ripple_xmanager_metricregnode* xmetricregnode       = NULL;

    thrnode = (ripple_thrnode*)args;
    xmetric = (ripple_xmanager_metric*)thrnode->data;

    /* 查看状态 */
    if (RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "xmanager metric stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    clock_gettime(CLOCK_MONOTONIC, &now_ts);
    last_flush_ts = now_ts;

    while(1)
    {
        items = NULL;
        if (RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 在队列中获取描述符 */
        items = ripple_queue_trygetbatch(xmetric->metricqueue);
        for (item = items; NULL != item; item = items)
        {
            items = item->next;
            xmetricregnode = (ripple_xmanager_metricregnode*)item->data;
            fd2node = xmetricregnode->metricfd2node;
            
            /* 预处理 */
            if (false == ripple_xmanager_metric_newnodepre(xmetric, xmetricregnode))
            {
                ripple_close(fd2node->fd);
                fd2node->fd = -1;
                ripple_xmanager_metricnode_destroy(fd2node->metricnode);
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
                continue;
            }

            if (1 == xmetricregnode->result)
            {
                /* 无需处理, 直接关闭 */
                ripple_close(fd2node->fd);
                fd2node->fd = -1;
                ripple_xmanager_metricnode_destroy(fd2node->metricnode);
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
                continue;
            }
            xmetricregnode->metricfd2node = NULL;
            ripple_xmanager_metricregnode_destroy(xmetricregnode);

            /* 检测在 metricnodes 中是否存在 */
            metricnode = dlist_isexist(xmetric->metricnodes, fd2node->metricnode, ripple_xmanager_metricnode_cmp);
            if (NULL != metricnode)
            {
                if (NULL == metricnode->data)
                {
                    metricnode->data = fd2node->metricnode->data;
                    fd2node->metricnode->data = NULL;
                }
                
                if (NULL == metricnode->traildir)
                {
                    metricnode->traildir = fd2node->metricnode->traildir;
                    fd2node->metricnode->traildir = NULL;
                }

                ripple_xmanager_metricnode_destroy(fd2node->metricnode);
                fd2node->metricnode = metricnode;
            }
            else
            {
                xmetric->metricnodes = dlist_put(xmetric->metricnodes, fd2node->metricnode);
            }

            /*
             * 1、设置为在线状态
             * 2、加入到队列中
             */
            /* 设置为在线状态 */
            fd2node->metricnode->stat = RIPPLE_XMANAGER_METRICNODESTAT_ONLINE;

            /* 将 fd2node 加入到队列中 */
            xmetric->fd2metricnodes = dlist_put(xmetric->fd2metricnodes, fd2node);

            /*-----------------------add entry to pool   begin-----------------------------*/
            /* 
             * 将描述符加入到网络监听池中
             *  1、申请空间
             *  2、设置监听描述符
             *  3、加入到池子中
             */
            /* 申请空间 */
            npoolentry = ripple_netpoolentry_init();
            if (NULL == npoolentry)
            {
                elog(RLOG_WARNING, "xmanager metric net pool entry error");

                ripple_close(fd2node->fd);
                ripple_xmanager_metric_removenode(xmetric, fd2node);
                ripple_queueitem_free(item, NULL);
                continue;
            }

            /* 设置监听描述符 */
            ripple_netpoolentry_setfd(npoolentry, fd2node->fd);

            /* 添加到监听队列中 */
            if (false == ripple_netpool_add(xmetric->npool, npoolentry))
            {
                elog(RLOG_WARNING, "xmanager metric add entry to net pool error");
                ripple_xmanager_metric_removenode(xmetric, fd2node);
                ripple_netpoolentry_destroy(npoolentry);
                ripple_queueitem_free(item, NULL);
                continue;
            }
            /*-----------------------add entry to pool end-----------------------------*/

            /* 添加 identity 验证发送包 */
            if (false == ripple_xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, RIPPLE_XMANAGER_MSG_IDENTITYCMD))
            {
                elog(RLOG_WARNING, "xmanager metric add entry to net pool error");
                ripple_xmanager_metric_removenode(xmetric, fd2node);

                /* 删除该描述符 */
                ripple_netpool_del(xmetric->npool, npoolentry->fd);
                ripple_queueitem_free(item, NULL);
                continue;
            }

            ripple_queueitem_free(item, NULL);
        }

        /* 监听 */
        errorfdscnt = 0;
        errorfds = NULL;
        if (false == ripple_netpool_desc(xmetric->npool, &errorfdscnt, &errorfds))
        {
            elog(RLOG_WARNING, "xmanager metric net pool desc error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 先处理异常的描述符 */
        for (index = 0; index < errorfdscnt; index++)
        {
            /*
             * 1、获取并设置为 offset
             * 2、在活跃链表中移除
             */
            fd2node = dlist_get(xmetric->fd2metricnodes, (void*)((uintptr_t)errorfds[index]), ripple_xmanager_metricfd2node_cmp);
            ripple_xmanager_metric_removenode(xmetric, fd2node);

            /* 删除该描述符 */
            ripple_netpool_del(xmetric->npool, errorfds[index]);
            continue;
        }

        /* 遍历读队列并处理 */
        for (index = 0; index < xmetric->npool->fdcnt; index++)
        {
            if (NULL == xmetric->npool->fds[index])
            {
                continue;
            }

            npoolentry = xmetric->npool->fds[index];
            if (RIPPLE_NETPOOLENTRY_STAT_CLOSEUTILWPACKETNULL == npoolentry->stat)
            {
                fd2node = dlist_get(xmetric->fd2metricnodes, (void*)((uintptr_t)npoolentry->fd), ripple_xmanager_metricfd2node_cmp);
                ripple_xmanager_metric_removenode(xmetric, fd2node);

                /* 删除该描述符 */
                ripple_netpool_del(xmetric->npool, npoolentry->fd);
                continue;
            }

            if (true == ripple_queue_isnull(npoolentry->rpackets))
            {
                continue;
            }

            /* 处理读队列 */
            if (false == ripple_xmanager_metric_parsepacket(xmetric, index))
            {
                /* 处理读数据包有问题, 关闭掉连接并释放资源 */
                elog(RLOG_WARNING, "xmanager metric parse packet error");
                fd2node = dlist_get(xmetric->fd2metricnodes, (void*)((uintptr_t)npoolentry->fd), ripple_xmanager_metricfd2node_cmp);
                ripple_xmanager_metric_removenode(xmetric, fd2node);

                /* 删除该描述符 */
                ripple_netpool_del(xmetric->npool, npoolentry->fd);
                continue;
            }
        }

        /* 遍历 xscsci 节点, 查看是否有异步超时的消息 */
        ripple_xmanager_metric_asyncmsgtimeout(xmetric);

        /* 定时落盘metricnode文件，每10秒落盘一次 */
        clock_gettime(CLOCK_MONOTONIC, &now_ts);
        if (now_ts.tv_sec - last_flush_ts.tv_sec >= 10)
        {
            ripple_xmanager_metricnode_flush(xmetric->metricnodes);
            last_flush_ts = now_ts;
        }
        
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_xmanager_metric_destroy(ripple_xmanager_metric* xmetric)
{
    if (NULL == xmetric)
    {
        return;
    }

    if (NULL != xmetric->xsynchpath)
    {
        rfree(xmetric->xsynchpath);
        xmetric->xsynchpath = NULL;
    }

    if (NULL != xmetric->configpath)
    {
        rfree(xmetric->configpath);
        xmetric->configpath = NULL;
    }

    dlist_free(xmetric->fd2metricnodes, ripple_xmanager_metricfd2node_destroyvoid);
    xmetric->fd2metricnodes = NULL;
    dlist_free(xmetric->metricnodes, ripple_xmanager_metricnode_destroyvoid);
    xmetric->metricnodes = NULL;
    ripple_netpool_destroy(xmetric->npool);
    rfree(xmetric);
}
