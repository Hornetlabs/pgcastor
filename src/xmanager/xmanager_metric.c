#include "app_incl.h"
#include "port/file/fd.h"
#include "port/net/net.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "command/cmd.h"
#include "threads/threads.h"
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
#include "xmanager/xmanager_metricmsg.h"

xmanager_metric* xmanager_metric_init(void)
{
    xmanager_metric* xmetric = NULL;

    xmetric = rmalloc0(sizeof(xmanager_metric));
    if (NULL == xmetric)
    {
        elog(RLOG_WARNING, "xmanager metric init error");
        return NULL;
    }
    rmemset0(xmetric, 0, '\0', sizeof(xmanager_metric));
    xmetric->fd2metricnodes = NULL;
    xmetric->metricnodes = NULL;
    xmetric->npool = netpool_init();
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

    /* Load metricnode */
    if (false == xmanager_metricnode_load(&xmetric->metricnodes))
    {
        elog(RLOG_WARNING, "xmanager metricnode init error, load metric node error");
        return NULL;
    }

    return xmetric;
}

/* Set configpath */
bool xmanager_metric_setxsynchpath(xmanager_metric* xmetric, char* xsynchpath)
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
 * Parse network packet
 * Returns false when caller needs to free index's npoolentry
 */
static bool xmanager_metric_parsepacket(xmanager_metric* xmetric, int index)
{
    int           msgtype = 0;
    uint8*        uptr = NULL;
    netpacket*    npacket = NULL;
    netpoolentry* npoolentry = NULL;
    char          errormsg[512] = {0};

    npoolentry = xmetric->npool->fds[index];
    while (1)
    {
        npacket = queue_tryget(npoolentry->rpackets);
        if (NULL == npacket)
        {
            return true;
        }

        if (npacket->offset != npacket->used)
        {
            if (false == queue_puthead(npoolentry->rpackets, npacket))
            {
                /* Assemble error packet */
                uptr = npacket->data;
                rmemcpy1(&msgtype, 0, uptr, 4);
                msgtype = r_ntoh32(msgtype);
                snprintf(errormsg, 512, "unknown error happend");
                if (false == xmanager_metricmsg_assembleerrormsg(
                                 xmetric, npoolentry->wpackets, msgtype, ERROR_OOM, errormsg))
                {
                    elog(RLOG_WARNING, "metric parse packet error");
                    return false;
                }
                return true;
            }
            return true;
        }

        /* Parse data */
        if (false == xmanager_metricmsg_parsenetpacket(xmetric, npoolentry, npacket))
        {
            netpacket_destroy(npacket);
            return false;
        }

        netpacket_destroy(npacket);
    }
    return true;
}

/*
 * Preprocessing after receiving new node
 *  1、xscsci node does not need processing
 *
 *  2、Find corresponding xscsci node in metricnodes based on connected node
 *    2.1 Traverse metricnodes to find xscsci node
 *    2.2 Match messages in xscsci->asyncmsgs->msg, matching principle: operation type/job type/name
 * match
 *
 *  3、After matching msg, assemble success or failure message based on regnode success or failure
 *  4、Delete matched msg from asyncmsgs->msg
 *  5、Check if asyncmsg is empty, if empty reorganize packets
 */
static bool xmanager_metric_newnodepre(xmanager_metric*        xmetric,
                                       xmanager_metricregnode* xmetricregnode)
{
    bool                       found = false;
    dlistnode*                 dlnodemsg = NULL;
    dlistnode*                 dlnodemsgtmp = NULL;
    dlistnode*                 dlnode = NULL;
    netpoolentry*              npoolentry = NULL;
    xmanager_metricfd2node*    xmetricfd2node = NULL;
    xmanager_metricxscscinode* xmetricxscscinode = NULL;
    xmanager_metricasyncmsg*   asyncmsg = NULL;

    if (true == dlist_isnull(xmetric->fd2metricnodes))
    {
        return true;
    }

    /* xscsci node does not process */
    if (XMANAGER_METRICNODETYPE_XSCSCI == xmetricregnode->nodetype)
    {
        return true;
    }

    /*
     * Among messages sent via xscsci, init/stat/stop messages need async response
     *  Can return immediately:
     *    XMANAGER_MSG_CREATECMD
     *    XMANAGER_MSG_ALTERCMD
     *    XMANAGER_MSG_REMOVECMD
     *    XMANAGER_MSG_DROPCMD
     *    XMANAGER_MSG_EDITCMD
     *    XMANAGER_MSG_RELOADCMD
     *    XMANAGER_MSG_INFOCMD
     *    XMANAGER_MSG_WATCHCMD
     *    XMANAGER_MSG_LISTCMD
     *
     *  Need to wait for feedback async messages:
     *    XMANAGER_MSG_INITCMD
     *    XMANAGER_MSG_STARTCMD
     *    XMANAGER_MSG_STOPCMD
     *    onlinerefresh
     */
    if (XMANAGER_MSG_INITCMD != xmetricregnode->msgtype &&
        XMANAGER_MSG_STARTCMD != xmetricregnode->msgtype &&
        XMANAGER_MSG_STOPCMD != xmetricregnode->msgtype)
    {
        return true;
    }

    for (dlnode = xmetric->fd2metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricfd2node = (xmanager_metricfd2node*)dlnode->value;
        if (XMANAGER_METRICNODETYPE_XSCSCI != xmetricfd2node->metricnode->type)
        {
            continue;
        }

        xmetricxscscinode = (xmanager_metricxscscinode*)xmetricfd2node->metricnode;
        if (NULL == xmetricxscscinode->asyncmsgs ||
            true == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
        {
            continue;
        }

        dlnodemsg = xmetricxscscinode->asyncmsgs->msgs->head;
        while (NULL != dlnodemsg)
        {
            asyncmsg = (xmanager_metricasyncmsg*)dlnodemsg->value;
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

            npoolentry = netpool_getentrybyfd(xmetric->npool, xmetricfd2node->fd);
            if (NULL == npoolentry)
            {
                elog(RLOG_WARNING, "can not get pool entry by fd:%d", xmetricfd2node->fd);
                return true;
            }

            /* Build message */
            found = true;

            /* Move asyncmsg to results */
            asyncmsg->result = xmetricregnode->result;
            if (1 == xmetricregnode->result)
            {
                asyncmsg->errcode = xmetricregnode->errcode;
                asyncmsg->errormsg = xmetricregnode->msg;
                xmetricregnode->msg = NULL;
            }

            /* Delete dlnodemsg from messages */
            xmetricxscscinode->asyncmsgs->msgs =
                dlist_delete(xmetricxscscinode->asyncmsgs->msgs, dlnodemsg, NULL);

            /* Add asyncmsg to results */
            xmetricxscscinode->asyncmsgs->results =
                dlist_put(xmetricxscscinode->asyncmsgs->results, asyncmsg);
            break;
        }
    }

    /* Set metricnode status */
    if (0 == xmetricregnode->result)
    {
        if (XMANAGER_MSG_INITCMD == xmetricregnode->msgtype)
        {
            xmetricregnode->metricfd2node->metricnode->stat = XMANAGER_METRICNODESTAT_OFFLINE;
        }
        else if (XMANAGER_MSG_STARTCMD == xmetricregnode->msgtype)
        {
            xmetricregnode->metricfd2node->metricnode->stat = XMANAGER_METRICNODESTAT_ONLINE;
        }
        else if (XMANAGER_MSG_STOPCMD == xmetricregnode->msgtype)
        {
            xmetricregnode->metricfd2node->metricnode->stat = XMANAGER_METRICNODESTAT_OFFLINE;
        }

        /* Persist metricnode to disk */
        xmanager_metricnode_flush(xmetric->metricnodes);
    }

    if (false == found)
    {
        /* Not processed, return */
        return true;
    }

    /* Check if there are still async messages waiting for feedback */
    if (false == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
    {
        elog(RLOG_WARNING, "un done");
        return true;
    }

    /* Message reorganization, init/start error/stop messages need to reorganize return content */
    if (false ==
        xmanager_metricmsg_assembleresponse(
            xmetric, npoolentry, xmetricregnode->msgtype, xmetricxscscinode->asyncmsgs->results))
    {
        elog(RLOG_WARNING, "assemble response to xscsci error, close xscsci connect");

        /* Remove node from netpool */
        netpool_del(xmetric->npool, npoolentry->fd);

        /* Remove metricnode from list */
        xmetric->metricnodes = dlist_deletebyvaluefirstmatch(xmetric->metricnodes,
                                                             xmetricxscscinode,
                                                             xmanager_metricnode_cmp,
                                                             xmanager_metricnode_destroyvoid);

        /* Remove data from fd mapping list */
        xmetric->fd2metricnodes =
            dlist_delete(xmetric->fd2metricnodes, dlnode, xmanager_metricfd2node_destroyvoid);
        return true;
    }

    /* Clean up results */
    dlist_free(xmetricxscscinode->asyncmsgs->results, xmanager_metricasyncmsg_destroyvoid);
    xmetricxscscinode->asyncmsgs->results = NULL;
    xmetricxscscinode->asyncmsgs->timeout = 0;
    return true;
}

/*
 * Clean up dynamic nodes
 *  xscsci
 *  xmanager
 */
static void xmanager_metric_removenode(xmanager_metric*        xmetric,
                                       xmanager_metricfd2node* xmetricfd2node)
{
    xmanager_metricnode* xmetricnode = NULL;

    xmetricnode = xmetricfd2node->metricnode;
    xmetricfd2node->metricnode->stat = XMANAGER_METRICNODESTAT_OFFLINE;
    xmetric->fd2metricnodes = dlist_deletebyvalue(xmetric->fd2metricnodes,
                                                  (void*)((uintptr_t)xmetricfd2node->fd),
                                                  xmanager_metricfd2node_cmp,
                                                  xmanager_metricfd2node_destroyvoid);

    /* xscsci and xmanager both need to be removed from xmanager */
    if (XMANAGER_METRICNODETYPE_XSCSCI != xmetricnode->type &&
        XMANAGER_METRICNODETYPE_MANAGER != xmetricnode->type)
    {
        return;
    }

    xmetric->metricnodes = dlist_deletebyvaluefirstmatch(xmetric->metricnodes,
                                                         (void*)xmetricnode,
                                                         xmanager_metricnode_cmp,
                                                         xmanager_metricnode_destroyvoid);
}

/* Async message timeout detection */
static void xmanager_metric_asyncmsgtimeout(xmanager_metric* xmetric)
{
    int                        ilen = 0;
    dlist*                     dlmsgs = NULL;
    dlistnode*                 dlnode = NULL;
    dlistnode*                 dlnodemsg = NULL;
    netpoolentry*              npoolentry = NULL;
    xmanager_metricfd2node*    xmetricfd2node = NULL;
    xmanager_metricasyncmsg*   xmetricasyncmsg = NULL;
    xmanager_metricxscscinode* xmetricxscscinode = NULL;

    if (true == dlist_isnull(xmetric->fd2metricnodes))
    {
        return;
    }

    for (dlnode = xmetric->fd2metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        xmetricfd2node = (xmanager_metricfd2node*)dlnode->value;
        if (XMANAGER_METRICNODETYPE_XSCSCI != xmetricfd2node->metricnode->type)
        {
            continue;
        }
        xmetricxscscinode = (xmanager_metricxscscinode*)xmetricfd2node->metricnode;

        if (true == dlist_isnull(xmetricxscscinode->asyncmsgs->msgs))
        {
            xmetricxscscinode->asyncmsgs->timeout = 0;
            continue;
        }

        if (xmetricxscscinode->asyncmsgs->timeout < XMANAGER_METRICASYNCHMSG_TIMEOUT)
        {
            xmetricxscscinode->asyncmsgs->timeout += xmetric->npool->base->timeout;
            continue;
        }

        /* Get fd info */
        npoolentry = netpool_getentrybyfd(xmetric->npool, xmetricfd2node->fd);

        /* Message timeout, move asyncmsgs->msgs to asyncmsgs->result and set to timeout */
        dlmsgs = xmetricxscscinode->asyncmsgs->msgs;
        for (dlnodemsg = dlmsgs->head; NULL != dlnodemsg; dlnodemsg = dlnodemsg->next)
        {
            xmetricasyncmsg = (xmanager_metricasyncmsg*)dlnodemsg->value;
            xmetricasyncmsg->result = 1;
            xmetricasyncmsg->errcode = ERROR_TIMEOUT;
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

        /* Merge */
        xmetricxscscinode->asyncmsgs->results =
            dlist_concat(xmetricxscscinode->asyncmsgs->results, xmetricxscscinode->asyncmsgs->msgs);
        xmetricxscscinode->asyncmsgs->msgs = NULL;

        /* Assemble info */
        xmanager_metricmsg_assembleresponse(
            xmetric, npoolentry, xmetricasyncmsg->msgtype, xmetricxscscinode->asyncmsgs->results);

        /* Clean up results */
        dlist_free(xmetricxscscinode->asyncmsgs->results, xmanager_metricasyncmsg_destroyvoid);
        xmetricxscscinode->asyncmsgs->results = NULL;
        xmetricxscscinode->asyncmsgs->timeout = 0;
    }
    return;
}

/* Main loop */
void* xmanager_metric_main(void* args)
{
    struct timespec         last_flush_ts = {0, 0};
    struct timespec         now_ts = {0, 0};
    int                     index = 0;
    int                     errorfdscnt = 0;
    int*                    errorfds = NULL;
    queueitem*              item = NULL;
    queueitem*              items = NULL;
    thrnode*                thrnode_ptr = NULL;
    netpoolentry*           npoolentry = NULL;
    xmanager_metric*        xmetric = NULL;
    xmanager_metricnode*    metricnode = NULL;
    xmanager_metricfd2node* fd2node = NULL;
    xmanager_metricregnode* xmetricregnode = NULL;

    thrnode_ptr = (thrnode*)args;
    xmetric = (xmanager_metric*)thrnode_ptr->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thrnode_ptr->stat)
    {
        elog(RLOG_WARNING,
             "xmanager metric stat exception, expected state is THRNODE_STAT_STARTING");
        thrnode_ptr->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thrnode_ptr->stat = THRNODE_STAT_WORK;

    clock_gettime(CLOCK_MONOTONIC, &now_ts);
    last_flush_ts = now_ts;

    while (1)
    {
        items = NULL;
        if (THRNODE_STAT_TERM == thrnode_ptr->stat)
        {
            thrnode_ptr->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get fd from queue */
        items = queue_trygetbatch(xmetric->metricqueue);
        for (item = items; NULL != item; item = items)
        {
            items = item->next;
            xmetricregnode = (xmanager_metricregnode*)item->data;
            fd2node = xmetricregnode->metricfd2node;

            /* Preprocessing */
            if (false == xmanager_metric_newnodepre(xmetric, xmetricregnode))
            {
                close(fd2node->fd);
                fd2node->fd = -1;
                xmanager_metricnode_destroy(fd2node->metricnode);
                xmanager_metricregnode_destroy(xmetricregnode);
                continue;
            }

            if (1 == xmetricregnode->result)
            {
                /* No need to process, just close */
                close(fd2node->fd);
                fd2node->fd = -1;
                xmanager_metricnode_destroy(fd2node->metricnode);
                xmanager_metricregnode_destroy(xmetricregnode);
                continue;
            }
            xmetricregnode->metricfd2node = NULL;
            xmanager_metricregnode_destroy(xmetricregnode);

            /* Check if exists in metricnodes */
            metricnode =
                dlist_isexist(xmetric->metricnodes, fd2node->metricnode, xmanager_metricnode_cmp);
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

                xmanager_metricnode_destroy(fd2node->metricnode);
                fd2node->metricnode = metricnode;
            }
            else
            {
                xmetric->metricnodes = dlist_put(xmetric->metricnodes, fd2node->metricnode);
            }

            /*
             * 1、Set to online state
             * 2、Add to queue
             */
            /* Set to online state */
            fd2node->metricnode->stat = XMANAGER_METRICNODESTAT_ONLINE;

            /* Add fd2node to queue */
            xmetric->fd2metricnodes = dlist_put(xmetric->fd2metricnodes, fd2node);

            /*-----------------------add entry to pool   begin-----------------------------*/
            /*
             * Add fd to network listen pool
             *  1、Allocate space
             *  2、Set listen fd
             *  3、Add to pool
             */
            /* Allocate space */
            npoolentry = netpoolentry_init();
            if (NULL == npoolentry)
            {
                elog(RLOG_WARNING, "xmanager metric net pool entry error");

                close(fd2node->fd);
                xmanager_metric_removenode(xmetric, fd2node);
                queueitem_free(item, NULL);
                continue;
            }

            /* Set listen fd */
            netpoolentry_setfd(npoolentry, fd2node->fd);

            /* Add to listen queue */
            if (false == netpool_add(xmetric->npool, npoolentry))
            {
                elog(RLOG_WARNING, "xmanager metric add entry to net pool error");
                xmanager_metric_removenode(xmetric, fd2node);
                netpoolentry_destroy(npoolentry);
                queueitem_free(item, NULL);
                continue;
            }
            /*-----------------------add entry to pool end-----------------------------*/

            /* Add identity verification send packet */
            if (false ==
                xmanager_metricmsg_assemblecmdresult(xmetric, npoolentry, XMANAGER_MSG_IDENTITYCMD))
            {
                elog(RLOG_WARNING, "xmanager metric add entry to net pool error");
                xmanager_metric_removenode(xmetric, fd2node);

                /* Delete the fd */
                netpool_del(xmetric->npool, npoolentry->fd);
                queueitem_free(item, NULL);
                continue;
            }

            queueitem_free(item, NULL);
        }

        /* Monitor */
        errorfdscnt = 0;
        errorfds = NULL;
        if (false == netpool_desc(xmetric->npool, &errorfdscnt, &errorfds))
        {
            elog(RLOG_WARNING, "xmanager metric net pool desc error");
            thrnode_ptr->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Handle abnormal fds first */
        for (index = 0; index < errorfdscnt; index++)
        {
            /*
             * 1. Get and set offset
             * 2. Remove from active list
             */
            fd2node = dlist_get(xmetric->fd2metricnodes,
                                (void*)((uintptr_t)errorfds[index]),
                                xmanager_metricfd2node_cmp);
            xmanager_metric_removenode(xmetric, fd2node);

            /* Delete the fd */
            netpool_del(xmetric->npool, errorfds[index]);
            continue;
        }

        /* Iterate read queue and process */
        for (index = 0; index < xmetric->npool->fdcnt; index++)
        {
            if (NULL == xmetric->npool->fds[index])
            {
                continue;
            }

            npoolentry = xmetric->npool->fds[index];
            if (NETPOOLENTRY_STAT_CLOSEUTILWPACKETNULL == npoolentry->stat)
            {
                fd2node = dlist_get(xmetric->fd2metricnodes,
                                    (void*)((uintptr_t)npoolentry->fd),
                                    xmanager_metricfd2node_cmp);
                xmanager_metric_removenode(xmetric, fd2node);

                /* Delete the fd */
                netpool_del(xmetric->npool, npoolentry->fd);
                continue;
            }

            if (true == queue_isnull(npoolentry->rpackets))
            {
                continue;
            }

            /* Process read queue */
            if (false == xmanager_metric_parsepacket(xmetric, index))
            {
                /* Error processing read packet, close connection and release resources */
                elog(RLOG_WARNING, "xmanager metric parse packet error");
                fd2node = dlist_get(xmetric->fd2metricnodes,
                                    (void*)((uintptr_t)npoolentry->fd),
                                    xmanager_metricfd2node_cmp);
                xmanager_metric_removenode(xmetric, fd2node);

                /* Delete the fd */
                netpool_del(xmetric->npool, npoolentry->fd);
                continue;
            }
        }

        /* Traverse xscsci nodes to check for async timeout messages */
        xmanager_metric_asyncmsgtimeout(xmetric);

        /* Periodic flush metricnode file every 10 seconds */
        clock_gettime(CLOCK_MONOTONIC, &now_ts);
        if (now_ts.tv_sec - last_flush_ts.tv_sec >= 10)
        {
            xmanager_metricnode_flush(xmetric->metricnodes);
            last_flush_ts = now_ts;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void xmanager_metric_destroy(xmanager_metric* xmetric)
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

    dlist_free(xmetric->fd2metricnodes, xmanager_metricfd2node_destroyvoid);
    xmetric->fd2metricnodes = NULL;
    dlist_free(xmetric->metricnodes, xmanager_metricnode_destroyvoid);
    xmetric->metricnodes = NULL;
    netpool_destroy(xmetric->npool);
    rfree(xmetric);
}
