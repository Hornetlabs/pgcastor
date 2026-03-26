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
#include "net/netserver.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricasyncmsg.h"
#include "xmanager/xmanager_auth.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metricxscscinode.h"
#include "xmanager/xmanager_metriccapturenode.h"
#include "xmanager/xmanager_metricintegratenode.h"
#include "xmanager/xmanager_listen.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsg.h"

typedef struct XMANAGER_AUTHFD2TIMEOUT
{
    int fd;
    int timeout;
} xmanager_authfd2timeout;

static xmanager_authfd2timeout* xmanager_authfd2timeout_init(void)
{
    xmanager_authfd2timeout* fd2timeout = NULL;

    fd2timeout = rmalloc0(sizeof(xmanager_authfd2timeout));
    if (NULL == fd2timeout)
    {
        elog(RLOG_WARNING, "authfd2timeout init error");
        return NULL;
    }
    fd2timeout->fd = -1;
    fd2timeout->timeout = 0;
    return fd2timeout;
}

/* Compare */
static int xmanager_authfd2timeout_cmp(void* s1, void* s2)
{
    int                      fd = -1;
    xmanager_authfd2timeout* fd2timeout = NULL;

    fd = (int)((uintptr_t)s1);
    fd2timeout = (xmanager_authfd2timeout*)s2;

    if (fd == fd2timeout->fd)
    {
        return 0;
    }
    return 1;
}

/* Destroy */
static void xmanager_authfd2timeout_destroy(xmanager_authfd2timeout* fd2timeout)
{
    if (NULL == fd2timeout)
    {
        return;
    }

    rfree(fd2timeout);
}

static void xmanager_authfd2timeout_destroyvoid(void* args)
{
    xmanager_authfd2timeout_destroy((xmanager_authfd2timeout*)args);
}

xmanager_auth* xmanager_auth_init(void)
{
    xmanager_auth* xauth = NULL;

    xauth = rmalloc0(sizeof(xmanager_auth));
    if (NULL == xauth)
    {
        elog(RLOG_WARNING, "xmanager auth error");
        return NULL;
    }
    rmemset0(xauth, 0, '\0', sizeof(xmanager_auth));
    xauth->timeout = XMANAGER_AUTH_DEFAULTTIMEOUT;
    xauth->no = 1;
    xauth->npool = netpool_init();
    if (NULL == xauth->npool)
    {
        rfree(xauth);
        elog(RLOG_WARNING, "xmanager auth net pool init error");
        return NULL;
    }
    xauth->authqueue = NULL;
    return xauth;
}

/* xscsci */
static xmanager_metricregnode* xmanager_auth_identityxscsci(xmanager_auth* xauth, uint8* uptr)
{
    int                        jobnamelen = 0;
    char*                      jobname = NULL;
    xmanager_metricregnode*    xmetricregnode = NULL;
    xmanager_metricfd2node*    xmetricfd2node = NULL;
    xmanager_metricxscscinode* xmetricxscsci = NULL;

    xmetricregnode = xmanager_metricregnode_init();
    if (NULL == xmetricregnode)
    {
        elog(RLOG_WARNING, "xmanager auth xscsci identity regist node error, out of memory");
        return NULL;
    }
    xmetricregnode->result = 0;
    xmetricregnode->nodetype = XMANAGER_METRICNODETYPE_XSCSCI;

    xmetricfd2node = xmanager_metricfd2node_init();
    if (NULL == xmetricfd2node)
    {
        elog(RLOG_WARNING, "xmanager auth xscsci identity fd2node error, out of memeory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricregnode->metricfd2node = xmetricfd2node;

    xmetricfd2node->metricnode = xmanager_metricnode_init(xmetricregnode->nodetype);
    if (NULL == xmetricfd2node->metricnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity xscsci error, out of memory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricxscsci = (xmanager_metricxscscinode*)xmetricfd2node->metricnode;
    xmetricxscsci->number = xauth->no;
    xauth->no++;

    /* Name length */
    rmemcpy1(&jobnamelen, 0, uptr, 4);
    uptr += 4;
    jobnamelen = r_ntoh32(jobnamelen);

    /* Name */
    if (0 != jobnamelen)
    {
        jobnamelen += 1;
        jobname = rmalloc0(jobnamelen);
        if (NULL == jobname)
        {
            elog(RLOG_WARNING, "auth identity jobname error");
            xmanager_metricregnode_destroy(xmetricregnode);
            return NULL;
        }
        rmemset0(jobname, 0, '\0', jobnamelen);
        jobnamelen -= 1;
        rmemcpy0(jobname, 0, uptr, jobnamelen);
    }
    else
    {
        jobname = NULL;
    }

    xmetricxscsci->base.conf = NULL;
    xmetricxscsci->base.data = NULL;
    xmetricxscsci->base.traildir = NULL;
    xmetricxscsci->base.name = jobname;
    xmetricxscsci->base.remote = false;
    xmetricxscsci->base.stat = XMANAGER_METRICNODESTAT_ONLINE;
    return xmetricregnode;
}

/* capture/integrate */
static xmanager_metricregnode* xmanager_auth_identitycapture(xmanager_auth* xauth, uint8* uptr)
{
    int8                        result = 0;
    int                         ivalue = 0;
    int                         len = 0;
    char*                       value = NULL;
    xmanager_metricregnode*     xmetricregnode = NULL;
    xmanager_metricfd2node*     xmetricfd2node = NULL;
    xmanager_metriccapturenode* xmetriccapture = NULL;

    xmetricregnode = xmanager_metricregnode_init();
    if (NULL == xmetricregnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity regist node error, out of memory");
        return NULL;
    }
    xmetricregnode->result = 0;
    xmetricregnode->nodetype = XMANAGER_METRICNODETYPE_CAPTURE;

    xmetricfd2node = xmanager_metricfd2node_init();
    if (NULL == xmetricfd2node)
    {
        elog(RLOG_WARNING, "xmanager auth capture identity fd2node error, out of memeory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricregnode->metricfd2node = xmetricfd2node;

    xmetricfd2node->metricnode = xmanager_metricnode_init(xmetricregnode->nodetype);
    if (NULL == xmetricfd2node->metricnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity capture error, out of memory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetriccapture = (xmanager_metriccapturenode*)xmetricfd2node->metricnode;

    /* Name length */
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity capture jobname can not be zero");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* job name */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity capture jobname error");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetriccapture->base.name = value;

    /* Get command type */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    xmetricregnode->msgtype = ivalue;

    /* Get success or fail */
    rmemcpy1(&result, 0, uptr, 1);
    uptr += 1;

    if (1 == result)
    {
        /* Failed, get error info */
        /* Get total length */
        rmemcpy1(&len, 0, uptr, 4);
        len = r_ntoh32(len);
        uptr += 4;

        if (0 == len)
        {
            xmetricregnode->errcode = 0;
            xmetricregnode->msg = rmalloc0(128);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity capture errmsg error");
                xmanager_metricregnode_destroy(xmetricregnode);
                return NULL;
            }
            rmemset0(xmetricregnode->msg, 0, '\0', 128);
            snprintf(xmetricregnode->msg,
                     128,
                     "%s error",
                     xmanager_metricmsg_getdesc(xmetricregnode->msgtype));
        }
        else
        {
            /* Offset error code */
            rmemcpy1(&ivalue, 0, uptr, 4);
            ivalue = r_ntoh32(ivalue);
            uptr += 4;
            xmetricregnode->errcode = ivalue;

            len -= 4;

            /* Get error info */
            len += 1;
            xmetricregnode->msg = rmalloc0(len);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity capture errmsg error");
                xmanager_metricregnode_destroy(xmetricregnode);
                return NULL;
            }
            rmemset0(xmetricregnode->msg, 0, '\0', len);
            len -= 1;
            rmemcpy0(xmetricregnode->msg, 0, uptr, len);
        }

        xmetricregnode->result = 1;
        return xmetricregnode;
    }

    /*
     * data directory
     * 1. Get length
     * 2. Copy directory
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity capture data len can not be zero");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* Data content */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity capture data error, out of memory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetriccapture->base.data = value;
    return xmetricregnode;
}

/* integrate*/
static xmanager_metricregnode* xmanager_auth_identityintegrate(xmanager_auth* xauth, uint8* uptr)
{
    int8                          result = 0;
    int                           ivalue = 0;
    int                           len = 0;
    char*                         value = NULL;
    xmanager_metricregnode*       xmetricregnode = NULL;
    xmanager_metricfd2node*       xmetricfd2node = NULL;
    xmanager_metricintegratenode* xmetricintegrate = NULL;

    xmetricregnode = xmanager_metricregnode_init();
    if (NULL == xmetricregnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity regist node error, out of memory");
        return NULL;
    }
    xmetricregnode->result = 0;
    xmetricregnode->nodetype = XMANAGER_METRICNODETYPE_INTEGRATE;

    xmetricfd2node = xmanager_metricfd2node_init();
    if (NULL == xmetricfd2node)
    {
        elog(RLOG_WARNING, "xmanager auth integrate identity fd2node error, out of memeory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricregnode->metricfd2node = xmetricfd2node;

    xmetricfd2node->metricnode = xmanager_metricnode_init(xmetricregnode->nodetype);
    if (NULL == xmetricfd2node->metricnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity integrate error, out of memory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricintegrate = (xmanager_metricintegratenode*)xmetricfd2node->metricnode;

    /* Name length */
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity integrate jobname can not be zero");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* job name */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity integrate jobname error");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricintegrate->base.name = value;

    /* Get command type */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    xmetricregnode->msgtype = ivalue;

    /* Get success or fail */
    rmemcpy1(&result, 0, uptr, 1);
    uptr += 1;

    if (1 == result)
    {
        /* Failed, get error info */
        rmemcpy1(&len, 0, uptr, 4);
        len = r_ntoh32(len);
        uptr += 4;
        if (0 == len)
        {
            xmetricregnode->msg = rmalloc0(128);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity integrate errmsg error");
                xmanager_metricregnode_destroy(xmetricregnode);
                return NULL;
            }
            rmemset0(xmetricregnode->msg, 0, '\0', 128);
            snprintf(xmetricregnode->msg,
                     128,
                     "%s error",
                     xmanager_metricmsg_getdesc(xmetricregnode->msgtype));
        }
        else
        {
            /* Offset error code */
            rmemcpy1(&ivalue, 0, uptr, 4);
            ivalue = r_ntoh32(ivalue);
            uptr += 4;
            xmetricregnode->errcode = ivalue;
            len -= 4;

            len += 1;
            xmetricregnode->msg = rmalloc0(len);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity integrate errmsg error");
                xmanager_metricregnode_destroy(xmetricregnode);
                return NULL;
            }
            rmemset0(xmetricregnode->msg, 0, '\0', len);
            len -= 1;
            rmemcpy0(xmetricregnode->msg, 0, uptr, len);
        }

        xmetricregnode->result = 1;
        return xmetricregnode;
    }

    /*
     * data directory
     * 1. Get length
     * 2. Copy directory
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity integrate data len can not be zero");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* Data content */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity integrate data error, out of memory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricintegrate->base.data = value;

    /*
     * trail directory
     * 1. Get length
     * 2. Copy directory
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity integrate trail len can not be zero");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* Data content */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity integrate trail error, out of memory");
        xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricintegrate->base.traildir = value;
    return xmetricregnode;
}

/* Identity info */
static bool xmanager_auth_identity(xmanager_auth* xauth,
                                   netpoolentry*  npoolentry,
                                   netpacket*     npacket)
{
    int                     msglen = 0;
    int                     crc32 = 0;
    int                     msgtype = 0;
    int                     jobtype = 0;
    uint8*                  uptr = NULL;
    xmanager_metricregnode* metricregnode = NULL;

    /* Parse content */
    uptr = npacket->data;
    rmemcpy1(&msglen, 0, uptr, 4);
    uptr += 4;

    rmemcpy1(&crc32, 0, uptr, 4);
    uptr += 4;

    rmemcpy1(&msgtype, 0, uptr, 4);
    uptr += 4;
    msgtype = r_ntoh32(msgtype);

    /* Not identitycmd */
    if (XMANAGER_MSG_IDENTITYCMD != msgtype)
    {
        elog(RLOG_WARNING, "need identity command, but now msgtype:%d", msgtype);
        return false;
    }

    /* Business type */
    rmemcpy1(&jobtype, 0, uptr, 4);
    uptr += 4;
    jobtype = r_ntoh32(jobtype);

    switch (jobtype)
    {
        case XMANAGER_METRICNODETYPE_XSCSCI:
            metricregnode = xmanager_auth_identityxscsci(xauth, uptr);
            break;
        case XMANAGER_METRICNODETYPE_CAPTURE:
            metricregnode = xmanager_auth_identitycapture(xauth, uptr);
            break;
        case XMANAGER_METRICNODETYPE_INTEGRATE:
            metricregnode = xmanager_auth_identityintegrate(xauth, uptr);
            break;
        default:
            elog(RLOG_WARNING, "unknown jobtype, %d.", jobtype);
            break;
    }

    if (NULL == metricregnode)
    {
        elog(RLOG_WARNING, "auth identity node init error");
        xmanager_metricregnode_destroy(metricregnode);
        return false;
    }

    /* Add to queue */
    metricregnode->metricfd2node->fd = npoolentry->fd;
    npoolentry->fd = -1;
    queue_put(xauth->metricqueue, metricregnode);
    return true;
}

/* Process read queue */
static bool xmanager_auth_msg(xmanager_auth* xauth, int index)
{
    netpacket*    npacket = NULL;
    netpoolentry* npoolentry = NULL;

    npoolentry = xauth->npool->fds[index];
    while (1)
    {
        npacket = queue_tryget(npoolentry->rpackets);
        if (NULL == npacket)
        {
            return true;
        }

        if (npacket->offset != npacket->used)
        {
            if (false == queue_put(npoolentry->rpackets, npacket))
            {
                return false;
            }
            return true;
        }

        /* Parse data */
        if (false == xmanager_auth_identity(xauth, npoolentry, npacket))
        {
            netpacket_destroy(npacket);
            return false;
        }
        netpacket_destroy(npacket);

        /* In auth module only one data */
        break;
    }

    /* Process npoolentry */
    netpoolentry_destroy(npoolentry);
    xauth->npool->fds[index] = NULL;
    return true;
}

/* Main loop */
void* xmanager_auth_main(void* args)
{
    int                      fd = -1;
    int                      index = 0;
    int                      errorfdscnt = 0;
    int*                     errorfds = NULL;
    dlist*                   dlfd2timeout = NULL;
    thrnode*                 thrnode_ptr = NULL;
    queueitem*               item = NULL;
    queueitem*               items = NULL;
    xmanager_auth*           xauth = NULL;
    netpoolentry*            npoolentry = NULL;
    xmanager_authfd2timeout* fd2timeout = NULL;

    thrnode_ptr = (thrnode*)args;

    xauth = (xmanager_auth*)thrnode_ptr->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thrnode_ptr->stat)
    {
        elog(RLOG_WARNING, "xmanager auth stat exception, expected state is THRNODE_STAT_STARTING");
        thrnode_ptr->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set working state */
    thrnode_ptr->stat = THRNODE_STAT_WORK;

    while (1)
    {
        items = NULL;
        if (THRNODE_STAT_TERM == thrnode_ptr->stat)
        {
            thrnode_ptr->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get fd from queue */
        items = queue_trygetbatch(xauth->authqueue);
        for (item = items; NULL != item; item = items)
        {
            items = item->next;
            fd = (int)((uintptr_t)item->data);
            npoolentry = netpoolentry_init();
            if (NULL == npoolentry)
            {
                elog(RLOG_WARNING, "xmanager auth net pool entry error");
                close(fd);
                fd = -1;
                queueitem_free(item, NULL);
                continue;
            }
            netpoolentry_setfd(npoolentry, fd);

            fd2timeout = xmanager_authfd2timeout_init();
            if (NULL == fd2timeout)
            {
                elog(RLOG_WARNING, "xmanager auth auth fd2timeout out of memory");
                netpoolentry_destroy(npoolentry);
                queueitem_free(item, NULL);
                continue;
            }
            fd2timeout->fd = fd;
            dlfd2timeout = dlist_put(dlfd2timeout, fd2timeout);

            /* Add to queue */
            if (false == netpool_add(xauth->npool, npoolentry))
            {
                elog(RLOG_WARNING, "xmanager auth add entry to net pool error");
                dlist_deletebyvalue(dlfd2timeout,
                                    (void*)((uintptr_t)fd2timeout->fd),
                                    xmanager_authfd2timeout_cmp,
                                    xmanager_authfd2timeout_destroyvoid);

                netpoolentry_destroy(npoolentry);
                queueitem_free(item, NULL);
                continue;
            }
            queueitem_free(item, NULL);
        }

        /* Monitor */
        if (false == netpool_desc(xauth->npool, &errorfdscnt, &errorfds))
        {
            elog(RLOG_WARNING, "xmanager auth net pool desc error");
            thrnode_ptr->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Handle abnormal fds first */
        for (index = 0; index < errorfdscnt; index++)
        {
            dlist_deletebyvalue(dlfd2timeout,
                                (void*)((uintptr_t)errorfds[index]),
                                xmanager_authfd2timeout_cmp,
                                xmanager_authfd2timeout_destroyvoid);
        }

        /* Iterate read queue and process */
        for (index = 0; index < xauth->npool->fdcnt; index++)
        {
            if (NULL == xauth->npool->fds[index])
            {
                continue;
            }

            npoolentry = xauth->npool->fds[index];
            if (true == queue_isnull(npoolentry->rpackets))
            {
                continue;
            }

            fd = npoolentry->fd;
            if (false == xmanager_auth_msg(xauth, index))
            {
                elog(RLOG_WARNING, "xmanager auth deal identity msg error");
                dlist_deletebyvalue(dlfd2timeout,
                                    (void*)((uintptr_t)npoolentry->fd),
                                    xmanager_authfd2timeout_cmp,
                                    xmanager_authfd2timeout_destroyvoid);

                netpoolentry_destroy(npoolentry);
                xauth->npool->fds[index] = NULL;
                continue;
            }

            if (NULL == xauth->npool->fds[index])
            {
                dlist_deletebyvalue(dlfd2timeout,
                                    (void*)((uintptr_t)fd),
                                    xmanager_authfd2timeout_cmp,
                                    xmanager_authfd2timeout_destroyvoid);
            }
        }
    }

    dlist_free(dlfd2timeout, xmanager_authfd2timeout_destroyvoid);
    pthread_exit(NULL);
    return NULL;
}

void xmanager_auth_destroy(xmanager_auth* xauth)
{
    if (NULL == xauth)
    {
        return;
    }

    netpool_destroy(xauth->npool);
    rfree(xauth);
}
