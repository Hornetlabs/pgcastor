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
#include "net/ripple_netserver.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricasyncmsg.h"
#include "xmanager/ripple_xmanager_auth.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metricxscscinode.h"
#include "xmanager/ripple_xmanager_metriccapturenode.h"
#include "xmanager/ripple_xmanager_metricpumpnode.h"
#include "xmanager/ripple_xmanager_metriccollectornode.h"
#include "xmanager/ripple_xmanager_metricintegratenode.h"
#include "xmanager/ripple_xmanager_listen.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager_metricmsg.h"

typedef struct RIPPLE_XMANAGER_AUTHFD2TIMEOUT
{
    int                         fd;
    int                         timeout;
} ripple_xmanager_authfd2timeout;

static ripple_xmanager_authfd2timeout* ripple_xmanager_authfd2timeout_init(void)
{
    ripple_xmanager_authfd2timeout* fd2timeout = NULL;

    fd2timeout = rmalloc0(sizeof(ripple_xmanager_authfd2timeout));
    if (NULL == fd2timeout)
    {
        elog(RLOG_WARNING, "authfd2timeout init error");
        return NULL;
    }
    fd2timeout->fd = -1;
    fd2timeout->timeout = 0;
    return fd2timeout;
}

/* 比较 */
static int ripple_xmanager_authfd2timeout_cmp(void* s1, void* s2)
{
    int fd = -1;
    ripple_xmanager_authfd2timeout* fd2timeout = NULL;

    fd = (int)((uintptr_t)s1);
    fd2timeout = (ripple_xmanager_authfd2timeout*)s2;

    if (fd == fd2timeout->fd)
    {
        return 0;
    }
    return 1;
}

/* 销毁 */
static void ripple_xmanager_authfd2timeout_destroy(ripple_xmanager_authfd2timeout* fd2timeout)
{
    if (NULL == fd2timeout)
    {
        return;
    }

    rfree(fd2timeout);
}

static void ripple_xmanager_authfd2timeout_destroyvoid(void* args)
{
    ripple_xmanager_authfd2timeout_destroy((ripple_xmanager_authfd2timeout*)args);
}

ripple_xmanager_auth* ripple_xmanager_auth_init(void)
{
    ripple_xmanager_auth* xauth = NULL;

    xauth = rmalloc0(sizeof(ripple_xmanager_auth));
    if (NULL == xauth)
    {
        elog(RLOG_WARNING, "xmanager auth error");
        return NULL;
    }
    rmemset0(xauth, 0, '\0', sizeof(ripple_xmanager_auth));
    xauth->timeout = RIPPLE_XMANAGER_AUTH_DEFAULTTIMEOUT;
    xauth->no = 1;
    xauth->npool = ripple_netpool_init();
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
static ripple_xmanager_metricregnode* ripple_xmanager_auth_identityxscsci(ripple_xmanager_auth* xauth, uint8* uptr)
{
    int jobnamelen                                  = 0;
    char* jobname                                   = NULL;
    ripple_xmanager_metricregnode* xmetricregnode   = NULL;
    ripple_xmanager_metricfd2node* xmetricfd2node   = NULL;
    ripple_xmanager_metricxscscinode* xmetricxscsci = NULL;

    xmetricregnode = ripple_xmanager_metricregnode_init();
    if (NULL == xmetricregnode)
    {
        elog(RLOG_WARNING, "xmanager auth xscsci identity regist node error, out of memory");
        return NULL;
    }
    xmetricregnode->result = 0;
    xmetricregnode->nodetype = RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI;

    xmetricfd2node = ripple_xmanager_metricfd2node_init();
    if (NULL == xmetricfd2node)
    {
        elog(RLOG_WARNING, "xmanager auth xscsci identity fd2node error, out of memeory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricregnode->metricfd2node = xmetricfd2node;

    xmetricfd2node->metricnode = ripple_xmanager_metricnode_init(xmetricregnode->nodetype);
    if (NULL == xmetricfd2node->metricnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity xscsci error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricxscsci = (ripple_xmanager_metricxscscinode*)xmetricfd2node->metricnode;
    xmetricxscsci->number = xauth->no;
    xauth->no++;

    /* 名称长度 */
    rmemcpy1(&jobnamelen, 0, uptr, 4);
    uptr += 4;
    jobnamelen = r_ntoh32(jobnamelen);

    /* 名称 */
    if (0 != jobnamelen)
    {
        jobnamelen += 1;
        jobname = rmalloc0(jobnamelen);
        if (NULL == jobname)
        {
            elog(RLOG_WARNING, "auth identity jobname error");
            ripple_xmanager_metricregnode_destroy(xmetricregnode);
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
    xmetricxscsci->base.stat = RIPPLE_XMANAGER_METRICNODESTAT_ONLINE;
    return xmetricregnode;
}

/* capture/pump/collector/integrate */
static ripple_xmanager_metricregnode* ripple_xmanager_auth_identitycapture(ripple_xmanager_auth* xauth, uint8* uptr)
{
    int8 result                                         = 0;
    int ivalue                                          = 0;
    int len                                             = 0;
    char* value                                         = NULL;
    ripple_xmanager_metricregnode* xmetricregnode       = NULL;
    ripple_xmanager_metricfd2node* xmetricfd2node       = NULL;
    ripple_xmanager_metriccapturenode* xmetriccapture   = NULL;

    xmetricregnode = ripple_xmanager_metricregnode_init();
    if (NULL == xmetricregnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity regist node error, out of memory");
        return NULL;
    }
    xmetricregnode->result = 0;
    xmetricregnode->nodetype = RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE;

    xmetricfd2node = ripple_xmanager_metricfd2node_init();
    if (NULL == xmetricfd2node)
    {
        elog(RLOG_WARNING, "xmanager auth capture identity fd2node error, out of memeory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricregnode->metricfd2node = xmetricfd2node;

    xmetricfd2node->metricnode = ripple_xmanager_metricnode_init(xmetricregnode->nodetype);
    if (NULL == xmetricfd2node->metricnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity capture error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetriccapture = (ripple_xmanager_metriccapturenode*)xmetricfd2node->metricnode;

    /* 名称长度 */
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity capture jobname can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* job name */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity capture jobname error");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetriccapture->base.name = value;

    /* 获取 command type */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    xmetricregnode->msgtype = ivalue;

    /* 获取成功/失败 */
    rmemcpy1(&result, 0, uptr, 1);
    uptr += 1;

    if (1 == result)
    {
        /* 失败了, 那么获取错误信息 */
        /* 获取总长度 */
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
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
                return NULL;
            }
            rmemset0(xmetricregnode->msg, 0, '\0', 128);
            snprintf(xmetricregnode->msg, 128, "%s error", ripple_xmanager_metricmsg_getdesc(xmetricregnode->msgtype));
        }
        else
        {
            /* 偏移错误码 */
            rmemcpy1(&ivalue, 0, uptr, 4);
            ivalue = r_ntoh32(ivalue);
            uptr += 4;
            xmetricregnode->errcode = ivalue;

            len -= 4;

            /* 获取错误信息 */
            len += 1;
            xmetricregnode->msg = rmalloc0(len);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity capture errmsg error");
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
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
     * data 目录
     * 1、获取长度
     * 2、复制目录
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity capture data len can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* 数据内容 */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity capture data error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetriccapture->base.data = value;
    return xmetricregnode;
}

/* pump */
static ripple_xmanager_metricregnode* ripple_xmanager_auth_identitypump(ripple_xmanager_auth* xauth, uint8* uptr)
{
    int8 result                                         = 0;
    int ivalue                                          = 0;
    int len                                             = 0;
    char* value                                         = NULL;
    ripple_xmanager_metricregnode* xmetricregnode       = NULL;
    ripple_xmanager_metricfd2node* xmetricfd2node       = NULL;
    ripple_xmanager_metricpumpnode* xmetricpump         = NULL;

    xmetricregnode = ripple_xmanager_metricregnode_init();
    if (NULL == xmetricregnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity regist node error, out of memory");
        return NULL;
    }
    xmetricregnode->result = 0;
    xmetricregnode->nodetype = RIPPLE_XMANAGER_METRICNODETYPE_PUMP;

    xmetricfd2node = ripple_xmanager_metricfd2node_init();
    if (NULL == xmetricfd2node)
    {
        elog(RLOG_WARNING, "xmanager auth pump identity fd2node error, out of memeory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricregnode->metricfd2node = xmetricfd2node;

    xmetricfd2node->metricnode = ripple_xmanager_metricnode_init(xmetricregnode->nodetype);
    if (NULL == xmetricfd2node->metricnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity pump error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricpump = (ripple_xmanager_metricpumpnode*)xmetricfd2node->metricnode;

    /* 名称长度 */
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity pump jobname can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* job name */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity pump jobname error");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricpump->base.name = value;

    /* 获取 command type */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    xmetricregnode->msgtype = ivalue;

    /* 获取成功/失败 */
    rmemcpy1(&result, 0, uptr, 1);
    uptr += 1;

    if (1 == result)
    {
        /* 失败了, 那么获取错误信息 */
        rmemcpy1(&len, 0, uptr, 4);
        len = r_ntoh32(len);
        uptr += 4;
        if (0 == len)
        {
            xmetricregnode->msg = rmalloc0(128);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity pump errmsg error");
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
                return NULL;
            }
            rmemset0(xmetricregnode->msg, 0, '\0', 128);
            snprintf(xmetricregnode->msg, 128, "%s error", ripple_xmanager_metricmsg_getdesc(xmetricregnode->msgtype));
        }
        else
        {
            /* 偏移错误码 */
            rmemcpy1(&ivalue, 0, uptr, 4);
            ivalue = r_ntoh32(ivalue);
            uptr += 4;
            xmetricregnode->errcode = ivalue;
            len -= 4;

            len += 1;
            xmetricregnode->msg = rmalloc0(len);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity pump errmsg error");
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
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
     * data 目录
     * 1、获取长度
     * 2、复制目录
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity pump data len can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* 数据内容 */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity pump data error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricpump->base.data = value;

    /* 
     * trail 目录
     * 1、获取长度
     * 2、复制目录
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity pump trail len can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* 数据内容 */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity pump trail error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricpump->base.traildir = value;
    return xmetricregnode;
}

/* collector*/
static ripple_xmanager_metricregnode* ripple_xmanager_auth_identitycollector(ripple_xmanager_auth* xauth, uint8* uptr)
{
    int8 result                                             = 0;
    int ivalue                                              = 0;
    int len                                                 = 0;
    char* value                                             = NULL;
    ripple_xmanager_metricregnode* xmetricregnode           = NULL;
    ripple_xmanager_metricfd2node* xmetricfd2node           = NULL;
    ripple_xmanager_metriccollectornode* xmetriccollector   = NULL;

    xmetricregnode = ripple_xmanager_metricregnode_init();
    if (NULL == xmetricregnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity regist node error, out of memory");
        return NULL;
    }
    xmetricregnode->result = 0;
    xmetricregnode->nodetype = RIPPLE_XMANAGER_METRICNODETYPE_COLLECTOR;

    xmetricfd2node = ripple_xmanager_metricfd2node_init();
    if (NULL == xmetricfd2node)
    {
        elog(RLOG_WARNING, "xmanager auth collector identity fd2node error, out of memeory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricregnode->metricfd2node = xmetricfd2node;

    xmetricfd2node->metricnode = ripple_xmanager_metricnode_init(xmetricregnode->nodetype);
    if (NULL == xmetricfd2node->metricnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity collector error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetriccollector = (ripple_xmanager_metriccollectornode*)xmetricfd2node->metricnode;

    /* 名称长度 */
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity collector jobname can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* job name */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity collector jobname error");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetriccollector->base.name = value;

    /* 获取 command type */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    xmetricregnode->msgtype = ivalue;

    /* 获取成功/失败 */
    rmemcpy1(&result, 0, uptr, 1);
    uptr += 1;

    if (1 == result)
    {
        /* 失败了, 那么获取错误信息 */
        rmemcpy1(&len, 0, uptr, 4);
        len = r_ntoh32(len);
        uptr += 4;
        if (0 == len)
        {
            xmetricregnode->msg = rmalloc0(128);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity collector errmsg error");
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
                return NULL;
            }
            rmemset0(xmetricregnode->msg, 0, '\0', 128);
            snprintf(xmetricregnode->msg, 128, "%s error", ripple_xmanager_metricmsg_getdesc(xmetricregnode->msgtype));
        }
        else
        {
            /* 偏移错误码 */
            rmemcpy1(&ivalue, 0, uptr, 4);
            ivalue = r_ntoh32(ivalue);
            uptr += 4;
            xmetricregnode->errcode = ivalue;
            len -= 4;

            len += 1;
            xmetricregnode->msg = rmalloc0(len);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity collector errmsg error");
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
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
     * data 目录
     * 1、获取长度
     * 2、复制目录
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity collector data len can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* 数据内容 */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity collector data error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetriccollector->base.data = value;
    return xmetricregnode;
}

/* integrate*/
static ripple_xmanager_metricregnode* ripple_xmanager_auth_identityintegrate(ripple_xmanager_auth* xauth, uint8* uptr)
{
    int8 result                                             = 0;
    int ivalue                                              = 0;
    int len                                                 = 0;
    char* value                                             = NULL;
    ripple_xmanager_metricregnode* xmetricregnode           = NULL;
    ripple_xmanager_metricfd2node* xmetricfd2node           = NULL;
    ripple_xmanager_metricintegratenode* xmetricintegrate   = NULL;

    xmetricregnode = ripple_xmanager_metricregnode_init();
    if (NULL == xmetricregnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity regist node error, out of memory");
        return NULL;
    }
    xmetricregnode->result = 0;
    xmetricregnode->nodetype = RIPPLE_XMANAGER_METRICNODETYPE_INTEGRATE;

    xmetricfd2node = ripple_xmanager_metricfd2node_init();
    if (NULL == xmetricfd2node)
    {
        elog(RLOG_WARNING, "xmanager auth integrate identity fd2node error, out of memeory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricregnode->metricfd2node = xmetricfd2node;

    xmetricfd2node->metricnode = ripple_xmanager_metricnode_init(xmetricregnode->nodetype);
    if (NULL == xmetricfd2node->metricnode)
    {
        elog(RLOG_WARNING, "xmanager auth identity integrate error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    xmetricintegrate = (ripple_xmanager_metricintegratenode*)xmetricfd2node->metricnode;

    /* 名称长度 */
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity integrate jobname can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* job name */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity integrate jobname error");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricintegrate->base.name = value;

    /* 获取 command type */
    rmemcpy1(&ivalue, 0, uptr, 4);
    ivalue = r_ntoh32(ivalue);
    uptr += 4;
    xmetricregnode->msgtype = ivalue;

    /* 获取成功/失败 */
    rmemcpy1(&result, 0, uptr, 1);
    uptr += 1;

    if (1 == result)
    {
        /* 失败了, 那么获取错误信息 */
        rmemcpy1(&len, 0, uptr, 4);
        len = r_ntoh32(len);
        uptr += 4;
        if (0 == len)
        {
            xmetricregnode->msg = rmalloc0(128);
            if (NULL == xmetricregnode->msg)
            {
                elog(RLOG_WARNING, "auth identity integrate errmsg error");
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
                return NULL;
            }
            rmemset0(xmetricregnode->msg, 0, '\0', 128);
            snprintf(xmetricregnode->msg, 128, "%s error", ripple_xmanager_metricmsg_getdesc(xmetricregnode->msgtype));
        }
        else
        {
            /* 偏移错误码 */
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
                ripple_xmanager_metricregnode_destroy(xmetricregnode);
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
     * data 目录
     * 1、获取长度
     * 2、复制目录
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity integrate data len can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* 数据内容 */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity integrate data error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricintegrate->base.data = value;

    /* 
     * trail 目录
     * 1、获取长度
     * 2、复制目录
     */
    len = 0;
    value = NULL;
    rmemcpy1(&len, 0, uptr, 4);
    uptr += 4;
    len = r_ntoh32(len);
    if (0 == len)
    {
        elog(RLOG_WARNING, "auth identity integrate trail len can not be zero");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }

    /* 数据内容 */
    len += 1;
    value = rmalloc0(len);
    if (NULL == value)
    {
        elog(RLOG_WARNING, "auth identity integrate trail error, out of memory");
        ripple_xmanager_metricregnode_destroy(xmetricregnode);
        return NULL;
    }
    rmemset0(value, 0, '\0', len);
    len -= 1;
    rmemcpy0(value, 0, uptr, len);
    uptr += len;
    xmetricintegrate->base.traildir = value;
    return xmetricregnode;
}

/* 身份信息 */
static bool ripple_xmanager_auth_identity(ripple_xmanager_auth* xauth, ripple_netpoolentry* npoolentry, ripple_netpacket* npacket)
{
    int msglen                                      = 0;
    int crc32                                       = 0;
    int msgtype                                     = 0;
    int jobtype                                     = 0;
    uint8* uptr                                     = NULL;
    ripple_xmanager_metricregnode* metricregnode    = NULL;

    /* 拆解内容 */
    uptr = npacket->data;
    rmemcpy1(&msglen, 0, uptr, 4);
    uptr += 4;

    rmemcpy1(&crc32, 0, uptr, 4);
    uptr += 4;

    rmemcpy1(&msgtype, 0, uptr, 4);
    uptr += 4;
    msgtype = r_ntoh32(msgtype);

    /* 不是 identitycmd */
    if (RIPPLE_XMANAGER_MSG_IDENTITYCMD != msgtype)
    {
        elog(RLOG_WARNING, "need identity command, but now msgtype:%d", msgtype);
        return false;
    }

    /* 业务类型 */
    rmemcpy1(&jobtype, 0, uptr, 4);
    uptr += 4;
    jobtype = r_ntoh32(jobtype);

    switch (jobtype)
    {
        case RIPPLE_XMANAGER_METRICNODETYPE_XSCSCI:
            metricregnode = ripple_xmanager_auth_identityxscsci(xauth, uptr);
            break;
        case RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE:
            metricregnode = ripple_xmanager_auth_identitycapture(xauth, uptr);
            break;
        case RIPPLE_XMANAGER_METRICNODETYPE_PUMP:
            metricregnode = ripple_xmanager_auth_identitypump(xauth, uptr);
            break;
        case RIPPLE_XMANAGER_METRICNODETYPE_COLLECTOR:
            metricregnode = ripple_xmanager_auth_identitycollector(xauth, uptr);
            break;
        case RIPPLE_XMANAGER_METRICNODETYPE_INTEGRATE:
            metricregnode = ripple_xmanager_auth_identityintegrate(xauth, uptr);
            break;
        default:
            elog(RLOG_WARNING, "unknown jobtype, %d.", jobtype);
            break;
    }

    if (NULL == metricregnode)
    {
        elog(RLOG_WARNING, "auth identity node init error");
        ripple_xmanager_metricregnode_destroy(metricregnode);
        return false;
    }

    /* 加入到队列中 */
    metricregnode->metricfd2node->fd = npoolentry->fd;
    npoolentry->fd = -1;
    ripple_queue_put(xauth->metricqueue, metricregnode);
    return true;
}

/* 处理读队列 */
static bool ripple_xmanager_auth_msg(ripple_xmanager_auth* xauth, int index)
{
    ripple_netpacket* npacket = NULL;
    ripple_netpoolentry* npoolentry = NULL;

    npoolentry = xauth->npool->fds[index];
    while(1)
    {
        npacket = ripple_queue_tryget(npoolentry->rpackets);
        if (NULL == npacket)
        {
            return true;
        }

        if (npacket->offset != npacket->used)
        {
            if (false == ripple_queue_put(npoolentry->rpackets, npacket))
            {
                return false;
            }
            return true;
        }

        /* 解析数据 */
        if (false == ripple_xmanager_auth_identity(xauth, npoolentry, npacket))
        {
            ripple_netpacket_destroy(npacket);
            return false;
        }
        ripple_netpacket_destroy(npacket);

        /* 在 auth 模块只会有一条数据 */
        break;
    }

    /* 处理 npoolentry */
    ripple_netpoolentry_destroy(npoolentry);
    xauth->npool->fds[index] = NULL;
    return true;
}


/* 主流程 */
void* ripple_xmanager_auth_main(void *args)
{
    int fd                                      = -1;
    int index                                   = 0;
    int errorfdscnt                             = 0;
    int* errorfds                               = NULL;
    dlist* dlfd2timeout                         = NULL;
    ripple_thrnode* thrnode                     = NULL;
    ripple_queueitem* item                      = NULL;
    ripple_queueitem* items                     = NULL;
    ripple_xmanager_auth* xauth                 = NULL;
    ripple_netpoolentry* npoolentry             = NULL;
    ripple_xmanager_authfd2timeout* fd2timeout  = NULL;

    thrnode = (ripple_thrnode*)args;

    xauth = (ripple_xmanager_auth*)thrnode->data;

    /* 查看状态 */
    if (RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "xmanager auth stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    while(1)
    {
        items = NULL;
        if (RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 在队列中获取描述符 */
        items = ripple_queue_trygetbatch(xauth->authqueue);
        for (item = items; NULL != item; item = items)
        {
            items = item->next;
            fd = (int)((uintptr_t)item->data);
            npoolentry = ripple_netpoolentry_init();
            if (NULL == npoolentry)
            {
                elog(RLOG_WARNING, "xmanager auth net pool entry error");
                ripple_close(fd);
                fd = -1;
                ripple_queueitem_free(item, NULL);
                continue;
            }
            ripple_netpoolentry_setfd(npoolentry, fd);

            fd2timeout = ripple_xmanager_authfd2timeout_init();
            if (NULL == fd2timeout)
            {
                elog(RLOG_WARNING, "xmanager auth auth fd2timeout out of memory");
                ripple_netpoolentry_destroy(npoolentry);
                ripple_queueitem_free(item, NULL);
                continue;
            }
            fd2timeout->fd = fd;
            dlfd2timeout = dlist_put(dlfd2timeout, fd2timeout);

            /* 加入到队列中 */
            if (false == ripple_netpool_add(xauth->npool, npoolentry))
            {
                elog(RLOG_WARNING, "xmanager auth add entry to net pool error");
                dlist_deletebyvalue(dlfd2timeout,
                                    (void*)((uintptr_t)fd2timeout->fd),
                                    ripple_xmanager_authfd2timeout_cmp,
                                    ripple_xmanager_authfd2timeout_destroyvoid);

                ripple_netpoolentry_destroy(npoolentry);
                ripple_queueitem_free(item, NULL);
                continue;
            }
            ripple_queueitem_free(item, NULL);
        }

        /* 监听 */
        if (false == ripple_netpool_desc(xauth->npool, &errorfdscnt, &errorfds))
        {
            elog(RLOG_WARNING, "xmanager auth net pool desc error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 先处理异常的描述符 */
        for (index = 0; index < errorfdscnt; index++)
        {
            dlist_deletebyvalue(dlfd2timeout,
                                (void*)((uintptr_t)errorfds[index]),
                                ripple_xmanager_authfd2timeout_cmp,
                                ripple_xmanager_authfd2timeout_destroyvoid);
        }

        /* 遍历读队列并处理 */
        for (index = 0; index < xauth->npool->fdcnt; index++)
        {
            if (NULL == xauth->npool->fds[index])
            {
                continue;
            }

            npoolentry = xauth->npool->fds[index];
            if (true == ripple_queue_isnull(npoolentry->rpackets))
            {
                continue;
            }

            fd = npoolentry->fd;
            if (false == ripple_xmanager_auth_msg(xauth, index))
            {
                elog(RLOG_WARNING, "xmanager auth deal identity msg error");
                dlist_deletebyvalue(dlfd2timeout,
                                    (void*)((uintptr_t)npoolentry->fd),
                                    ripple_xmanager_authfd2timeout_cmp,
                                    ripple_xmanager_authfd2timeout_destroyvoid);
                
                ripple_netpoolentry_destroy(npoolentry);
                xauth->npool->fds[index] = NULL;
                continue;
            }

            if (NULL == xauth->npool->fds[index])
            {
                dlist_deletebyvalue(dlfd2timeout,
                                    (void*)((uintptr_t)fd),
                                    ripple_xmanager_authfd2timeout_cmp,
                                    ripple_xmanager_authfd2timeout_destroyvoid);
            }
        }
    }

    dlist_free(dlfd2timeout, ripple_xmanager_authfd2timeout_destroyvoid);
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_xmanager_auth_destroy(ripple_xmanager_auth* xauth)
{
    if (NULL == xauth)
    {
        return;
    }

    ripple_netpool_destroy(xauth->npool);
    rfree(xauth);
}
