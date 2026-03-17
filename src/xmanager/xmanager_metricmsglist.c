#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/string/stringinfo.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netiomp/netiomp_poll.h"
#include "net/netpacket/netpacket.h"
#include "net/netpool.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"
#include "xmanager/xmanager_metriccapturenode.h"
#include "xmanager/xmanager_metricintegratenode.h"
#include "xmanager/xmanager_metric.h"
#include "xmanager/xmanager_metricmsglist.h"
#include "xmanager/xmanager_metricmsg.h"
#include "xmanager/xmanager_metricprogressnode.h"

typedef struct XMANAGER_METRICNODESTAT_NAME
{
    xmanager_metricnodestat      stat;      /* 状态 */
    char                                *name;      /* 状态名称 */
} xmanager_metricnodestat_name;

static xmanager_metricnodestat_name metricnodestat_name[] =
{
    { XMANAGER_METRICNODESTAT_NOP,       "NOP" },
    { XMANAGER_METRICNODESTAT_INIT,      "INIT" },
    { XMANAGER_METRICNODESTAT_ONLINE,    "ONLINE" },
    { XMANAGER_METRICNODESTAT_OFFLINE,   "OFFLIN" },
    { XMANAGER_METRICNODESTAT_MAX,       "MAX" },
};


typedef struct XMANAGER_METRICNODETYPE_NAME
{
    xmanager_metricnodetype      stat;      /* 状态 */
    char                                *name;      /* 状态名称 */
} xmanager_metricnodetype_name;

static xmanager_metricnodestat_name metricnodetype_name[] =
{
    { XMANAGER_METRICNODETYPE_NOP,           "NOP" },
    { XMANAGER_METRICNODETYPE_CAPTURE,       "CAPTURE" },
    { XMANAGER_METRICNODETYPE_INTEGRATE,     "INTEGRATE" },
    { XMANAGER_METRICNODETYPE_PGRECEIVELOG,  "PGRECEIVELOG" },
    { XMANAGER_METRICNODETYPE_PROCESS,       "PROGRESS" }
};

/* list 命令处理 */
static bool xmanager_metricmsg_assemblelist(xmanager_metric* xmetric,
                                                   netpoolentry* npoolentry)
{
    uint8 u8value                                           = 0;
    uint16 u16value                                         = 0;
    int rowlen                                              = 0;
    int msglen                                              = 0;
    int ivalue                                              = 0;
    int valuelen                                            = 0;
    int nodecnt                                             = 0;
    int maxoptionlen                                        = 0;
    uint8* nullmap                                          = NULL;
    uint8* rowuptr                                          = NULL;
    uint8* uptr                                             = NULL;
    dlistnode* opdlnode                                     = NULL;
    dlistnode* dlnode                                       = NULL;
    StringInfo option                                       = NULL;
    netpacket* npacket                               = NULL;
    xmanager_metricnode* pxmetricnode                = NULL;

    /* 4 总长度 + 4 crc32 + 4 msgtype + 1 成功/失败 + 4 rowcnt */
    msglen =( 4 + 4 + 4 + 1 + 4);

    /* 第一行 name + state + type + option */
    msglen += (4 + strlen("state") + 4 + strlen("name") + 4 + strlen("state")+ 4 + strlen("option"));

    /* 计算option长度，和node个数 */
    for (dlnode = xmetric->metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        pxmetricnode = (xmanager_metricnode*)dlnode->value;
        if (XMANAGER_METRICNODETYPE_PROCESS < pxmetricnode->type)
        {
            continue;
        }

        if(XMANAGER_METRICNODETYPE_PROCESS == pxmetricnode->type)
        {
            xmanager_metricprogressnode* xmetricprogressnode = NULL;
            xmetricprogressnode = (xmanager_metricprogressnode*)pxmetricnode;
            if (false == dlist_isnull(xmetricprogressnode->progressjop))
            {
                /* 类型 16 + jobname 128 */
                ivalue = ((16 + 128) * xmetricprogressnode->progressjop->length);
                if (ivalue > maxoptionlen)
                {
                    maxoptionlen = ivalue;
                }
            }
        }
        else
        {
            if (NULL != pxmetricnode->traildir && '\0' != pxmetricnode->traildir[0])
            {
                ivalue = strlen(pxmetricnode->traildir);
                ivalue += strlen("trail:");
                if (ivalue > maxoptionlen)
                {
                    maxoptionlen = ivalue;
                }
            }
        }
        nodecnt ++;
    }

    if (0 == nodecnt)
    {
        elog(RLOG_WARNING, "xmanager metric not find valid node");
        return false;
    }

    /* rowlen 4 + nullmapcnt 2 + nullmap 1 */
    rowlen += (4 + 2 + 1);

    /* type */
    rowlen += 4;
    rowlen += 32;

    /* name */
    rowlen += 4;
    rowlen += 128;

    /* state */
    rowlen += 4;
    rowlen += 32;

    /* option */
    rowlen += 4;
    rowlen += maxoptionlen;

    msglen += (rowlen * nodecnt);

    /* 申请空间 */
    npacket = netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info list msg out of memory");
        return false;
    }
    msglen += 1;
    npacket->data = netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble info list msg data, out of memory");
        netpacket_destroy(npacket);
        return false;
    }
    msglen -= 1;

    /* 组装数据 */
    uptr = npacket->data;

    /* 数据总长度 */
    uptr += 4;
    npacket->used += 4;

    /* crc32 */
    uptr += 4;
    npacket->used += 4;

    /* 类型 */
    ivalue = XMANAGER_MSG_LISTCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    /* 类型成功标识 */
    u8value = 0;
    rmemcpy1(uptr, 0, &u8value, 1);
    uptr += 1;
    npacket->used += 1;

    /* rowcnt */
    ivalue = (nodecnt + 1);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    npacket->used += 4;

    rowlen = 0;
    rowuptr = uptr;

    /* 偏过行长度 */
    uptr += 4;
    rowlen = 4;
    npacket->used += 4;

    /* 列头组装 */
    /* type len */
    ivalue = strlen("type") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* type */
    ivalue = strlen("type");
    rmemcpy1(uptr, 0, "type", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* name len */
    ivalue = strlen("name") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* name */
    ivalue = strlen("name");
    rmemcpy1(uptr, 0, "name", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* state len */
    ivalue = strlen("state") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* state */
    ivalue = strlen("state");
    rmemcpy1(uptr, 0, "state", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* option len */
    ivalue = strlen("option") ;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;
    rowlen += 4;
    npacket->used += 4;

    /* option */
    ivalue = strlen("option");
    rmemcpy1(uptr, 0, "option", ivalue);
    uptr += ivalue;
    rowlen += ivalue;
    npacket->used += ivalue;

    /* 行总长度 */
    rowlen = r_hton32(rowlen);
    rmemcpy1(rowuptr, 0, &rowlen, 4);

    option = makeStringInfo();

    for (dlnode = xmetric->metricnodes->head; NULL != dlnode; dlnode = dlnode->next)
    {
        pxmetricnode = (xmanager_metricnode*)dlnode->value;
        if (XMANAGER_METRICNODETYPE_PROCESS < pxmetricnode->type)
        {
            continue;
        }
        resetStringInfo(option);

        rowlen = 0;
        rowuptr = uptr;

        /* 跳过行长度 */
        uptr += 4;
        rowlen = 4;
        npacket->used += 4;

        /* 空列map的个数 */
        u16value = 1;
        u16value = r_hton16(u16value);
        rmemcpy1(uptr, 0, &u16value, 2);
        uptr += 2;
        rowlen += 2;
        npacket->used += 2;

        /* 空列 map */
        u16value = 1;
        uptr += u16value;
        rowlen += u16value;
        npacket->used += 1;

        nullmap = rmalloc0(u16value);
        if (NULL == nullmap)
        {
            elog(RLOG_WARNING, "xmanager metric assemble info list nullmap, out of memory");
            deleteStringInfo(option);
            netpacket_destroy(npacket);
            return false;
        }
        rmemset0(nullmap, 0, 0, u16value);

        /* type len */
        valuelen = strlen(metricnodetype_name[pxmetricnode->type].name);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* type */
        rmemcpy1(uptr, 0, metricnodetype_name[pxmetricnode->type].name, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* name len */
        valuelen = strlen(pxmetricnode->name);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* name */
        rmemcpy1(uptr, 0, pxmetricnode->name, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        /* state len */
        valuelen = strlen(metricnodestat_name[pxmetricnode->stat].name);
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;

        /* state */
        rmemcpy1(uptr, 0, metricnodestat_name[pxmetricnode->stat].name, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;

        if(XMANAGER_METRICNODETYPE_PROCESS == pxmetricnode->type)
        {
            xmanager_metricnode* metricnode                  = NULL;
            xmanager_metricprogressnode* xmetricprogressnode = NULL;

            xmetricprogressnode = (xmanager_metricprogressnode*)pxmetricnode;

            opdlnode = xmetricprogressnode->progressjop ? xmetricprogressnode->progressjop->head : NULL;
            while (opdlnode != NULL)
            {
                metricnode = (xmanager_metricnode*)opdlnode->value;
                if (XMANAGER_METRICNODETYPE_CAPTURE == metricnode->type)
                {
                    appendStringInfo(option, "capture %s", metricnode->name);
                }
                else if (XMANAGER_METRICNODETYPE_INTEGRATE == metricnode->type)
                {
                    appendStringInfo(option, "integrate %s", metricnode->name);
                }
                else
                {
                    opdlnode = opdlnode->next;
                    continue;
                }

                if (NULL != opdlnode->next)
                {
                    appendStringInfo(option, ", ");
                }

                opdlnode = opdlnode->next;
            }
        }
        else
        {
            if (NULL != pxmetricnode->traildir && '\0' != pxmetricnode->traildir[0])
            {
                appendStringInfo(option, "trail:%s", pxmetricnode->traildir);
            }
        }

        if (0 == option->len)
        {
            nullmap[3 / 8] |= (1U << (3 % 8));
            /* 行总长度 */
            rowlen = r_hton32(rowlen);
            rmemcpy1(rowuptr, 0, &rowlen, 4);
            rowuptr += 4;

            rowuptr += 2;
            rmemcpy1(rowuptr, 0, nullmap, u16value);

            rfree(nullmap);
            nullmap = NULL;
            continue;
        }

        /* option len */
        valuelen = option->len;
        ivalue = valuelen;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        rowlen += 4;
        npacket->used += 4;
        npacket->used += 4;

        /* option */
        rmemcpy1(uptr, 0, option->data, valuelen);
        uptr += valuelen;
        rowlen += valuelen;
        npacket->used += valuelen;
        npacket->used += valuelen;

        /* 行总长度 */
        rowlen = r_hton32(rowlen);
        rmemcpy1(rowuptr, 0, &rowlen, 4);
        rowuptr += 4;

        rowuptr += 2;
        rmemcpy1(rowuptr, 0, nullmap, u16value);

        rfree(nullmap);
        nullmap = NULL;
    }

    /* 数据总长度 */
    ivalue = npacket->used;
    ivalue = r_hton32(ivalue);
    rmemcpy1(npacket->data, 0, &ivalue, 4);
    deleteStringInfo(option);

    /* 将 netpacket 挂载到待发送队列中 */
    if (false == queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble edit msg add message to queue error");
        netpacket_destroy(npacket);
        return false;
    }

    return true;
}

/*
 * 处理 list 命令
 *  1、返回metricnode信息
*/
bool xmanager_metricmsg_parselist(xmanager_metric* xmetric,
                                         netpoolentry* npoolentry,
                                         netpacket* npacket)
{
    /* 错误码 */
    int errcode                                         = 0;
    int commandtype                                     = 0;
    uint8* uptr                                         = NULL;
    char errormsg[512]                                  = { 0 };

    /* 获取作业类型 */
    uptr = npacket->data;

    /* msglen + crc32 */
    uptr += 8;

    /* jobtype */
    rmemcpy1(&commandtype, 0, uptr, 4);
    commandtype = r_ntoh32(commandtype);
    uptr += 4;

    if (true == dlist_isnull(xmetric->metricnodes))
    {
        errcode = ERROR_NOENT;
        snprintf(errormsg, 2048, "ERROR: metricnodes is null.");
        goto xmanager_metricmsg_parselist_error;
    }

    /* 组装返回信息 */
    if(false == xmanager_metricmsg_assemblelist(xmetric, npoolentry))
    {
        errcode = ERROR_OOM;
        snprintf(errormsg, 2048, "ERROR: does not assemble list result.");
        goto xmanager_metricmsg_parselist_error;
    }

    return true;

xmanager_metricmsg_parselist_error:

    elog(RLOG_WARNING, errormsg);
    return xmanager_metricmsg_assembleerrormsg(xmetric,
                                                      npoolentry->wpackets,
                                                      XMANAGER_MSG_LISTCMD,
                                                      errcode,
                                                      errormsg);
}
