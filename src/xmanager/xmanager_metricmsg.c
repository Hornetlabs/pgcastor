#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "port/net/ripple_net.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "command/ripple_cmd.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netiomp/ripple_netiomp_poll.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netpool.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricnode.h"
#include "xmanager/ripple_xmanager_metriccapturenode.h"
#include "xmanager/ripple_xmanager_metricintegratenode.h"
#include "xmanager/ripple_xmanager_metric.h"
#include "xmanager/ripple_xmanager_metricmsg.h"
#include "xmanager/ripple_xmanager_metricmsgcreate.h"
#include "xmanager/ripple_xmanager_metricmsginit.h"
#include "xmanager/ripple_xmanager_metricmsgstart.h"
#include "xmanager/ripple_xmanager_metricmsgstop.h"
#include "xmanager/ripple_xmanager_metricmsginfo.h"
#include "xmanager/ripple_xmanager_metricmsgdrop.h"
#include "xmanager/ripple_xmanager_metricmsgedit.h"
#include "xmanager/ripple_xmanager_metricmsgremove.h"
#include "xmanager/ripple_xmanager_metricmsgwatch.h"
#include "xmanager/ripple_xmanager_metricmsgconffile.h"
#include "xmanager/ripple_xmanager_metricmsgrefresh.h"
#include "xmanager/ripple_xmanager_metricmsglist.h"
#include "xmanager/ripple_xmanager_metricmsgalter.h"
#include "xmanager/ripple_xmanager_metricmsgcapturerefresh.h"

typedef struct RIPPLE_XMANAGER_METRICMSGOP
{
    ripple_xmanager_msg                             type;
    char*                                           desc;
    bool (*assemble)(ripple_xmanager_metric* xmetric, ripple_netpoolentry* npoolentry, dlist* dlmsgs);
    bool (*parse)(ripple_xmanager_metric* xmetric, ripple_netpoolentry* npoolentry, ripple_netpacket* npacket);
} ripple_xmanager_metricmsgop;

/* 组装 identity 返回信息 */
bool ripple_xmanager_metricmsg_assemblecmdresult(ripple_xmanager_metric* xmetric,
                                                 ripple_netpoolentry* npoolentry,
                                                 ripple_xmanager_msg msgtype)
{
    int flag = 0;
    int msglen = 0;
    uint8* uptr = NULL;
    ripple_netpacket* npacket = NULL;

    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "assemble identity netpacket init error");
        return false;
    }
    
    /* 4 总长度 + 4 crc32 */
    msglen = (4 + 4 + 4 + 1);
    msglen += 1;
    npacket->data = ripple_netpacket_data_init(msglen);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "assemble identity netpacket init data error");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    msglen -= 1;

    /* 总长度 */
    npacket->used = msglen;
    msglen = r_hton32(msglen);
    uptr = npacket->data;
    rmemcpy1(uptr, 0, &msglen, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* 类型 */
    msgtype = msgtype;
    msgtype = r_hton32(msgtype);
    rmemcpy1(uptr, 0, &msgtype, 4);
    uptr += 4;

    /* 成功 */
    rmemcpy1(uptr, 0, &flag, 1);

    /* 将 packet 挂载到待写队列中 */
    if (false == ripple_queue_put(npoolentry->wpackets, (void*)npacket))
    {
        elog(RLOG_WARNING, "assemble identity add packet to queue error");
        return false;
    }
    return true;
}

/* 组装错误信息 */
bool ripple_xmanager_metricmsg_assembleerrormsg(ripple_xmanager_metric* xmetric,
                                                ripple_queue* queue,
                                                int type,
                                                int errorcode,
                                                char* errormsg)
{
    uint8 flag = 1;
    int len = 0;
    int errmsglen = 0;
    uint8* uptr = NULL;
    ripple_netpacket* npacket = NULL;

    npacket = ripple_netpacket_init();
    if (NULL == npacket)
    {
        elog(RLOG_WARNING, "xmanager metric assemble error msg out of memory");
        return false;
    }

    /* 4 总长度 + 4 crc32 + 4 msgtype + 1 成功/失败 */
    len = 4 + 4 + 4 + 1;

    /* 4 长度 + 4 错误码 + 错误信息 */
    errmsglen = 4 + 4;
    errmsglen += strlen(errormsg);

    len += errmsglen;
    len += 1;
    npacket->data = ripple_netpacket_data_init(len);
    if (NULL == npacket->data)
    {
        elog(RLOG_WARNING, "xmanager metric assemble error msg data out of memory");
        ripple_netpacket_destroy(npacket);
        return false;
    }
    len -= 1;

    npacket->used = len;
    /* 总长度 */
    uptr = npacket->data;
    len = r_hton32(len);
    rmemcpy1(uptr, 0, &len, 4);
    uptr += 4;

    /* crc32, 暂不处理 */
    uptr += 4;

    /* 消息标识 */
    type = r_hton32(type);
    rmemcpy1(uptr, 0, &type, 4);
    uptr += 4;

    /* 错误标识 */
    rmemcpy1(uptr, 0, &flag, 1);
    uptr++;

    /* 错误总长度 */
    len = errmsglen;
    len = r_hton32(len);
    rmemcpy1(uptr, 0, &len, 4);
    uptr += 4;

    /* 错误码 */
    errorcode = r_hton32(errorcode);
    rmemcpy1(uptr, 0, &errorcode, 4);
    uptr += 4;

    /* 错误信息 */
    errmsglen -= 8;
    rmemcpy1(uptr, 0, errormsg, errmsglen);

    /* 挂载到待发送队列中 */
    if (false == ripple_queue_put(queue, (void*)npacket))
    {
        elog(RLOG_WARNING, "xmanager metric assemble error msg add message to queue error");
        ripple_netpacket_destroy(npacket);
        return false;
    }

    return true;
}


/*------------------capture parse begin----------------------------*/
static bool ripple_xmanager_metricmsg_parsecaptureincmsg(ripple_xmanager_metric* xmetric,
                                                         ripple_netpoolentry* npoolentry,
                                                         ripple_netpacket* npacket)
{
    int len                                                 = 0;
    uint8* uptr                                             = NULL;
    ripple_xmanager_metriccapturenode* pxmetriccapturenode  = NULL;
    ripple_xmanager_metriccapturenode xmetricapturenode     = { {0} };

    uptr = npacket->data;

    /* 偏移 12 字节, 4 msglen + 4 crc32 + 4 msgtype */
    uptr += 12;

    /* 获取 jobnamelen */
    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;

    /* jobname */
    xmetricapturenode.base.name = (char*)uptr;
    uptr += len;

    /* 继续获取 redolsn */
    rmemcpy1(&xmetricapturenode.redolsn, 0, uptr, 8);
    uptr[0] = '\0';
    uptr += 8;

    /* 根据名称获取 metricnode */
    xmetricapturenode.base.type = RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE;
    pxmetriccapturenode = dlist_get(xmetric->metricnodes, &xmetricapturenode, ripple_xmanager_metricnode_cmp);
    if (NULL == pxmetriccapturenode)
    {
        elog(RLOG_WARNING, "xmanager metric capture msg can not get capture by name:%s", xmetricapturenode.base.name);
        return false;
    }

    /* 设置 redolsn */
    pxmetriccapturenode->redolsn = xmetricapturenode.redolsn;
    pxmetriccapturenode->redolsn = r_ntoh64(pxmetriccapturenode->redolsn);

    /* 设置 restartlsn */
    rmemcpy1(&pxmetriccapturenode->restartlsn, 0, uptr, 8);
    pxmetriccapturenode->restartlsn = r_ntoh64(pxmetriccapturenode->restartlsn);
    uptr += 8;

    /* 获取 confirmlsn */
    rmemcpy1(&pxmetriccapturenode->confirmlsn, 0, uptr, 8);
    pxmetriccapturenode->confirmlsn = r_ntoh64(pxmetriccapturenode->confirmlsn);
    uptr += 8;

    /* 获取 loadlsn */
    rmemcpy1(&pxmetriccapturenode->loadlsn, 0, uptr, 8);
    pxmetriccapturenode->loadlsn = r_ntoh64(pxmetriccapturenode->loadlsn);
    uptr += 8;

    /* 获取 parselsn */
    rmemcpy1(&pxmetriccapturenode->parselsn, 0, uptr, 8);
    pxmetriccapturenode->parselsn = r_ntoh64(pxmetriccapturenode->parselsn);
    uptr += 8;

    /* 获取 flushlsn */
    rmemcpy1(&pxmetriccapturenode->flushlsn, 0, uptr, 8);
    pxmetriccapturenode->flushlsn = r_ntoh64(pxmetriccapturenode->flushlsn);
    uptr += 8;

    /* 获取 trailno */
    rmemcpy1(&pxmetriccapturenode->trailno, 0, uptr, 8);
    pxmetriccapturenode->trailno = r_ntoh64(pxmetriccapturenode->trailno);
    uptr += 8;

    /* 获取 trailstart */
    rmemcpy1(&pxmetriccapturenode->trailstart, 0, uptr, 8);
    pxmetriccapturenode->trailstart = r_ntoh64(pxmetriccapturenode->trailstart);
    uptr += 8;

    /* 获取 parsetimestamp */
    rmemcpy1(&pxmetriccapturenode->parsetimestamp, 0, uptr, 8);
    pxmetriccapturenode->parsetimestamp = r_ntoh64(pxmetriccapturenode->parsetimestamp);
    uptr += 8;

    /* 获取 flushtimestamp */
    rmemcpy1(&pxmetriccapturenode->flushtimestamp, 0, uptr, 8);
    pxmetriccapturenode->flushtimestamp = r_ntoh64(pxmetriccapturenode->flushtimestamp);
    uptr += 8;

    (void)npoolentry;
    return true;
}

/*------------------capture parse   end----------------------------*/

/*------------------integrate parse begin----------------------------*/
static bool ripple_xmanager_metricmsg_parseintegrateincmsg(ripple_xmanager_metric* xmetric,
                                                           ripple_netpoolentry* npoolentry,
                                                           ripple_netpacket* npacket)
{
    int len                                                     = 0;
    uint8* uptr                                                 = NULL;
    ripple_xmanager_metricintegratenode* pxmetricintegratenode  = NULL;
    ripple_xmanager_metricintegratenode xmetriintegratenode     = { {0} };

    uptr = npacket->data;

    /* 偏移 12 字节, 4 msglen + 4 crc32 + 4 msgtype */
    uptr += 12;

    /* 获取 jobnamelen */
    rmemcpy1(&len, 0, uptr, 4);
    len = r_ntoh32(len);
    uptr += 4;

    /* jobname */
    xmetriintegratenode.base.name = (char*)uptr;
    uptr += len;

    /* 继续获取 redolsn */
    rmemcpy1(&xmetriintegratenode.loadlsn, 0, uptr, 8);
    uptr[0] = '\0';
    uptr += 8;

    /* 根据名称获取 metricnode */
    xmetriintegratenode.base.type = RIPPLE_XMANAGER_METRICNODETYPE_INTEGRATE;
    pxmetricintegratenode = dlist_get(xmetric->metricnodes, &xmetriintegratenode, ripple_xmanager_metricnode_cmp);
    if (NULL == pxmetricintegratenode)
    {
        elog(RLOG_WARNING, "xmanager metric integrate msg can not get integrate by name:%s", xmetriintegratenode.base.name);
        return false;
    }

    /* 设置 redolsn */
    pxmetricintegratenode->loadlsn = xmetriintegratenode.loadlsn;
    pxmetricintegratenode->loadlsn = r_ntoh64(pxmetricintegratenode->loadlsn);

    /* 设置 synclsn */
    rmemcpy1(&pxmetricintegratenode->synclsn, 0, uptr, 8);
    pxmetricintegratenode->synclsn = r_ntoh64(pxmetricintegratenode->synclsn);
    uptr += 8;

    /* 获取 loadtrailno */
    rmemcpy1(&pxmetricintegratenode->loadtrailno, 0, uptr, 8);
    pxmetricintegratenode->loadtrailno = r_ntoh64(pxmetricintegratenode->loadtrailno);
    uptr += 8;

    /* 获取 loadtrailstart */
    rmemcpy1(&pxmetricintegratenode->loadtrailstart, 0, uptr, 8);
    pxmetricintegratenode->loadtrailstart = r_ntoh64(pxmetricintegratenode->loadtrailstart);
    uptr += 8;

    /* 获取 synctrailno */
    rmemcpy1(&pxmetricintegratenode->synctrailno, 0, uptr, 8);
    pxmetricintegratenode->synctrailno = r_ntoh64(pxmetricintegratenode->synctrailno);
    uptr += 8;

    /* 获取 synctrailstart */
    rmemcpy1(&pxmetricintegratenode->synctrailstart, 0, uptr, 8);
    pxmetricintegratenode->synctrailstart = r_ntoh64(pxmetricintegratenode->synctrailstart);
    uptr += 8;

    /* 获取 loadtimestamp */
    rmemcpy1(&pxmetricintegratenode->loadtimestamp, 0, uptr, 8);
    pxmetricintegratenode->loadtimestamp = r_ntoh64(pxmetricintegratenode->loadtimestamp);
    uptr += 8;

    /* 获取 synctimestamp */
    rmemcpy1(&pxmetricintegratenode->synctimestamp, 0, uptr, 8);
    pxmetricintegratenode->synctimestamp = r_ntoh64(pxmetricintegratenode->synctimestamp);
    uptr += 8;

    (void)npoolentry;
    return true;
}

/*------------------capture parse   end----------------------------*/


static ripple_xmanager_metricmsgop m_metricmsgops[] =
{
    {
        RIPPLE_XMANAGER_MSG_NOP,
        "XManager Msg NOP",
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_MSG_IDENTITYCMD,
        "XManager Msg Identity",
        NULL
    },
    {
        RIPPLE_XMANAGER_MSG_CREATECMD,
        "XManager Msg Create",
        NULL,
        ripple_xmanager_metricmsg_parsecreate
    },
    {
        RIPPLE_XMANAGER_MSG_ALTERCMD,
        "XManager Msg Alter",
        NULL,
        ripple_xmanager_metricmsg_parsealter
    },
    {
        RIPPLE_XMANAGER_MSG_REMOVECMD,
        "XManager Msg Remove",
        NULL,
        ripple_xmanager_metricmsg_parseremove
    },
    {
        RIPPLE_XMANAGER_MSG_DROPCMD,
        "XManager Msg Drop",
        NULL,
        ripple_xmanager_metricmsg_parsedrop
    },
    {
        RIPPLE_XMANAGER_MSG_INITCMD,
        "XManager Msg Init",
        ripple_xmanager_metricmsg_assembleinit,
        ripple_xmanager_metricmsg_parseinit
    },
    {
        RIPPLE_XMANAGER_MSG_EDITCMD,
        "XManager Msg Edit",
        NULL,
        ripple_xmanager_metricmsg_parseedit
    },
    {
        RIPPLE_XMANAGER_MSG_STARTCMD,
        "XManager Msg Start",
        ripple_xmanager_metricmsg_assemblestart,
        ripple_xmanager_metricmsg_parsestart
    },
    {
        RIPPLE_XMANAGER_MSG_STOPCMD,
        "XManager Msg Stop",
        ripple_xmanager_metricmsg_assemblestop,
        ripple_xmanager_metricmsg_parsestop
    },
    {
        RIPPLE_XMANAGER_MSG_RELOADCMD,
        "XManager Msg Reload",
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_MSG_INFOCMD,
        "XManager Msg Info",
        NULL,
        ripple_xmanager_metricmsg_parseinfo
    },
    {
        RIPPLE_XMANAGER_MSG_WATCHCMD,
        "XManager Msg Watch",
        NULL,
        ripple_xmanager_metricmsg_parsewatch
    },
    {
        RIPPLE_XMANAGER_MSG_CONFFILECMD,
        "XManager Msg Conffile",
        NULL,
        ripple_xmanager_metricmsg_parseconffile
    },
    {
        RIPPLE_XMANAGER_MSG_REFRESHCMD,
        "XManager Msg Refresh",
        ripple_xmanager_metricmsg_assemblerefresh,
        ripple_xmanager_metricmsg_parserefresh
    },
    {
        RIPPLE_XMANAGER_MSG_LISTCMD,
        "XManager Msg List",
        NULL,
        ripple_xmanager_metricmsg_parselist
    },

    /*---------------------------xmanager 内部通信使用 begin-----------------*/
    {
        RIPPLE_XMANAGER_MSG_CAPTUREINCREMENT,
        "XManager Msg Capture Increment",
        NULL,
        ripple_xmanager_metricmsg_parsecaptureincmsg
    },
    {
        RIPPLE_XMANAGER_MSG_CAPTUREREFRESH,
        "XManager Msg Capture Online Refresh",
        NULL,
        ripple_xmanager_metricmsg_parsecapturerefresh
    },
    {
        RIPPLE_XMANAGER_MSG_CAPTUREBIGTXN,
        "XManager Msg Capture Big Transaction",
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_MSG_INTEGRATEINCREMENT,
        "XManager Msg Integrate Increment",
        NULL,
        ripple_xmanager_metricmsg_parseintegrateincmsg
    },
    {
        RIPPLE_XMANAGER_MSG_INTEGRATEONLINEREFRESH,
        "XManager Msg Integrate Online Refresh",
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_MSG_INTEGRATEBIGTXN,
        "XManager Msg Integrate Big Transaction",
        NULL,
        NULL
    },
    {
        RIPPLE_XMANAGER_MSG_PGRECEIVELOG,
        "XManager Msg PGReceivelog",
        NULL,
        NULL
    },
    /*---------------------------xmanager 内部通信使用   end-----------------*/
    /* 在此前添加 */
    {
        RIPPLE_XMANAGER_MSG_MAX,
        "XManager Msg Max",
        NULL,
        NULL
    }
};

char* ripple_xmanager_metricmsg_getdesc(ripple_xmanager_msg msgtype)
{
    return m_metricmsgops[msgtype].desc;
}

/* 
 * 解析数据包
 *  返回 false 时, 需要在外面释放 npoolentry
*/
bool ripple_xmanager_metricmsg_assembleresponse(ripple_xmanager_metric* xmetric,
                                                ripple_netpoolentry* npoolentry,
                                                ripple_xmanager_msg msgtype,
                                                dlist* dlmsgs)
{
    char errormsg[512] = { 0 };

    if (msgtype >= RIPPLE_XMANAGER_MSG_MAX)
    {
        snprintf(errormsg, 512, "unknown msgtype %d.", msgtype);
        return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                          npoolentry->wpackets,
                                                          msgtype,
                                                          RIPPLE_ERROR_MSGUNSPPORT,
                                                          errormsg);
    }

    if (NULL == m_metricmsgops[msgtype].assemble)
    {
        snprintf(errormsg, 512, "%s unsupport, please wait.", m_metricmsgops[msgtype].desc);
        return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                          npoolentry->wpackets,
                                                          msgtype,
                                                          RIPPLE_ERROR_MSGUNSPPORT,
                                                          errormsg);
    }

    return m_metricmsgops[msgtype].assemble(xmetric, npoolentry, dlmsgs);
}

/* 
 * 解析数据包
 *  返回 false 时, 需要在外面释放 npoolentry
*/
bool ripple_xmanager_metricmsg_parsenetpacket(ripple_xmanager_metric* xmetric,
                                              ripple_netpoolentry* npoolentry,
                                              ripple_netpacket* npacket)
{
    int msgtype     = 0;
    uint8* uptr     = NULL;
    char errormsg[512] = { 0 };

    /* 根据 msgtype 做分发 */
    uptr = npacket->data;
    uptr += 8;
    rmemcpy1(&msgtype, 0, uptr, 4);
    msgtype = r_ntoh32(msgtype);

    if (msgtype >= RIPPLE_XMANAGER_MSG_MAX)
    {
        snprintf(errormsg, 512, "unknown msgtype %d.", msgtype);
        return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                          npoolentry->wpackets,
                                                          msgtype,
                                                          RIPPLE_ERROR_MSGUNSPPORT,
                                                          errormsg);
    }

    if (NULL == m_metricmsgops[msgtype].parse)
    {
        snprintf(errormsg, 512, "%s unsupport, please wait.", m_metricmsgops[msgtype].desc);
        return ripple_xmanager_metricmsg_assembleerrormsg(xmetric,
                                                          npoolentry->wpackets,
                                                          msgtype,
                                                          RIPPLE_ERROR_MSGUNSPPORT,
                                                          errormsg);
    }

    return m_metricmsgops[msgtype].parse(xmetric, npoolentry, npacket);
}
