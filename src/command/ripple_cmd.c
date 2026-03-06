#include "ripple_app_incl.h"
#include "utils/list/list_func.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "queue/ripple_queue.h"
#include "net/netiomp/ripple_netiomp.h"
#include "net/netpacket/ripple_netpacket.h"
#include "net/ripple_netclient.h"
#include "command/ripple_cmd.h"
#include "xmanager/ripple_xmanager_msg.h"
#include "xmanager/ripple_xmanager_metricnode.h"

typedef bool (*cmdfunc)(void *extra_config);

typedef enum PROC2CMDFLAG
{
    PROC2CMDFLAG_NOP            = 0x00,

    /* 发送反馈消息到 xmanager */
    PROC2CMDFLAG_XMANAGER       ,
} proc2cmdflag;

typedef struct PROC2CMD
{
    ripple_optype               type;
    proc2cmdflag                flag;
    ripple_xmanager_msg         msgtype;
    char*                       desc;
    cmdfunc                     func;
    char*                       errmsg;
} proc2cmd;

static proc2cmd     m_typ2cmd[]=
{
    {
        RIPPLE_OPTYPE_NOP,
        PROC2CMDFLAG_NOP,
        RIPPLE_XMANAGER_MSG_NOP,
        "NOP",
        NULL,
        "op nop unsupport"
    },
    {
        RIPPLE_OPTYPE_INIT,
        PROC2CMDFLAG_XMANAGER,
        RIPPLE_XMANAGER_MSG_INITCMD,
        "init",
        ripple_cmd_init,
        "init error"
    },
    {
        RIPPLE_OPTYPE_START,
        PROC2CMDFLAG_XMANAGER,
        RIPPLE_XMANAGER_MSG_STARTCMD,
        "start",
        ripple_cmd_start,
        "start error"
    },
    {
        RIPPLE_OPTYPE_STOP,
        PROC2CMDFLAG_XMANAGER,
        RIPPLE_XMANAGER_MSG_STOPCMD,
        "stop",
        ripple_cmd_stop,
        "stop error"
    },
    {
        RIPPLE_OPTYPE_STATUS,
        PROC2CMDFLAG_NOP,
        RIPPLE_XMANAGER_MSG_NOP,
        "status",
        ripple_cmd_status,
        "status error"
    },
    {
        RIPPLE_OPTYPE_RELOAD,
        PROC2CMDFLAG_NOP,
        RIPPLE_XMANAGER_MSG_RELOADCMD,
        "reload",
        ripple_cmd_reload,
        "reload error"
    },
    {
        RIPPLE_OPTYPE_ONLINEREFRESH,
        PROC2CMDFLAG_XMANAGER,
        RIPPLE_XMANAGER_MSG_CAPTUREREFRESH,
        "onlinerefresh",
        ripple_cmd_onlinerefresh,
        "onlinerefresh error"
    }
};

bool ripple_cmd(ripple_optype type, void *extra_config)
{
    bool bret                               = false;
    int8 flag                               = 0;
    int port                                = 0;
    int ivalue                              = 0;
    int msglen                              = 0;
    int valuelen                            = 0;
    ripple_xmanager_metricnodetype nodetype = RIPPLE_XMANAGER_METRICNODETYPE_NOP;
    uint8* uptr                             = NULL;
    char* datadir                           = NULL;
    char* traildir                          = NULL;
    char* jobname                           = NULL;
    char* errormsg                          = NULL;
    uint8* netdata                          = NULL;
    char svrport[128]                       = { 0 };

    if(NULL == m_typ2cmd[type].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2cmd[type].errmsg);
        return false;
    }

    ripple_log_initerrorstack();

    /* 执行 */
    bret = m_typ2cmd[type].func(extra_config);
    if(false == bret)
    {
        errormsg = ripple_log_geterrormsg();
        if (NULL == errormsg)
        {
            errormsg = m_typ2cmd[type].errmsg;
        }
        elog(RLOG_WARNING, "%s", errormsg);
    }
    else
    {
        errormsg = NULL;
    }

    switch (g_proctype)
    {
        case RIPPLE_PROC_TYPE_CAPTURE:
            /* code */
            nodetype = RIPPLE_XMANAGER_METRICNODETYPE_CAPTURE;
            break;
        case RIPPLE_PROC_TYPE_INTEGRATE:
            nodetype = RIPPLE_XMANAGER_METRICNODETYPE_INTEGRATE;
            break;
        case RIPPLE_PROC_TYPE_HGRECEIVEWAL:
            nodetype = RIPPLE_XMANAGER_METRICNODETYPE_HGRECEIVELOG;
            break;
        case RIPPLE_PROC_TYPE_PGRECEIVEWAL:
            nodetype = RIPPLE_XMANAGER_METRICNODETYPE_PGRECEIVELOG;
            break;
        default:
            break;
    }

    if (RIPPLE_XMANAGER_METRICNODETYPE_NOP == nodetype)
    {
        goto ripple_cmd_done;
    }

    if (PROC2CMDFLAG_XMANAGER != m_typ2cmd[type].flag)
    {
        goto ripple_cmd_done;
    }
    jobname = guc_getConfigOption(RIPPLE_CFG_KEY_JOBNAME);

    /* 
     * 构建identity反馈消息
     */
    /* totallen + crc32 */
    msglen = 8;

    /* 命令类型 */
    msglen += 4;

    /* jobtype */
    msglen += 4;

    /* joblen */
    msglen += 4;

    if (NULL == jobname || '\0' == jobname[0])
    {
        msglen += 0;
    }
    else
    {
        msglen += strlen(jobname);
    }

    /* 操作类型 */
    msglen += 4;

    /* 成功失败 */
    msglen += 1;

    if (false == bret)
    {
        flag = 1;
        /* 总长度 */
        msglen += 4;

        /* 错误码 */
        msglen += 4;

        /* 错误信息 */
        msglen += strlen(errormsg);
    }
    else
    {
        msglen += 4;
        datadir = guc_getdata();
        if (NULL != datadir && '\0' != datadir[0])
        {
            msglen += strlen(datadir);
        }

        /* trail */
        msglen += 4;
        traildir = guc_gettrail();
        if (NULL != traildir && '\0' != traildir[0])
        {
            msglen += strlen(traildir);
        }
    }

    netdata = rmalloc0(msglen);
    if (NULL == netdata)
    {
        bret = false;
        goto ripple_cmd_done;
    }
    rmemset0(netdata, 0, '\0', msglen);
    uptr = netdata;

    /* 总长度 */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* 注册消息 */
    ivalue = RIPPLE_XMANAGER_MSG_IDENTITYCMD;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobtype */
    ivalue = nodetype;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobnamelen */
    ivalue = strlen(jobname);
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* jobname */
    rmemcpy1(uptr, 0, jobname, strlen(jobname));
    uptr += strlen(jobname);

    /* commandtype */
    ivalue = m_typ2cmd[type].msgtype;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* flag */
    rmemcpy1(uptr, 0, &flag, 1);
    uptr += 1;

    if (1 == flag)
    {
        ivalue = 4;
        ivalue += strlen(errormsg);

        /* 总长度 */
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr,0, &ivalue, 4);
        uptr += 4;

        /* 错误码 */
        ivalue = RIPPLE_ERROR_MSGCOMMAND;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;

        /* 错误信息 */
        rmemcpy1(uptr, 0, errormsg, strlen(errormsg));
    }
    else
    {
        if (NULL == datadir || '\0' == datadir[0])
        {
            valuelen = ivalue = 0;
        }
        else
        {
            valuelen = ivalue = strlen(datadir);
            uptr += 4;
            rmemcpy1(uptr, 0, datadir, ivalue);
            uptr -= 4;
        }
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr,0, &ivalue, 4);
        uptr += 4;
        uptr += valuelen;

        if (NULL == traildir || '\0' == traildir[0])
        {
            valuelen = ivalue = 0;
        }
        else
        {
            valuelen = ivalue = strlen(traildir);
            uptr += 4;
            rmemcpy1(uptr, 0, traildir, ivalue);
            uptr -= 4;
        }
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;
        uptr += valuelen;
    }

    port = guc_getConfigOptionInt(RIPPLE_CFG_KEY_XMANAGER_PORT);
    if (0 == port)
    {
        sprintf(svrport, "%s", RMANAGER_PORT);
    }
    else
    {
        sprintf(svrport, "%d", port);
    }

    bret = ripple_netclient_senddata(RIPPLE_NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN, NULL, svrport, netdata, msglen);

ripple_cmd_done:
    if (NULL != netdata)
    {
        rfree(netdata);
        netdata = NULL;
    }
    return bret;
}

char* ripple_cmd_getdesc(ripple_optype type)
{
    return m_typ2cmd[type].desc;
}

void ripple_cmd_printmsg(const char *msg)
{
    fputs(msg, stdout);
    fflush(stdout);
}
