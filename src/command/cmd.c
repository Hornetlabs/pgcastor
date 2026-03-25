#include "app_incl.h"
#include "utils/list/list_func.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "queue/queue.h"
#include "net/netiomp/netiomp.h"
#include "net/netpacket/netpacket.h"
#include "net/netclient.h"
#include "command/cmd.h"
#include "xmanager/xmanager_msg.h"
#include "xmanager/xmanager_metricnode.h"

typedef bool (*cmdfunc)(void* extra_config);

typedef enum PROC2CMDFLAG
{
    PROC2CMDFLAG_NOP = 0x00,

    /* Send feedback message to xmanager */
    PROC2CMDFLAG_XMANAGER,
} proc2cmdflag;

typedef struct PROC2CMD
{
    optype       type;
    proc2cmdflag flag;
    xmanager_msg msgtype;
    char*        desc;
    cmdfunc      func;
    char*        errmsg;
} proc2cmd;

static proc2cmd m_typ2cmd[] = {
    {OPTYPE_NOP, PROC2CMDFLAG_NOP, XMANAGER_MSG_NOP, "NOP", NULL, "op nop unsupport"},
    {OPTYPE_INIT, PROC2CMDFLAG_XMANAGER, XMANAGER_MSG_INITCMD, "init", cmd_init, "init error"},
    {OPTYPE_START, PROC2CMDFLAG_XMANAGER, XMANAGER_MSG_STARTCMD, "start", cmd_start, "start error"},
    {OPTYPE_STOP, PROC2CMDFLAG_XMANAGER, XMANAGER_MSG_STOPCMD, "stop", cmd_stop, "stop error"},
    {OPTYPE_STATUS, PROC2CMDFLAG_NOP, XMANAGER_MSG_NOP, "status", cmd_status, "status error"},
    {OPTYPE_RELOAD, PROC2CMDFLAG_NOP, XMANAGER_MSG_RELOADCMD, "reload", cmd_reload, "reload error"},
    {OPTYPE_ONLINEREFRESH, PROC2CMDFLAG_XMANAGER, XMANAGER_MSG_CAPTUREREFRESH, "onlinerefresh",
     cmd_onlinerefresh, "onlinerefresh error"}};

bool cmd(optype type, void* extra_config)
{
    bool                    bret = false;
    int8                    flag = 0;
    int                     port = 0;
    int                     ivalue = 0;
    int                     msglen = 0;
    int                     valuelen = 0;
    xmanager_metricnodetype nodetype = XMANAGER_METRICNODETYPE_NOP;
    uint8*                  uptr = NULL;
    char*                   datadir = NULL;
    char*                   traildir = NULL;
    char*                   jobname = NULL;
    char*                   errormsg = NULL;
    uint8*                  netdata = NULL;
    char                    svrport[128] = {0};

    if (NULL == m_typ2cmd[type].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2cmd[type].errmsg);
        return false;
    }

    log_initerrorstack();

    /* Execute */
    bret = m_typ2cmd[type].func(extra_config);
    if (false == bret)
    {
        errormsg = log_geterrormsg();
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
        case PROC_TYPE_CAPTURE:
            /* code */
            nodetype = XMANAGER_METRICNODETYPE_CAPTURE;
            break;
        case PROC_TYPE_INTEGRATE:
            nodetype = XMANAGER_METRICNODETYPE_INTEGRATE;
            break;
        case PROC_TYPE_PGRECEIVEWAL:
            nodetype = XMANAGER_METRICNODETYPE_PGRECEIVELOG;
            break;
        default:
            break;
    }

    if (XMANAGER_METRICNODETYPE_NOP == nodetype)
    {
        goto cmd_done;
    }

    if (PROC2CMDFLAG_XMANAGER != m_typ2cmd[type].flag)
    {
        goto cmd_done;
    }
    jobname = guc_getConfigOption(CFG_KEY_JOBNAME);

    /*
     * Construct identity feedback message
     */
    /* totallen + crc32 */
    msglen = 8;

    /* Command type */
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

    /* Operation type */
    msglen += 4;

    /* Success/failure flag */
    msglen += 1;

    if (false == bret)
    {
        flag = 1;
        /* Total length */
        msglen += 4;

        /* Error code */
        msglen += 4;

        /* Error message */
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
        goto cmd_done;
    }
    rmemset0(netdata, 0, '\0', msglen);
    uptr = netdata;

    /* Total length */
    ivalue = msglen;
    ivalue = r_hton32(ivalue);
    rmemcpy1(uptr, 0, &ivalue, 4);
    uptr += 4;

    /* crc32 */
    uptr += 4;

    /* Register message */
    ivalue = XMANAGER_MSG_IDENTITYCMD;
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

        /* Total length */
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;

        /* Error code */
        ivalue = ERROR_MSGCOMMAND;
        ivalue = r_hton32(ivalue);
        rmemcpy1(uptr, 0, &ivalue, 4);
        uptr += 4;

        /* Error message */
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
        rmemcpy1(uptr, 0, &ivalue, 4);
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

    port = guc_getConfigOptionInt(CFG_KEY_XMANAGER_PORT);
    if (0 == port)
    {
        sprintf(svrport, "%s", RMANAGER_PORT);
    }
    else
    {
        sprintf(svrport, "%d", port);
    }

    bret = netclient_senddata(NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN, NULL, svrport, netdata, msglen);

cmd_done:
    if (NULL != netdata)
    {
        rfree(netdata);
        netdata = NULL;
    }
    return bret;
}

char* cmd_getdesc(optype type)
{
    return m_typ2cmd[type].desc;
}

void cmd_printmsg(const char* msg)
{
    fputs(msg, stdout);
    fflush(stdout);
}
