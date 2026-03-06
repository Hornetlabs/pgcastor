#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "misc/ripple_misc_lockfiles.h"
#include "command/ripple_cmd.h"

typedef bool (*procstop)();

typedef struct RPOC2STOP
{
    ripple_proc_type    type;
    procstop            func;
    char*               desc;
    char*               errmsg;
} proc2stop;

static bool ripple_cmd_stopproc(void);

static proc2stop           m_typ2stop[]=
{
    {
        RIPPLE_PROC_TYPE_NOP,
        NULL,
        " proc nop ",
        "proc nop unsupport stop"
    },
    {
        RIPPLE_PROC_TYPE_CAPTURE,
        ripple_cmd_stopproc,
        "capture",
        "capture stop error"
    },
    {
        RIPPLE_PROC_TYPE_INTEGRATE,
        ripple_cmd_stopproc,
        "integrate",
        "integrate stop error"
    },
    {
        RIPPLE_PROC_TYPE_PGRECEIVEWAL,
        ripple_cmd_stopproc,
        "receivewal",
        "receivewal stop error"
    },
    {
        RIPPLE_PROC_TYPE_XMANAGER,
        ripple_cmd_stopproc,
        "xmanager",
        "xmanager stop error"
    }
};

static bool ripple_cmd_wait_stop()
{
    int cnt = 0;
    for (cnt = 0; cnt < (100*RIPPLE_WAITS_PER_SEC); cnt++)
    {
        long ripplepid = 0;

        ripplepid = ripple_misc_lockfiles_getpid();
        if (0 == ripplepid)
        {
            return true;
        }

        if (kill((pid_t) ripplepid, 0) != 0)
        {
            if (0 == ripple_misc_lockfiles_getpid())
            {
                return true;
            }
            else
            {
                continue;
            }
        }

        sleep(1);
        ripple_cmd_printmsg(".");
    }
    return false;
}

/* caputre */
bool ripple_cmd_stopproc(void)
{
    long ripplepid = 0;
    char*   wdata = NULL;
    char    szMsg[256] = { 0 };

    wdata = guc_getdata();

    chdir(wdata);

    ripplepid = ripple_misc_lockfiles_getpid();
    if(0 == ripplepid)
    {
        ripple_cmd_printmsg("Is ");
        ripple_cmd_printmsg(m_typ2stop[g_proctype].desc);
        ripple_cmd_printmsg(" running ? ");
        return false;
    }

    snprintf(szMsg, 128, "found %s process:%d, will send sigterm\n", m_typ2stop[g_proctype].desc , (pid_t)ripplepid);
    ripple_cmd_printmsg(szMsg);
    if(0 != kill((pid_t) ripplepid, SIGTERM))
    {
        snprintf(szMsg, 128, "could not send stop signal (PID:%ld) : %s\n", ripplepid, strerror(errno));
        ripple_cmd_printmsg(szMsg);
        return false;
    }

    ripple_cmd_printmsg("waiting for ");
    ripple_cmd_printmsg(m_typ2stop[g_proctype].desc);
    ripple_cmd_printmsg(" to shutdown...\n");
    if(false == ripple_cmd_wait_stop())
    {
        ripple_cmd_printmsg("\n can not shutdown ");
        ripple_cmd_printmsg(m_typ2stop[g_proctype].desc);
        ripple_cmd_printmsg(" !!!!!!\n");
        return false;
    }
    else
    {
        ripple_cmd_printmsg("\n");
        ripple_cmd_printmsg(m_typ2stop[g_proctype].desc);
        ripple_cmd_printmsg(" stopped.\n");
    }

    return true;
}

/* 关闭命令 */
bool ripple_cmd_stop(void *extra_config)
{
    RIPPLE_UNUSED(extra_config);
    if(NULL == m_typ2stop[g_proctype].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2stop[g_proctype].errmsg);
        return false;
    }

    return m_typ2stop[g_proctype].func();
}
