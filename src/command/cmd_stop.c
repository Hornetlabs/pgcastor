#include "app_incl.h"
#include "utils/guc/guc.h"
#include "misc/misc_lockfiles.h"
#include "command/cmd.h"

typedef bool (*procstop)();

typedef struct RPOC2STOP
{
    proc_type type;
    procstop  func;
    char*     desc;
    char*     errmsg;
} proc2stop;

static bool cmd_stopproc(void);

static proc2stop m_typ2stop[] = {
    {PROC_TYPE_NOP, NULL, " proc nop ", "proc nop unsupport stop"},
    {PROC_TYPE_CAPTURE, cmd_stopproc, "capture", "capture stop error"},
    {PROC_TYPE_INTEGRATE, cmd_stopproc, "integrate", "integrate stop error"},
    {PROC_TYPE_PGRECEIVEWAL, cmd_stopproc, "receivewal", "receivewal stop error"},
    {PROC_TYPE_XMANAGER, cmd_stopproc, "xmanager", "xmanager stop error"}};

static bool cmd_wait_stop()
{
    int cnt = 0;
    for (cnt = 0; cnt < (100 * WAITS_PER_SEC); cnt++)
    {
        long ripplepid = 0;

        ripplepid = misc_lockfiles_getpid();
        if (0 == ripplepid)
        {
            return true;
        }

        if (kill((pid_t)ripplepid, 0) != 0)
        {
            if (0 == misc_lockfiles_getpid())
            {
                return true;
            }
            else
            {
                continue;
            }
        }

        sleep(1);
        cmd_printmsg(".");
    }
    return false;
}

/* caputre */
bool cmd_stopproc(void)
{
    long  ripplepid = 0;
    char* wdata = NULL;
    char  szMsg[256] = {0};

    wdata = guc_getdata();

    chdir(wdata);

    ripplepid = misc_lockfiles_getpid();
    if (0 == ripplepid)
    {
        cmd_printmsg("Is ");
        cmd_printmsg(m_typ2stop[g_proctype].desc);
        cmd_printmsg(" running ? ");
        return false;
    }

    snprintf(szMsg, 128, "found %s process:%d, will send sigterm\n", m_typ2stop[g_proctype].desc,
             (pid_t)ripplepid);
    cmd_printmsg(szMsg);
    if (0 != kill((pid_t)ripplepid, SIGTERM))
    {
        snprintf(szMsg, 128, "could not send stop signal (PID:%ld) : %s\n", ripplepid,
                 strerror(errno));
        cmd_printmsg(szMsg);
        return false;
    }

    cmd_printmsg("waiting for ");
    cmd_printmsg(m_typ2stop[g_proctype].desc);
    cmd_printmsg(" to shutdown...\n");
    if (false == cmd_wait_stop())
    {
        cmd_printmsg("\n can not shutdown ");
        cmd_printmsg(m_typ2stop[g_proctype].desc);
        cmd_printmsg(" !!!!!!\n");
        return false;
    }
    else
    {
        cmd_printmsg("\n");
        cmd_printmsg(m_typ2stop[g_proctype].desc);
        cmd_printmsg(" stopped.\n");
    }

    return true;
}

/* Stop command */
bool cmd_stop(void* extra_config)
{
    UNUSED(extra_config);
    if (NULL == m_typ2stop[g_proctype].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2stop[g_proctype].errmsg);
        return false;
    }

    return m_typ2stop[g_proctype].func();
}
