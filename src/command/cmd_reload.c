#include "app_incl.h"
#include "utils/guc/guc.h"
#include "misc/misc_lockfiles.h"
#include "command/cmd.h"

typedef void (*procreload)();

typedef struct RPOC2RELOAD
{
    proc_type  type;
    procreload func;
    char*      errmsg;
} proc2reload;

/* Reload */
static void cmd_reloadcapture(void);

static proc2reload m_typ2reload[] = {
    {PROC_TYPE_NOP,     NULL,              "proc nop unsupport start"},
    {PROC_TYPE_CAPTURE, cmd_reloadcapture, "capture reload error"    }
};

/* Reload */
static void cmd_reloadcapture(void)
{
    long  castorpid;
    char  szMsg[256] = {0};
    char* wdata = NULL;

    wdata = guc_getdata();

    chdir(wdata);

    castorpid = misc_lockfiles_getpid();
    if (0 == castorpid)
    {
        cmd_printmsg("Is castor running?\n");
        return;
    }

    if (0 != kill((pid_t)castorpid, SIGHUP))
    {
        snprintf(szMsg, 128, "could not send reload signal (PID:%ld) : %s\n", castorpid, strerror(errno));
        cmd_printmsg(szMsg);
    }

    printf("castor reload\n");
}

/* Reload command */
bool cmd_reload(void* extra_config)
{
    UNUSED(extra_config);
    if (NULL == m_typ2reload[g_proctype].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2reload[g_proctype].errmsg);
        return false;
    }

    m_typ2reload[g_proctype].func();
    return true;
}
