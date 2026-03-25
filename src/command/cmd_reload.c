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
    {PROC_TYPE_NOP, NULL, "proc nop unsupport start"},
    {PROC_TYPE_CAPTURE, cmd_reloadcapture, "capture reload error"}};

/* Reload */
static void cmd_reloadcapture(void)
{
    long  ripplepid;
    char  szMsg[256] = {0};
    char* wdata = NULL;

    wdata = guc_getdata();

    chdir(wdata);

    ripplepid = misc_lockfiles_getpid();
    if (0 == ripplepid)
    {
        cmd_printmsg("Is ripple running?\n");
        return;
    }

    if (0 != kill((pid_t)ripplepid, SIGHUP))
    {
        snprintf(szMsg, 128, "could not send reload signal (PID:%ld) : %s\n", ripplepid,
                 strerror(errno));
        cmd_printmsg(szMsg);
    }

    printf("ripple reload\n");
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
