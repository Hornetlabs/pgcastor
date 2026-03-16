#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "misc/ripple_misc_lockfiles.h"
#include "command/ripple_cmd.h"

typedef void (*procreload)();

typedef struct RPOC2RELOAD
{
    ripple_proc_type    type;
    procreload          func;
    char*               errmsg;
} proc2reload;

/* 重新加载 */
static void ripple_cmd_reloadcapture(void);

static proc2reload           m_typ2reload[]=
{
    { RIPPLE_PROC_TYPE_NOP,         NULL,                           "proc nop unsupport start"},
    { RIPPLE_PROC_TYPE_CAPTURE,     ripple_cmd_reloadcapture,       "capture reload error" }
};

/* 重新加载 */
static void ripple_cmd_reloadcapture(void)
{
    long    ripplepid;
    char    szMsg[256] = { 0 };
    char*   wdata = NULL;

    wdata = guc_getdata();

    chdir(wdata);

    ripplepid = ripple_misc_lockfiles_getpid();
    if(0 == ripplepid)
    {
        ripple_cmd_printmsg("Is ripple running?\n");
        return;
    }

    if(0 != kill((pid_t) ripplepid, SIGHUP))
    {
        snprintf(szMsg, 128, "could not send reload signal (PID:%ld) : %s\n", ripplepid, strerror(errno));
        ripple_cmd_printmsg(szMsg);
    }


    printf("ripple reload\n");
}


/* 启动命令 */
bool ripple_cmd_reload(void *extra_config)
{
    RIPPLE_UNUSED(extra_config);
    if(NULL == m_typ2reload[g_proctype].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2reload[g_proctype].errmsg);
        return false;
    }

    m_typ2reload[g_proctype].func();
    return true;
}
