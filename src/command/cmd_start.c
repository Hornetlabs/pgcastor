#include "app_incl.h"
#include "command/cmd_startcapture.h"
#include "command/cmd_startintegrate.h"
#include "command/cmd_startxmanager.h"
#include "command/cmd.h"

typedef bool (*procstart)();

typedef struct RPOC2START
{
    proc_type    type;
    procstart            func;
    char*               errmsg;
} proc2start;

/* caputre 启动处理 */
static proc2start           m_typ2start[]=
{
    { PROC_TYPE_NOP,             NULL,                           "proc nop unsupport start"},
    { PROC_TYPE_CAPTURE,         cmd_startcapture,        "capture start error" },
    { PROC_TYPE_INTEGRATE,       cmd_startintegrate,      "integrate start error" },
    { PROC_TYPE_PGRECEIVEWAL,    NULL,                           "pg receivelog start error" },
    { PROC_TYPE_XMANAGER,        cmd_startxmanager,       "xmanager start error" }
};

/* 启动命令 */
bool cmd_start(void *extra_config)
{
    UNUSED(extra_config);
    if(NULL == m_typ2start[g_proctype].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2start[g_proctype].errmsg);
        return false;
    }

    return m_typ2start[g_proctype].func();
}

