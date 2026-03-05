#include "ripple_app_incl.h"
#include "command/ripple_cmd_startcapture.h"
#include "command/ripple_cmd_startpump.h"
#include "command/ripple_cmd_startcollector.h"
#include "command/ripple_cmd_startintegrate.h"
#include "command/ripple_cmd_startxmanager.h"
#include "command/ripple_cmd.h"

typedef bool (*procstart)();

typedef struct RPOC2START
{
    ripple_proc_type    type;
    procstart            func;
    char*               errmsg;
} proc2start;

/* caputre 启动处理 */
static proc2start           m_typ2start[]=
{
    { RIPPLE_PROC_TYPE_NOP,             NULL,                           "proc nop unsupport start"},
    { RIPPLE_PROC_TYPE_CAPTURE,         ripple_cmd_startcapture,        "capture start error" },
    { RIPPLE_PROC_TYPE_PUMP,            ripple_cmd_startpump,           "pump start error" },
    { RIPPLE_PROC_TYPE_COLLECTOR,       ripple_cmd_startcollector,      "collector start error" },
    { RIPPLE_PROC_TYPE_INTEGRATE,       ripple_cmd_startintegrate,      "integrate start error" },
    { RIPPLE_PROC_TYPE_FASTCMPCLIENT,   NULL,                           "fast compare client" },
    { RIPPLE_PROC_TYPE_FASTCMPSVR,      NULL,                           "fast compare server" },
    { RIPPLE_PROC_TYPE_HGRECEIVEWAL,    NULL,                           "hg receivelog start error" },
    { RIPPLE_PROC_TYPE_PGRECEIVEWAL,    NULL,                           "pg receivelog start error" },
    { RIPPLE_PROC_TYPE_XMANAGER,        ripple_cmd_startxmanager,       "xmanager start error" }
};

/* 启动命令 */
bool ripple_cmd_start(void *extra_config)
{
    RIPPLE_UNUSED(extra_config);
    if(NULL == m_typ2start[g_proctype].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2start[g_proctype].errmsg);
        return false;
    }

    return m_typ2start[g_proctype].func();
}

