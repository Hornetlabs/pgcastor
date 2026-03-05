#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/init/ripple_databaserecv.h"
#include "utils/init/ripple_datainit.h"
#include "utils/init/ripple_init.h"
#include "misc/ripple_misc_control.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "catalog/ripple_catalog.h"

typedef void (*procinit)();

typedef struct RPOC2INIT
{
    ripple_proc_type    type;
    bool                (*init)();
    char*               errmsg;
} proc2init;

/* capture 初始化 */
static bool ripple_init_capture(void)
{
    char* wdata = NULL;

    wdata = guc_getdata();
    if (false == ripple_datainit_init(wdata))
    {
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    /* 创建control文件 */
    ripple_misc_controldata_init();

    /* 创建状态文件 */
    ripple_misc_capturestat_init();

    return true;
}


/* send 初始化 */
static bool ripple_init_pump(void)
{
    /*
     * 1、获取执行目录，初始化执行目录信息
     * 2、切换工作目录
    */
    char* wdata = NULL;

    wdata = guc_getdata();
    if (false == ripple_datainit_init(wdata))
    {
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    return true;
}

/* collector 初始化 */
static bool ripple_init_collector(void)
{
    /*
     * 1、获取执行目录，初始化执行目录信息
     * 2、切换工作目录
    */
    char* wdata = NULL;

    wdata = guc_getdata();
    if (false == ripple_datainit_init(wdata))
    {
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    return true;
}

/* integrate 初始化 */
static bool ripple_init_integrate(void)
{
    /*
     * 1、获取执行目录，初始化执行目录信息
     * 2、切换工作目录
    */
    char* wdata = NULL;

    wdata = guc_getdata();
    if (false == ripple_datainit_init(wdata))
    {
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    return true;
}

/* integrate 初始化 */
static bool ripple_init_xmanager(void)
{
    /*
     * 1、获取执行目录，初始化执行目录信息
     * 2、切换工作目录
    */
    char* wdata = NULL;

    wdata = guc_getdata();
    if (false == ripple_datainit_init(wdata))
    {
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    return true;
}

static proc2init    m_typ2init[] =
{
    { RIPPLE_PROC_TYPE_NOP,             NULL,                   "proc nop unsupport init"},
    { RIPPLE_PROC_TYPE_CAPTURE,         ripple_init_capture,    "capture init error" },
    { RIPPLE_PROC_TYPE_PUMP,            ripple_init_pump,       "pump init error" },
    { RIPPLE_PROC_TYPE_COLLECTOR,       ripple_init_collector,  "collector init error" },
    { RIPPLE_PROC_TYPE_INTEGRATE,       ripple_init_integrate,  "integrate init error" },
    { RIPPLE_PROC_TYPE_FASTCMPCLIENT,   NULL,                   "fast compare client init error" },
    { RIPPLE_PROC_TYPE_FASTCMPSVR,      NULL,                   "fast compare server init error" },
    { RIPPLE_PROC_TYPE_HGRECEIVEWAL,    NULL,                   "hg receive log init error" },
    { RIPPLE_PROC_TYPE_PGRECEIVEWAL,    NULL,                   "pg receive log init error" },
    { RIPPLE_PROC_TYPE_XMANAGER,        ripple_init_xmanager,   "xmanager init error" }
};

bool ripple_init(void)
{
    if(NULL == m_typ2init[g_proctype].init)
    {
        elog(RLOG_WARNING, "%s", m_typ2init[g_proctype].errmsg);
        return false;
    }

    if (false == m_typ2init[g_proctype].init())
    {
        printf("%s\n", m_typ2init[g_proctype].errmsg);
        return false;
    }

    printf("-------------------------------------\n");
    printf("|                                   |\n");
    printf("|           xsynch init success     |\n");
    printf("|                                   |\n");
    printf("-------------------------------------\n");
    return true;
}
