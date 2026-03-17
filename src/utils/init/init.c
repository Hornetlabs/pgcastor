#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/init/databaserecv.h"
#include "utils/init/datainit.h"
#include "utils/init/init.h"
#include "misc/misc_control.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "catalog/catalog.h"

typedef void (*procinit)();

typedef struct RPOC2INIT
{
    proc_type    type;
    bool                (*init)();
    char*               errmsg;
} proc2init;

/* capture 初始化 */
static bool init_capture(void)
{
    char* wdata = NULL;

    wdata = guc_getdata();
    if (false == datainit_init(wdata))
    {
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    /* 创建control文件 */
    misc_controldata_init();

    /* 创建状态文件 */
    misc_capturestat_init();

    return true;
}

/* integrate 初始化 */
static bool init_integrate(void)
{
    /*
     * 1、获取执行目录，初始化执行目录信息
     * 2、切换工作目录
    */
    char* wdata = NULL;

    wdata = guc_getdata();
    if (false == datainit_init(wdata))
    {
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    return true;
}

/* integrate 初始化 */
static bool init_xmanager(void)
{
    /*
     * 1、获取执行目录，初始化执行目录信息
     * 2、切换工作目录
    */
    char* wdata = NULL;

    wdata = guc_getdata();
    if (false == datainit_init(wdata))
    {
        return false;
    }

    /* 切换工作目录 */
    chdir(wdata);

    return true;
}

static proc2init    m_typ2init[] =
{
    { PROC_TYPE_NOP,             NULL,                   "proc nop unsupport init"},
    { PROC_TYPE_CAPTURE,         init_capture,    "capture init error" },
    { PROC_TYPE_INTEGRATE,       init_integrate,  "integrate init error" },
    { PROC_TYPE_PGRECEIVEWAL,    NULL,                   "pg receive log init error" },
    { PROC_TYPE_XMANAGER,        init_xmanager,   "xmanager init error" }
};

bool init(void)
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
