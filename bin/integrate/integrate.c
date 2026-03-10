/*
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
*/

#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "port/ipc/ipc.h"
#include "port/file/fd.h"
#include "utils/path/ripple_path.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "command/ripple_cmd.h"

static void
help()
{
    printf("Usage:\n  integrate [OPTION]\n");
    printf("Options:\n");
    printf("  -f config.cfg        config file\n");
    printf("  operation            init/start/stop/status/reload\n");
}

int main(int argc, char **argv)
{
    ripple_optype   optype      = RIPPLE_OPTYPE_NOP;
    const char*     loglevel    = NULL;
    char*           profilepath = NULL;

    if(1 < argc)
    {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
        {
            help();
            exit(0);
        }

        /* 检查个数 */
        if(4 != argc)
        {
            help();
            exit(0);
        }
        
        if(0 != strcmp(argv[1], "-f"))
        {
            help();
            exit(0);
        }

        if(strlen(argv[3]) == strlen("init")
            && 0 == strcasecmp(argv[3], "init"))
        {
            optype = RIPPLE_OPTYPE_INIT;
        }
        else if(strlen(argv[3]) == strlen("start")
                && 0 == strcasecmp(argv[3], "start"))
        {
            optype = RIPPLE_OPTYPE_START;
        }
        else if(strlen(argv[3]) == strlen("stop")
                && 0 == strcasecmp(argv[3], "stop"))
        {
            optype = RIPPLE_OPTYPE_STOP;
        }
        else if(strlen(argv[3]) == strlen("status")
                && 0 == strcasecmp(argv[3], "status"))
        {
            optype = RIPPLE_OPTYPE_STATUS;
        }
        else if(strlen(argv[3]) == strlen("reload")
                && 0 == strcasecmp(argv[3], "reload"))
        {
            optype = RIPPLE_OPTYPE_RELOAD;
        }
        else
        {
            help();
            exit(0);
        }
    }
    else
    {
        help();
        exit(0);
    }

    g_proctype = RIPPLE_PROC_TYPE_INTEGRATE;

    /* 保存配置文件路径绝对路径 */
    profilepath = ripple_make_absolute_path(argv[2]);
    rmemcpy1(g_profilepath, 0, profilepath, strlen(profilepath));
    rfree(profilepath);

    /* 参数解析 */
    guc_loadcfg(argv[2], false);

    /* 查看解析内容是否正确 */
    guc_debug();

    /* 设置 日志级别 */
    loglevel = guc_getConfigOption(RIPPLE_CFG_KEY_LOG_LEVEL);
    if(NULL == loglevel)
    {
        elog(RLOG_ERROR, "unrecognized configuration parameter:%s", loglevel);
    }

    elog_seteloglevel(loglevel);

    /* 获取主线程号 */
    g_mainthrid = pthread_self();

    /* 执行 */
    ripple_cmd(optype, NULL);

    return 0;
}

