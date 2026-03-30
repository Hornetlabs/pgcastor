/*
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 */

#include "app_incl.h"
#include "utils/guc/guc.h"
#include "port/ipc/ipc.h"
#include "port/file/fd.h"
#include "utils/path/path.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "command/cmd.h"

static void help()
{
    printf("Usage:\n  integrate [OPTION]\n");
    printf("Options:\n");
    printf("  -f config.cfg        config file\n");
    printf("  operation            init/start/stop/status/reload\n");
}

int main(int argc, char** argv)
{
    optype      optype = OPTYPE_NOP;
    const char* loglevel = NULL;
    char*       profilepath = NULL;

    if (1 < argc)
    {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
        {
            help();
            exit(0);
        }

        /* check argument count */
        if (4 != argc)
        {
            help();
            exit(0);
        }

        if (0 != strcmp(argv[1], "-f"))
        {
            help();
            exit(0);
        }

        if (strlen(argv[3]) == strlen("init") && 0 == strcasecmp(argv[3], "init"))
        {
            optype = OPTYPE_INIT;
        }
        else if (strlen(argv[3]) == strlen("start") && 0 == strcasecmp(argv[3], "start"))
        {
            optype = OPTYPE_START;
        }
        else if (strlen(argv[3]) == strlen("stop") && 0 == strcasecmp(argv[3], "stop"))
        {
            optype = OPTYPE_STOP;
        }
        else if (strlen(argv[3]) == strlen("status") && 0 == strcasecmp(argv[3], "status"))
        {
            optype = OPTYPE_STATUS;
        }
        else if (strlen(argv[3]) == strlen("reload") && 0 == strcasecmp(argv[3], "reload"))
        {
            optype = OPTYPE_RELOAD;
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

    g_proctype = PROC_TYPE_INTEGRATE;

    /* save config file path as absolute path */
    profilepath = osal_make_absolute_path(argv[2]);
    rmemcpy1(g_profilepath, 0, profilepath, strlen(profilepath));
    rfree(profilepath);

    /* parse parameters */
    guc_loadcfg(argv[2], false);

    /* check if parsed content is correct */
    guc_debug();

    /* set log level */
    loglevel = guc_getConfigOption(CFG_KEY_LOG_LEVEL);
    if (NULL == loglevel)
    {
        elog(RLOG_ERROR, "unrecognized configuration parameter:%s", loglevel);
    }

    elog_seteloglevel(loglevel);

    /* get main thread id */
    g_mainthrid = pthread_self();

    /* execute */
    cmd(optype, NULL);

    return 0;
}
