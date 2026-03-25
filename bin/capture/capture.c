/*
 * All Copyright (c) 2024-2024, Byte Sync Development Group
 *
 */

#include "app_incl.h"
#include "utils/guc/guc.h"
#include "port/ipc/ipc.h"
#include "port/file/fd.h"
#include "utils/path/path.h"
#include "utils/list/list_func.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "command/cmd.h"

static void help()
{
    printf("Usage:\n  capture [OPTION]\n");
    printf("Options:\n");
    printf("  -f config.cfg        config file\n");
    printf("  operation            init/start/stop/status/reload\n");
}

int main(int argc, char** argv)
{
    char*       dbtype = NULL;
    char*       dbversion = NULL;
    optype      optype = OPTYPE_NOP;
    const char* loglevel = NULL;
    char*       profilepath = NULL;
    List*       extra_config = NULL;

    if (1 < argc)
    {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
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
        else if (strlen(argv[3]) == strlen("onlinerefresh") &&
                 0 == strcasecmp(argv[3], "onlinerefresh"))
        {
            int index_guc = 0;
            optype = OPTYPE_ONLINEREFRESH;
            for (index_guc = 4; index_guc < argc; index_guc++)
            {
                extra_config = lappend(extra_config, rstrdup(argv[index_guc]));
            }
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

    if (!extra_config)
    {
        /* check argument count */
        if (4 != argc)
        {
            help();
            exit(0);
        }
    }

    mem_init();
    g_proctype = PROC_TYPE_CAPTURE;

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
        elog(RLOG_WARNING, "unrecognized configuration parameter:%s", loglevel);
        return 1;
    }

    elog_seteloglevel(loglevel);

    /* validate parameter values */
    dbtype = guc_getConfigOption(CFG_KEY_DBTYPE);
    dbversion = guc_getConfigOption(CFG_KEY_DBVERION);

    if (strlen(dbtype) == strlen(DBTYPE_POSTGRES) && 0 == strcmp(dbtype, DBTYPE_POSTGRES))
    {
        g_idbtype = DATABASE_TYPE_POSTGRESQL;
        if (strlen(dbversion) == strlen(DBVERSION_POSTGRES_12) &&
            0 == strcmp(dbversion, DBVERSION_POSTGRES_12))
        {
            g_idbversion = PGDBVERSION_12;
        }
        else
        {
            elog(RLOG_WARNING, "unknow postgres dbversion, support 12");
            return 1;
        }
    }
    else
    {
        elog(RLOG_WARNING, "unknow dbtype, support postgres");
        return 1;
    }

    /* execute */
    if (false == cmd(optype, extra_config))
    {
        return 1;
    }

    return 0;
}
