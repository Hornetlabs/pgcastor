/*
 * All Copyright (c) 2024-2025, Byte Sync Development Group
 *
*/

#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/path/ripple_path.h"
#include "utils/license/license.h"
#include "utils/daemon/ripple_process.h"
#include "signal/ripple_signal.h"
#include "misc/ripple_misc_lockfiles.h"
#include "command/ripple_cmd.h"
#include "translog/ripple_translog_recvlogdb.h"
#include "translog/wal/ripple_translog_walcontrol.h"
#include "translog/wal/ripple_translog_recvlog.h"

static void help()
{
    printf("Usage:\n  receivewal [OPTION] start/stop\n");
    printf("Options:\n");
    printf("  -f config.cfg        config file\n");
}

static void hgreceivewalversion()
{
    printf("receivelog for hgdb\n");
}

/* receivewal */
int main(int argc, char** argv)
{
    int index                           = 0;
    uint32 hlsn                         = 0;
    uint32 llsn                         = 0;
    ripple_optype optype                = RIPPLE_OPTYPE_NOP;
    TimeLineID tli                      = InvalidTimeLineID;
    XLogRecPtr lsnstartpos              = InvalidXLogRecPtr;
    char* profilepath                   = NULL;
    const char* loglevel                = NULL;
    char* datadir                       = NULL;
    char* slotname                      = NULL;
    char* startpos                      = NULL;
    char* restorecmd                    = NULL;
    ripple_translog_recvlog* recvwal    = NULL;
    ripple_translog_walcontrol* walctrl = NULL;
    char abscfgpath[RIPPLE_MAXPATH]     = { 0 };
    char controlfile[RIPPLE_ABSPATH]    = { 0 };

    if (1 < argc)
    {
        if (0 == strcmp(argv[1], "--help") || 0 == strcmp(argv[1], "-?"))
        {
            help();
            exit(0);
        }
        else if(0 == strcmp(argv[1], "-v"))
        {
            hgreceivewalversion();
            exit(0);
        }
        else if (0 != strcmp(argv[1], "-f"))
        {
            help();
            exit(0);
        }

        /* 检查个数 */
        if (4 != argc)
        {
            help();
            exit(0);
        }

        if (strlen(argv[3]) == strlen("start")
                && 0 == strcasecmp(argv[3], "start"))
        {
            optype = RIPPLE_OPTYPE_START;
        }
        else if (strlen(argv[3]) == strlen("stop")
                && 0 == strcasecmp(argv[3], "stop"))
        {
            optype = RIPPLE_OPTYPE_STOP;
        }
        else if (strlen(argv[3]) == strlen("status")
                && 0 == strcasecmp(argv[3], "status"))
        {
            optype = RIPPLE_OPTYPE_STATUS;
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

    g_proctype = RIPPLE_PROC_TYPE_HGRECEIVEWAL;
    ripple_mem_init();

    /* 保存配置文件路径绝对路径 */
    profilepath = ripple_make_absolute_path(argv[2]);

    /* 获取绝对路径，并加载 license */
    rmemcpy1(abscfgpath, 0, argv[2], strlen(argv[2]));
    ripple_path_canonicalize_path(abscfgpath);
    for(index = strlen(abscfgpath); index > 0; index--)
    {
        if(abscfgpath[index - 1] != '/')
        {
            abscfgpath[index - 1] = '\0';
            continue;
        }
        break;
    }

    /* 参数解析 */
    guc_loadcfg(profilepath, false);

    /* 设置 日志级别 */
    loglevel = guc_getConfigOption(RIPPLE_CFG_KEY_LOG_LEVEL);
    if (NULL == loglevel)
    {
        elog(RLOG_WARNING, "unrecognized configuration parameter:%s", loglevel);
        return 1;
    }
    elog_seteloglevel(loglevel);

    /*
     * 加载 control 文件
     */
    datadir = guc_getConfigOption(RIPPLE_CFG_KEY_DATA);
    if (NULL == datadir || '\0' == datadir[0])
    {
        elog(RLOG_WARNING, "please config receivewal work dir");
        return 1;
    }

    /* 切换工作目录 */
    chdir(datadir);

    /* 操作类型为 stop */
    if (RIPPLE_OPTYPE_STOP == optype)
    {
        ripple_cmd(optype, NULL);
        return 0;
    }
    else if(RIPPLE_OPTYPE_STATUS == optype)
    {
        elog(RLOG_WARNING, "receivewal not support status command.");
        return 0;
    }

    /* 查看解析内容是否正确 */
    guc_debug();

    /* 校验license */
    if (false == ripple_license_check(abscfgpath))
    {
        elog(RLOG_WARNING, "license valid error");
        return 1;
    }

    /* 设置为后台运行 */
    ripple_makedaemon();

    /* 获取主线程号 */
    g_mainthrid = pthread_self();

    /* 在 wdata 查看锁文件是否存在,不存在则创建,存在则检测进程是否启动 */
    ripple_misc_lockfiles_create(RIPPLE_LOCK_FILE);

    /* log 初始化 */
    ripple_log_init();

    /* 设置信号处理函数 */
    ripple_signal_init();

    /*
     * 根据状态做不同的处理
     *  RIPPLE_TRANSLOG_WALCONTROL_STAT_INIT                初次启动, 根据配置做处理
     */
    snprintf(controlfile, RIPPLE_ABSPATH, "%s/receivewal.control", datadir);
    walctrl = ripple_translog_walcontrol_load(controlfile);
    if (NULL == walctrl)
    {
        elog(RLOG_WARNING, "init receivewal control error");
        ripple_misc_lockfiles_unlink(0, NULL);
        return 1;
    }

    if (RIPPLE_TRANSLOG_WALCONTROL_STAT_INIT == walctrl->stat)
    {
        /* 获取 slotname */
        slotname = guc_getConfigOption(RIPPLE_CFG_KEY_SLOT_NAME);
        if (NULL != slotname && '\0' != slotname[0])
        {
            ripple_translog_walcontrol_setslotname(walctrl, slotname);
        }

        /* startpos */
        startpos = guc_getConfigOption(RIPPLE_CFG_KEY_STARTPOS);
        sscanf(startpos, "%X/%X", &hlsn, &llsn);
        lsnstartpos = ((uint64)hlsn)<<32 | llsn;
        ripple_translog_walcontrol_setstartpos(walctrl, lsnstartpos);

        /* timeline */
        tli = guc_getConfigOptionInt(RIPPLE_CFG_KEY_TIMELINE);
        ripple_translog_walcontrol_settli(walctrl, tli);

        /* restorecommand */
        restorecmd = guc_getConfigOption(RIPPLE_CFG_KEY_RESTORE_COMMAND);
        ripple_translog_walcontrol_setrestorecmd(walctrl, restorecmd);
    }

    /* 设置 receivewal 的信息 */
    recvwal = ripple_translog_recvlog_init();
    if(NULL == recvwal)
    {
        elog(RLOG_WARNING, "init recv wal error");
        ripple_misc_lockfiles_unlink(0, NULL);
        return 1;
    }
    ripple_translog_recvlog_settli(recvwal, walctrl->tli);
    ripple_translog_recvlog_setdbtli(recvwal, walctrl->dbtli);
    ripple_translog_recvlog_setstartpos(recvwal, walctrl->startpos);
    ripple_translog_recvlog_setsegsize(recvwal, walctrl->segsize);
    ripple_translog_recvlog_setdbtype(recvwal, RIPPLE_TRANSLOG_RECVLOG_DBTYPE_HGDB);
    ripple_translog_recvlog_setdata(recvwal, datadir);
    ripple_translog_recvlog_setslotname(recvwal, walctrl->slotname);
    ripple_translog_recvlog_setrestorecmd(recvwal, walctrl->restorecmd);

    /* 解除信号屏蔽 */
    ripple_singal_setmask();

    elog(RLOG_INFO, "receivewal start, pid:%d", getpid());

    ripple_log_destroyerrorstack();

    /* 关闭标准输入/输出/错误 */
    ripple_closestd();

    /* 启动同步 */
    if(false == ripple_translog_recvlog_main(recvwal))
    {
        elog(RLOG_WARNING, "receivewal error");
        ripple_misc_lockfiles_unlink(0, NULL);
        return 1;
    }

    /* 将 control 文件落盘 */
    walctrl->dbtli = recvwal->dbtli;
    walctrl->startpos = recvwal->startpos;
    walctrl->tli = recvwal->tli;
    walctrl->stat = RIPPLE_TRANSLOG_WALCONTROL_STAT_SHUTDOWN;

    ripple_translog_walcontrol_flush(walctrl, recvwal->data);
    ripple_translog_recvlog_free(recvwal);

    rfree(walctrl);
    ripple_misc_lockfiles_unlink(0, NULL);
    return 0;
}
