/*
 * All Copyright (c) 2024-2025, Byte Sync Development Group
 *
*/

#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/path/path.h"
#include "utils/daemon/process.h"
#include "signal/app_signal.h"
#include "misc/misc_lockfiles.h"
#include "command/cmd.h"
#include "translog/translog_recvlogdb.h"
#include "translog/wal/translog_walcontrol.h"
#include "translog/wal/translog_recvlog.h"

static void help()
{
    printf("Usage:\n  receivewal [OPTION] start/stop\n");
    printf("Options:\n");
    printf("  -f config.cfg        config file\n");
}

static void pgreceivewalversion()
{
    printf("receivelog for postgresql\n");
}

/* receivewal */
int main(int argc, char** argv)
{
    uint32 hlsn                         = 0;
    uint32 llsn                         = 0;
    optype optype                = OPTYPE_NOP;
    TimeLineID tli                      = InvalidTimeLineID;
    XLogRecPtr lsnstartpos              = InvalidXLogRecPtr;
    char* profilepath                   = NULL;
    const char* loglevel                = NULL;
    char* datadir                       = NULL;
    char* slotname                      = NULL;
    char* startpos                      = NULL;
    char* restorecmd                    = NULL;
    translog_recvlog* recvwal    = NULL;
    translog_walcontrol* walctrl = NULL;
    char controlfile[ABSPATH]    = { 0 };

    if (1 < argc)
    {
        if (0 == strcmp(argv[1], "--help") || 0 == strcmp(argv[1], "-?"))
        {
            help();
            exit(0);
        }
        else if(0 == strcmp(argv[1], "-v"))
        {
            pgreceivewalversion();
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
            optype = OPTYPE_START;
        }
        else if (strlen(argv[3]) == strlen("stop")
                && 0 == strcasecmp(argv[3], "stop"))
        {
            optype = OPTYPE_STOP;
        }
        else if (strlen(argv[3]) == strlen("status")
                && 0 == strcasecmp(argv[3], "status"))
        {
            optype = OPTYPE_STATUS;
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

    g_proctype = PROC_TYPE_PGRECEIVEWAL;
    mem_init();

    /* 保存配置文件路径绝对路径 */
    profilepath = osal_make_absolute_path(argv[2]);

    /* 参数解析 */
    guc_loadcfg(profilepath, false);

    /* 设置 日志级别 */
    loglevel = guc_getConfigOption(CFG_KEY_LOG_LEVEL);
    if (NULL == loglevel)
    {
        elog(RLOG_WARNING, "unrecognized configuration parameter:%s", loglevel);
        return 1;
    }
    elog_seteloglevel(loglevel);

    /*
     * 加载 control 文件
     */
    datadir = guc_getConfigOption(CFG_KEY_DATA);
    if (NULL == datadir || '\0' == datadir[0])
    {
        elog(RLOG_WARNING, "please config receivewal work dir");
        return 1;
    }

    /* 切换工作目录 */
    chdir(datadir);

    /* 操作类型为 stop */
    if (OPTYPE_STOP == optype)
    {
        cmd(optype, NULL);
        return 0;
    }
    else if(OPTYPE_STATUS == optype)
    {
        elog(RLOG_WARNING, "receivewal not support status command.");
        return 1;
    }

    /* 查看解析内容是否正确 */
    guc_debug();

    /* 设置为后台运行 */
    makedaemon();

    /* 获取主线程号 */
    g_mainthrid = pthread_self();

    /* 在 wdata 查看锁文件是否存在,不存在则创建,存在则检测进程是否启动 */
    misc_lockfiles_create(LOCK_FILE);

    /* log 初始化 */
    log_init();

    /* 设置信号处理函数 */
    signal_init();

    /*
     * 根据状态做不同的处理
     *  TRANSLOG_WALCONTROL_STAT_INIT                初次启动, 根据配置做处理
     */
    snprintf(controlfile, ABSPATH, "%s/receivewal.control", datadir);
    walctrl = translog_walcontrol_load(controlfile);
    if (NULL == walctrl)
    {
        elog(RLOG_WARNING, "init receivewal control error");
        misc_lockfiles_unlink(0, NULL);
        return 1;
    }

    if (TRANSLOG_WALCONTROL_STAT_INIT == walctrl->stat)
    {
        /* 获取 slotname */
        slotname = guc_getConfigOption(CFG_KEY_SLOT_NAME);
        if (NULL != slotname && '\0' != slotname[0])
        {
            translog_walcontrol_setslotname(walctrl, slotname);
        }

        /* startpos */
        startpos = guc_getConfigOption(CFG_KEY_STARTPOS);
        sscanf(startpos, "%X/%X", &hlsn, &llsn);
        lsnstartpos = ((uint64)hlsn)<<32 | llsn;
        translog_walcontrol_setstartpos(walctrl, lsnstartpos);

        /* timeline */
        tli = guc_getConfigOptionInt(CFG_KEY_TIMELINE);
        translog_walcontrol_settli(walctrl, tli);

        /* restorecommand */
        restorecmd = guc_getConfigOption(CFG_KEY_RESTORE_COMMAND);
        translog_walcontrol_setrestorecmd(walctrl, restorecmd);
    }

    /* 设置 receivewal 的信息 */
    recvwal = translog_recvlog_init();
    if(NULL == recvwal)
    {
        elog(RLOG_WARNING, "init recv wal error");
        misc_lockfiles_unlink(0, NULL);
        return 1;
    }
    translog_recvlog_settli(recvwal, walctrl->tli);
    translog_recvlog_setdbtli(recvwal, walctrl->dbtli);
    translog_recvlog_setstartpos(recvwal, walctrl->startpos);
    translog_recvlog_setsegsize(recvwal, walctrl->segsize);
    translog_recvlog_setdbtype(recvwal, TRANSLOG_RECVLOG_DBTYPE_PG);
    translog_recvlog_setdata(recvwal, datadir);
    translog_recvlog_setslotname(recvwal, walctrl->slotname);
    translog_recvlog_setrestorecmd(recvwal, walctrl->restorecmd);

    /* 解除信号屏蔽 */
    singal_setmask();

    elog(RLOG_INFO, "receivewal start, pid:%d", getpid());

    log_destroyerrorstack();

    /* 关闭标准输入/输出/错误 */
    closestd();

    /* 启动同步 */
    if(false == translog_recvlog_main(recvwal))
    {
        elog(RLOG_WARNING, "receivewal error");
        misc_lockfiles_unlink(0, NULL);
        return 1;
    }

    /* 将 control 文件落盘 */
    walctrl->dbtli = recvwal->dbtli;
    walctrl->startpos = recvwal->startpos;
    walctrl->tli = recvwal->tli;
    walctrl->stat = TRANSLOG_WALCONTROL_STAT_SHUTDOWN;

    translog_walcontrol_flush(walctrl, recvwal->data);
    translog_recvlog_free(recvwal);

    rfree(walctrl);
    misc_lockfiles_unlink(0, NULL);
    return 0;
}
