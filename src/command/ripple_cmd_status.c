#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/license/license.h"
#include "utils/dlist/dlist.h"
#include "misc/ripple_misc_lockfiles.h"
#include "net/netpacket/ripple_netpacket.h"
#include "metric/capture/ripple_metric_capture.h"
#include "metric/pump/ripple_statework_pump.h"
#include "metric/collector/ripple_metric_collector.h"
#include "metric/integrate/ripple_metric_integrate.h"
#include "command/ripple_cmd.h"

typedef bool (*procstatus)();

#define procstatusprint     procstatus

typedef struct RPOC2STATUS
{
    ripple_proc_type    type;
    procstatus          func;
    procstatusprint     printfunc;
    char*               errmsg;
} proc2status;

static bool ripple_cmd_statuscapture(void);
static bool ripple_cmd_statuspump(void);
static bool ripple_cmd_statuscollector(void);
static bool ripple_cmd_statusintegrate(void);

static proc2status           m_typ2status[]=
{
    {
        RIPPLE_PROC_TYPE_NOP,
        NULL,
        NULL,
        "proc nop unsupport status"
    },
    {
        RIPPLE_PROC_TYPE_CAPTURE,
        ripple_cmd_statuscapture,
        NULL,
         "capture status error"
    },
    {
        RIPPLE_PROC_TYPE_PUMP,
        ripple_cmd_statuspump,
        NULL,
         "pump status error"
    },
    {
        RIPPLE_PROC_TYPE_COLLECTOR,
        ripple_cmd_statuscollector,
        NULL,
         "collector status error"
    },
    {
        RIPPLE_PROC_TYPE_INTEGRATE,
        ripple_cmd_statusintegrate,
        NULL,
         "tegrate status error"
    }
};

/* 获取状态信息 */
bool ripple_cmd_statuscapture(void)
{
    int fd = -1;
    long    ripplepid;
    ripple_metric_capture mcapture = { 0 };
    
    ripplepid = ripple_misc_lockfiles_getpid();
    if(0 == ripplepid)
    {
        ripple_cmd_printmsg("Is ripple capture running?\n");
        return false;
    }

    fd = BasicOpenFile(RIPPLE_CAPTURE_STATUS_FILE,
                            O_RDWR | RIPPLE_BINARY);

    if(-1 == fd)
    {
        printf("capture state open file error %s\n", strerror(errno));
        exit(-1);
    }

    FileRead(fd, (char*)&mcapture, sizeof(mcapture));

    FileClose(fd);

    /* 输出内容 */
    printf("\n---------------RIPPLE PARSER INFO----------------\n");

    printf("capture redolsn         :%X/%X\n", (uint32)(mcapture.redolsn>>32),  (uint32)(mcapture.redolsn));
    printf("capture restartlsn      :%X/%X\n", (uint32)(mcapture.restartlsn>>32),  (uint32)(mcapture.restartlsn));
    printf("capture confirmlsn      :%X/%X\n", (uint32)(mcapture.confirmlsn>>32),  (uint32)(mcapture.confirmlsn));
    printf("capture loadlsn         :%X/%X\n", (uint32)(mcapture.loadlsn>>32),  (uint32)(mcapture.loadlsn));
    printf("capture parselsn        :%X/%X\n", (uint32)(mcapture.parselsn>>32),  (uint32)(mcapture.parselsn));
    printf("capture flushlsn        :%X/%X\n", (uint32)(mcapture.flushlsn>>32),  (uint32)(mcapture.flushlsn));
    printf("capture trail           :%lX/%lX\n", mcapture.trailno, mcapture.trailstart);
    printf("capture parsetimestamp  :%lu\n", mcapture.parsetimestamp);
    printf("capture flushtimestamp  :%lu\n", mcapture.flushtimestamp);
    
    printf("-------------------------------------------------\n");
    return true;
}

/* 获取状态信息 */
bool ripple_cmd_statuspump(void)
{
    int fd = -1;
    long    ripplepid;
    ripple_state_pump_state mpump = { 0 };
    
    ripplepid = ripple_misc_lockfiles_getpid();
    if(0 == ripplepid)
    {
        ripple_cmd_printmsg("Is ripple pump running?\n");
        return false;
    }

    fd = BasicOpenFile(RIPPLE_PUMP_STATUS_FILE,
                            O_RDWR | RIPPLE_BINARY);

    if(-1 == fd)
    {
        printf("pump state open file error %s\n", strerror(errno));
        exit(-1);
    }

    FileRead(fd, (char*)&mpump, sizeof(mpump));

    FileClose(fd);

    /* 输出内容 */
    printf("\n---------------RIPPLE PARSER INFO----------------\n");

    printf("pump loadlsn:           %X/%X", (uint32)(mpump.loadlsn >> 32), (uint32)(mpump.loadlsn));
    printf("pump sendlsn:           %X/%X", (uint32)(mpump.sendlsn >> 32), (uint32)(mpump.sendlsn));
    printf("pump loadtrail:         %lX/%lX", mpump.loadtrailno, mpump.loadtrailstart);
    printf("pump sendtrail:         %lX/%lX", mpump.sendtrailno, mpump.sendtrailstart);
    printf("pump loadtimestamp:     %lu", mpump.loadtimestamp);
    printf("pump sendtimestamp:     %lu", mpump.sendtimestamp);
    
    printf("-------------------------------------------------\n");
    return true;
}

/* 获取状态信息 */
bool ripple_cmd_statuscollector(void)
{
    int read = 0;
    int fd = -1;
    long    ripplepid;
    uint32 fileoffset = 0;
    ripple_metric_collectornode statusnode = { {0} };
    
    ripplepid = ripple_misc_lockfiles_getpid();
    if(0 == ripplepid)
    {
        ripple_cmd_printmsg("Is ripple collector running?\n");
        return false;
    }

    fd = BasicOpenFile(RIPPLE_COLLECTOR_STATUS_FILE,
                            O_RDWR | RIPPLE_BINARY);

    if(-1 == fd)
    {
        printf("collector state open file error %s\n", strerror(errno));
        exit(-1);
    }

    /* 输出内容 */
    printf("\n---------------RIPPLE PARSER INFO----------------\n");
    while((read = FilePRead(fd, (char*)&statusnode, sizeof(ripple_metric_collectornode), fileoffset)) > 0)
    {
        fileoffset += sizeof(statusnode);

        printf("-------- Collector Job: %s --------", statusnode.jobname);
        printf("collector recvlsn:          %X/%X", (uint32)(statusnode.recvlsn >> 32), (uint32)(statusnode.recvlsn));
        printf("collector flushlsn:         %X/%X", (uint32)(statusnode.flushlsn >> 32), (uint32)(statusnode.flushlsn));
        printf("collector recvtrail:        %lX/%lX", statusnode.recvtrailno, statusnode.recvtrailstart);
        printf("collector flushtrail:       %lX/%lX", statusnode.flushtrailno, statusnode.flushtrailstart);
        printf("collector recvtimestamp:    %lu", statusnode.recvtimestamp);
        printf("collector flushtimestamp:   %lu", statusnode.flushtimestamp);
        rmemset1(&statusnode, 0, '\0', sizeof(ripple_metric_collectornode));
    }

    if (read < 0)
    {
        printf("collector state read file error %s\n", strerror(errno));
    }
    printf("-------------------------------------------------\n");

    FileClose(fd);
    return true;
}

/* 获取状态信息 */
bool ripple_cmd_statusintegrate(void)
{
    int fd = -1;
    long    ripplepid;
    ripple_metric_integrate mintegrate = { 0 };
    
    ripplepid = ripple_misc_lockfiles_getpid();
    if(0 == ripplepid)
    {
        ripple_cmd_printmsg("Is ripple integrate running?\n");
        return false;
    }

    fd = BasicOpenFile(RIPPLE_INTEGRATE_STATUS_FILE,
                            O_RDWR | RIPPLE_BINARY);

    if(-1 == fd)
    {
        printf("integrate state open file error %s\n", strerror(errno));
        exit(-1);
    }

    FileRead(fd, (char*)&mintegrate, sizeof(mintegrate));

    FileClose(fd);

    /* 输出内容 */
    printf("\n---------------RIPPLE PARSER INFO----------------\n");

    printf("integrate loadlsn:           %X/%X", (uint32)(mintegrate.loadlsn >> 32), (uint32)(mintegrate.loadlsn));
    printf("integrate synclsn:           %X/%X", (uint32)(mintegrate.synclsn >> 32), (uint32)(mintegrate.synclsn));
    printf("integrate loadtrail:         %lX/%lX", mintegrate.loadtrailno, mintegrate.loadtrailno);
    printf("integrate synctrail:         %lX/%lX", mintegrate.synctrailno, mintegrate.synctrailstart);
    printf("integrate loadTimestamp:     %lu", mintegrate.loadtimestamp);
    printf("integrate syncTimestamp:     %lu", mintegrate.synctimestamp);

    printf("-------------------------------------------------\n");
    return true;
}

/* 状态命令 */
bool ripple_cmd_status(void *extra_config)
{
    RIPPLE_UNUSED(extra_config);
    if(NULL == m_typ2status[g_proctype].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2status[g_proctype].errmsg);
        return false;
    }

    m_typ2status[g_proctype].func();
    return true;
}