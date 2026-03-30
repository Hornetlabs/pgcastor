#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "misc/misc_lockfiles.h"
#include "net/netpacket/netpacket.h"
#include "metric/capture/metric_capture.h"
#include "metric/integrate/metric_integrate.h"
#include "command/cmd.h"

typedef bool (*procstatus)();

#define procstatusprint procstatus

typedef struct RPOC2STATUS
{
    proc_type       type;
    procstatus      func;
    procstatusprint printfunc;
    char*           errmsg;
} proc2status;

static bool cmd_statuscapture(void);
static bool cmd_statusintegrate(void);

static proc2status m_typ2status[] = {
    {PROC_TYPE_NOP,       NULL,                NULL, "proc nop unsupport status"},
    {PROC_TYPE_CAPTURE,   cmd_statuscapture,   NULL, "capture status error"     },
    {PROC_TYPE_INTEGRATE, cmd_statusintegrate, NULL, "tegrate status error"     }
};

/* Get status information */
bool cmd_statuscapture(void)
{
    int            fd = -1;
    long           ripplepid;
    metric_capture mcapture = {0};

    ripplepid = misc_lockfiles_getpid();
    if (0 == ripplepid)
    {
        cmd_printmsg("Is ripple capture running?\n");
        return false;
    }

    fd = osal_basic_open_file(CAPTURE_STATUS_FILE, O_RDWR | BINARY);

    if (-1 == fd)
    {
        printf("capture state open file error %s\n", strerror(errno));
        exit(-1);
    }

    osal_file_read(fd, (char*)&mcapture, sizeof(mcapture));

    osal_file_close(fd);

    /* Output content */
    printf("\n---------------RIPPLE PARSER INFO----------------\n");

    printf("capture redolsn         :%X/%X\n", (uint32)(mcapture.redolsn >> 32), (uint32)(mcapture.redolsn));
    printf("capture restartlsn      :%X/%X\n", (uint32)(mcapture.restartlsn >> 32), (uint32)(mcapture.restartlsn));
    printf("capture confirmlsn      :%X/%X\n", (uint32)(mcapture.confirmlsn >> 32), (uint32)(mcapture.confirmlsn));
    printf("capture loadlsn         :%X/%X\n", (uint32)(mcapture.loadlsn >> 32), (uint32)(mcapture.loadlsn));
    printf("capture parselsn        :%X/%X\n", (uint32)(mcapture.parselsn >> 32), (uint32)(mcapture.parselsn));
    printf("capture flushlsn        :%X/%X\n", (uint32)(mcapture.flushlsn >> 32), (uint32)(mcapture.flushlsn));
    printf("capture trail           :%lX/%lX\n", mcapture.trailno, mcapture.trailstart);
    printf("capture parsetimestamp  :%lu\n", mcapture.parsetimestamp);
    printf("capture flushtimestamp  :%lu\n", mcapture.flushtimestamp);

    printf("-------------------------------------------------\n");
    return true;
}

/* Get status information */
bool cmd_statusintegrate(void)
{
    int              fd = -1;
    long             ripplepid;
    metric_integrate mintegrate = {0};

    ripplepid = misc_lockfiles_getpid();
    if (0 == ripplepid)
    {
        cmd_printmsg("Is ripple integrate running?\n");
        return false;
    }

    fd = osal_basic_open_file(INTEGRATE_STATUS_FILE, O_RDWR | BINARY);

    if (-1 == fd)
    {
        printf("integrate state open file error %s\n", strerror(errno));
        exit(-1);
    }

    osal_file_read(fd, (char*)&mintegrate, sizeof(mintegrate));

    osal_file_close(fd);

    /* Output content */
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

/* Status command */
bool cmd_status(void* extra_config)
{
    UNUSED(extra_config);
    if (NULL == m_typ2status[g_proctype].func)
    {
        elog(RLOG_WARNING, "%s", m_typ2status[g_proctype].errmsg);
        return false;
    }

    m_typ2status[g_proctype].func();
    return true;
}