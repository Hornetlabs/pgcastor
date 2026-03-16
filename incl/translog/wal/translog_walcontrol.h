#ifndef _RIPPLE_TRANSLOG_WALCONTROL_H_
#define _RIPPLE_TRANSLOG_WALCONTROL_H_

typedef enum RIPPLE_TRANSLOG_WALCONTROL_STAT
{
    /* 初始化 */
    RIPPLE_TRANSLOG_WALCONTROL_STAT_INIT               = 0x00,

    /* 恢复状态 */
    RIPPLE_TRANSLOG_WALCONTROL_STAT_RECOVERY           ,

    /* 工作状态 */
    RIPPLE_TRANSLOG_WALCONTROL_STAT_WORK               ,

    /* 关闭 */
    RIPPLE_TRANSLOG_WALCONTROL_STAT_SHUTDOWN
} ripple_translog_walcontrol_stat;

typedef struct RIPPLE_TRANSLOG_WALCONTROL
{
    /* 状态 */
    ripple_translog_walcontrol_stat    stat;

    /* 日志大小 */
    uint32                              segsize;

    /* 流复制起点 */
    XLogRecPtr                          startpos;

    /* 正在同步的时间线 */
    TimeLineID                          tli;

    /* 数据库的时间线 */
    TimeLineID                          dbtli;

    /* slotname */
    char                                slotname[RIPPLE_NAMEDATALEN];

    /* restorecommand */
    char                                restorecmd[RIPPLE_MAXPATH];
} ripple_translog_walcontrol;

/* 设置流复制的起始 lsn */
void ripple_translog_walcontrol_setstartpos(ripple_translog_walcontrol* walctrl, XLogRecPtr lsn);

/* 设置流复制的 timeline */
void ripple_translog_walcontrol_settli(ripple_translog_walcontrol* walctrl, TimeLineID tli);

/* 设置流复制中数据库当前的 timeline */
void ripple_translog_walcontrol_setdbtli(ripple_translog_walcontrol* walctrl, TimeLineID tli);

/* 设置流复制的起始 lsn */
void ripple_translog_walcontrol_setsegsize(ripple_translog_walcontrol* walctrl, uint32 segsize);

/* 设置复制槽名称 */
void ripple_translog_walcontrol_setslotname(ripple_translog_walcontrol* walctrl, char* slotname);

/* 设置restore command */
void ripple_translog_walcontrol_setrestorecmd(ripple_translog_walcontrol* walctrl, char* restorecmd);

/* 加载 Control 文件 */
ripple_translog_walcontrol* ripple_translog_walcontrol_load(char* abspath);

/* Control 文件落盘 */
bool ripple_translog_walcontrol_flush(ripple_translog_walcontrol* walctrl, char* data);

#endif
