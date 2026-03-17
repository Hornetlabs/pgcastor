#ifndef _TRANSLOG_WALCONTROL_H_
#define _TRANSLOG_WALCONTROL_H_

typedef enum TRANSLOG_WALCONTROL_STAT
{
    /* 初始化 */
    TRANSLOG_WALCONTROL_STAT_INIT               = 0x00,

    /* 恢复状态 */
    TRANSLOG_WALCONTROL_STAT_RECOVERY           ,

    /* 工作状态 */
    TRANSLOG_WALCONTROL_STAT_WORK               ,

    /* 关闭 */
    TRANSLOG_WALCONTROL_STAT_SHUTDOWN
} translog_walcontrol_stat;

typedef struct TRANSLOG_WALCONTROL
{
    /* 状态 */
    translog_walcontrol_stat    stat;

    /* 日志大小 */
    uint32                              segsize;

    /* 流复制起点 */
    XLogRecPtr                          startpos;

    /* 正在同步的时间线 */
    TimeLineID                          tli;

    /* 数据库的时间线 */
    TimeLineID                          dbtli;

    /* slotname */
    char                                slotname[NAMEDATALEN];

    /* restorecommand */
    char                                restorecmd[MAXPATH];
} translog_walcontrol;

/* 设置流复制的起始 lsn */
void translog_walcontrol_setstartpos(translog_walcontrol* walctrl, XLogRecPtr lsn);

/* 设置流复制的 timeline */
void translog_walcontrol_settli(translog_walcontrol* walctrl, TimeLineID tli);

/* 设置流复制中数据库当前的 timeline */
void translog_walcontrol_setdbtli(translog_walcontrol* walctrl, TimeLineID tli);

/* 设置流复制的起始 lsn */
void translog_walcontrol_setsegsize(translog_walcontrol* walctrl, uint32 segsize);

/* 设置复制槽名称 */
void translog_walcontrol_setslotname(translog_walcontrol* walctrl, char* slotname);

/* 设置restore command */
void translog_walcontrol_setrestorecmd(translog_walcontrol* walctrl, char* restorecmd);

/* 加载 Control 文件 */
translog_walcontrol* translog_walcontrol_load(char* abspath);

/* Control 文件落盘 */
bool translog_walcontrol_flush(translog_walcontrol* walctrl, char* data);

#endif
