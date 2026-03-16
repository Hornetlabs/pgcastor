#ifndef _RIPPLE_TRANSLOG_RECVLOG_H_
#define _RIPPLE_TRANSLOG_RECVLOG_H_

#define RIPPLE_XLOG_BLKSIZE                     8192
#define PGWALSEGMENTSPERXLOGID(segsize)         (UINT64CONST(0x100000000)/(segsize))
#define PGWALBYTETOSEG(lsn, segsize)            (lsn / segsize)
#define PGWALSEGMENTOFFSET(lsn, segsize)        ((lsn) & ((segsize) - 1))

typedef struct RIPPLE_TRANSLOG_RECVLOG
{
    /* fde 加密 */
    bool                                enablefde;

    /* 用于标记是否为主动发起 done, 0 未发送, 1 已发送 */
    bool                                senddone;

    /* 数据库类型 */
    ripple_translog_recvlog_dbtype      dbtype;

    /* 数据库版本 */
    ripple_translog_recvlog_dbversion   dbversion;

    /* 日志文件描述符 */
    int                                 fd;

    /* 事务日志大小 */
    uint32                              segsize;

    /* 同步时间线 */
    TimeLineID                          tli;

    /* 数据库时间线 */
    TimeLineID                          dbtli;

    /* 事务日志编号 */
    uint64                              segno;

    /* 同步的 LSN */
    XLogRecPtr                          startpos;

    /* 数据库连接串 */
    /* 工作目录 */
    char*                               data;

    /* 数据库实例标识 */
    char*                               sysidentifier;

    /* slotname */
    char*                               slotname;

    /* restorecommand */
    char*                               restorecmd;
} ripple_translog_recvlog;

/* 初始化结构 */
ripple_translog_recvlog* ripple_translog_recvlog_init(void);

/* timeline */
void ripple_translog_recvlog_settli(ripple_translog_recvlog* recvwal, TimeLineID tli);

/* 设置 startpos */
void ripple_translog_recvlog_setstartpos(ripple_translog_recvlog* recvwal, XLogRecPtr lsn);

/* 数据库 timeline */
void ripple_translog_recvlog_setdbtli(ripple_translog_recvlog* recvwal, TimeLineID tli);

/* 设置 segsize */
void ripple_translog_recvlog_setsegsize(ripple_translog_recvlog* recvwal, uint32 segsize);

/* 设置 dbtype */
void ripple_translog_recvlog_setdbtype(ripple_translog_recvlog* recvwal, ripple_translog_recvlog_dbtype dbtype);

/* 设置 data 目录 */
bool ripple_translog_recvlog_setdata(ripple_translog_recvlog* recvwal, char* data);

/* 设置 sysidentifier */
bool ripple_translog_recvlog_setsysidentifier(ripple_translog_recvlog* recvwal, char* sysidentifier);

/* 设置 slotname 目录 */
bool ripple_translog_recvlog_setslotname(ripple_translog_recvlog* recvwal, char* slotname);

/* 设置 restorcommand 目录 */
bool ripple_translog_recvlog_setrestorecmd(ripple_translog_recvlog* recvwal, char* restorecmd);

/* 流复制接收日志 */
bool ripple_translog_recvlog_main(ripple_translog_recvlog* recvwal);

/* 释放 */
void ripple_translog_recvlog_free(ripple_translog_recvlog* recvwal);

#endif
