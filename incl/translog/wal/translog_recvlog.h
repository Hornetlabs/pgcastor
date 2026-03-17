#ifndef _TRANSLOG_RECVLOG_H_
#define _TRANSLOG_RECVLOG_H_

#define XLOG_BLKSIZE                     8192
#define PGWALSEGMENTSPERXLOGID(segsize)         (UINT64CONST(0x100000000)/(segsize))
#define PGWALBYTETOSEG(lsn, segsize)            (lsn / segsize)
#define PGWALSEGMENTOFFSET(lsn, segsize)        ((lsn) & ((segsize) - 1))

typedef struct TRANSLOG_RECVLOG
{
    /* fde 加密 */
    bool                                enablefde;

    /* 用于标记是否为主动发起 done, 0 未发送, 1 已发送 */
    bool                                senddone;

    /* 数据库类型 */
    translog_recvlog_dbtype      dbtype;

    /* 数据库版本 */
    translog_recvlog_dbversion   dbversion;

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
} translog_recvlog;

/* 初始化结构 */
translog_recvlog* translog_recvlog_init(void);

/* timeline */
void translog_recvlog_settli(translog_recvlog* recvwal, TimeLineID tli);

/* 设置 startpos */
void translog_recvlog_setstartpos(translog_recvlog* recvwal, XLogRecPtr lsn);

/* 数据库 timeline */
void translog_recvlog_setdbtli(translog_recvlog* recvwal, TimeLineID tli);

/* 设置 segsize */
void translog_recvlog_setsegsize(translog_recvlog* recvwal, uint32 segsize);

/* 设置 dbtype */
void translog_recvlog_setdbtype(translog_recvlog* recvwal, translog_recvlog_dbtype dbtype);

/* 设置 data 目录 */
bool translog_recvlog_setdata(translog_recvlog* recvwal, char* data);

/* 设置 sysidentifier */
bool translog_recvlog_setsysidentifier(translog_recvlog* recvwal, char* sysidentifier);

/* 设置 slotname 目录 */
bool translog_recvlog_setslotname(translog_recvlog* recvwal, char* slotname);

/* 设置 restorcommand 目录 */
bool translog_recvlog_setrestorecmd(translog_recvlog* recvwal, char* restorecmd);

/* 流复制接收日志 */
bool translog_recvlog_main(translog_recvlog* recvwal);

/* 释放 */
void translog_recvlog_free(translog_recvlog* recvwal);

#endif
