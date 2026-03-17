#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/dttime/dttimestamp.h"
#include "utils/conn/conn.h"
#include "utils/init/databaserecv.h"
#include "translog/translog_recvlogdb.h"
#include "translog/wal/translog_walcontrol.h"
#include "translog/wal/translog_waltimeline.h"
#include "translog/wal/translog_walmsg.h"
#include "translog/wal/translog_recvlog.h"
#include "translog/wal/translog_recvpglog.h"

/* 生成事务日志空文件 */
static bool translog_recvpglog_initemptywalfile(translog_recvlog* recvwal)
{
    uint64 segno                        = 0;
    char xlogfile[MAXPATH]       = { 0 };
    char blkdata[XLOG_BLKSIZE]   = { 0 };

    struct stat st;

    segno = PGWALBYTETOSEG(recvwal->startpos, recvwal->segsize);
    snprintf(xlogfile,
             MAXPATH,
             "%s/%08X%08X%08X",                         /* 目录/timeline segno size */
             recvwal->data,
             recvwal->tli,
             (uint32)(( segno) / PGWALSEGMENTSPERXLOGID(recvwal->segsize)),
             (uint32)(( segno) % PGWALSEGMENTSPERXLOGID(recvwal->segsize)));

    recvwal->segno = segno;
    /* 文件存在, 不做处理 */
    if (0 == stat(xlogfile, &st))
    {
        return true;
    }

    /* 查看错误码 */
    if (ENOENT != errno)
    {
        elog(RLOG_WARNING, "stat file %s error:%s", xlogfile, strerror(errno));
        return false;
    }

    /* 写空文件 */
    if(false == osal_create_file_with_size(xlogfile,
                                   O_RDWR | O_CREAT | O_EXCL | BINARY,
                                   recvwal->segsize,
                                   XLOG_BLKSIZE,
                                   (uint8*)blkdata))
    {
        elog(RLOG_WARNING, "create empty file %s error", xlogfile);
        return false;
    }

    return true;
}

/* 打开文件 */
static bool translog_recvpglog_openwalfile(translog_recvlog* recvwal)
{
    char xlogfile[MAXPATH]       = { 0 };

    /* 生成空文件 */
    if(false == translog_recvpglog_initemptywalfile(recvwal))
    {
        elog(RLOG_WARNING, "open wal file error");
        return false;
    }

    snprintf(xlogfile,
             MAXPATH,
             "%s/%08X%08X%08X",                         /* 目录/timeline segno size */
             recvwal->data,
             recvwal->tli,
             (uint32)(( recvwal->segno) / PGWALSEGMENTSPERXLOGID(recvwal->segsize)),
             (uint32)(( recvwal->segno) % PGWALSEGMENTSPERXLOGID(recvwal->segsize)));

    recvwal->fd = osal_basic_open_file(xlogfile, O_RDWR | BINARY);
    if(-1 == recvwal->fd)
    {
        elog(RLOG_WARNING, "open file error, %s", xlogfile);
        return false;
    }
    return true;
}

/* 解析下个时间线和开始位置 */
static bool translog_recvpglog_parsenewpos(PGresult* res, TimeLineID* ptli, XLogRecPtr* pstartpos)
{
    uint32 xlogid   = InvalidXLogRecPtr;
    uint32 xrecoff  = InvalidXLogRecPtr;

    if (2 > PQnfields(res) || 1 != PQntuples(res))
    {
        elog(RLOG_WARNING,
             "unexpected result set after end-of-timeline: got %d rows and %d fields, expected %d rows and %d fields",
             PQntuples(res), PQnfields(res), 1, 2);
        return false;
    }

    *ptli = atoi(PQgetvalue(res, 0, 0));
    if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &xlogid, &xrecoff) != 2)
    {
        elog(RLOG_WARNING,"could not parse next timeline's starting point %s", PQgetvalue(res, 0, 1));
        return false;
    }
    *pstartpos = ((uint64) xlogid << 32) | xrecoff;
    return true;
}

/*
 * 处理数据
*/
static bool translog_recvpglog_datamsg(translog_recvlog* recvwal, char* buffer, int blen)
{
    /*
     * PG 12 的消息格式
     *  0                       消息类型, 'w'
     *  1---8                   datastart
     *  9---16                  walend
     *  17--24                  sendtime
     */
    int hdrlen              = 0;
    int xlogoff             = 0;
    int byteswrite          = 0;
    char* cptr              = NULL;
    XLogRecPtr datastartlsn = InvalidXLogRecPtr;

    hdrlen = (1 + 8 + 8 + 8);

    /* 获取起始 lsn */
    cptr = buffer;
    cptr++;
    rmemcpy1(&datastartlsn, 0, cptr, 8);
    datastartlsn = r_ntoh64(datastartlsn);

    if (recvwal->startpos != datastartlsn)
    {
        elog(RLOG_WARNING,
             "got stream lsn %08X/%08X, expected %08X/%08X",
             (uint32)(datastartlsn>>32),
             (uint32)datastartlsn,
             (uint32)(recvwal->startpos>>32),
             (uint32)recvwal->startpos);

        return false;
    }

    if (-1 == recvwal->fd)
    {
        /* 打开文件 */
        if(false == translog_recvpglog_openwalfile(recvwal))
        {
            elog(RLOG_WARNING, "open wal file error");
            return false;
        }
    }

    /*
     * 写文件
     *  写文件时要考虑到跨文件的场景
     */
    blen -= hdrlen;
    cptr += (hdrlen - 1);
    xlogoff = PGWALSEGMENTOFFSET(datastartlsn, recvwal->segsize);

    while (0 < blen)
    {
        if ((xlogoff + blen) > recvwal->segsize)
        {
            byteswrite = (recvwal->segsize - xlogoff);
        }
        else
        {
            byteswrite = blen;
        }

        /* 将 wal 数据落盘 */
        byteswrite = osal_file_write(recvwal->fd, cptr, byteswrite);
        if (-1 == byteswrite)
        {
            elog(RLOG_WARNING, "write data to wal file error, %s", strerror(errno));
            return false;
        }

        xlogoff += byteswrite;
        blen -= byteswrite;
        cptr += byteswrite;
        recvwal->startpos += byteswrite;

        /* 校验是否发生了文件切换 */
        if (0 == PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize))
        {
            /* 文件切换 */
            osal_file_close(recvwal->fd);
            recvwal->fd = -1;
            xlogoff = 0;

            /* 打开新文件 */
            if(false == translog_recvpglog_openwalfile(recvwal))
            {
                elog(RLOG_WARNING, "open wal file error");
                return false;
            }
        }
    }

    return true;
}

/*
 * 心跳
*/
static bool translog_recvpglog_keepalivemsg(translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen)
{
    /*
     * 源端发送的消息格式为:
     *  1               msgtype
     *  8               walend              最后一个包中的 endlsn
     *  8               timestamp           组装包时的时间戳
     *  1               reply               用于标识是否需要发送心跳包
     */
    bool reply          = false;
    char* cptr          = NULL;

    cptr = buffer;
    cptr += (1 + 8 + 8);

    /* 查看是否需要返回 reply */
    reply = cptr[0];

    if (false == reply)
    {
        return true;
    }

    /* 组装心跳包并发送 */
    if(false == translog_walmsg_sendkeepalivemsg(recvwal->startpos, conn))
    {
        elog(RLOG_WARNING, "recvwal keepalive msg type error");
        return false;
    }
    return true;
}

/* 根据消息类型处理 */
bool translog_recvpglog_msgop(translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen)
{
    if (PG_REPLICATION_MSGTYPE_LK == buffer[0])
    {
        /* 保活 */
        if(false == translog_recvpglog_keepalivemsg(recvwal, conn, buffer, blen))
        {
            elog(RLOG_WARNING, "keepalive error");
            return false;
        }
    }
    else if (PG_REPLICATION_MSGTYPE_LW == buffer[0])
    {
        /* 数据流 */
        if(false == translog_recvpglog_datamsg(recvwal, buffer, blen))
        {
            elog(RLOG_WARNING, "write wal file error");
            return false;
        }
    }
    return true;
}

/* 获取版本 */
bool translog_recvpglog_getpgversion(PGconn* conn, translog_recvlog_dbversion* dbversion)
{
    *dbversion = (PQserverVersion(conn) / (100*100));
    return true;
}

/*
 * 用于处理源端发送的 'c' 消息,两种场景
 *  1、主动发送
 *      recivewal 主动发送了 'c' 消息, 此时只需要退出即可
 *  2、被动发送
 *    检测 PGresult->resultStatus 的状态值
 *      当开始流复制时, resultstatus 的值为 PGRES_COPY_BOTH
 *      源端结束流复制时, resultstatus 的状态会转换为 PGRES_COPY_IN, 所以在此处需要判断 resultstatus 的状态
*/
bool translog_recvpglog_endreplication(translog_recvlog* recvwal,
                                              translog_walcontrol* walctrl,
                                              PGconn* conn,
                                              bool* endcommand,
                                              int* error)
{
    XLogRecPtr startpos                 = InvalidXLogRecPtr;
    TimeLineID tli                      = InvalidTimeLineID;
    PGresult* res                       = NULL;

    *endcommand = false;
    if(NULL == conn || 1 == recvwal->senddone)
    {
        return true;
    }

    /* 获取状态 */
    res = PQgetResult(conn);
    /* 被动断开 */
    if (PGRES_COPY_IN == PQresultStatus(res))
    {
        /*
         * 发送结束标识, 并获取返回信息
         */
        if(0 == recvwal->senddone)
        {
            if(false == translog_walmsg_senddone(conn))
            {
                elog(RLOG_WARNING, "endreplication error");
                PQclear(res);
                return false;
            }
            recvwal->senddone = 1;
            res = PQgetResult(conn);
        }
    }

    /* 再次查看返回类型并处理 */
    if (PGRES_TUPLES_OK == PQresultStatus(res))
    {
        /* 到达了时间线的结束位置, 那么获取下个时间线 */
        if(false == translog_recvpglog_parsenewpos(res, &tli, &startpos))
        {
            elog(RLOG_WARNING, "parse new pos error");
            PQclear(res);
            return false;
        }

        /* 更新 recvwal 中的 startpos 和 tli 并落盘 */
        translog_recvlog_setstartpos(recvwal, startpos);
        translog_recvlog_settli(recvwal, tli);
        translog_walcontrol_setstartpos(walctrl, startpos);
        translog_walcontrol_settli(walctrl, tli);

        /* 写 control 文件 */
        translog_walcontrol_flush(walctrl, recvwal->data);

        elog(RLOG_WARNING,
             "receivewal will shift replication timeline:%u, pos:%08X/%08X",
             recvwal->tli,
             (uint32)(recvwal->startpos>>32),
             (uint32)recvwal->startpos);
    }
    else if (PGRES_COMMAND_OK == PQresultStatus(res))
    {
        /* 服务端完全退出 */
        *endcommand = true;
        recvwal->senddone = 0;

        elog(RLOG_INFO,
             "stop replication at timeline:%u, pos:%08X/%08X, database timelie:%u",
             recvwal->tli,
             (uint32)(recvwal->startpos>>32),
             (uint32)recvwal->startpos,
             recvwal->dbtli);
    }
    else
    {
        elog(RLOG_WARNING,
             "unexpected termination of replication stream: %s, %s",
             PQresultErrorMessage(res),
             PQresultErrorField(res, PG_DIAG_SQLSTATE));

        *error = ERROR_SUCCESS;
        if(0 == strcmp(PG_ERROR_FILEREMOVED, PQresultErrorField(res, PG_DIAG_SQLSTATE)))
        {
            *error = ERROR_FILEREMOVED;
        }
        
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}
