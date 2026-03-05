#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/dttime/dttimestamp.h"
#include "utils/conn/ripple_conn.h"
#include "utils/init/ripple_databaserecv.h"
#include "translog/ripple_translog_recvlogdb.h"
#include "translog/wal/ripple_translog_walcontrol.h"
#include "translog/wal/ripple_translog_waltimeline.h"
#include "translog/wal/ripple_translog_walmsg.h"
#include "translog/wal/ripple_translog_recvlog.h"
#include "translog/wal/ripple_translog_recvhglog.h"


/* 生成事务日志空文件 */
static bool ripple_translog_recvhglog_initemptywalfile(ripple_translog_recvlog* recvwal)
{
    uint64 segno                        = 0;
    char xlogfile[RIPPLE_MAXPATH]       = { 0 };
    char blkdata[RIPPLE_XLOG_BLKSIZE]   = { 0 };

    struct stat st;

    segno = PGWALBYTETOSEG(recvwal->startpos, recvwal->segsize);
    snprintf(xlogfile,
             RIPPLE_MAXPATH,
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
    if(false == CreateFileWithSize(xlogfile,
                                   O_RDWR | O_CREAT | O_EXCL | RIPPLE_BINARY,
                                   recvwal->segsize,
                                   RIPPLE_XLOG_BLKSIZE,
                                   (uint8*)blkdata))
    {
        elog(RLOG_WARNING, "create empty file %s error", xlogfile);
        return false;
    }

    return true;
}

/* 打开文件 */
static bool ripple_translog_recvhglog_openwalfile(ripple_translog_recvlog* recvwal)
{
    char xlogfile[RIPPLE_MAXPATH]       = { 0 };

    /* 生成空文件 */
    if(false == ripple_translog_recvhglog_initemptywalfile(recvwal))
    {
        elog(RLOG_WARNING, "open wal file error");
        return false;
    }

    snprintf(xlogfile,
             RIPPLE_MAXPATH,
             "%s/%08X%08X%08X",                         /* 目录/timeline segno size */
             recvwal->data,
             recvwal->tli,
             (uint32)(( recvwal->segno) / PGWALSEGMENTSPERXLOGID(recvwal->segsize)),
             (uint32)(( recvwal->segno) % PGWALSEGMENTSPERXLOGID(recvwal->segsize)));

    recvwal->fd = BasicOpenFile(xlogfile, O_RDWR | RIPPLE_BINARY);
    if(-1 == recvwal->fd)
    {
        elog(RLOG_WARNING, "open file error, %s", xlogfile);
        return false;
    }
    return true;
}

/* 执行 SHOW server_version */
static bool ripple_translog_recvhglog_showserverversion(PGconn* conn, char** strversion)
{
    if(false == ripple_databaserecv_showserverversion(conn, strversion))
    {
        elog(RLOG_WARNING, "show server version error");
    }

    return true;
}

/* 解析下个时间线和开始位置 */
static bool ripple_translog_recvhglog_parsenewpos(PGresult* res, TimeLineID* ptli, XLogRecPtr* pstartpos)
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
 * 心跳
*/
static bool ripple_translog_recvhglog_keepalivemsg(ripple_translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen)
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
    if(false == ripple_translog_walmsg_sendkeepalivemsg(recvwal->startpos, conn))
    {
        elog(RLOG_WARNING, "recvwal keepalive msg type error");
        return false;
    }
    return true;
}


/*
 * 4510处理数据
*/
static bool ripple_translog_recvhglog_4510datamsg(ripple_translog_recvlog* recvwal, char* buffer, int blen)
{
    /*
     * PG 12 的消息格式
     *  0                       消息类型, 'w'
     *  1---8                   datastart
     *  9---16                  walend
     *  17--24                  sendentptr               --只有开启了 FDE 才会有此字段
     *  25--32                  sendtime
     */
    int hdrlen                  = 0;
    int xlogoff                 = 0;
    int byteswrite              = 0;
    int wlen                    = 0;
    char* cptr                  = NULL;
    XLogRecPtr dataendlsn       = InvalidXLogRecPtr;
    XLogRecPtr datastartlsn     = InvalidXLogRecPtr;

    wlen = blen;
    if (false == recvwal->enablefde)
    {
        hdrlen = (1 + 8 + 8 + 8);
    }
    else
    {
        hdrlen = (1 + 8 + 8 + 8 + 8);
    }

    /* 换算真实数据的长度 */
    wlen -= hdrlen;

    /* 获取起始 lsn */
    cptr = buffer;

    /* 偏移msgtype */
    cptr++;
    rmemcpy1(&datastartlsn, 0, cptr, 8);
    datastartlsn = r_ntoh64(datastartlsn);

    /*
     * 换算结束 lsn
     * 1、偏移 startlsn/walend
     * 2、开启 FDE, 那么在数据流中获取
     * 3、未开启 FDE, 换算 endlsn, startlsn + 数据长度
     */
    cptr += (8 + 8);
    if (true == recvwal->enablefde)
    {
        rmemcpy1(&dataendlsn, 0, cptr, 8);
        dataendlsn = r_ntoh64(dataendlsn);
        cptr += 8;
    }
    else
    {
        dataendlsn = datastartlsn + wlen;
    }

    /* time */
    cptr += 8;

    /* 在瀚高数据库中, startpos 只会大于等于 datastartlsn */
    if (recvwal->startpos < datastartlsn)
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
        if(false == ripple_translog_recvhglog_openwalfile(recvwal))
        {
            elog(RLOG_WARNING, "open wal file error");
            return false;
        }
    }

    /*
     * 写文件
     *  写文件时要考虑到跨文件的场景
     */
    xlogoff = PGWALSEGMENTOFFSET(datastartlsn, recvwal->segsize);
    if (-1 == FileSeek(recvwal->fd, xlogoff))
    {
        elog(RLOG_WARNING, "seek error");
        return false;
    }

    while (0 < wlen)
    {
        if ((xlogoff + wlen) > recvwal->segsize)
        {
            byteswrite = (recvwal->segsize - xlogoff);
        }
        else
        {
            byteswrite = wlen;
        }

        /* 将 wal 数据落盘 */
        byteswrite = FileWrite(recvwal->fd, cptr, byteswrite);
        if (-1 == byteswrite)
        {
            elog(RLOG_WARNING, "write data to wal file error, %s", strerror(errno));
            return false;
        }

        xlogoff += byteswrite;
        wlen -= byteswrite;
        cptr += byteswrite;
        datastartlsn += byteswrite;

        /* 校验是否发生了文件切换 */
        if (0 == PGWALSEGMENTOFFSET(datastartlsn, recvwal->segsize))
        {
            /* 文件切换 */
            FileClose(recvwal->fd);
            recvwal->fd = -1;
            xlogoff = 0;

            /* 打开新文件 */
            /* 暂时设置未 startlsn */
            recvwal->startpos = datastartlsn;
            if(false == ripple_translog_recvhglog_openwalfile(recvwal))
            {
                elog(RLOG_WARNING, "open wal file error");
                return false;
            }
        }
    }

    recvwal->startpos = dataendlsn;

    return true;
}


/* 根据消息类型处理 */
bool ripple_translog_recvhglog_4510msgop(ripple_translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen)
{
    if (PG_REPLICATION_MSGTYPE_LK == buffer[0])
    {
        /* 保活 */
        if(false == ripple_translog_recvhglog_keepalivemsg(recvwal, conn, buffer, blen))
        {
            elog(RLOG_WARNING, "keepalive error");
            return false;
        }
    }
    else if (PG_REPLICATION_MSGTYPE_LW == buffer[0])
    {
        /* 数据流 */
        if(false == ripple_translog_recvhglog_4510datamsg(recvwal, buffer, blen))
        {
            elog(RLOG_WARNING, "write wal file error");
            return false;
        }
    }
    return true;
}

/*
 * 4511处理数据
*/
static bool ripple_translog_recvhglog_4511datamsg(ripple_translog_recvlog* recvwal, char* buffer, int blen)
{
    /*
     * PG 12 的消息格式
     *  0                       消息类型, 'w'
     *  1---8                   datastart
     *  9---16                  walend
     *  25--32                  sendtime
     */
    int hdrlen                  = 0;
    int xlogoff                 = 0;
    int byteswrite              = 0;
    int wlen                    = 0;
    char* cptr                  = NULL;
    XLogRecPtr datastartlsn     = InvalidXLogRecPtr;

    wlen = blen;
    hdrlen = (1 + 8 + 8 + 8);

    /* 换算真实数据的长度 */
    wlen -= hdrlen;

    /* 获取起始 lsn */
    cptr = buffer;

    /* 偏移msgtype */
    cptr++;
    rmemcpy1(&datastartlsn, 0, cptr, 8);
    datastartlsn = r_ntoh64(datastartlsn);

    /*
     * 换算结束 lsn
     * 偏移 startlsn/walend/time
     */
    cptr += (8 + 8 + 8);

    /* 在瀚高数据库中, startpos 只会大于等于 datastartlsn */
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
        if(false == ripple_translog_recvhglog_openwalfile(recvwal))
        {
            elog(RLOG_WARNING, "open wal file error");
            return false;
        }
    }

    /*
     * 写文件
     *  写文件时要考虑到跨文件的场景
     */
    xlogoff = PGWALSEGMENTOFFSET(datastartlsn, recvwal->segsize);

    while (0 < wlen)
    {
        if ((xlogoff + wlen) > recvwal->segsize)
        {
            byteswrite = (recvwal->segsize - xlogoff);
        }
        else
        {
            byteswrite = wlen;
        }

        /* 将 wal 数据落盘 */
        byteswrite = FileWrite(recvwal->fd, cptr, byteswrite);
        if (-1 == byteswrite)
        {
            elog(RLOG_WARNING, "write data to wal file error, %s", strerror(errno));
            return false;
        }

        xlogoff += byteswrite;
        wlen -= byteswrite;
        cptr += byteswrite;
        recvwal->startpos += byteswrite;

        /* 校验是否发生了文件切换 */
        if (0 == PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize))
        {
            /* 文件切换 */
            FileClose(recvwal->fd);
            recvwal->fd = -1;
            xlogoff = 0;

            /* 打开新文件 */
            if(false == ripple_translog_recvhglog_openwalfile(recvwal))
            {
                elog(RLOG_WARNING, "open wal file error");
                return false;
            }
        }
    }
    return true;
}

/* 根据消息类型处理 */
bool ripple_translog_recvhglog_4511msgop(ripple_translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen)
{
    if (PG_REPLICATION_MSGTYPE_LK == buffer[0])
    {
        /* 保活 */
        if(false == ripple_translog_recvhglog_keepalivemsg(recvwal, conn, buffer, blen))
        {
            elog(RLOG_WARNING, "keepalive error");
            return false;
        }
    }
    else if (PG_REPLICATION_MSGTYPE_LW == buffer[0])
    {
        /* 数据流 */
        if(false == ripple_translog_recvhglog_4511datamsg(recvwal, buffer, blen))
        {
            elog(RLOG_WARNING, "write wal file error");
            return false;
        }
    }
    return true;
}

/* 获取版本 */
bool ripple_translog_recvhglog_gethgversion(PGconn* conn, ripple_translog_recvlog_dbversion* dbversion)
{
    ripple_translog_recvlog_dbversion pgversion = 0;
    char* serverversion                         = NULL;

    /*
     * 瀚高数据库版本规则如下:
     *  1、v457--v4510, 可通过 show server_version 获取到版本号                     基于PG12
     *      
     *  2、v4511 可通过 select build_version 获取 "4.5 11" 的标识                   基于PG12
     *      show server_version 获取PG版本
     *  3、v901 可通过 select highgo_version 获取 hgdb-enterprise-9.0.1 的标识      基于PG14.13
     *  4、v903 无法获取详细的版本                                                  基于PG14.15
     *  5、v904 可通过 select build_version 获取 "V9 04" 的标识                     基于PG14.15
     * 
     *  区分方式:
     *   1、根据 PG 大版本划分 v45 和 v9
     *   2、v45 使用 show server_version 获取值, 4.5 开始的为 v457---v4510, 否则为 v4511 后的版本
     *   3、v9 不需要区分详细的版本
     */
    pgversion = (PQserverVersion(conn) / (100*100));
    if (RIPPLE_TRANSLOG_RECVLOG_PGVERSION_14 == pgversion)
    {
        *dbversion = RIPPLE_TRANSLOG_RECVLOG_HGVERSION_9;
        return true;
    }

    /* 获取 server version */
    if(false == ripple_translog_recvhglog_showserverversion(conn, &serverversion))
    {
        elog(RLOG_WARNING, "get highgo database version error");
        return false;
    }

    if (0 == strncmp("4.5", serverversion, 3))
    {
        /* 4.5.7---4.5.10 */
        *dbversion = RIPPLE_TRANSLOG_RECVLOG_HGVERSION_4510;
    }
    else
    {
        /* 4.5.11 */
        *dbversion = RIPPLE_TRANSLOG_RECVLOG_HGVERSION_4511;
    }

    return true;
}

/* 获取版本 */
bool ripple_translog_recvhglog_getconfigurefde(char* conninfo, bool* fde)
{
    PGconn* conn = NULL;
    /* 查看是否开启了 FDE */
    *fde = false;

    /* 连接数据库 */
    conn = ripple_conn_get(conninfo);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "get configure fde error");
        return false;
    }

    if (false == ripple_databaserecv_getconfigurefde(conn, fde))
    {
        elog(RLOG_WARNING, "get --enable-fde configure error");
        ripple_conn_close(conn);
        return false;
    }

    ripple_conn_close(conn);
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
bool ripple_translog_recvhglog_endreplication(ripple_translog_recvlog* recvwal,
                                              ripple_translog_walcontrol* walctrl,
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
            if(false == ripple_translog_walmsg_senddone(conn))
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
        if(false == ripple_translog_recvhglog_parsenewpos(res, &tli, &startpos))
        {
            elog(RLOG_WARNING, "parse new pos error");
            PQclear(res);
            return false;
        }

        /* 更新 recvwal 中的 startpos 和 tli 并落盘 */
        ripple_translog_recvlog_setstartpos(recvwal, startpos);
        ripple_translog_recvlog_settli(recvwal, tli);
        ripple_translog_walcontrol_setstartpos(walctrl, startpos);
        ripple_translog_walcontrol_settli(walctrl, tli);

        /* 写 control 文件 */
        ripple_translog_walcontrol_flush(walctrl, recvwal->data);

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

        *error = RIPPLE_ERROR_SUCCESS;
        if(0 == strcmp(PG_ERROR_FILEREMOVED, PQresultErrorField(res, PG_DIAG_SQLSTATE)))
        {
            *error = RIPPLE_ERROR_FILEREMOVED;
        }
        
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}
