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

/* generate empty transaction log file */
static bool translog_recvpglog_initemptywalfile(translog_recvlog* recvwal)
{
    uint64      segno = 0;
    char        xlogfile[MAXPATH] = {0};
    char        blkdata[XLOG_BLKSIZE] = {0};

    struct stat st;

    segno = PGWALBYTETOSEG(recvwal->startpos, recvwal->segsize);
    snprintf(xlogfile,
             MAXPATH,
             "%s/%08X%08X%08X", /* directory/timeline segno size */
             recvwal->data,
             recvwal->tli,
             (uint32)((segno) / PGWALSEGMENTSPERXLOGID(recvwal->segsize)),
             (uint32)((segno) % PGWALSEGMENTSPERXLOGID(recvwal->segsize)));

    recvwal->segno = segno;
    /* file exists, skip */
    if (0 == stat(xlogfile, &st))
    {
        return true;
    }

    /* check error code */
    if (ENOENT != errno)
    {
        elog(RLOG_WARNING, "stat file %s error:%s", xlogfile, strerror(errno));
        return false;
    }

    /* write empty file */
    if (false == osal_create_file_with_size(xlogfile,
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

/* open file */
static bool translog_recvpglog_openwalfile(translog_recvlog* recvwal)
{
    char xlogfile[MAXPATH] = {0};

    /* generate empty file */
    if (false == translog_recvpglog_initemptywalfile(recvwal))
    {
        elog(RLOG_WARNING, "open wal file error");
        return false;
    }

    snprintf(xlogfile,
             MAXPATH,
             "%s/%08X%08X%08X", /* directory/timeline segno size */
             recvwal->data,
             recvwal->tli,
             (uint32)((recvwal->segno) / PGWALSEGMENTSPERXLOGID(recvwal->segsize)),
             (uint32)((recvwal->segno) % PGWALSEGMENTSPERXLOGID(recvwal->segsize)));

    recvwal->fd = osal_basic_open_file(xlogfile, O_RDWR | BINARY);
    if (-1 == recvwal->fd)
    {
        elog(RLOG_WARNING, "open file error, %s", xlogfile);
        return false;
    }
    return true;
}

/* parse next timeline and start position */
static bool translog_recvpglog_parsenewpos(PGresult* res, TimeLineID* ptli, XLogRecPtr* pstartpos)
{
    uint32 xlogid = InvalidXLogRecPtr;
    uint32 xrecoff = InvalidXLogRecPtr;

    if (2 > PQnfields(res) || 1 != PQntuples(res))
    {
        elog(RLOG_WARNING,
             "unexpected result set after end-of-timeline: got %d rows and %d fields, expected %d "
             "rows and %d fields",
             PQntuples(res),
             PQnfields(res),
             1,
             2);
        return false;
    }

    *ptli = atoi(PQgetvalue(res, 0, 0));
    if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &xlogid, &xrecoff) != 2)
    {
        elog(RLOG_WARNING,
             "could not parse next timeline's starting point %s",
             PQgetvalue(res, 0, 1));
        return false;
    }
    *pstartpos = ((uint64)xlogid << 32) | xrecoff;
    return true;
}

/*
 * process data
 */
static bool translog_recvpglog_datamsg(translog_recvlog* recvwal, char* buffer, int blen)
{
    /*
     * PG 12 message format
     *  0                       message type, 'w'
     *  1---8                   datastart
     *  9---16                  walend
     *  17--24                  sendtime
     */
    int        hdrlen = 0;
    int        xlogoff = 0;
    int        byteswrite = 0;
    char*      cptr = NULL;
    XLogRecPtr datastartlsn = InvalidXLogRecPtr;

    hdrlen = (1 + 8 + 8 + 8);

    /* get starting lsn */
    cptr = buffer;
    cptr++;
    rmemcpy1(&datastartlsn, 0, cptr, 8);
    datastartlsn = r_ntoh64(datastartlsn);

    if (recvwal->startpos != datastartlsn)
    {
        elog(RLOG_WARNING,
             "got stream lsn %08X/%08X, expected %08X/%08X",
             (uint32)(datastartlsn >> 32),
             (uint32)datastartlsn,
             (uint32)(recvwal->startpos >> 32),
             (uint32)recvwal->startpos);

        return false;
    }

    if (-1 == recvwal->fd)
    {
        /* open file */
        if (false == translog_recvpglog_openwalfile(recvwal))
        {
            elog(RLOG_WARNING, "open wal file error");
            return false;
        }
    }

    /*
     * write file
     *  must consider cross-file scenarios when writing
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

        /* flush wal data to disk */
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

        /* check if file switch occurred */
        if (0 == PGWALSEGMENTOFFSET(recvwal->startpos, recvwal->segsize))
        {
            /* file switch */
            osal_file_close(recvwal->fd);
            recvwal->fd = -1;
            xlogoff = 0;

            /* open new file */
            if (false == translog_recvpglog_openwalfile(recvwal))
            {
                elog(RLOG_WARNING, "open wal file error");
                return false;
            }
        }
    }

    return true;
}

/*
 * heartbeat
 */
static bool translog_recvpglog_keepalivemsg(translog_recvlog* recvwal,
                                            PGconn*           conn,
                                            char*             buffer,
                                            int               blen)
{
    /*
     * source message format:
     *  1               msgtype
     *  8               walend              endlsn from last packet
     *  8               timestamp           timestamp when assembling packet
     *  1               reply               flag indicating whether to send heartbeat
     */
    bool  reply = false;
    char* cptr = NULL;

    cptr = buffer;
    cptr += (1 + 8 + 8);

    /* check if reply is needed */
    reply = cptr[0];

    if (false == reply)
    {
        return true;
    }

    /* assemble and send heartbeat packet */
    if (false == translog_walmsg_sendkeepalivemsg(recvwal->startpos, conn))
    {
        elog(RLOG_WARNING, "recvwal keepalive msg type error");
        return false;
    }
    return true;
}

/* process by message type */
bool translog_recvpglog_msgop(translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen)
{
    if (PG_REPLICATION_MSGTYPE_LK == buffer[0])
    {
        /* keepalive */
        if (false == translog_recvpglog_keepalivemsg(recvwal, conn, buffer, blen))
        {
            elog(RLOG_WARNING, "keepalive error");
            return false;
        }
    }
    else if (PG_REPLICATION_MSGTYPE_LW == buffer[0])
    {
        /* data stream */
        if (false == translog_recvpglog_datamsg(recvwal, buffer, blen))
        {
            elog(RLOG_WARNING, "write wal file error");
            return false;
        }
    }
    return true;
}

/* get version */
bool translog_recvpglog_getpgversion(PGconn* conn, translog_recvlog_dbversion* dbversion)
{
    *dbversion = (PQserverVersion(conn) / (100 * 100));
    return true;
}

/*
 * handle 'c' message from source, two scenarios:
 *  1. active send
 *      receivewal actively sent 'c' message, just exit
 *  2. passive send
 *      check PGresult->resultStatus status value
 *      when streaming replication starts, resultstatus is PGRES_COPY_BOTH
 *      when source ends streaming replication, resultstatus changes to PGRES_COPY_IN,
 *      so need to check resultstatus here
 */
bool translog_recvpglog_endreplication(translog_recvlog*    recvwal,
                                       translog_walcontrol* walctrl,
                                       PGconn*              conn,
                                       bool*                endcommand,
                                       int*                 error)
{
    XLogRecPtr startpos = InvalidXLogRecPtr;
    TimeLineID tli = InvalidTimeLineID;
    PGresult*  res = NULL;

    *endcommand = false;
    if (NULL == conn || 1 == recvwal->senddone)
    {
        return true;
    }

    /* get status */
    res = PQgetResult(conn);
    /* passive disconnect */
    if (PGRES_COPY_IN == PQresultStatus(res))
    {
        /*
         * send end flag and get response
         */
        if (0 == recvwal->senddone)
        {
            if (false == translog_walmsg_senddone(conn))
            {
                elog(RLOG_WARNING, "endreplication error");
                PQclear(res);
                return false;
            }
            recvwal->senddone = 1;
            res = PQgetResult(conn);
        }
    }

    /* check return type again and process */
    if (PGRES_TUPLES_OK == PQresultStatus(res))
    {
        /* reached end of timeline, get next timeline */
        if (false == translog_recvpglog_parsenewpos(res, &tli, &startpos))
        {
            elog(RLOG_WARNING, "parse new pos error");
            PQclear(res);
            return false;
        }

        /* update startpos and tli in recvwal and flush */
        translog_recvlog_setstartpos(recvwal, startpos);
        translog_recvlog_settli(recvwal, tli);
        translog_walcontrol_setstartpos(walctrl, startpos);
        translog_walcontrol_settli(walctrl, tli);

        /* write control file */
        translog_walcontrol_flush(walctrl, recvwal->data);

        elog(RLOG_WARNING,
             "receivewal will shift replication timeline:%u, pos:%08X/%08X",
             recvwal->tli,
             (uint32)(recvwal->startpos >> 32),
             (uint32)recvwal->startpos);
    }
    else if (PGRES_COMMAND_OK == PQresultStatus(res))
    {
        /* server fully exited */
        *endcommand = true;
        recvwal->senddone = 0;

        elog(RLOG_INFO,
             "stop replication at timeline:%u, pos:%08X/%08X, database timelie:%u",
             recvwal->tli,
             (uint32)(recvwal->startpos >> 32),
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
        if (0 == strcmp(PG_ERROR_FILEREMOVED, PQresultErrorField(res, PG_DIAG_SQLSTATE)))
        {
            *error = ERROR_FILEREMOVED;
        }

        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}
