#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/dttime/dttimestamp.h"
#include "utils/conn/conn.h"
#include "utils/init/databaserecv.h"
#include "translog/wal/translog_walmsg.h"

/*
 * send keepalive packet
 */
bool translog_walmsg_sendkeepalivemsg(XLogRecPtr startpos, PGconn* conn)
{
    int         msglen = 34;
    XLogRecPtr  nlsn = 0;
    TimestampTz tnow = 0;
    char*       cptr = NULL;
    /*
     * 0                msgtype             'r'
     * 1---8            write lsn           write lsn
     * 9---16           flush lsn           flush to disk lsn
     * 17---24          apply               standby applied lsn
     * 25               replyrequest        whether to send heartbeat
     */
    char        replybuf[1 + 8 + 8 + 8 + 8 + 1];

    /* get timestamp */
    tnow = dt_gettimestamp();

    /* set message type to 'r' */
    cptr = replybuf;
    cptr[0] = PG_REPLICATION_MSGTYPE_LR;
    cptr++;

    /* convert host byte order to network byte order */
    nlsn = startpos;
    nlsn = r_hton64(nlsn);

    /* writelsn */
    rmemcpy1(cptr, 0, &nlsn, 8);
    cptr += 8;

    /* flushlsn */
    rmemcpy1(cptr, 0, &nlsn, 8);
    cptr += 8;

    /* applylsn */
    nlsn = InvalidXLogRecPtr;
    rmemcpy1(cptr, 0, &nlsn, 8);
    cptr += 8;

    /* timestamp */
    tnow = r_hton64(tnow);
    rmemcpy1(cptr, 0, &tnow, 8);
    cptr += 8;

    /* replyrequest */
    cptr[0] = 0;

    /* send data */
    if (0 >= PQputCopyData(conn, replybuf, msglen) || PQflush(conn))
    {
        elog(RLOG_WARNING, "could not send keepalive packet: %s", PQerrorMessage(conn));
        return false;
    }

    return true;
}

/* send end flag */
bool translog_walmsg_senddone(PGconn* conn)
{
    if (0 >= PQputCopyEnd(conn, NULL) || PQflush(conn))
    {
        /* failed to send */
        elog(RLOG_WARNING, "send done replication error");
        return false;
    }
    return true;
}

/* async io, temporary approach, will be improved later */
static bool translog_walmsg_poll(PGconn* conn, long timeoutms, int* perror)
{
    int             ret = 0;
    fd_set          input_mask;
    int             connsocket;
    int             maxfd;
    struct timeval  timeout;
    struct timeval* timeoutptr;

translog_recvwallog_poll_retry:
    connsocket = PQsocket(conn);
    if (connsocket < 0)
    {
        elog(RLOG_WARNING, "invalid socket: %s", PQerrorMessage(conn));
        return false;
    }

    FD_ZERO(&input_mask);
    FD_SET(connsocket, &input_mask);
    maxfd = connsocket;

    if (timeoutms < 0)
    {
        timeoutptr = NULL;
    }
    else
    {
        timeout.tv_sec = timeoutms / 1000L;
        timeout.tv_usec = (timeoutms % 1000L) * 1000L;
        timeoutptr = &timeout;
    }

    /* listen for read events */
    ret = select(maxfd + 1, &input_mask, NULL, NULL, timeoutptr);
    if (ret < 0)
    {
        if (errno == EINTR)
        {
            goto translog_recvwallog_poll_retry;
        }
        elog(RLOG_WARNING, "select() failed: %s", strerror(errno));
        return false;
    }
    if (ret > 0 && FD_ISSET(connsocket, &input_mask))
    {
        /* read event triggered */
        return true;
    }

    /* timeout */
    *perror = ERROR_TIMEOUT;
    return false;
}

/* get data */
bool translog_walmsg_getdata(PGconn* conn, char** buffer, int* perror, int* recvlen)
{
    long sleeptime = 1000;

    /* clear data */
    if (NULL != *buffer)
    {
        PQfreemem(*buffer);
        *buffer = NULL;
    }

    *perror = ERROR_SUCCESS;
    *recvlen = PQgetCopyData(conn, buffer, 1);
    if (0 == *recvlen)
    {
        /* no data available, need to wait for data */
        if (false == translog_walmsg_poll(conn, sleeptime, perror))
        {
            if (ERROR_TIMEOUT == *perror)
            {
                return true;
            }
            else
            {
                /* non-timeout error, connection issue */
                elog(RLOG_WARNING, "recvwallog poll error");
                *perror = ERROR_REPLICATION;
                return false;
            }
        }

        /* data available, try to read */
        if (0 == PQconsumeInput(conn))
        {
            elog(RLOG_WARNING, "could not receive data from WAL stream: %s", PQerrorMessage(conn));
            *perror = ERROR_REPLICATION;
            return false;
        }

        /* data read, copy data */
        *recvlen = PQgetCopyData(conn, buffer, 1);
        if (0 == *recvlen)
        {
            /* not enough data read, need to retry */
            *perror = ERROR_RETRY;
            return false;
        }
    }

    if (-1 == *recvlen)
    {
        /*
         * this -1 has the following meanings:
         *  1. received 'c' message
         *  2. received message not relevant to streaming replication, e.g. 'C'
         *      so ERROR_ENDREPLICATION should be handled with classification outside
         */
        *perror = ERROR_ENDREPLICATION;
        return false;
    }
    else if (-2 == *recvlen)
    {
        *perror = ERROR_REPLICATION;
        return false;
    }
    return true;
}
