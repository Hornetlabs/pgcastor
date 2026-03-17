#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/dttime/dttimestamp.h"
#include "utils/conn/conn.h"
#include "utils/init/databaserecv.h"
#include "translog/wal/translog_walmsg.h"

/*
 * 发送心跳包
*/
bool translog_walmsg_sendkeepalivemsg(XLogRecPtr startpos, PGconn* conn)
{
    int msglen = 34;
    XLogRecPtr nlsn     = 0;
    TimestampTz tnow    = 0;
    char* cptr = NULL;
    /*
     * 0                msgtype             'r'
     * 1---8            write lsn           落盘 lsn
     * 9---16           flush lsn           刷新到磁盘的 lsn
     * 17---24          apply               备库应用到的 lsn
     * 25               replyrequest        是否需要发送心跳
     */
    char replybuf[1 + 8 + 8 + 8 + 8 + 1];

    /* 获取时间戳 */
    tnow = dt_gettimestamp();

    /* 设置消息类型为 'r' */
    cptr = replybuf;
    cptr[0] = PG_REPLICATION_MSGTYPE_LR;
    cptr++;

    /* 主机字节序转网络字节序 */
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
    
    /* 发送数据 */
    if (0 >= PQputCopyData(conn, replybuf, msglen) || PQflush(conn))
    {
        elog(RLOG_WARNING, "could not send keepalive packet: %s", PQerrorMessage(conn));
        return false;
    }

    return true;
}

/* 发送结束标识 */
bool translog_walmsg_senddone(PGconn* conn)
{
    if (0 >= PQputCopyEnd(conn, NULL) || PQflush(conn))
    {
        /* 没有发送成功 */
        elog(RLOG_WARNING, "send done replication error");
        return false;
    }
    return true;
}

/* 异步 io, 临时使用此方式, 后续再改进 */
static bool translog_walmsg_poll(PGconn *conn, long timeoutms, int* perror)
{
    int ret             = 0;
    fd_set input_mask;
    int connsocket;
    int maxfd;
    struct timeval timeout;
    struct timeval *timeoutptr;

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

    /* 监听读事件 */
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
        /* 读事件触发 */
        return true;
    }

    /* 超时 */
    *perror = ERROR_TIMEOUT;
    return false;
}

/* 获取数据 */
bool translog_walmsg_getdata(PGconn* conn, char** buffer, int* perror, int *recvlen)
{
    long sleeptime          = 1000;

    /* 清理数据 */
    if(NULL != *buffer)
    {
        PQfreemem(*buffer);
        *buffer = NULL;
    }

    *perror = ERROR_SUCCESS;
    *recvlen = PQgetCopyData(conn, buffer, 1);
    if (0 == *recvlen)
    {
        /* 没有数据, 那么需要等待数据 */
        if(false == translog_walmsg_poll(conn, sleeptime, perror))
        {
            if (ERROR_TIMEOUT == *perror)
            {
                return true;
            }
            else
            {
                /* 非超时错误, 那么证明连接出问题了 */
                elog(RLOG_WARNING, "recvwallog poll error");
                *perror = ERROR_REPLICATION;
                return false;
            }
        }

        /* 有数据了, 那么尝试读数据 */
        if (0 == PQconsumeInput(conn))
        {
            elog(RLOG_WARNING, "could not receive data from WAL stream: %s", PQerrorMessage(conn));
            *perror = ERROR_REPLICATION;
            return false;
        }

        /* 读取到了数据, 复制数据 */
        *recvlen = PQgetCopyData(conn, buffer, 1);
        if (0 == *recvlen)
        {
            /* 没有读取到足够的数据, 那么还需要再此读取 */
            *perror = ERROR_RETRY;
            return false;
        }
    }

    if(-1 == *recvlen)
    {
        /*
         * 这个 -1 包含的含义如下:
         *  1、接收到了 'c' 消息
         *  2、接收到了流复制不感兴趣的消息, 比如: 'C'
         *      所以在外面应该对 ERROR_ENDREPLICATION 进行分类处理
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
