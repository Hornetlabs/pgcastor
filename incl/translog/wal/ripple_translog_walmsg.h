#ifndef _RIPPLE_TRANSLOG_RECVLOGMSG_H_
#define _RIPPLE_TRANSLOG_RECVLOGMSG_H_

/* 数据消息 */
#define PG_REPLICATION_MSGTYPE_LW                   'w'

/* 源端发送的保活 */
#define PG_REPLICATION_MSGTYPE_LK                   'k'

/* 发送到源端的保活 */
#define PG_REPLICATION_MSGTYPE_LR                   'r'

/* 源端错误码 */
#define PG_ERROR_FILEREMOVED                        "58P01"

/*
 * 发送心跳包
*/
bool ripple_translog_walmsg_sendkeepalivemsg(XLogRecPtr startpos, PGconn* conn);

/* 发送结束标识 */
bool ripple_translog_walmsg_senddone(PGconn* conn);

/* 获取数据 */
bool ripple_translog_walmsg_getdata(PGconn* conn, char** buffer, int* perror, int *recvlen);

#endif
