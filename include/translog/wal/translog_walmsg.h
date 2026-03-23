#ifndef _TRANSLOG_RECVLOGMSG_H_
#define _TRANSLOG_RECVLOGMSG_H_

/* Data message */
#define PG_REPLICATION_MSGTYPE_LW 'w'

/* Keepalive sent from source */
#define PG_REPLICATION_MSGTYPE_LK 'k'

/* Keepalive sent to source */
#define PG_REPLICATION_MSGTYPE_LR 'r'

/* Source error code */
#define PG_ERROR_FILEREMOVED "58P01"

/*
 * Send heartbeat packet
 */
bool translog_walmsg_sendkeepalivemsg(XLogRecPtr startpos, PGconn* conn);

/* Send end flag */
bool translog_walmsg_senddone(PGconn* conn);

/* Get data */
bool translog_walmsg_getdata(PGconn* conn, char** buffer, int* perror, int* recvlen);

#endif
