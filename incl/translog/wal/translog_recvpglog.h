#ifndef _TRANSLOG_RECVPGLOG_H_
#define _TRANSLOG_RECVPGLOG_H_

bool translog_recvpglog_msgop(translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen);

/* Get version */
bool translog_recvpglog_getpgversion(PGconn* conn, translog_recvlog_dbversion* dbversion);

bool translog_recvpglog_endreplication(translog_recvlog*    recvwal,
                                       translog_walcontrol* walctrl,
                                       PGconn*              conn,
                                       bool*                endcommand,
                                       int*                 error);

#endif
