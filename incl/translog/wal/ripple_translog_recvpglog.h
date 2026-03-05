#ifndef _RIPPLE_TRANSLOG_RECVPGLOG_H_
#define _RIPPLE_TRANSLOG_RECVPGLOG_H_

bool ripple_translog_recvpglog_msgop(ripple_translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen);

/* 获取版本 */
bool ripple_translog_recvpglog_getpgversion(PGconn* conn, ripple_translog_recvlog_dbversion* dbversion);

bool ripple_translog_recvpglog_endreplication(ripple_translog_recvlog* recvwal,
                                            ripple_translog_walcontrol* walctrl,
                                            PGconn* conn,
                                             bool* endcommand,
                                            int* error);

#endif
