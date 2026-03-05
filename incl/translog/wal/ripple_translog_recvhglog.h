#ifndef _RIPPLE_TRANSLOG_RECVHGLOG_H_
#define _RIPPLE_TRANSLOG_RECVHGLOG_H_

/* 根据消息类型处理 */
bool ripple_translog_recvhglog_4510msgop(ripple_translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen);

/* 根据消息类型处理 */
bool ripple_translog_recvhglog_4511msgop(ripple_translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen);

/* 获取版本 */
bool ripple_translog_recvhglog_gethgversion(PGconn* conn, ripple_translog_recvlog_dbversion* dbversion);

/* 获取版本 */
bool ripple_translog_recvhglog_getconfigurefde(char* conninfo, bool* fde);

/* end replication 处理 */
bool ripple_translog_recvhglog_endreplication(ripple_translog_recvlog* recvwal,
                                              ripple_translog_walcontrol* walctrl,
                                              PGconn* conn,
                                              bool* endcommand,
                                              int* error);

#endif
