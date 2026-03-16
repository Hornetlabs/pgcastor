#ifndef _RIPPLE_TRANSLOG_RECVLOGAM_H_
#define _RIPPLE_TRANSLOG_RECVLOGAM_H_

typedef struct RIPPLE_TRANSLOG_RECVLOG_AMROUTINE
{
    ripple_translog_recvlog_dbversion   version;
    char*                               desc;
    bool                                (*msgop)(ripple_translog_recvlog* recvwal,
                                                 PGconn* conn,
                                                 char* buffer,
                                                 int blen);

    bool                                (*endreplication)(ripple_translog_recvlog* recvwal,
                                                          ripple_translog_walcontrol* walctrl,
                                                          PGconn* conn,
                                                          bool* endcommand,
                                                          int* error);
} ripple_translog_recvlog_amroutine;

typedef struct RIPPLE_TRANSLOG_RECVLOG_DBTYPEROUTINE
{
    /* 数据库类型 */
    ripple_translog_recvlog_dbtype  type;

    /* 描述 */
    char*                           desc;

    /* 获取数据库版本 */
    bool                            (*getdbversion)(PGconn* conn, ripple_translog_recvlog_dbversion* version);

    /* fde 加密 */
    bool                            (*getconfigurefde)(char* conninfo, bool* fde);
} ripple_translog_recvlog_dbtyperoutine;

/* 根据数据库类型获取数据库版本 */
bool ripple_translog_recvlog_getconfigurefde(ripple_translog_recvlog_dbtype type,
                                             char* conninfo,
                                             bool* fde);

/* 根据数据库类型获取数据库版本 */
bool ripple_translog_recvlog_getdbversion(ripple_translog_recvlog_dbtype type,
                                          PGconn* conn,
                                          ripple_translog_recvlog_dbversion* dbversion);

ripple_translog_recvlog_amroutine* ripple_translog_recvlog_getroutine(ripple_translog_recvlog_dbversion dbversion);

#endif
