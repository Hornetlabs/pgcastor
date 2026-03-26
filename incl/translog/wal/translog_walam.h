#ifndef _TRANSLOG_RECVLOGAM_H_
#define _TRANSLOG_RECVLOGAM_H_

typedef struct TRANSLOG_RECVLOG_AMROUTINE
{
    translog_recvlog_dbversion version;
    char*                      desc;
    bool (*msgop)(translog_recvlog* recvwal, PGconn* conn, char* buffer, int blen);

    bool (*endreplication)(translog_recvlog*    recvwal,
                           translog_walcontrol* walctrl,
                           PGconn*              conn,
                           bool*                endcommand,
                           int*                 error);
} translog_recvlog_amroutine;

typedef struct TRANSLOG_RECVLOG_DBTYPEROUTINE
{
    /* database type */
    translog_recvlog_dbtype type;

    /* description */
    char*                   desc;

    /* getdatalibraryversion */
    bool                    (*getdbversion)(PGconn* conn, translog_recvlog_dbversion* version);

    /* fde encrypt */
    bool                    (*getconfigurefde)(char* conninfo, bool* fde);
} translog_recvlog_dbtyperoutine;

/* according todatalibrarytypegetdatalibraryversion */
bool translog_recvlog_getconfigurefde(translog_recvlog_dbtype type, char* conninfo, bool* fde);

/* according todatalibrarytypegetdatalibraryversion */
bool translog_recvlog_getdbversion(translog_recvlog_dbtype     type,
                                   PGconn*                     conn,
                                   translog_recvlog_dbversion* dbversion);

translog_recvlog_amroutine* translog_recvlog_getroutine(translog_recvlog_dbversion dbversion);

#endif
