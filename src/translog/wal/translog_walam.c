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
#include "translog/wal/translog_walam.h"

/*----------------------------------version router PG begin---------------------*/

/* PG12 version handler */
static translog_recvlog_amroutine m_pg12routine = {
    .version = TRANSLOG_RECVLOG_PGVERSION_12,
    .desc = "PG12",
    .msgop = translog_recvpglog_msgop,
    .endreplication = translog_recvpglog_endreplication};

/* PG13 version handler */
static translog_recvlog_amroutine m_pg13routine = {
    .version = TRANSLOG_RECVLOG_PGVERSION_13,
    .desc = "PG13",
    .msgop = translog_recvpglog_msgop,
    .endreplication = translog_recvpglog_endreplication};

/* PG14 version handler */
static translog_recvlog_amroutine m_pg14routine = {
    .version = TRANSLOG_RECVLOG_PGVERSION_14,
    .desc = "PG14",
    .msgop = translog_recvpglog_msgop,
    .endreplication = translog_recvpglog_endreplication};

/* PG15 version handler */
static translog_recvlog_amroutine m_pg15routine = {
    .version = TRANSLOG_RECVLOG_PGVERSION_15,
    .desc = "PG15",
    .msgop = translog_recvpglog_msgop,
    .endreplication = translog_recvpglog_endreplication};

/* PG16 version handler */
static translog_recvlog_amroutine m_pg16routine = {
    .version = TRANSLOG_RECVLOG_PGVERSION_16,
    .desc = "PG16",
    .msgop = translog_recvpglog_msgop,
    .endreplication = translog_recvpglog_endreplication};

/* PG17 version handler */
static translog_recvlog_amroutine m_pg17routine = {
    .version = TRANSLOG_RECVLOG_PGVERSION_17,
    .desc = "PG17",
    .msgop = translog_recvpglog_msgop,
    .endreplication = translog_recvpglog_endreplication};

/* PG18 version handler */
static translog_recvlog_amroutine m_pg18routine = {
    .version = TRANSLOG_RECVLOG_PGVERSION_18,
    .desc = "PG18",
    .msgop = translog_recvpglog_msgop,
    .endreplication = translog_recvpglog_endreplication};

/*----------------------------------version router PG   end---------------------*/

/*----------------------------------database version begin-----------------------*/
static translog_recvlog_dbtyperoutine m_pgtyperoutine[] = {
    {.type = TRANSLOG_RECVLOG_DBTYPE_NOP,
     .desc = "NOP",
     .getdbversion = NULL,
     .getconfigurefde = NULL},
    {.type = TRANSLOG_RECVLOG_DBTYPE_PG,
     .desc = "Postgres",
     .getdbversion = translog_recvpglog_getpgversion,
     .getconfigurefde = NULL}};

/*----------------------------------database version   end-----------------------*/

/* get database version by database type */
bool translog_recvlog_getdbversion(translog_recvlog_dbtype type, PGconn* conn,
                                   translog_recvlog_dbversion* dbversion)
{
    if (TRANSLOG_RECVLOG_DBTYPE_MAX <= type)
    {
        elog(RLOG_WARNING, "unknown database type");
        return false;
    }

    if (NULL == m_pgtyperoutine[type].getdbversion)
    {
        elog(RLOG_WARNING, "%s database unsupport get database version",
             m_pgtyperoutine[type].desc);
        return false;
    }

    return m_pgtyperoutine[type].getdbversion(conn, dbversion);
}

/* get database version by database type */
bool translog_recvlog_getconfigurefde(translog_recvlog_dbtype type, char* conninfo, bool* fde)
{
    if (TRANSLOG_RECVLOG_DBTYPE_MAX <= type)
    {
        elog(RLOG_WARNING, "unknown database type");
        return false;
    }

    if (NULL == m_pgtyperoutine[type].getconfigurefde)
    {
        *fde = false;
        return true;
    }

    return m_pgtyperoutine[type].getconfigurefde(conninfo, fde);
}

/* set processing method */
translog_recvlog_amroutine* translog_recvlog_getroutine(translog_recvlog_dbversion dbversion)
{
    switch (dbversion)
    {
        case TRANSLOG_RECVLOG_PGVERSION_12:
            return &m_pg12routine;
        case TRANSLOG_RECVLOG_PGVERSION_13:
            return &m_pg13routine;
        case TRANSLOG_RECVLOG_PGVERSION_14:
            return &m_pg14routine;
        case TRANSLOG_RECVLOG_PGVERSION_15:
            return &m_pg15routine;
        case TRANSLOG_RECVLOG_PGVERSION_16:
            return &m_pg16routine;
        case TRANSLOG_RECVLOG_PGVERSION_17:
            return &m_pg17routine;
        case TRANSLOG_RECVLOG_PGVERSION_18:
            return &m_pg18routine;
        default:
            elog(RLOG_WARNING, "not support database version %d", dbversion);
            return NULL;
    }

    return NULL;
}
