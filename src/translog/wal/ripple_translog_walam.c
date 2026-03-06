#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/dttime/dttimestamp.h"
#include "utils/conn/ripple_conn.h"
#include "utils/init/ripple_databaserecv.h"
#include "translog/ripple_translog_recvlogdb.h"
#include "translog/wal/ripple_translog_walcontrol.h"
#include "translog/wal/ripple_translog_waltimeline.h"
#include "translog/wal/ripple_translog_walmsg.h"
#include "translog/wal/ripple_translog_recvlog.h"
#include "translog/wal/ripple_translog_recvpglog.h"
#include "translog/wal/ripple_translog_walam.h"

/*----------------------------------版本路由器PG begin---------------------*/

/* PG12 版本处理 */
static ripple_translog_recvlog_amroutine m_pg12routine =
{
    .version        = RIPPLE_TRANSLOG_RECVLOG_PGVERSION_12,
    .desc           = "PG12",
    .msgop          = ripple_translog_recvpglog_msgop,
    .endreplication = ripple_translog_recvpglog_endreplication
};

/* PG13 版本处理 */
static ripple_translog_recvlog_amroutine m_pg13routine =
{
    .version        = RIPPLE_TRANSLOG_RECVLOG_PGVERSION_13,
    .desc           = "PG13",
    .msgop          = ripple_translog_recvpglog_msgop,
    .endreplication = ripple_translog_recvpglog_endreplication
};

/* PG14 版本处理 */
static ripple_translog_recvlog_amroutine m_pg14routine =
{
    .version        = RIPPLE_TRANSLOG_RECVLOG_PGVERSION_14,
    .desc           = "PG14",
    .msgop          = ripple_translog_recvpglog_msgop,
    .endreplication = ripple_translog_recvpglog_endreplication
};

/* PG15 版本处理 */
static ripple_translog_recvlog_amroutine m_pg15routine =
{
    .version        = RIPPLE_TRANSLOG_RECVLOG_PGVERSION_15,
    .desc           = "PG15",
    .msgop          = ripple_translog_recvpglog_msgop,
    .endreplication = ripple_translog_recvpglog_endreplication
};

/* PG16 版本处理 */
static ripple_translog_recvlog_amroutine m_pg16routine =
{
    .version        = RIPPLE_TRANSLOG_RECVLOG_PGVERSION_16,
    .desc           = "PG16",
    .msgop          = ripple_translog_recvpglog_msgop,
    .endreplication = ripple_translog_recvpglog_endreplication
};

/* PG17 版本处理 */
static ripple_translog_recvlog_amroutine m_pg17routine =
{
    .version        = RIPPLE_TRANSLOG_RECVLOG_PGVERSION_17,
    .desc           = "PG17",
    .msgop          = ripple_translog_recvpglog_msgop,
    .endreplication = ripple_translog_recvpglog_endreplication
};

/* PG18 版本处理 */
static ripple_translog_recvlog_amroutine m_pg18routine =
{
    .version        = RIPPLE_TRANSLOG_RECVLOG_PGVERSION_18,
    .desc           = "PG18",
    .msgop          = ripple_translog_recvpglog_msgop,
    .endreplication = ripple_translog_recvpglog_endreplication
};

/*----------------------------------版本路由器PG   end---------------------*/

/*----------------------------------数据库版本 begin-----------------------*/
static ripple_translog_recvlog_dbtyperoutine m_pgtyperoutine[] =
{
    {
        .type               = RIPPLE_TRANSLOG_RECVLOG_DBTYPE_NOP,
        .desc               = "NOP",
        .getdbversion       = NULL,
        .getconfigurefde    = NULL
    },
    {
        .type               = RIPPLE_TRANSLOG_RECVLOG_DBTYPE_PG,
        .desc               = "Postgres",
        .getdbversion       = ripple_translog_recvpglog_getpgversion,
        .getconfigurefde    = NULL
    }
};

/*----------------------------------数据库版本   end-----------------------*/

/* 根据数据库类型获取数据库版本 */
bool ripple_translog_recvlog_getdbversion(ripple_translog_recvlog_dbtype type,
                                          PGconn* conn,
                                          ripple_translog_recvlog_dbversion* dbversion)
{
    if (RIPPLE_TRANSLOG_RECVLOG_DBTYPE_MAX <= type)
    {
        elog(RLOG_WARNING, "unknown database type");
        return false;
    }

    if (NULL == m_pgtyperoutine[type].getdbversion)
    {
        elog(RLOG_WARNING, "%s database unsupport get database version", m_pgtyperoutine[type].desc);
        return false;
    }

    return m_pgtyperoutine[type].getdbversion(conn, dbversion);
}

/* 根据数据库类型获取数据库版本 */
bool ripple_translog_recvlog_getconfigurefde(ripple_translog_recvlog_dbtype type,
                                             char* conninfo,
                                             bool* fde)
{
    if (RIPPLE_TRANSLOG_RECVLOG_DBTYPE_MAX <= type)
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

/* 设置处理方法 */
ripple_translog_recvlog_amroutine* ripple_translog_recvlog_getroutine(ripple_translog_recvlog_dbversion dbversion)
{
    switch (dbversion)
    {
        case RIPPLE_TRANSLOG_RECVLOG_PGVERSION_12:
            return &m_pg12routine;
        case RIPPLE_TRANSLOG_RECVLOG_PGVERSION_13:
            return &m_pg13routine;
        case RIPPLE_TRANSLOG_RECVLOG_PGVERSION_14:
            return &m_pg14routine;
        case RIPPLE_TRANSLOG_RECVLOG_PGVERSION_15:
            return &m_pg15routine;
        case RIPPLE_TRANSLOG_RECVLOG_PGVERSION_16:
            return &m_pg16routine;
        case RIPPLE_TRANSLOG_RECVLOG_PGVERSION_17:
            return &m_pg17routine;
        case RIPPLE_TRANSLOG_RECVLOG_PGVERSION_18:
            return &m_pg18routine;
        default:
            elog(RLOG_WARNING, "not support database version %d", dbversion);
            return NULL;
    }

    return NULL;
}

