#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/conn/conn.h"

/* Connect to database */
PGconn* conn_get(const char* conninfo)
{
    PGconn* conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(RLOG_WARNING, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        conn = NULL;
    }
    elog(RLOG_DEBUG, "Connection to database success");
    return conn;
}

/* Connect to database/streaming replication */
PGconn* conn_getphysical(const char* conninfo, char* appname)
{
    /* dbname/replication/fallback_app_name/host/user/port/password */
    int               index = 0;
    int               argcnt = 7;
    char*             errmsg = NULL;
    const char**      keywords = NULL;
    const char**      values = NULL;
    PGconn*           conn = NULL;
    PQconninfoOption* connopt = NULL;
    PQconninfoOption* connopts = NULL;

    /* Parse conninfo */
    connopts = PQconninfoParse(conninfo, &errmsg);
    if (connopts == NULL)
    {
        elog(RLOG_WARNING, "can not parse conninfo %s", conninfo);
        return NULL;
    }

    /* Iterate each configuration item */
    for (connopt = connopts; connopt->keyword != NULL; connopt++)
    {
        if (NULL != connopt->val && '\0' != connopt->val[0] &&
            0 != strcmp(connopt->keyword, "dbname"))
        {
            argcnt++;
        }
    }

    /* Allocate space */
    keywords = rmalloc0((argcnt + 1) * sizeof(*keywords));
    if (NULL == keywords)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(keywords, 0, '\0', (argcnt + 1) * sizeof(*keywords));

    values = rmalloc0((argcnt + 1) * sizeof(*values));
    if (NULL == values)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(values, 0, '\0', (argcnt + 1) * sizeof(*values));

    for (connopt = connopts; connopt->keyword != NULL; connopt++)
    {
        if (connopt->val != NULL && '\0' != connopt->val[0] &&
            0 != strcmp(connopt->keyword, "dbname"))
        {
            keywords[index] = connopt->keyword;
            values[index] = connopt->val;
            index++;
        }
    }

    keywords[index] = "dbname";
    values[index] = "replication";
    index++;
    keywords[index] = "replication";
    values[index] = "true";
    index++;
    keywords[index] = "fallback_application_name";
    values[index] = appname;

    /* Connect to database */
    conn = PQconnectdbParams(keywords, values, true);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "can not connect server %s", conninfo);
        goto conn_getphysical_error;
    }

    if (CONNECTION_OK != PQstatus(conn))
    {
        elog(RLOG_WARNING, "connect server %s error: %s", conninfo, PQerrorMessage(conn));
        PQfinish(conn);
        goto conn_getphysical_error;
    }

    rfree(values);
    rfree(keywords);
    PQconninfoFree(connopts);
    return conn;

conn_getphysical_error:
    rfree(values);
    rfree(keywords);
    PQconninfoFree(connopts);
    return NULL;
}

/* Close database */
void conn_close(PGconn* conn)
{
    PQfinish(conn);
}

/* Execute and get result */
PGresult* conn_exec(PGconn* conn, const char* query)
{
    PGresult* res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        elog(RLOG_WARNING, "SQL query execution failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return NULL;
    }

    return res;
}

/* Begin transaction */
bool conn_begin(PGconn* conn)
{
    PGresult* res = PQexec(conn, "BEGIN");

    if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        elog(RLOG_WARNING, "SQL query execution failed: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    PQclear(res);

    return true;
}

/* Commit transaction */
bool conn_commit(PGconn* conn)
{
    PGresult* res = PQexec(conn, "COMMIT");

    if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        elog(RLOG_WARNING, "SQL query execution failed: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    PQclear(res);

    return true;
}

/* Rollback transaction */
bool conn_rollback(PGconn* conn)
{
    PGresult* res = PQexec(conn, "ROLLBACK");

    if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        elog(RLOG_WARNING, "SQL query execution failed: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    PQclear(res);

    return true;
}

/* Begin transaction and set isolation level */
void conn_settxnisolationlevel(PGconn* conn, int level)
{
    PGresult* res = NULL;
    char      stmtsql[1024] = {'\0'};

    res = conn_exec(conn, "begin");
    if (NULL == res)
    {
        elog(RLOG_ERROR, "Execute begin failed");
    }

    PQclear(res);

    /* Get database identifier */
    rmemset1(stmtsql, 0, 0, 1024);
    switch (level)
    {
        case TXNISOLVL_READ_UNCOMMITTED:
            sprintf(stmtsql, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
            break;
        case TXNISOLVL_READ_COMMITTED:
            sprintf(stmtsql, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
            break;
        case TXNISOLVL_REPEATABLE_READ:
            sprintf(stmtsql, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
            break;
        case TXNISOLVL_SERIALIZABLE:
            sprintf(stmtsql, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
            break;

        default:
            PQclear(res);
            PQfinish(conn);
            elog(RLOG_ERROR, "Invalid transaction isolation level");
            break;
    }
    res = conn_exec(conn, stmtsql);
    if (NULL == res)
    {
        elog(RLOG_ERROR, "Set transaction isolation level failed");
    }

    PQclear(res);

    return;
}
