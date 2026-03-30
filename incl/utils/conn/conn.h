#ifndef _CONN_H
#define _CONN_H

typedef enum TXN_ISOLATION_LEVEL
{
    TXNISOLVL_READ_UNCOMMITTED = 0x00,
    TXNISOLVL_READ_COMMITTED = 0x01,
    TXNISOLVL_REPEATABLE_READ = 0x02,
    TXNISOLVL_SERIALIZABLE = 0x03
} txn_isolation_level;

void conn_close(PGconn* conn);
PGconn* conn_get(const char* conninfo);

/* Connect database/stream replication */
PGconn* conn_getphysical(const char* conninfo, char* appname);

PGresult* conn_exec(PGconn* conn, const char* querystring);

bool conn_begin(PGconn* conn);

bool conn_commit(PGconn* conn);

bool conn_rollback(PGconn* conn);

void conn_settxnisolationlevel(PGconn* conn, int level);

#endif