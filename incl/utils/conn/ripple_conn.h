#ifndef _RIPPLE_CONN_H
#define _RIPPLE_CONN_H

typedef enum RIPPLE_TXN_ISOLATION_LEVEL
{
    RIPPLE_TXNISOLVL_READ_UNCOMMITTED   = 0x00,
    RIPPLE_TXNISOLVL_READ_COMMITTED     = 0x01,
    RIPPLE_TXNISOLVL_REPEATABLE_READ    = 0x02,
    RIPPLE_TXNISOLVL_SERIALIZABLE       = 0x03
}ripple_txn_isolation_level;

void ripple_conn_close(PGconn *conn);
PGconn *ripple_conn_get(const char * conninfo);

/* 连接数据库/流复制 */
PGconn* ripple_conn_getphysical(const char* conninfo, char* appname);

PGresult *ripple_conn_exec(PGconn *conn,const char * querystring);

bool ripple_conn_begin(PGconn* conn);

bool ripple_conn_commit(PGconn* conn);

bool ripple_conn_rollback(PGconn* conn);

void ripple_conn_settxnisolationlevel(PGconn* conn, int level);


#endif