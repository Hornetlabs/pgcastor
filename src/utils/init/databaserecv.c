#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/conn/conn.h"
#include "utils/init/databaserecv.h"

checkpoint* databaserecv_checkpoint_get(PGconn* conn)
{
    char *redolsn = NULL;
    char stmtsql[1024] = {'\0'};
    PGresult*    res = NULL;
    checkpoint* checkpoint_obj = NULL;

    uint32      tlid = 0;
    uint32      epoch = 0;
    uint32      nextfullxid = 0;
    uint32      hi = 0,
                lo = 0;

    /*获取当前数据库的检查点信息*/
    sprintf(stmtsql, "SELECT redo_lsn, timeline_id, next_xid FROM pg_control_checkpoint();");
    res =  conn_exec(conn, stmtsql);
    if (PQnfields(res) != 3 && PQntuples(res) != 1)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_ERROR, "failed get excute SQL result: %s", stmtsql);
    }

    checkpoint_obj = (checkpoint*)rmalloc0(sizeof(checkpoint));
    if(NULL == checkpoint_obj)
    {
        elog(RLOG_ERROR,"out of memory, %s", strerror(errno));
    }
    rmemset0(checkpoint_obj, 0, 0, sizeof(checkpoint));

    /*获取执行结果，为字符串信息*/
    redolsn = PQgetvalue(res, 0, 0);

    /*以"%X/%X"格式读取redolsn中数据，并把值赋给hi，lo*/
    if (sscanf(redolsn, "%X/%X", &hi, &lo) != 2)
    {
        elog(RLOG_ERROR," could not parse end position \"%s\"", redolsn);
    }

    /*组装redolsn信息*/
    checkpoint_obj->redolsn = ((uint64) hi) << 32 | lo;

    if (sscanf(PQgetvalue(res, 0, 1), "%u", &tlid) != 1)
    {
        elog(RLOG_ERROR," could not parse end position \"%s\"", PQgetvalue(res, 0, 1));
    }
    checkpoint_obj->tlid = (TimeLineID)tlid;

    if (sscanf(PQgetvalue(res, 0, 2), "%u:%u", &epoch, &nextfullxid) != 2)
    {
        elog(RLOG_ERROR," could not parse end position \"%s\"", PQgetvalue(res, 0, 2));
    }
    checkpoint_obj->nextfullxid = (TransactionId)nextfullxid;

    PQclear(res);
    return checkpoint_obj;

}

XLogRecPtr databaserecv_currentlsn_get(PGconn* conn)
{
    char* lsn = NULL;
    XLogRecPtr currentlsn = 0;
    PGresult *res = NULL;

    uint32        hi = 0,
                lo = 0;

    /*获取当前数据库currentlsn信息*/
    res =  conn_exec(conn, "SELECT pg_current_wal_insert_lsn();");
    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_ERROR, "failed get excute SQL result: SELECT pg_current_wal_insert_lsn()");
    }

    /*获取执行结果，为字符串信息*/
    lsn = PQgetvalue(res, 0, 0);

    /*以"%X/%X"格式读取redolsn中数据，并把值赋给hi，lo*/
    if (sscanf(lsn, "%X/%X", &hi, &lo) != 2)
    {
        elog(RLOG_ERROR," could not parse end position \"%s\"", lsn);
    }

    /*组装currentlsn信息*/
    currentlsn = ((uint64) hi) << 32 | lo;

    PQclear(res);
    return currentlsn;
}

char* databaserecv_monetary_get(PGconn* conn)
{
    char *monetary = NULL;
    PGresult *res = NULL;

    /*获取当前数据库currentlsn信息*/
    res =  conn_exec(conn, "show lc_monetary");
    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQfinish(conn);
        PQclear(res);
        elog(RLOG_ERROR, "failed get excute SQL result: show lc_monetary");
    }

    monetary = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == monetary)
    {
        elog(RLOG_ERROR,"out of memory, %s", strerror(errno));
    }
    rmemset0(monetary, 0, '\0', NAMEDATALEN);

    /*以"%X/%X"格式读取redolsn中数据，并把值赋给hi，lo*/
    if (sscanf(PQgetvalue(res, 0, 0), "%s", monetary) != 1)
    {
        elog(RLOG_ERROR," could not parse end position \"%s\"", PQgetvalue(res, 0, 0));
    }

    PQclear(res);
    return monetary;
}

char* databaserecv_numeric_get(PGconn* conn)
{
    char *numeric = NULL;
    PGresult *res = NULL;

    /*获取当前数据库currentlsn信息*/
    res =  conn_exec(conn, "show lc_numeric");
    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: show lc_numeric");
    }

    numeric = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == numeric)
    {
        elog(RLOG_ERROR,"out of memory, %s", strerror(errno));
    }
    rmemset0(numeric, 0, '\0', NAMEDATALEN);

    /*以"%X/%X"格式读取redolsn中数据，并把值赋给hi，lo*/
    if (sscanf(PQgetvalue(res, 0, 0), "%s", numeric) != 1)
    {
        elog(RLOG_ERROR," could not parse end position \"%s\"", PQgetvalue(res, 0, 0));
    }

    PQclear(res);
    return numeric;
}

char* databaserecv_timezone_get(PGconn* conn)
{
    char *timezone = NULL;
    PGresult *res = NULL;

    /*获取当前数据库currentlsn信息*/
    res =  conn_exec(conn, "show timezone");
    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result:show timezone");
    }

    timezone = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == timezone)
    {
        elog(RLOG_ERROR,"out of memory, %s", strerror(errno));
    }
    rmemset0(timezone, 0, '\0', NAMEDATALEN);

    /*以"%X/%X"格式读取redolsn中数据，并把值赋给hi，lo*/
    if (sscanf(PQgetvalue(res, 0, 0), "%s", timezone) != 1)
    {
        elog(RLOG_ERROR," could not parse end position \"%s\"", PQgetvalue(res, 0, 0));
    }

    PQclear(res);
    return timezone;
}

Oid databaserecv_database_get(PGconn* conn)
{
    Oid database = 0;
    PGresult *res = NULL;
    char    stmtsql[1024] = {'\0'};

    /* 获取数据库的标识 */
    rmemset1(stmtsql, 0, 0, 1024);
    sprintf(stmtsql, "SELECT oid FROM pg_database WHERE datname = current_database();");
    res =  conn_exec(conn, stmtsql);
    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: %s", stmtsql);
    }

    sscanf(PQgetvalue(res, 0, 0), "%u", &database);

    PQclear(res);

    return database;
}

char* databaserecv_orgencoding_get(PGconn* conn)
{
    char* orgencoding = NULL;
    char    stmtsql[1024] = {'\0'};
    PGresult *res = NULL;

    /* 获取数据库的标识 */
    rmemset1(stmtsql, 0, 0, 1024);
    sprintf(stmtsql, "show server_encoding");
    res =  conn_exec(conn, stmtsql);
    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: show server_encoding");
    }

    orgencoding = (char*)rmalloc0(NAMEDATALEN);
    if(NULL == orgencoding)
    {
        elog(RLOG_ERROR,"out of memory, %s", strerror(errno));
    }
    rmemset0(orgencoding, 0, '\0', NAMEDATALEN);

    sscanf(PQgetvalue(res, 0, 0), "%s", orgencoding);

    PQclear(res);

    return orgencoding;
}

TransactionId databaserecv_transactionid_get(PGconn* conn)
{
    uint64 tid = 0;
    PGresult *res = NULL;
    char    stmtsql[1024] = {'\0'};

    /* 获取数据库的标识 */
    rmemset1(stmtsql, 0, 0, 1024);
    sprintf(stmtsql, "select txid_current();");
    res =  conn_exec(conn, stmtsql);
    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: %s", stmtsql);
    }

    sscanf(PQgetvalue(res, 0, 0), "%lu", &tid);

    PQclear(res);

    return (TransactionId)tid;
}

TimestampTz databaserecv_timestamp_get(PGconn* conn)
{
    int64 timestamp = 0;
    PGresult *res = NULL;
    char    stmtsql[1024] = {'\0'};

    /* 获取数据库的标识 */
    rmemset1(stmtsql, 0, 0, 1024);
    sprintf(stmtsql, "SELECT backend_start FROM pg_stat_activity ORDER BY backend_start ASC LIMIT 1;");
    res =  conn_exec(conn, stmtsql);
    if (PQnfields(res) != 1 && PQntuples(res) != 1)
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: %s", stmtsql);
    }

    sscanf(PQgetvalue(res, 0, 0), "%lu", &timestamp);

    return (TimestampTz)timestamp;
}
/*
 * 执行 checkpoint
*/
void databaserecv_checkpoint(PGconn* conn)
{
    PGresult   *res = NULL;
    char        stmt[MAX_EXEC_SQL_LEN] = {'\0'};
    if(NULL == conn)
    {
        return;
    }

    snprintf(stmt, MAX_EXEC_SQL_LEN, "CHECKPOINT;");
    res =  conn_exec(conn, stmt);
    PQclear(res);
}

/*
 * 3、创建触发器相关
*/
bool databaserecv_trigger_set(PGconn* conn)
{
    PGresult   *res = NULL;
    char        stmt[MAX_EXEC_SQL_LEN] = {'\0'};
    if(NULL == conn)
    {
        return false;
    }

    /* 1、创建函数 */
    snprintf(stmt, MAX_EXEC_SQL_LEN, "CREATE OR REPLACE FUNCTION public.xsc_set_supplement() "
                                            "    RETURNS event_trigger  "
                                            "    AS $$                  "
                                            "   DECLARE                 "
                                            "       obj record;         "
                                            "       tboid oid;          "
                                            "    BEGIN                  "
                                            "       FOR obj IN SELECT objid,object_identity FROM pg_event_trigger_ddl_commands() WHERE command_tag IN ('CREATE TABLE','CREATE TABLE AS')  "
                                            "           LOOP            "
                                            "               SELECT oid INTO tboid FROM  pg_class    "
                                            "               WHERE relreplident != 'f'               "
                                            "               AND   relpersistence = 'p';             "
                                            "               IF FOUND THEN                           "
                                            "                   EXECUTE format('alter table %%s replica identity full',obj.object_identity); "
                                            "               END IF;                                 "
                                            "           END LOOP;                                   "
                                            "    END;                   "
                                            "    $$ LANGUAGE plpgsql;");

    res =  conn_exec(conn, stmt);
    if (NULL == res)
    {
        return false;
    }
    
    PQclear(res);

    /* 2 创建触发器 */
    /* 查看触发器是否存在,不存在则创建 */
    snprintf(stmt, MAX_EXEC_SQL_LEN, "SELECT oid FROM pg_event_trigger WHERE evtname = 'xsc_set_supplement_trigger'");
    res =  conn_exec(conn, stmt);
    if (NULL == res)
    {
        return false;
    }
    if (0 == PQntuples(res))
    {
        PQclear(res);

        /* 创建触发器 1, 执行完CREATE TABL DDL后获取表信息 */
        snprintf(stmt, MAX_EXEC_SQL_LEN, "CREATE EVENT TRIGGER xsc_set_supplement_trigger                      "
                                                "    ON DDL_COMMAND_END                                     "
                                                "    WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')         "
                                                "    EXECUTE FUNCTION public.xsc_set_supplement();          ");
        res =  conn_exec(conn, stmt);
        if (NULL == res)
        {
            return false;
        }
    }
    PQclear(res);

    return true;
}

/*
 * 2、创建同步表
*/
bool databaserecv_synctable_set(PGconn* conn)
{
    PGresult   *res = NULL;
    char        stmt[MAX_EXEC_SQL_LEN] = {'\0'};
    if(NULL == conn)
    {
        return false;
    }

    snprintf(stmt, MAX_EXEC_SQL_LEN, "SELECT oid FROM pg_namespace WHERE nspname = '%s';", guc_getConfigOption(CFG_KEY_CATALOGSCHEMA));
    res =  conn_exec(conn, stmt);
    if (NULL == res )
    {
        return false;
    }
    
    if (0 == PQntuples(res))
    {
        PQclear(res);
        /* 创建schema */
        snprintf(stmt, MAX_EXEC_SQL_LEN, "CREATE SCHEMA %s", guc_getConfigOption(CFG_KEY_CATALOGSCHEMA));
        res =  conn_exec(conn, stmt);
        if (NULL == res )
        {
            return false;
        }
    }
    PQclear(res);

    /* 对模式赋权 */
    snprintf(stmt, MAX_EXEC_SQL_LEN, "GRANT ALL ON SCHEMA %s TO PUBLIC ;", guc_getConfigOption(CFG_KEY_CATALOGSCHEMA));
    res =  conn_exec(conn, stmt);
    if (NULL == res )
    {
        return false;
    }
    PQclear(res);

    /* 创建同步表 */
    snprintf(stmt, MAX_EXEC_SQL_LEN, "CREATE TABLE IF NOT EXISTS %s.sync_status (name CHAR(64) UNIQUE, type smallint, stat smallint, emit_fileid text, emit_offset text, rewind_fileid text, rewind_offset text, xid text, lsn text);", guc_getConfigOption(CFG_KEY_CATALOGSCHEMA));
    res =  conn_exec(conn, stmt);
    if (NULL == res )
    {
        return false;
    }
    PQclear(res);
    return true;
}

bool databaserecv_integrate_dbinit(void)
{
    const char* url = NULL;
    PGconn* conn = NULL;

    /*获取连接信息*/
    url = guc_getConfigOption("url");

    /*连接数据库*/
    conn = conn_get(url);

    /* 连接错误退出 */
    if(NULL == conn)
    {
        return false;
    }

    /* 创建同步表 */
    if(!databaserecv_synctable_set(conn))
    {
        return false;
    }

    /*关闭 conn*/
    PQfinish(conn);
    conn = NULL;

    return true;
}

/* 执行 IDENTIFY_SYSTEM 并获取返回值 */
bool databaserecv_identifysystem(PGconn* conn, TimeLineID* dbtli, XLogRecPtr* dblsn)
{
    uint32 hi       = 0;
    uint32 lo       = 0;
    PGresult* res   = NULL;

    /* 执行 identify_system 命令, 获取数据库时间线和数据库的lsn */
    res = PQexec(conn, "IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(RLOG_WARNING, "exec IDENTIFY_SYSTEM error, %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    /* 获取返回值 */
    if (1 != PQntuples(res) || 3 > PQnfields(res))
    {
        elog(RLOG_WARNING, "could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields",
                           PQntuples(res),
                           PQnfields(res),
                            1,
                            3);

        PQclear(res);
        return false;
    }

    *dbtli = atoi(PQgetvalue(res, 0, 1));
    if (sscanf(PQgetvalue(res, 0, 2), "%X/%X", &hi, &lo) != 2)
    {
        elog(RLOG_WARNING, "could not parse write-ahead log location %s", PQgetvalue(res, 0, 2));
        PQclear(res);
        return false;
    }
    *dblsn = ((uint64) hi) << 32 | lo;

    PQclear(res);
    return true;
}

/* 执行 SHOW wal_segment_size 并获取返回值 */
bool databaserecv_showwalsegmentsize(PGconn* conn, uint32* segsize)
{
    int xlogval         = 0;
    int multiplier      = 0;
    char xlogunit[3]    = { 0 };
    PGresult* res       = NULL;

    /* 执行 SHOW wal_segment_size 命令, 获取事务日志大小 */
    res = PQexec(conn, "SHOW wal_segment_size");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(RLOG_WARNING, "exec SHOW wal_segment_size error, %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    /* 获取返回值 */
    if (1 != PQntuples(res) || 1 > PQnfields(res))
    {
        elog(RLOG_WARNING, "could not fetch WAL segment size: got %d rows and %d fields, expected %d rows and %d or more fields",
                           PQntuples(res),
                           PQnfields(res),
                           1,
                           1);

        PQclear(res);
        return false;
    }

    if (sscanf(PQgetvalue(res, 0, 0), "%d%s", &xlogval, xlogunit) != 2)
    {
        elog(RLOG_WARNING, "WAL segment size could not be parsed");
        PQclear(res);
        return false;
    }

    PQclear(res);
    if (0 == strcmp(xlogunit, "MB"))
    {
        multiplier = 1024 * 1024;
    }
    else if (0 == strcmp(xlogunit, "GB"))
    {
        multiplier = 1024 * 1024 * 1024;
    }
    *segsize = xlogval * multiplier;
    return true;
}

/* 获取 xlogblocksize */
bool databaserecv_showwalblocksize(PGconn* conn, int* blksize)
{
    PGresult* res       = NULL;

    /* 执行 SHOW wal_block_size 命令, 获取事务日志大小 */
    res = PQexec(conn, "SHOW wal_block_size");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(RLOG_WARNING, "exec SHOW wal_block_size error, %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    /* 获取返回值 */
    if (1 != PQntuples(res) || 1 > PQnfields(res))
    {
        elog(RLOG_WARNING, "could not fetch WAL block size: got %d rows and %d fields, expected %d rows and %d or more fields",
                           PQntuples(res),
                           PQnfields(res),
                           1,
                           1);

        PQclear(res);
        return false;
    }

    if (1 != sscanf(PQgetvalue(res, 0, 0), "%d", blksize))
    {
        elog(RLOG_WARNING, "WAL block size could not be parsed");
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}

/* 获取 server version */
bool databaserecv_showserverversion(PGconn* conn, char** strversion)
{
    int len = 0;
    PGresult* res       = NULL;

    /* 执行 SHOW wal_block_size 命令, 获取事务日志大小 */
    res = PQexec(conn, "SHOW server_version");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(RLOG_WARNING, "exec SHOW server_version error, %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    /* 获取返回值 */
    if (1 != PQntuples(res) || 1 > PQnfields(res))
    {
        elog(RLOG_WARNING, "could not fetch server_version: got %d rows and %d fields, expected %d rows and %d or more fields",
                           PQntuples(res),
                           PQnfields(res),
                           1,
                           1);

        PQclear(res);
        return false;
    }

    len = strlen(PQgetvalue(res, 0, 0));
    len += 1;
    *strversion = rmalloc0(len);
    if (NULL == *strversion)
    {
        elog(RLOG_WARNING, "out of memory");
        PQclear(res);
        return false;
    }
    rmemset0(*strversion, 0, '\0', len);
    len -= 1;
    rmemcpy0(*strversion, 0, PQgetvalue(res, 0, 0), len);

    PQclear(res);
    return true;
}

/* 编译时是否开启 FDE */
bool databaserecv_getconfigurefde(PGconn* conn, bool *fde)
{
    char* configure     = NULL;
    PGresult* res       = NULL;

    /* 执行 SHOW wal_block_size 命令, 获取事务日志大小 */
    res = PQexec(conn, "SELECT pg_config()");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(RLOG_WARNING, "exec SELECT pg_config() error, %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    /* 获取 CONFIGURE 项 */
    configure = PQgetvalue(res, 13, 0);
    if (NULL == strstr(configure, "--enable-fde"))
    {
        *fde = false;
    }
    else
    {
        *fde = true;
    }

    PQclear(res);
    return true;
}

/* 获取时间线文件数据 */
bool databaserecv_timelinehistory(PGconn* conn, TimeLineID tli, char** pfilename, char** pcontent)
{
    int len = 0;
    PGresult* res       = NULL;
    char sqlcmd[MAX_EXEC_SQL_LEN] = { 0 };

    /* 组装 timeline history 命令 */
    snprintf(sqlcmd, MAX_EXEC_SQL_LEN, "TIMELINE_HISTORY %u", tli);
    res = PQexec(conn, sqlcmd);
    if (PGRES_TUPLES_OK != PQresultStatus(res))
    {
        elog(RLOG_WARNING,
             "could not send replication command %s, %s,%s",
             sqlcmd,
             PQresultErrorMessage(res),
             PQresultErrorField(res, PG_DIAG_SQLSTATE));
        PQclear(res);
        return false;
    }

    if (2 != PQnfields(res) || 1 != PQntuples(res))
    {
        elog(RLOG_WARNING, 
             "unexpected response to TIMELINE_HISTORY command: got %d rows and %d fields, expected %d rows and %d fields",
             PQntuples(res),
             PQnfields(res),
             1,
             2);
        PQclear(res);
        return false;
    }

    /* 文件名称 */
    len = strlen(PQgetvalue(res, 0, 0));
    len += 1;
    *pfilename = rmalloc0(len);
    if(NULL == *pfilename)
    {
        elog(RLOG_WARNING, "out of memory");
        PQclear(res);
        return false;
    }
    rmemset0(*pfilename, 0, '\0', len);
    len -= 1;
    rmemcpy0(*pfilename, 0, PQgetvalue(res, 0, 0), len);

    /* 文件内容 */
    len = strlen(PQgetvalue(res, 0, 1));
    len += 1;
    *pcontent = rmalloc0(len);
    if(NULL == *pcontent)
    {
        elog(RLOG_WARNING, "out of memory");
        rfree(*pfilename);
        *pfilename = NULL;
        PQclear(res);
        return false;
    }
    rmemset0(*pcontent, 0, '\0', len);
    len -= 1;
    rmemcpy0(*pcontent, 0, PQgetvalue(res, 0, 1), len);

    PQclear(res);
    return true;
}

/* 执行 start replication */
bool databaserecv_startreplication(PGconn* conn, TimeLineID tli, XLogRecPtr startpos, char* slotname)
{
    PGresult* res       = NULL;
    char sqlslot[128] = { 0 };
    char sqlcmd[MAX_EXEC_SQL_LEN] = { 0 };

    if (NULL == slotname || '\0' == slotname[0])
    {
        sqlslot[0] = '\0';
    }
    else
    {
        snprintf(sqlslot, 128, "SLOT %s ", slotname);
    }

    /* 组装 start replication 命令 */
    snprintf(sqlcmd,
             MAX_EXEC_SQL_LEN,
             "START_REPLICATION %s%X/%X TIMELINE %u",
             sqlslot,
             (uint32) (startpos >> 32), (uint32) startpos,
             tli);

    res = PQexec(conn, sqlcmd);
    if (PGRES_COPY_BOTH != PQresultStatus(res))
    {
        elog(RLOG_WARNING,
             "could not send replication command START_REPLICATION : %s, %s",
             PQresultErrorMessage(res),
             PQresultErrorField(res, PG_DIAG_SQLSTATE));

        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}