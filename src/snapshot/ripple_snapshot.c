#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "signal/ripple_signal.h"
#include "command/ripple_cmd.h"
#include "snapshot/ripple_snapshot.h"
#include "utils/conn/ripple_conn.h"

ripple_snapshot* ripple_snapshot_buildfromdb(PGconn *conn)
{
    bool found = false;
    uint32 xid = 0;
    uint32 len = 0;
    PGresult *res = NULL;
    char* start = NULL;
    char* name = NULL;
    const char* result = NULL;
    ripple_snapshot* snapshot = NULL;
    char stmt[RIPPLE_MAX_EXEC_SQL_LEN] = {'\0'};

    HASHCTL hashCtl = {'\0'};
    ripple_snapshot_xid* entry = NULL;
    
    snapshot = (ripple_snapshot*)rmalloc0(sizeof(ripple_snapshot));
    if (NULL == snapshot)
    {
         elog(RLOG_ERROR,"out of memory, %s", strerror(errno));
    }
    rmemset0(snapshot, 0, 0, sizeof(ripple_snapshot));

    hashCtl.keysize = sizeof(TransactionId);
    hashCtl.entrysize = sizeof(ripple_snapshot_xid);
    snapshot->xids = hash_create("snapshot_xids_hash",128,&hashCtl,
                                HASH_ELEM | HASH_BLOBS);

    rmemset1(stmt, 0, 0, 1024);
    snprintf(stmt, RIPPLE_MAX_EXEC_SQL_LEN, "select pg_export_snapshot();");
    res = (PGresult*)ripple_conn_exec(conn, stmt);
    if ((PQnfields(res) != 1 && PQntuples(res) != 1))
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: %s", stmt);
    }
    name = PQgetvalue(res, 0, 0);
    len = strlen(name) + 1;

    snapshot->name = (char*)rmalloc0(len);
    if (NULL == snapshot->name)
    {
         elog(RLOG_ERROR,"out of memory, %s", strerror(errno));
    }
    rmemset0(snapshot->name, 0, '\0', len);
    rmemcpy0(snapshot->name, 0, name, len - 1);

    PQclear(res);

    rmemset1(stmt, 0, '\0', 1024);
    snprintf(stmt, RIPPLE_MAX_EXEC_SQL_LEN, "select txid_current_snapshot();");
    res = (PGresult*)ripple_conn_exec(conn, stmt);
    if ((PQnfields(res) != 1 && PQntuples(res) != 1))
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: %s", stmt);
    }

    result = PQgetvalue(res, 0, 0);

    elog(RLOG_DEBUG,"database snapshot result, %s", result);

    if (2 == sscanf(result, "%u:%u:%u", &(snapshot->xmin), &(snapshot->xmax), &xid))
    {
        PQclear(res);
        return snapshot;
    }
    else
    {
        /* 加载到 hash 中 */
        entry = hash_search(snapshot->xids, &xid, HASH_ENTER, &found);
        if(false == found)
        {
            entry->xid = (TransactionId)xid;
        }

        start = strstr(result, ",");
        while (start != NULL)
        {
            // 使用 sscanf 提取数字
            xid = 0;
            start++;
            if (sscanf(start, "%u", &xid) == 1)
            {
                /* 加载到 hash 中 */
                entry = hash_search(snapshot->xids, &xid, HASH_ENTER, &found);
                if(false == found)
                {
                    entry->xid = (TransactionId)xid;
                }
                start = strstr(start, ",");
            }
        }

        PQclear(res);
    }

    return snapshot;
}

ripple_snapshot *ripple_snapshot_copy(ripple_snapshot *snap)
{
    ripple_snapshot *result = NULL;
    HASHCTL hashCtl = {'\0'};
    ripple_snapshot_xid* entry = NULL;
    ripple_snapshot_xid* result_entry = NULL;
    HASH_SEQ_STATUS snap_status;

    result = rmalloc0(sizeof(ripple_snapshot));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_snapshot));

    result->name = rstrdup(snap->name);
    result->xmax = snap->xmax;
    result->xmin = snap->xmin;

    hashCtl.keysize = sizeof(TransactionId);
    hashCtl.entrysize = sizeof(ripple_snapshot_xid);
    result->xids = hash_create("snapshot_xids_hash",128,&hashCtl,
                                HASH_ELEM | HASH_BLOBS);

    hash_seq_init(&snap_status, snap->xids);
    while (NULL != (entry = hash_seq_search(&snap_status)))
    {
        /* 拷贝 hash */
        result_entry = hash_search(result->xids, &entry->xid, HASH_ENTER, NULL);
        result_entry->xid = entry->xid;
    }

    return result;
}

void ripple_snapshot_free(ripple_snapshot *snapshot)
{
    if (NULL == snapshot)
    {
        return;
    }

    if (NULL != snapshot->name)
    {
        rfree(snapshot->name);
    }

    rfree(snapshot);

    return;
}
