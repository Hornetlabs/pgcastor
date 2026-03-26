#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "signal/app_signal.h"
#include "command/cmd.h"
#include "snapshot/snapshot.h"
#include "utils/conn/conn.h"

snapshot* snapshot_buildfromdb(PGconn* conn)
{
    bool          found = false;
    uint32        xid = 0;
    uint32        len = 0;
    PGresult*     res = NULL;
    char*         start = NULL;
    char*         name = NULL;
    const char*   result = NULL;
    snapshot*     snapshot_obj = NULL;
    char          stmt[MAX_EXEC_SQL_LEN] = {'\0'};

    HASHCTL       hashCtl = {'\0'};
    snapshot_xid* entry = NULL;

    snapshot_obj = (snapshot*)rmalloc0(sizeof(snapshot));
    if (NULL == snapshot_obj)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(snapshot_obj, 0, 0, sizeof(snapshot));

    hashCtl.keysize = sizeof(TransactionId);
    hashCtl.entrysize = sizeof(snapshot_xid);
    snapshot_obj->xids = hash_create("snapshot_xids_hash", 128, &hashCtl, HASH_ELEM | HASH_BLOBS);

    rmemset1(stmt, 0, 0, 1024);
    snprintf(stmt, MAX_EXEC_SQL_LEN, "select pg_export_snapshot();");
    res = (PGresult*)conn_exec(conn, stmt);
    if ((PQnfields(res) != 1 && PQntuples(res) != 1))
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: %s", stmt);
    }
    name = PQgetvalue(res, 0, 0);
    len = strlen(name) + 1;

    snapshot_obj->name = (char*)rmalloc0(len);
    if (NULL == snapshot_obj->name)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(snapshot_obj->name, 0, '\0', len);
    rmemcpy0(snapshot_obj->name, 0, name, len - 1);

    PQclear(res);

    rmemset1(stmt, 0, '\0', 1024);
    snprintf(stmt, MAX_EXEC_SQL_LEN, "select txid_current_snapshot();");
    res = (PGresult*)conn_exec(conn, stmt);
    if ((PQnfields(res) != 1 && PQntuples(res) != 1))
    {
        PQclear(res);
        PQfinish(conn);
        elog(RLOG_ERROR, "failed get excute SQL result: %s", stmt);
    }

    result = PQgetvalue(res, 0, 0);

    elog(RLOG_DEBUG, "database snapshot result, %s", result);

    if (2 == sscanf(result, "%u:%u:%u", &(snapshot_obj->xmin), &(snapshot_obj->xmax), &xid))
    {
        PQclear(res);
        return snapshot_obj;
    }
    else
    {
        /* Load into hash */
        entry = hash_search(snapshot_obj->xids, &xid, HASH_ENTER, &found);
        if (false == found)
        {
            entry->xid = (TransactionId)xid;
        }

        start = strstr(result, ",");
        while (start != NULL)
        {
            /* Use sscanf to extract numbers */
            xid = 0;
            start++;
            if (sscanf(start, "%u", &xid) == 1)
            {
                /* Load into hash */
                entry = hash_search(snapshot_obj->xids, &xid, HASH_ENTER, &found);
                if (false == found)
                {
                    entry->xid = (TransactionId)xid;
                }
                start = strstr(start, ",");
            }
        }

        PQclear(res);
    }

    return snapshot_obj;
}

snapshot* snapshot_copy(snapshot* snap)
{
    snapshot*       result = NULL;
    HASHCTL         hashCtl = {'\0'};
    snapshot_xid*   entry = NULL;
    snapshot_xid*   result_entry = NULL;
    HASH_SEQ_STATUS snap_status;

    result = rmalloc0(sizeof(snapshot));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(snapshot));

    result->name = rstrdup(snap->name);
    result->xmax = snap->xmax;
    result->xmin = snap->xmin;

    hashCtl.keysize = sizeof(TransactionId);
    hashCtl.entrysize = sizeof(snapshot_xid);
    result->xids = hash_create("snapshot_xids_hash", 128, &hashCtl, HASH_ELEM | HASH_BLOBS);

    hash_seq_init(&snap_status, snap->xids);
    while (NULL != (entry = hash_seq_search(&snap_status)))
    {
        /* Copy hash */
        result_entry = hash_search(result->xids, &entry->xid, HASH_ENTER, NULL);
        result_entry->xid = entry->xid;
    }

    return result;
}

void snapshot_free(snapshot* snapshot)
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
