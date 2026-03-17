#ifndef _SNAPSHOT_H
#define _SNAPSHOT_H

typedef struct SNAPSHOT
{
    char*                   name;
    TransactionId           xmin;
    TransactionId           xmax;
    HTAB*                   xids;
}snapshot;

typedef struct SNAPSHOT_XID
{
    TransactionId   xid;
} snapshot_xid;


snapshot* snapshot_buildfromdb(PGconn *conn);

snapshot *snapshot_copy(snapshot *snap);

void snapshot_free(snapshot *snapshot);

#endif
