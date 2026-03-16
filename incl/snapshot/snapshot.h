#ifndef _RIPPLE_SNAPSHOT_H
#define _RIPPLE_SNAPSHOT_H

typedef struct RIPPLE_SNAPSHOT
{
    char*                   name;
    TransactionId           xmin;
    TransactionId           xmax;
    HTAB*                   xids;
}ripple_snapshot;

typedef struct RIPPLE_SNAPSHOT_XID
{
    TransactionId   xid;
} ripple_snapshot_xid;


ripple_snapshot* ripple_snapshot_buildfromdb(PGconn *conn);

ripple_snapshot *ripple_snapshot_copy(ripple_snapshot *snap);

void ripple_snapshot_free(ripple_snapshot *snapshot);

#endif
