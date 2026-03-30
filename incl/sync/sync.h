#ifndef _SYNC_H
#define _SYNC_H

typedef struct SYNCSTATE
{
    HTAB*   hpreparedno; /* oid-based prepared statements; those in hash don't need to be re-prepared
                            on database; cleared on disconnect */
    PGconn* conn;
    char*   conninfo;
    char*   name;
} syncstate;

/* hash of prepared statements that don't need to be re-prepared on database */
typedef struct SYNCSTATE_PREPARED
{
    uint64 number;          /* prepared statement sequence number */
    char   preparename[64]; /* prepared statement name */
} syncstate_prepared;

HTAB* syncstate_hpreparedno_init(void);

void syncstate_hpreparedno_free(syncstate* syncstate);

void syncstate_reset(syncstate* syncstate);

void syncstate_conninfo_set(syncstate* syncstate, char* conn);

bool syncstate_conn(syncstate* syncstate, void* thrnode);

bool syncstate_update_statustb(syncstate* syncstate, void* txn, bool exec);

bool syncstate_update_statustb_commitlsn(syncstate* syncstate, XLogRecPtr commitlsn);

bool syncstate_update_rewind(syncstate* syncstate, recpos rewind);

bool syncstate_applytxn(syncstate* syncstate, void* thrnode, void* txn, bool update);

bool sync_txncommit(syncstate* syncstate, void* txn);

bool sync_txnbegin(syncstate* syncstate);

bool syncstate_bigtxn_applytxn(syncstate* syncstate, void* thrnode, void* txn);

void syncstate_destroy(syncstate* syncstate);

#endif
