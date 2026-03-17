#ifndef _SYNC_H
#define _SYNC_H


typedef struct SYNCSTATE
{
    HTAB*           hpreparedno;        /* oid 为 prepared, 在 hash 中的不需要在数据库中执行 prepared 语句,当断开连接时,需要清空 */
    PGconn*         conn;
    char*           conninfo;
    char*           name;
}syncstate;

/* hash不需要在数据库中执行 prepared 语句 */
typedef struct SYNCSTATE_PREPARED
{
    uint64                      number;                              /* 预解析语句编号 */
    char                        preparename[64];                     /* 预解析语句名称 */
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
