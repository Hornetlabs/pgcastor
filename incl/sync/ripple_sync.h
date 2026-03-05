#ifndef _RIPPLE_SYNC_H
#define _RIPPLE_SYNC_H


typedef struct RIPPLE_SYNCSTATE
{
    HTAB*           hpreparedno;        /* oid 为 prepared, 在 hash 中的不需要在数据库中执行 prepared 语句,当断开连接时,需要清空 */
    PGconn*         conn;
    char*           conninfo;
    char*           name;
}ripple_syncstate;

/* hash不需要在数据库中执行 prepared 语句 */
typedef struct RIPPLE_SYNCSTATE_PREPARED
{
    uint64                      number;                              /* 预解析语句编号 */
    char                        preparename[64];                     /* 预解析语句名称 */
} ripple_syncstate_prepared;

HTAB* ripple_syncstate_hpreparedno_init(void);

void ripple_syncstate_hpreparedno_free(ripple_syncstate* syncstate);

void ripple_syncstate_reset(ripple_syncstate* syncstate);

void ripple_syncstate_conninfo_set(ripple_syncstate* syncstate, char* conn);

bool ripple_syncstate_conn(ripple_syncstate* syncstate, void* thrnode);

bool ripple_syncstate_update_statustb(ripple_syncstate* syncstate, void* txn, bool exec);

bool ripple_syncstate_update_statustb_commitlsn(ripple_syncstate* syncstate, XLogRecPtr commitlsn);

bool ripple_syncstate_update_rewind(ripple_syncstate* syncstate, ripple_recpos rewind);

bool ripple_syncstate_applytxn(ripple_syncstate* syncstate, void* thrnode, void* txn, bool update);

bool ripple_sync_txncommit(ripple_syncstate* syncstate, void* txn);

bool ripple_sync_txnbegin(ripple_syncstate* syncstate);

bool ripple_syncstate_bigtxn_applytxn(ripple_syncstate* syncstate, void* thrnode, void* txn);

void ripple_syncstate_destroy(ripple_syncstate* syncstate);

#endif
