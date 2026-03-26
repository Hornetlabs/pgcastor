#ifndef _TRANSCACHE_H
#define _TRANSCACHE_H

/* relfilenode hash table key and entry */
typedef struct RELFILENODE2OID
{
    RelFileNode relfilenode;
    Oid         oid;
} relfilenode2oid;

typedef struct TXN_SYSDICT
{
    pg_parser_translog_tbcol_values* colvalues;
    void*                            convert_colvalues;
} txn_sysdict;

typedef struct CHECKPOINTNODE
{
    TransactionId          xid;
    XLogRecPtr             redolsn;
    struct CHECKPOINTNODE* next;
    struct CHECKPOINTNODE* prev;
} checkpointnode;

typedef struct CHECKPOINTS
{
    checkpointnode* head;
    checkpointnode* tail;
} checkpoints;

typedef struct TRANSCACHE
{
    /* total size used by statement cache */
    uint64          totalsize;

    /* space occupied by capture_buffer transaction cache */
    uint64          capture_buffer;

    /*
     * Maintenance logic:
     *  chkpts is a linked list where head points to the first checkpoint and tail to the last
     *      When parser thread parses a checkpoint, add a node to chkpts.
     *      In the serialization thread, after restartlsn is updated, check whether to remove
     *          nodes from chkpts; when removing nodes, also clean fpwcache data.
     */
    checkpoints*    chkpts;
    txn_dlist*      transdlist;
    HTAB*           by_txns; /* pending transaction cache, key: transactionid, entry: txn */

    /* FPW (full page write) cache */
    HTAB*           by_fpwtuples;  /* hash table for storing full page write tuples */
    List*           fpwtupleslist; /* linked list of hash keys and lsn */

    cache_sysdicts* sysdicts; /* system dictionary cache */

    /*
     * Sync dataset
     */
    /* table include rules */
    List*           tableincludes;

    /* table exclude rules */
    List*           tableexcludes;

    /* new table pattern rules */
    List*           addtablepattern;

    /*
     * Sync dataset keyed by relid
     *  Generated from tableincludes/tableexcludes and runtime tableaddpattern
     *  At startup, initial sync dataset is generated from tableincludes and tableexcludes
     *  During runtime, when new tables match tableaddpattern rules, they are added to the sync
     * dataset
     */
    HTAB*           hsyncdataset;

    /*
     * Dataset that causes transactions to be filtered
     *    Key: Oid
     *    Value: filter_oid2datasetnode
     */
    HTAB*           htxnfilterdataset;
} transcache;

typedef transcache txnscontext;

extern void transcache_dlist_remove(void*       in_ctx,
                                    txn*        txn,
                                    bool*       brestart,
                                    XLogRecPtr* restartlsn,
                                    bool*       bconfirm,
                                    XLogRecPtr* confirmlsn,
                                    bool        bset);

extern txn* transcache_getTXNByXid(void* in_ctx, uint64_t xid);

extern txn* transcache_getTXNByXidFind(transcache* transcache, uint64_t xid);

extern void transcache_removeTXNByXid(transcache* in_transcache, uint64_t xid);

bool transcache_refreshlsn(void* in_ctx, txn* txn);

bool transcache_deletetxn(void* in_ctx, txn* txn);

extern void transcache_sysdict2his(txn* txn);

extern void transcache_sysdict_free(txn* txn);

extern void transcache_free(transcache* transcache);

/* get database oid */
extern Oid transcache_getdboid(void* in_transcache);

/* get database name */
extern char* transcache_getdbname(Oid dbid, void* in_transcache);

/* get namespace data */
extern void* transcache_getnamespace(Oid oid, void* in_transcache);

/* get class data */
extern void* transcache_getclass(Oid oid, void* in_transcache);

/* get attribute data */
extern void* transcache_getattributes(Oid oid, void* in_transcache);

extern void* transcache_getindex(Oid oid, void* in_transcache);

/* get type data */
extern void* transcache_gettype(Oid oid, void* in_transcache);

#endif
