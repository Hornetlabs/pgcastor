#ifndef ONLINEREFRESH_CAPTUR_H
#define ONLINEREFRESH_CAPTUR_H

typedef struct onlinerefresh_capture
{
    bool              increment;
    int               parallelcnt; /* Stock worker threads, parallel count */
    recpos            redo;        /* redolsn */
    char*             conninfo;    /* Connection string */
    snapshot*         snapshot;    /* Snapshot */
    PGconn*           conn;        /* Database connection */
    PGconn*           snap_conn;   /* Database connection for exporting snapshot */
    uuid_t*           no;          /* onlinerefresh Number */
    char*             data;        /* Directory for storing data */
    refresh_tables*   tables;      /* Tables to sync stock */
    FullTransactionId txid;        /* Next transaction ID to be used when getting snapshot */
    dlist*            xids;        /* Transaction ID list, FullTransactionId */
    char              padding[CACHELINE_SIZE];
    queue*            refreshtqueue; /* Stock data sync */
    char              padding1[CACHELINE_SIZE];
    queue*            recordqueue; /* record cache */
    char              padding2[CACHELINE_SIZE];
    cache_txn*        parser2serialtxns; /* Serialize */
    char              padding3[CACHELINE_SIZE];
    file_buffers*     txn2filebuffer; /* Flush buffer */
    char              padding4[CACHELINE_SIZE];
    thrsubmgr*        thrsmgr;
    char              padding5[CACHELINE_SIZE];

    /* Clean up onlinerefresh remaining */
    void              (*removeolrefresh)(void* privdata, void* olrefresh);

    /* Parent structure */
    void*             privdata;

    char              padding6[CACHELINE_SIZE];
} onlinerefresh_capture;

extern void* onlinerefresh_capture_main(void* args);
extern void onlinerefresh_capture_destroy(void* privdata);
extern int onlinerefresh_capture_cmp(void* s1, void* s2);
extern onlinerefresh_capture* onlinerefresh_capture_init(bool increment);
extern void onlinerefresh_capture_increment_set(onlinerefresh_capture* onlinerefresh_capture, bool increment);
extern void onlinerefresh_capture_state_set(onlinerefresh_capture* onlinerefresh_capture, int state);
extern void onlinerefresh_capture_redo_set(onlinerefresh_capture* onlinerefresh_capture, XLogRecPtr redo);
extern void onlinerefresh_capture_conninfo_set(onlinerefresh_capture* onlinerefresh_capture, char* conninfo);
extern void onlinerefresh_capture_snapshot_set(onlinerefresh_capture* onlinerefresh_capture, snapshot* snapshot);
extern void onlinerefresh_capture_conn_set(onlinerefresh_capture* onlinerefresh_capture, PGconn* conn);
extern void onlinerefresh_capture_snap_conn_set(onlinerefresh_capture* onlinerefresh_capture, PGconn* snap_conn);
extern void onlinerefresh_capture_no_set(onlinerefresh_capture* onlinerefresh_capture, uuid_t* no);
extern void onlinerefresh_capture_txid_set(onlinerefresh_capture* onlinerefresh_capture, FullTransactionId txid);
extern void onlinerefresh_capture_xids_append(onlinerefresh_capture* onlinerefresh_capture, TransactionId xid);
extern void onlinerefresh_capture_add_xids_from_snapshot(onlinerefresh_capture* onlinerefresh_capture, snapshot* snap);
extern bool onlinerefresh_capture_isxidinsnapshot(onlinerefresh_capture* onlinerefresh_capture, FullTransactionId xid);
extern bool onlinerefresh_capture_isxidinxids(onlinerefresh_capture* onlinerefresh_capture, FullTransactionId xid);
extern void onlinerefresh_capture_xids_delete(onlinerefresh_capture* olcapture, dlistnode* dlnode);
extern bool onlinerefresh_capture_xids_isnull(onlinerefresh_capture* refresh);
extern void onlinerefresh_capture_tables_set(onlinerefresh_capture* onlinerefresh_capture, refresh_tables* tables);
#endif
