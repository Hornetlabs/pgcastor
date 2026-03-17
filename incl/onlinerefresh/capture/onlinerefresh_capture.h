#ifndef ONLINEREFRESH_CAPTUR_H
#define ONLINEREFRESH_CAPTUR_H

typedef struct onlinerefresh_capture
{
    bool                    increment;
    int                     parallelcnt;    /* 存量工作线程, 并行数量 */
    recpos           redo;           /* redolsn */
    char                   *conninfo;       /* 连接字串 */
    snapshot        *snapshot;       /* 快照 */
    PGconn                 *conn;           /* 数据库链接 */
    PGconn                 *snap_conn;      /* 导出快照的数据库链接 */
    uuid_t          *no;             /* onlinerefresh 编号 */
    char                   *data;           /* 存放数据的目录 */
    refresh_tables  *tables;         /* 待同步存量表 */
    FullTransactionId       txid;           /* 获取快照时下个将要使用的事务号 */
    dlist*                  xids;           /* 事务号链表, FullTransactionId */
    char                    padding[CACHELINE_SIZE];
    queue           *refreshtqueue;  /* 存量数据同步 */
    char                    padding1[CACHELINE_SIZE];
    queue           *recordqueue;   /* record缓存 */
    char                    padding2[CACHELINE_SIZE];
    cache_txn       *parser2serialtxns;  /* 序列化 */
    char                    padding3[CACHELINE_SIZE];
    file_buffers    *txn2filebuffer; /* 落盘buffer */
    char                    padding4[CACHELINE_SIZE];
    thrsubmgr*       thrsmgr;
    char                    padding5[CACHELINE_SIZE];

    /* 清理onlinerefresh剩余 */
    void                    (*removeolrefresh)(void* privdata, void* olrefresh);

    /* 父结构体 */
    void*                   privdata;

    char                    padding6[CACHELINE_SIZE];
} onlinerefresh_capture;

extern void *onlinerefresh_capture_main(void* args);
extern void onlinerefresh_capture_destroy(void* privdata);
extern int onlinerefresh_capture_cmp(void* s1, void* s2);
extern onlinerefresh_capture *onlinerefresh_capture_init(bool increment);
extern void onlinerefresh_capture_increment_set(onlinerefresh_capture *onlinerefresh_capture, bool increment);
extern void onlinerefresh_capture_state_set(onlinerefresh_capture *onlinerefresh_capture, int state);
extern void onlinerefresh_capture_redo_set(onlinerefresh_capture *onlinerefresh_capture, XLogRecPtr redo);
extern void onlinerefresh_capture_conninfo_set(onlinerefresh_capture *onlinerefresh_capture, char* conninfo);
extern void onlinerefresh_capture_snapshot_set(onlinerefresh_capture *onlinerefresh_capture, snapshot *snapshot);
extern void onlinerefresh_capture_conn_set(onlinerefresh_capture *onlinerefresh_capture, PGconn *conn);
extern void onlinerefresh_capture_snap_conn_set(onlinerefresh_capture *onlinerefresh_capture, PGconn *snap_conn);
extern void onlinerefresh_capture_no_set(onlinerefresh_capture *onlinerefresh_capture, uuid_t *no);
extern void onlinerefresh_capture_txid_set(onlinerefresh_capture *onlinerefresh_capture, FullTransactionId txid);
extern void onlinerefresh_capture_xids_append(onlinerefresh_capture *onlinerefresh_capture, TransactionId xid);
extern void onlinerefresh_capture_add_xids_from_snapshot(onlinerefresh_capture *onlinerefresh_capture, snapshot *snap);
extern bool onlinerefresh_capture_isxidinsnapshot(onlinerefresh_capture* onlinerefresh_capture, FullTransactionId xid);
extern bool onlinerefresh_capture_isxidinxids(onlinerefresh_capture* onlinerefresh_capture, FullTransactionId xid);
extern void onlinerefresh_capture_xids_delete(onlinerefresh_capture *olcapture, dlistnode *dlnode);
extern bool onlinerefresh_capture_xids_isnull(onlinerefresh_capture* refresh);
extern void onlinerefresh_capture_tables_set(onlinerefresh_capture *onlinerefresh_capture, refresh_tables *tables);
#endif
