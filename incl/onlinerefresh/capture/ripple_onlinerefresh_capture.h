#ifndef RIPPLE_ONLINEREFRESH_CAPTUR_H
#define RIPPLE_ONLINEREFRESH_CAPTUR_H

typedef struct ripple_onlinerefresh_capture
{
    bool                    increment;
    int                     parallelcnt;    /* 存量工作线程, 并行数量 */
    ripple_recpos           redo;           /* redolsn */
    char                   *conninfo;       /* 连接字串 */
    ripple_snapshot        *snapshot;       /* 快照 */
    PGconn                 *conn;           /* 数据库链接 */
    PGconn                 *snap_conn;      /* 导出快照的数据库链接 */
    ripple_uuid_t          *no;             /* onlinerefresh 编号 */
    char                   *data;           /* 存放数据的目录 */
    ripple_refresh_tables  *tables;         /* 待同步存量表 */
    FullTransactionId       txid;           /* 获取快照时下个将要使用的事务号 */
    dlist*                  xids;           /* 事务号链表, FullTransactionId */
    char                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_queue           *refreshtqueue;  /* 存量数据同步 */
    char                    padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue           *recordqueue;   /* record缓存 */
    char                    padding2[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn       *parser2serialtxns;  /* 序列化 */
    char                    padding3[RIPPLE_CACHELINE_SIZE];
    ripple_file_buffers    *txn2filebuffer; /* 落盘buffer */
    char                    padding4[RIPPLE_CACHELINE_SIZE];
    ripple_thrsubmgr*       thrsmgr;
    char                    padding5[RIPPLE_CACHELINE_SIZE];

    /* 清理onlinerefresh剩余 */
    void                    (*removeolrefresh)(void* privdata, void* olrefresh);

    /* 父结构体 */
    void*                   privdata;

    char                    padding6[RIPPLE_CACHELINE_SIZE];
} ripple_onlinerefresh_capture;

extern void *ripple_onlinerefresh_capture_main(void* args);
extern void ripple_onlinerefresh_capture_destroy(void* privdata);
extern int ripple_onlinerefresh_capture_cmp(void* s1, void* s2);
extern ripple_onlinerefresh_capture *ripple_onlinerefresh_capture_init(bool increment);
extern void ripple_onlinerefresh_capture_increment_set(ripple_onlinerefresh_capture *onlinerefresh_capture, bool increment);
extern void ripple_onlinerefresh_capture_state_set(ripple_onlinerefresh_capture *onlinerefresh_capture, int state);
extern void ripple_onlinerefresh_capture_redo_set(ripple_onlinerefresh_capture *onlinerefresh_capture, XLogRecPtr redo);
extern void ripple_onlinerefresh_capture_conninfo_set(ripple_onlinerefresh_capture *onlinerefresh_capture, char* conninfo);
extern void ripple_onlinerefresh_capture_snapshot_set(ripple_onlinerefresh_capture *onlinerefresh_capture, ripple_snapshot *snapshot);
extern void ripple_onlinerefresh_capture_conn_set(ripple_onlinerefresh_capture *onlinerefresh_capture, PGconn *conn);
extern void ripple_onlinerefresh_capture_snap_conn_set(ripple_onlinerefresh_capture *onlinerefresh_capture, PGconn *snap_conn);
extern void ripple_onlinerefresh_capture_no_set(ripple_onlinerefresh_capture *onlinerefresh_capture, ripple_uuid_t *no);
extern void ripple_onlinerefresh_capture_txid_set(ripple_onlinerefresh_capture *onlinerefresh_capture, FullTransactionId txid);
extern void ripple_onlinerefresh_capture_xids_append(ripple_onlinerefresh_capture *onlinerefresh_capture, TransactionId xid);
extern void ripple_onlinerefresh_capture_add_xids_from_snapshot(ripple_onlinerefresh_capture *onlinerefresh_capture, ripple_snapshot *snap);
extern bool ripple_onlinerefresh_capture_isxidinsnapshot(ripple_onlinerefresh_capture* onlinerefresh_capture, FullTransactionId xid);
extern bool ripple_onlinerefresh_capture_isxidinxids(ripple_onlinerefresh_capture* onlinerefresh_capture, FullTransactionId xid);
extern void ripple_onlinerefresh_capture_xids_delete(ripple_onlinerefresh_capture *olcapture, dlistnode *dlnode);
extern bool ripple_onlinerefresh_capture_xids_isnull(ripple_onlinerefresh_capture* refresh);
extern void ripple_onlinerefresh_capture_tables_set(ripple_onlinerefresh_capture *onlinerefresh_capture, ripple_refresh_tables *tables);
#endif
