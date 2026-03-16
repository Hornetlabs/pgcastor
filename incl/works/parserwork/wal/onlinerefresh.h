#ifndef RIPPLE_ONLINEREFRESH_H
#define RIPPLE_ONLINEREFRESH_H

typedef enum ripple_onlinerefresh_state
{
    /* 
     * 在 SEARCHMAX 状态下需要添加 snapshot->xmax ----> txid 的事务到onlinerefresh 活跃事务列表中
     *  当 挖掘的事务号 >= txid 时, 将状态转换为 FULLSNAPSHOT
     */
    RIPPLE_ONLINEREFRESH_STATE_SEARCHMAX = 0x00,

    /* 在 FULLSNAPSHOT 状态下, 活跃事务列表已经组装完成, 后面的处理逻辑会逐渐将活跃事务列表中的事务消除掉 */
    RIPPLE_ONLINEREFRESH_STATE_FULLSNAPSHOT
} ripple_onlinerefresh_state;

typedef struct ripple_onlinerefresh
{
    int                 state;      /* 状态标识 */
    bool                increment;  /* 是否需要增量 */
    ripple_uuid_t      *no;         /* uuid */
    FullTransactionId   txid;       /* 获取快照时下个将要使用的事务号 */
    ripple_snapshot    *snapshot;   /* 快照信息,原始的快照信息 */
    dlist              *xids;       /* 事务号链表, FullTransactionId */
    List               *newtables; /* onlinerefresh新增的表 */
} ripple_onlinerefresh;

extern List *ripple_onlinerefresh_get_newtable(HTAB *dataset, ripple_refresh_tables *tables);
extern ripple_refresh_tables *ripple_onlinerefresh_data_load(HTAB* namespace, HTAB* class);

/* 比较 */
extern int ripple_onlinerefresh_cmp(void* s1, void* s2);

extern ripple_onlinerefresh *ripple_onlinerefresh_init(void);
extern void ripple_onlinerefresh_state_setsearchmax(ripple_onlinerefresh *refresh);
extern void ripple_onlinerefresh_state_setfullsnapshot(ripple_onlinerefresh *refresh);
extern void ripple_onlinerefresh_no_set(ripple_onlinerefresh *refresh, ripple_uuid_t *no);
extern void ripple_onlinerefresh_txid_set(ripple_onlinerefresh *refresh, FullTransactionId txid);
extern void ripple_onlinerefresh_snapshot_set(ripple_onlinerefresh *refresh, ripple_snapshot *snapshot);
extern void ripple_onlinerefresh_xids_append(ripple_onlinerefresh *refresh, TransactionId xid);
extern void ripple_onlinerefresh_add_xids_from_snapshot(ripple_onlinerefresh *refresh, ripple_snapshot *snap);
extern void ripple_onlinerefresh_xids_delete(ripple_onlinerefresh *refresh, dlist *dl, dlistnode *dlnode);
extern bool ripple_onlinerefresh_xids_isnull(ripple_onlinerefresh* refresh);
extern void ripple_onlinerefresh_newtables_set(ripple_onlinerefresh *refresh, List *newtables);
extern void ripple_onlinerefresh_increment_set(ripple_onlinerefresh *refresh, bool increment);
extern bool ripple_onlinerefresh_isxidinsnapshot(ripple_onlinerefresh* onlinerefresh, FullTransactionId xid);
extern dlist *ripple_onlinerefresh_refreshdlist_delete(dlist *refresh_dlist, dlistnode *dlnode);
extern void ripple_transcache_make_xids_from_txn(void* in_ctx, ripple_onlinerefresh *olnode);

/* 根据 系统表 填充 refreshtables */
extern bool ripple_onlinerefresh_rebuildrefreshtables(ripple_refresh_tables* rtables,
                                                      HTAB* hnamespace,
                                                      HTAB* hclass,
                                                      bool* bmatch);

extern void ripple_onlinerefresh_destroy(ripple_onlinerefresh* olrefresh);

extern void ripple_onlinerefresh_destroyvoid(void *olrefresh);

#endif
