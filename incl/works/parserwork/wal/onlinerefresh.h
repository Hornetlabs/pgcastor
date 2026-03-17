#ifndef ONLINEREFRESH_H
#define ONLINEREFRESH_H

typedef enum onlinerefresh_state
{
    /* 
     * 在 SEARCHMAX 状态下需要添加 snapshot->xmax ----> txid 的事务到onlinerefresh 活跃事务列表中
     *  当 挖掘的事务号 >= txid 时, 将状态转换为 FULLSNAPSHOT
     */
    ONLINEREFRESH_STATE_SEARCHMAX = 0x00,

    /* 在 FULLSNAPSHOT 状态下, 活跃事务列表已经组装完成, 后面的处理逻辑会逐渐将活跃事务列表中的事务消除掉 */
    ONLINEREFRESH_STATE_FULLSNAPSHOT
} onlinerefresh_state;

typedef struct onlinerefresh
{
    int                 state;      /* 状态标识 */
    bool                increment;  /* 是否需要增量 */
    uuid_t      *no;         /* uuid */
    FullTransactionId   txid;       /* 获取快照时下个将要使用的事务号 */
    snapshot    *snapshot;   /* 快照信息,原始的快照信息 */
    dlist              *xids;       /* 事务号链表, FullTransactionId */
    List               *newtables; /* onlinerefresh新增的表 */
} onlinerefresh;

extern List *onlinerefresh_get_newtable(HTAB *dataset, refresh_tables *tables);
extern refresh_tables *onlinerefresh_data_load(HTAB* namespace, HTAB* class);

/* 比较 */
extern int onlinerefresh_cmp(void* s1, void* s2);

extern onlinerefresh *onlinerefresh_init(void);
extern void onlinerefresh_state_setsearchmax(onlinerefresh *refresh);
extern void onlinerefresh_state_setfullsnapshot(onlinerefresh *refresh);
extern void onlinerefresh_no_set(onlinerefresh *refresh, uuid_t *no);
extern void onlinerefresh_txid_set(onlinerefresh *refresh, FullTransactionId txid);
extern void onlinerefresh_snapshot_set(onlinerefresh *refresh, snapshot *snapshot);
extern void onlinerefresh_xids_append(onlinerefresh *refresh, TransactionId xid);
extern void onlinerefresh_add_xids_from_snapshot(onlinerefresh *refresh, snapshot *snap);
extern void onlinerefresh_xids_delete(onlinerefresh *refresh, dlist *dl, dlistnode *dlnode);
extern bool onlinerefresh_xids_isnull(onlinerefresh* refresh);
extern void onlinerefresh_newtables_set(onlinerefresh *refresh, List *newtables);
extern void onlinerefresh_increment_set(onlinerefresh *refresh, bool increment);
extern bool onlinerefresh_isxidinsnapshot(onlinerefresh* onlinerefresh, FullTransactionId xid);
extern dlist *onlinerefresh_refreshdlist_delete(dlist *refresh_dlist, dlistnode *dlnode);
extern void transcache_make_xids_from_txn(void* in_ctx, onlinerefresh *olnode);

/* 根据 系统表 填充 refreshtables */
extern bool onlinerefresh_rebuildrefreshtables(refresh_tables* rtables,
                                                      HTAB* hnamespace,
                                                      HTAB* hclass,
                                                      bool* bmatch);

extern void onlinerefresh_destroy(onlinerefresh* olrefresh);

extern void onlinerefresh_destroyvoid(void *olrefresh);

#endif
