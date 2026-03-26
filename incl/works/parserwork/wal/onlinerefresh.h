#ifndef ONLINEREFRESH_H
#define ONLINEREFRESH_H

typedef enum onlinerefresh_state
{
    /*
     * In SEARCHMAX state, need to add transaction with snapshot->xmax ----> txid to onlinerefresh
     * active transaction list When mined transaction ID >= txid, convert state to FULLSNAPSHOT
     */
    ONLINEREFRESH_STATE_SEARCHMAX = 0x00,

    /* In FULLSNAPSHOT state, active transaction list has been assembled,
       Subsequent processing logic will gradually eliminate transactions from active transaction
       list */
    ONLINEREFRESH_STATE_FULLSNAPSHOT
} onlinerefresh_state;

typedef struct onlinerefresh
{
    int               state;     /* Status flag */
    bool              increment; /* Whether incremental is needed */
    uuid_t*           no;        /* uuid */
    FullTransactionId txid;      /* Next transaction ID to be used when getting snapshot */
    snapshot*         snapshot;  /* Snapshot information, original snapshot information */
    dlist*            xids;      /* Transaction ID list, FullTransactionId */
    List*             newtables; /* Tables added by onlinerefresh */
} onlinerefresh;

extern List* onlinerefresh_get_newtable(HTAB* dataset, refresh_tables* tables);
extern refresh_tables* onlinerefresh_data_load(HTAB* namespace, HTAB* class);

/* Compare */
extern int onlinerefresh_cmp(void* s1, void* s2);

extern onlinerefresh* onlinerefresh_init(void);
extern void onlinerefresh_state_setsearchmax(onlinerefresh* refresh);
extern void onlinerefresh_state_setfullsnapshot(onlinerefresh* refresh);
extern void onlinerefresh_no_set(onlinerefresh* refresh, uuid_t* no);
extern void onlinerefresh_txid_set(onlinerefresh* refresh, FullTransactionId txid);
extern void onlinerefresh_snapshot_set(onlinerefresh* refresh, snapshot* snapshot);
extern void onlinerefresh_xids_append(onlinerefresh* refresh, TransactionId xid);
extern void onlinerefresh_add_xids_from_snapshot(onlinerefresh* refresh, snapshot* snap);
extern void onlinerefresh_xids_delete(onlinerefresh* refresh, dlist* dl, dlistnode* dlnode);
extern bool onlinerefresh_xids_isnull(onlinerefresh* refresh);
extern void onlinerefresh_newtables_set(onlinerefresh* refresh, List* newtables);
extern void onlinerefresh_increment_set(onlinerefresh* refresh, bool increment);
extern bool onlinerefresh_isxidinsnapshot(onlinerefresh* onlinerefresh, FullTransactionId xid);
extern dlist* onlinerefresh_refreshdlist_delete(dlist* refresh_dlist, dlistnode* dlnode);
extern void transcache_make_xids_from_txn(void* in_ctx, onlinerefresh* olnode);

/* Fill refreshtables based on system table */
extern bool onlinerefresh_rebuildrefreshtables(refresh_tables* rtables,
                                               HTAB*           hnamespace,
                                               HTAB*           hclass,
                                               bool*           bmatch);

extern void onlinerefresh_destroy(onlinerefresh* olrefresh);

extern void onlinerefresh_destroyvoid(void* olrefresh);

#endif
