#ifndef _RIPPLE_INCREMENT_PUMP_H
#define _RIPPLE_INCREMENT_PUMP_H

typedef struct RIPPLE_INCREMENT_PUMP
{
    uint64                                      persistno;
    char                                        padding[RIPPLE_CACHELINE_SIZE];
    ripple_threads*                             threads;                                /* 线程管理 */
    char                                        padding1[RIPPLE_CACHELINE_SIZE];
    ripple_increment_pumpsplittrail*            splittrailctx;
    char                                        padding2[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                               recordscache;
    char                                        padding3[RIPPLE_CACHELINE_SIZE];
    ripple_increment_pumpparsertrail*           pumpparsertrail;
    char                                        padding4[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                           parser2serialtxns;
    char                                        padding5[RIPPLE_CACHELINE_SIZE];
    ripple_increment_pumpserialstate*           serialstate;
    char                                        padding6[RIPPLE_CACHELINE_SIZE];
    ripple_file_buffers*                        txn2filebuffer;
    char                                        padding7[RIPPLE_CACHELINE_SIZE];
    ripple_increment_pumpnetstate*              clientstate;
    char                                        padding18[RIPPLE_CACHELINE_SIZE];
    ripple_onlinerefresh_persist*               olrpersist;
    char                                        padding9[RIPPLE_CACHELINE_SIZE];
    pthread_mutex_t                             onlinerefreshlock;
    dlist*                                      onlinerefresh;
    char                                        padding10[RIPPLE_CACHELINE_SIZE];
    pthread_mutex_t                             refreshlock;
    List*                                       refresh;
    char                                        padding11[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                               filetransfernode;
    char                                        padding12[RIPPLE_CACHELINE_SIZE];
    ripple_filetransfer_pump*                   ftptransfer;
    char                                        padding13[RIPPLE_CACHELINE_SIZE];
    ripple_state_pump_state*                    pumpstate;
    char                                        padding14[RIPPLE_CACHELINE_SIZE];
    ripple_bigtxn_persist*                      txnpersist;
    char                                        padding15[RIPPLE_CACHELINE_SIZE];
    pthread_mutex_t                             bigtxnlock;
    dlist*                                      bigtxn;
    char                                        padding16[RIPPLE_CACHELINE_SIZE];
} ripple_increment_pump;


void ripple_increment_pumpstate_splittrail_statefileid_set(void* privdata, int state, uint64 fileid);

void ripple_increment_pumpstate_splittrail_state_set(void* privdata, int state);

void ripple_increment_pumpstate_parsertrail_state_set(void* privdata, int state);

void ripple_increment_pumpstate_serialtrail_state_set(void* privdata, int state);

void ripple_increment_pumpstate_networkclient_state_set(void* privdata, int state);

uint64 ripple_increment_pumpstate_networkclient_cfileid_get(void* privdata);

void ripple_increment_pumpstate_addonlinerefresh(void* privdata, void* onlinerefresh);

/* 启动 refresh */
bool ripple_increment_pump_startrefresh(ripple_increment_pump* incpump);

/* 回收 refresh 节点 */
bool ripple_increment_pump_tryjoinonrefresh(ripple_increment_pump* incpump);

/* 启动 onlinerefresh 管理线程 */
bool ripple_increment_pump_startonlinerefresh(ripple_increment_pump* incpump);

/* 回收 onlinerefresh 节点 */
bool ripple_increment_pump_tryjoinononlinerefresh(ripple_increment_pump* incpump);

/* 启动 bigtxn 管理线程 */
bool ripple_increment_pump_startbigtxn(ripple_increment_pump* incpump);

/* 回收 bigtxn 节点 */
bool ripple_increment_pump_tryjoinonbigtxn(ripple_increment_pump* incpump);

ripple_increment_pump* ripple_increment_pump_init(void);

void ripple_increment_pumpstate_destroy(ripple_increment_pump* pumpstate);

#endif