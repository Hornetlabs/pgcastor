#ifndef _RIPPLE_BIGTXN_INTEGRATEMANAGER_H
#define _RIPPLE_BIGTXN_INTEGRATEMANAGER_H


typedef enum RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT
{
    RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_NOP                = 0x00,
    RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_INIT               ,
    RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_INPROCESS          ,
    RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_EXIT               ,            /* 结束 */
    RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_SIGTERM            ,            /* 接收到退出信号 */
    RIPPLE_BIGTXN_INTEGRATEMANAGER_STAT_FREE                            /* 释放空间 */
} ripple_bigtxn_integratemanager_stat;

typedef struct RIPPLE_BIGTXN_INTEGRATEMANAGER
{
    int                                         stat;                               /* 标识状态 */
    FullTransactionId                           xid;
    char                                        padding[RIPPLE_CACHELINE_SIZE];
    ripple_thrsubmgr*                           thrsmgr;
    char                                        padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                               recordscache;
    char                                        padding2[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                           parser2rebuild;
    char                                        padding3[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                           rebuild2sync;
    char                                        padding4[RIPPLE_CACHELINE_SIZE];
    ripple_onlinerefresh_integratedataset*      onlinerefreshdataset;
    char                                        padding5[RIPPLE_CACHELINE_SIZE];
    HTAB*                                       honlinerefreshfilterdataset;
    char                                        padding6[RIPPLE_CACHELINE_SIZE];
}ripple_bigtxn_integratemanager;

void ripple_bigtxn_integratemanager_stat_set(ripple_bigtxn_integratemanager* bigtxnmgr, int stat);

ripple_bigtxn_integratemanager* ripple_bigtxn_integratemanager_init(void);

void* ripple_bigtxn_integratemanager_main(void *args);

void ripple_bigtxn_integratemanager_free(void* args);

#endif
