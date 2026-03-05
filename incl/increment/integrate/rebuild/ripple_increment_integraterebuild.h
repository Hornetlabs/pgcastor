#ifndef _RIPPLE_INCREMENT_INTEGRATEREBUILD_H
#define _RIPPLE_INCREMENT_INTEGRATEREBUILD_H

typedef struct RIPPLE_INCREMENT_INTEGRATEREBUILD_CALLBACK
{
    /* refresh 是否完成，启动refresh检测是否有refresh在运行 */
    bool (*isrefreshdown)(void* privdata);

    /* integratestate添加onlinerefresh节点 */
    void (*addonlinerefresh)(void* privdata, void* onlinerefresh);

    /* sync是否空闲 */
    bool (*issyncidle)(void* privdata);

    /* onlinerefresh 是否完成 */
    bool (*isonlinerefreshdone)(void* privdata, void* no);

    /* onlinerefresh 存量是否完成 */
    bool (*isolrrefreshdone)(void* privdata, void* no);

    /* 设置onlinerefresh结束 */
    void (*setonlinerefreshfree)(void* privdata, void* no);

    /* 大事务 是否完成，检测对应大事务是否在运行 */
    bool (*isbigtxndown)(void* privdata, FullTransactionId xid);

    /* 大事务 是否sigterm */
    bool (*isbigtxnsigterm)(void* privdata, FullTransactionId xid);

    /* integratestate添加bigtxn节点 */
    void (*addbigtxn)(void* privdata, void* bigtxn);

} ripple_increment_integraterebuild_callback;

typedef enum RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT
{
    RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_NOP          = 0x00,
    RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_READY        ,               /* sync设置rebuild */
    RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_WORK         ,
    RIPPLE_INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM                     /* 接受到term或其他线程异常退出 */
}ripple_increment_integraterebuild_stat;

typedef struct RIPPLE_INCREMENT_INTEGRATEREBUILD
{
    ripple_rebuild                                          rebuild;
    bool                                                    mergetxn;
    bool                                                    burst;                              /* burst 模式 */
    ripple_increment_integraterebuild_stat                  stat;
    int                                                     txbundlesize;                       /* 合并事务的阈值 */
    XLogRecPtr                                              filterlsn;                          /* 小于等于此 Isn 不需要处理 */
    ripple_onlinerefresh_integratedataset*                  onlinerefreshdataset;
    HTAB*                                                   honlinerefreshfilterdataset;
    char                                                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                                       parser2rebuild;
    char                                                    padding1[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                                       rebuild2sync;
    char                                                    padding2[RIPPLE_CACHELINE_SIZE];
    ripple_bigtxn_persist*                                  txnpersist;
    char                                                    padding3[RIPPLE_CACHELINE_SIZE];
    ripple_onlinerefresh_persist*                           olpersist;
    char                                                    padding4[RIPPLE_CACHELINE_SIZE];
    void*                                                   privdata;
    char                                                    padding5[RIPPLE_CACHELINE_SIZE];
    ripple_increment_integraterebuild_callback              callback;
} ripple_increment_integraterebuild;

/* 初始化 */
ripple_increment_integraterebuild* ripple_increment_integraterebuild_init(void);

/* 工作 */
void* ripple_increment_integraterebuild_main(void* args);

void ripple_increment_integraterebuild_free(ripple_increment_integraterebuild* rebuild);

#endif
