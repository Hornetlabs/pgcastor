#ifndef _INCREMENT_INTEGRATEREBUILD_H
#define _INCREMENT_INTEGRATEREBUILD_H

typedef struct INCREMENT_INTEGRATEREBUILD_CALLBACK
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

} increment_integraterebuild_callback;

typedef enum INCREMENT_INTEGRATEREBUILD_STAT
{
    INCREMENT_INTEGRATEREBUILD_STAT_NOP          = 0x00,
    INCREMENT_INTEGRATEREBUILD_STAT_READY        ,               /* sync设置rebuild */
    INCREMENT_INTEGRATEREBUILD_STAT_WORK         ,
    INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM                     /* 接受到term或其他线程异常退出 */
}increment_integraterebuild_stat;

typedef struct INCREMENT_INTEGRATEREBUILD
{
    rebuild                                          rebuild;
    bool                                                    mergetxn;
    bool                                                    burst;                              /* burst 模式 */
    increment_integraterebuild_stat                  stat;
    int                                                     txbundlesize;                       /* 合并事务的阈值 */
    XLogRecPtr                                              filterlsn;                          /* 小于等于此 Isn 不需要处理 */
    onlinerefresh_integratedataset*                  onlinerefreshdataset;
    HTAB*                                                   honlinerefreshfilterdataset;
    char                                                    padding[CACHELINE_SIZE];
    cache_txn*                                       parser2rebuild;
    char                                                    padding1[CACHELINE_SIZE];
    cache_txn*                                       rebuild2sync;
    char                                                    padding2[CACHELINE_SIZE];
    bigtxn_persist*                                  txnpersist;
    char                                                    padding3[CACHELINE_SIZE];
    onlinerefresh_persist*                           olpersist;
    char                                                    padding4[CACHELINE_SIZE];
    void*                                                   privdata;
    char                                                    padding5[CACHELINE_SIZE];
    increment_integraterebuild_callback              callback;
} increment_integraterebuild;

/* 初始化 */
increment_integraterebuild* increment_integraterebuild_init(void);

/* 工作 */
void* increment_integraterebuild_main(void* args);

void increment_integraterebuild_free(increment_integraterebuild* rebuild);

#endif
