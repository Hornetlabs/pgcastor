#ifndef _INCREMENT_INTEGRATESYNC_H
#define _INCREMENT_INTEGRATESYNC_H


typedef enum INCREMENT_INTEGRATESYNC_STATE
{
    INCREMENT_INTEGRATESYNC_STATE_NOP            = 0x00,
    INCREMENT_INTEGRATESYNC_STATE_IDLE           ,               /* sync获取不到事务为空闲状态 onlinrefresh启动时要等待sync为空闲状态（保证之前事务都完成） */
    INCREMENT_INTEGRATESYNC_STATE_WORK                           /* sync获取到事务为工作状态 工作状态不启动onlinerefresh */
} increment_integratesync_state;

typedef struct INCREMENT_INTEGRATESYNCSTATE_PRIVDATACALLBACK
{
    /* 设置 splittrail的fileid和工作状态 */
    void (*splittrail_fileid_emitoffse_set)(void* privdata, uint64 fileid, uint64 emitoffset);

    /* 设置integratestate同步到库中的 lsn */
    void (*setmetricsynclsn)(void* privdata, XLogRecPtr synclsn);

    /* 设置integratestate同步到库中数据的trail文件编号*/
    void (*setmetricsynctrailno)(void* privdata, uint64 fileid);

    /* 设置integratestate同步到库中数据的trail文件偏移 */
    void (*setmetricsynctrailstart)(void* privdata, uint64 fileid);

    /* 设置integratestate同步到库中数据的时间戳 */
    void (*setmetricsynctimestamp)(void* privdata, TimestampTz synctimestamp);

    /* refresh 是否完成，启动refresh检测是否有refresh在运行 */
    bool (*integratestate_isrefreshdown)(void* privdata);

    /* 设置 rebuild 线程的 filterlsn */
    void (*integratestate_rebuildfilter_set)(void* privdata, XLogRecPtr lsn);

} increment_integratesyncstate_privdatacallback;

typedef struct INCREMENT_INTEGRATESYNCSTATE
{
    syncstate                                        base;
    int                                                     state;
    uint64                                                  lsn;
    uint64                                                  trailno;
    recpos                                           rewind;
    cache_txn*                                       rebuild2sync;
    void*                                                   privdata;       /* 内容为: intergratestate*/
    increment_integratesyncstate_privdatacallback    callback;
}increment_integratesyncstate;

increment_integratesyncstate* increment_integratesync_init(void);

void increment_integratesync_destroy(increment_integratesyncstate* syncworkstate);

void* increment_integratesync_main(void *args);

#endif
