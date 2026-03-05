#ifndef _RIPPLE_INCREMENT_INTEGRATESYNC_H
#define _RIPPLE_INCREMENT_INTEGRATESYNC_H


typedef enum RIPPLE_INCREMENT_INTEGRATESYNC_STATE
{
    RIPPLE_INCREMENT_INTEGRATESYNC_STATE_NOP            = 0x00,
    RIPPLE_INCREMENT_INTEGRATESYNC_STATE_IDLE           ,               /* sync获取不到事务为空闲状态 onlinrefresh启动时要等待sync为空闲状态（保证之前事务都完成） */
    RIPPLE_INCREMENT_INTEGRATESYNC_STATE_WORK                           /* sync获取到事务为工作状态 工作状态不启动onlinerefresh */
} ripple_increment_integratesync_state;

typedef struct RIPPLE_INCREMENT_INTEGRATESYNCSTATE_PRIVDATACALLBACK
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

} ripple_increment_integratesyncstate_privdatacallback;

typedef struct RIPPLE_INCREMENT_INTEGRATESYNCSTATE
{
    ripple_syncstate                                        base;
    int                                                     state;
    uint64                                                  lsn;
    uint64                                                  trailno;
    ripple_recpos                                           rewind;
    ripple_cache_txn*                                       rebuild2sync;
    void*                                                   privdata;       /* 内容为: ripple_intergratestate*/
    ripple_increment_integratesyncstate_privdatacallback    callback;
}ripple_increment_integratesyncstate;

ripple_increment_integratesyncstate* ripple_increment_integratesync_init(void);

void ripple_increment_integratesync_destroy(ripple_increment_integratesyncstate* syncworkstate);

void* ripple_increment_integratesync_main(void *args);

#endif
