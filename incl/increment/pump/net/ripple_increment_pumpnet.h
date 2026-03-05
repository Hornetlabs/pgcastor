#ifndef _RIPPLE_INCREMENT_PUMPNET_H
#define _RIPPLE_INCREMENT_PUMPNET_H

typedef struct RIPPLE_INCREMENT_PUMPNETSTATE_CALLBACK
{
    /* 设置拆分trail文件状态 */
    void (*splittrail_statefileid_set)(void* privdata, int state, uint64 fileid);

    /* 设置序列化状态和fileid */
    void (*serialstate_state_set)(void* privdata, int state);

    /* 设置pumpstate发送出去的lsn */
    void (*setmetricsendlsn)(void* privdata, XLogRecPtr sendlsn);

    /* 设置pumpstate发送出去的collector 的 trail 编号 */
    void (*setmetricsendtrailno)(void* privdata, uint64 sendtrailno);

    /* 设置pumpstate发送出去的pump的 trail 编号 */
    void (*setmetricloadtrailno)(void* privdata, uint64 sendtrailno);

    /* 设置pumpstate发送出去的 collector 的 trail 文件内的偏移 */
    void (*setmetricsendtrailstart)(void* privdata, uint64 sendtrailstart);

    /* 设置pumpstate发送出去的timestamp */
    void (*setmetricsendtimestamp)(void* privdata, TimestampTz sendtimestamp);

    /* 重启接收身份验证，回调添加refresh */
    void (*pumpstate_addrefresh)(void* privdata, void* refresh);

    /* refresh 是否完成，启动refresh检测是否有refresh在运行 */
    bool (*pumpstate_isrefreshdown)(void* privdata);

    /* 拆分线程添加filetransfer节点 */
    void (*pumpnet_filetransfernode_add)(void* privdata, void* filetransfernode);

    /* 大事务--设置管理线程状态为INPROCESS */
    void (*bigtxn_mgrstat_setinprocess)(void* privdata);

    /* 大事务--设置管理线程状态为RESET */
    void (*bigtxn_mgrstat_setreset)(void* privdata);

    /* 大事务--检查管理线程状态是否为reset */
    bool (*bigtxn_mgrstat_isreset)(void* privdata);

} ripple_increment_pumpnetstate_callback;

typedef struct RIPPLE_INCREMENT_PUMPNETSTATE
{
    ripple_netclient                        base;
    rsocket                                 state;                      /* 进程内线程状态变化 */
    ripple_recpos                           recpos;
    ripple_recpos                           crecpos;                        /* collector 文件起始编号 */
    void*                                   privdata;
    ripple_file_buffers*                    txn2filebuffer;
    ripple_increment_pumpnetstate_callback  callback;
} ripple_increment_pumpnetstate;

/* 初始化操作 */
extern ripple_increment_pumpnetstate* ripple_increment_pumpnet_init(void);

/* 网络客户端 */
extern void* ripple_increment_pumpnet_main(void *args);

extern void ripple_increment_pumpnet_set_status(ripple_increment_pumpnetstate* clientstate, int state);

extern void ripple_increment_pumpnet_destroy(ripple_increment_pumpnetstate* clientstate);

#endif
