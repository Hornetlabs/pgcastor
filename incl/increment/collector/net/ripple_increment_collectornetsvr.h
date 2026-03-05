#ifndef RIPPLE_INCREMENT_COLLECTORNETSVR_H
#define RIPPLE_INCREMENT_COLLECTORNETSVR_H


typedef struct RIPPLE_INCREMENT_COLLECTORNETSVR_CALLBACK
{
    /* 获取privdata的netbuffer */
    ripple_file_buffers* (*netsvr_netbuffer_get)(void* privdata, char* pumpname);

    /* 获取writestate刷新的pfileid和cfileid */
    void (*writestate_fileid_get)(void* privdata, char* name, uint64* pfileid, uint64* cfileid);

    /* 设置collectorstate接收到 pump 的 lsn */
    void (*setmetricrecvlsn)(void* privdata, char* name, XLogRecPtr recvlsn);

    /* 设置collectorstate接收到 trail 文件编号 */
    void (*setmetricrecvtrailno)(void* privdata, char* name, uint64 recvtrailno);

    /* 设置collectorstate接收到 trail 文件内的偏移 */
    void (*setmetricrecvtrailstart)(void* privdata, char* name, uint64 recvtrailstart);

    /* 设置collectorstate接收到 pump发送的时间戳*/
    void (*setmetricrecvtimestamp)(void* privdata, char* name, TimestampTz recvtimestamp);

    /* collector 添加flush */
    bool (*collector_increment_addflush)(void* privdata, char* name);

    /* collector 添加filetransfernode */
    void (*collectornetsvr_filetransfernode_add)(void* privdata, void* filetransfernode);

    /* collector 添加collectornetclient */
    bool (*collectornetsvr_netclient_add)(void* privdata, void* netclient);

} ripple_increment_collectornetsvr_callback;

typedef struct RIPPLE_INCREMENT_COLLECTORNETSVR
{
    ripple_netserver                            base;
    ripple_thrsubmgr*                           thrsmgr;
    void*                                       privdata;                   /* 内容为: ripple_collectorstate*/
    ripple_increment_collectornetsvr_callback   callback;
} ripple_increment_collectornetsvr;

/* 初始化操作 */
ripple_increment_collectornetsvr* ripple_increment_collectornetsvr_init(void);

/* 通过 netsvr 中 privdata获取netbuffer */
ripple_file_buffers* ripple_increment_collectornetsvr_netbuffer_get(void* privdata, char* pumpname);

/* 通过 netsvr 中 privdata获取 writestate, pfileid, cfileid */
void ripple_increment_collectornetsvr_writestate_fileid_get(void* privdata, char* name, uint64* pfileid, uint64* cfileid);

void ripple_increment_collectornetsvr_filetransfernode_add(void* privdata, void* filetransfernode);

/* 通过 networksvrstate中 privdata获设置recvlsn*/
void ripple_increment_collectornetsvr_collectorstate_recvlsn_set(void* privdata, char* name, XLogRecPtr recvlsn);

void ripple_increment_collectornetsvr_collectorstate_recvtrailno_set(void* privdata, char* name, uint64 recvtrailno);

void ripple_increment_collectornetsvr_collectorstate_recvtrailstart_set(void* privdata, char* name, uint64 recvtrailstart);

void ripple_increment_collectornetsvr_collectorstate_recvtimestamp_set(void* privdata, char* name, TimestampTz recvtimestamp);

/* 向 privdata中添加flash */
bool ripple_increment_collectornetsvr_collectorstate_addflush(void* privdata, char* name);

/* 网络服务端 */
void* ripple_increment_collectornetsvr_main(void *args);

/* 资源回收 */
void ripple_increment_collectornetsvr_destroy(void* args);

#endif
