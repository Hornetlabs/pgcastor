#ifndef _RIPPLE_INCREMENT_COLLECTORNETCLIENT_H
#define _RIPPLE_INCREMENT_COLLECTORNETCLIENT_H

//回调函数
typedef struct RIPPLE_COLLECTORINCREMENTSTATE_PRIVDATACALLBACK
{
    ripple_file_buffers* (*netbuffer_get)(void* privdata, char* name);

    /* 获取writestate刷新的pfileid和cfileid */
    void (*writestate_fileid_get)(void* privdata, char* name, uint64* pfileid, uint64* cfileid);

    /* 设置collectorstate接收到 pump 的 lsn */
    void (*client_setmetricrecvlsn)(void* privdata, char* name, XLogRecPtr recvlsn);

    /* 设置collectorstate接收到 trail 文件编号 */
    void (*client_setmetricrecvtrailno)(void* privdata, char* name, uint64 recvtrailno);

    /* 设置collectorstate接收到 trail 文件内的偏移 */
    void (*client_setmetricrecvtrailstart)(void* privdata, char* name, uint64 recvtrailstart);

    /* 设置collectorstate接收到 pump发送的时间戳*/
    void (*client_setmetricrecvtimestamp)(void* privdata, char* name, TimestampTz recvtimestamp);

    /* 添加flush线程节点 */
    bool (*increment_addflush)(void* privdata, char* name);

    /* collector 添加filetransfernode */
    void (*collector_filetransfernode_add)(void* privdata, void* filetransfernode);

} ripple_collectorincrementstate_privdatacallback;


/* refresh回调函数 */
typedef struct RIPPLE_COLLECTORREFRESHSHARDINGSTATE_PRIVDATACALLBACK
{
    /* collector 添加filetransfernode */
    void (*collector_filetransfernode_add)(void* privdata, void* filetransfernode);
} ripple_collectorrefreshshardingstate_privdatacallback;

/* refresh回调函数 */
typedef struct RIPPLE_COLLECTORONLINEREFRESHSHARDINGSTATE_PRIVDATACALLBACK
{
    /* collector 添加filetransfernode */
    void (*collector_filetransfernode_add)(void* privdata, void* filetransfernode);
} ripple_collectoronlinerefreshshardingstate_privdatacallback;

typedef struct RIPPLE_REFRESH_TABLEBASE
{
    ripple_uuid_t   onlinerefreshno;
    char            schema[NAMEDATALEN];
    char            table[NAMEDATALEN];
    uint32          shards;
    uint32          shardnum;
} ripple_refresh_tablebase;

typedef struct RIPPLE_INCREMENT_COLLECTORNETCLIENT
{
    rsocket                     sock;
    socklen_t                   addrlen;
    struct sockaddr             addr;
    void*                       privdata;           /* 保存ripple_networksvrstate 只传递不使用*/
} ripple_increment_collectornetclient;

typedef struct RIPPLE_INCREMENT_COLLECTORNETCLIENT_STATE
{
    int8                        type;                       /* 客户端类型 */
    int                         timeout;                    /* 超时时间                     */
    int                         hbtimeout;                  /* 心跳间隔                     */
    int                         pos;                        /* 在网络监听中的位置             */
    rsocket                     fd;                         /* 网络描述符                    */
    ripple_netiompbase*         base;
    ripple_netiompops*          ops;
    char                        clientname[128];
    void*                       privdata;                   /* 保存ripple_networksvrstate */
    void*                       callback;                   /* 接受到不同客户端类型保存不同callback函数
                                                             * pumpincrementstate保存pumpincrementstate_privdatacallback
                                                             */
    void*                       data;                        /* 客户端类型保存自有数据
                                                              * 增量 -- ripple_increment_collectornetclient_collectorincrementstate
                                                              * 存量 -- ripple_increment_collectornetclient_collectorrefreshshardingstate
                                                              */
} ripple_increment_collectornetclient_state;

typedef struct RIPPLE_COLLECTORNETCLIENT_INCREMENTSTATE
{
    int                                 bufid;
    uint64                              fileid;                /* 判断p2cdata中cfileid正确性 */
    uint64                              blknum;
    ripple_collectorbase                collectorbase;
    ripple_file_buffers*                netdata2filebuffer;
}ripple_collectornetclient_increment;

typedef struct RIPPLE_COLLECTORNETCLIENT_REFRESHSHARDING
{
    int                                 fd;                         /* 要写入数据的文件 */
    bool                                upload;
    char                                refresh_path[MAXPGPATH];    /* refresh文件夹路径 */
    ripple_refresh_tablebase            refreshtablebase;           /* refreshsharding的表名和分片信息 */
}ripple_collectornetclient_refreshsharding;

typedef struct RIPPLE_COLLECTORNETCLIENT_ONLINEREFRESHINC
{
    int                                 fd;
    int                                 bufid;
    ripple_uuid_t                       onlinerefreshno;
    bool                                upload;
    uint64                              fileid;                /* 判断p2cdata中cfileid正确性 */
    uint64                              blknum;
    ripple_file_buffers*                netdata2filebuffer;
}ripple_collectornetclient_onlinerefreshinc;

typedef struct RIPPLE_COLLECTORNETCLIENT_ONLINEREFRESHSHARDING
{
    ripple_uuid_t                       onlinerefreshno;
    bool                                upload;
    int                                 fd;                         /* 要写入数据的文件 */
    char                                refresh_path[MAXPGPATH];    /* refresh文件夹路径 */
    ripple_refresh_tablebase            refreshtablebase;           /* refreshsharding的表名和分片信息 */
}ripple_collectornetclient_onlinerefreshsharding;


typedef struct RIPPLE_COLLECTORNETCLIENT_BIGTXN
{
    FullTransactionId                   xid;
    bool                                upload;
    int                                 fd;                         /* 要写入数据的文件 */
    char                                trailpath[MAXPGPATH];       /* 大事务trail文件路径 */
    uint64                              fileid;
    uint64                              blknum;
}ripple_collectornetclient_bigtxn;

/* 申请空间 */
void* ripple_increment_collectornetclientalloc(rsocket sock);

void* ripple_collectornetclient_pumpincreament_alloc(void);

void* ripple_collectornetclient_pumprefreshsharding_alloc(void);

ripple_collectornetclient_onlinerefreshinc* ripple_collectornetclient_onlinerefreshinc_alloc(void);

ripple_collectornetclient_onlinerefreshsharding* ripple_collectornetclient_onlinerefreshsharding_alloc(void);

ripple_collectornetclient_bigtxn* ripple_collectornetclient_bigtxn_alloc(void);

/* 释放 */
void ripple_increment_collectornetclient_free(void* args);

/* 接收主线程 */
void* ripple_increment_collectornetclient_main(void *args);

#endif
