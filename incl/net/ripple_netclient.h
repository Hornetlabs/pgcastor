#ifndef _RIPPLE_NETCLIENT_H
#define _RIPPLE_NETCLIENT_H

typedef bool (*netclient_packets_handler)(void* netclient, ripple_netpacket* netpacket);

typedef enum RIPPLE_NETCLIENT_PROTOCOLTYPE
{
    RIPPLE_NETCLIENT_PROTOCOLTYPE_NOP               = 0x00,
    RIPPLE_NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN        ,
    RIPPLE_NETCLIENT_PROTOCOLTYPE_IPTCP
} ripple_netclient_protocoltype;

typedef enum RIPPLE_NETCLIENTCONN_STATUS
{
    RIPPLE_NETCLIENTCONN_STATUS_NOP                 = 0x00,                     /* 待连接服务端         */
    RIPPLE_NETCLIENTCONN_STATUS_INPROCESS           = 0x01,                     /* 正在连接服务端       */
    RIPPLE_NETCLIENTCONN_STATUS_CONNECTED           = 0x02                      /* 连接上了服务端       */
} ripple_netclientconn_status;

typedef struct RIPPLE_NETCLIENT
{
    int                                             hbtimeout;
    int                                             timeout;
    ripple_netclient_protocoltype                   protocoltype;           /* 协议类型 */
    ripple_netclientconn_status                     status;                 /* 连接状态 */
    rsocket                                         fd;                     /* 网络描述符 */
    int                                             pos;                    /* 在模型中的位置下标 */
    char                                            svrhost[128];            /* 服务端监听地址 */
    char                                            svrport[128];           /* 服务端监听端口 */
    ripple_netiompbase*                             base;                   /* IO复用基础信息 */
    ripple_netiompops*                              ops;                    /* IO 复用模型 */
    char                                            szport[128];            /* 端口 */
    ripple_queue*                                   rpackets;               /* 读到的数据 */
    ripple_queue*                                   wpackets;               /* 待发送数据 */
    netclient_packets_handler                       callback;               /* 回调函数 */
} ripple_netclient;


/* 重置状态、超时时间、关闭描述符、清理packets内存、设置 iompbase和iompops等 */
void ripple_netclient_reset(ripple_netclient* netclient);

void ripple_netclient_type_set(ripple_netclient* netclient, int type);

void ripple_netclient_setprotocoltype(ripple_netclient* netclient, ripple_netclient_protocoltype protocoltype);

void ripple_netclient_settimeout(ripple_netclient* netclient, int timeout);

void ripple_netclient_sethbtimeout(ripple_netclient* netclient, int hbtimeout);

void ripple_netclient_setsvrhost(ripple_netclient* netclient, char* host);

void ripple_netclient_setsvrport(ripple_netclient* netclient, char* port);

/* 连接服务端 */
bool ripple_netclient_conn(ripple_netclient* netclient);

/* 用于查看是否连接上目标端, 当状态为 INPROCESS 时,检测是否可以转化状态为 CONNECTED */
bool ripple_netclient_isconnect(ripple_netclient* netclient);

/* 
 * 尝试连接服务端
 *  conn
 *  sleep(1)
 *  is conn ?
 * 
 *  true    连接上
 *  false   未连接上
 */
bool ripple_netclient_tryconn(ripple_netclient* netclient);

/* 创建连接并发送数据 */
bool ripple_netclient_senddata(ripple_netclient_protocoltype ptype,
                               char* host,
                               char* port,
                               uint8* data,
                               int datalen);

/* 创建监听事件并等待事件触发,处理触发的事件 */
bool ripple_netclient_desc(ripple_netclient* netclient);

/*
 * 创建监听事件并等待事件触发, 接收或发送数据,仅接收或发送, 不做业务处理
*/
bool ripple_netclient_desc2(ripple_netclient* netclient);

bool ripple_netclient_addwpacket(ripple_netclient* netclient, void* packet);

bool ripple_netclient_wpacketisnull(ripple_netclient* netclient);

bool ripple_netclient_rpacketisnull(ripple_netclient* netclient);

bool ripple_netclient_packets_handler(void* netclient, ripple_netpacket* netpacket);

void ripple_netclient_wpacketsadd_hb(ripple_netclient* netclient);

/* 清理描述符/队列 */
void ripple_netclient_clear(ripple_netclient* netclient);

/* 资源回收 */
void ripple_netclient_destroy(ripple_netclient* netclient);

#endif
