#ifndef _NETCLIENT_H
#define _NETCLIENT_H

typedef bool (*netclient_packets_handler)(void* netclient, netpacket* netpacket);

typedef enum NETCLIENT_PROTOCOLTYPE
{
    NETCLIENT_PROTOCOLTYPE_NOP               = 0x00,
    NETCLIENT_PROTOCOLTYPE_UNIXDOMAIN        ,
    NETCLIENT_PROTOCOLTYPE_IPTCP
} netclient_protocoltype;

typedef enum NETCLIENTCONN_STATUS
{
    NETCLIENTCONN_STATUS_NOP                 = 0x00,                     /* 待连接服务端         */
    NETCLIENTCONN_STATUS_INPROCESS           = 0x01,                     /* 正在连接服务端       */
    NETCLIENTCONN_STATUS_CONNECTED           = 0x02                      /* 连接上了服务端       */
} netclientconn_status;

typedef struct NETCLIENT
{
    int                                             hbtimeout;
    int                                             timeout;
    netclient_protocoltype                   protocoltype;           /* 协议类型 */
    netclientconn_status                     status;                 /* 连接状态 */
    rsocket                                         fd;                     /* 网络描述符 */
    int                                             pos;                    /* 在模型中的位置下标 */
    char                                            svrhost[128];            /* 服务端监听地址 */
    char                                            svrport[128];           /* 服务端监听端口 */
    netiompbase*                             base;                   /* IO复用基础信息 */
    netiompops*                              ops;                    /* IO 复用模型 */
    char                                            szport[128];            /* 端口 */
    queue*                                   rpackets;               /* 读到的数据 */
    queue*                                   wpackets;               /* 待发送数据 */
    netclient_packets_handler                       callback;               /* 回调函数 */
} netclient;


/* 重置状态、超时时间、关闭描述符、清理packets内存、设置 iompbase和iompops等 */
void netclient_reset(netclient* netclient);

void netclient_type_set(netclient* netclient, int type);

void netclient_setprotocoltype(netclient* netclient, netclient_protocoltype protocoltype);

void netclient_settimeout(netclient* netclient, int timeout);

void netclient_sethbtimeout(netclient* netclient, int hbtimeout);

void netclient_setsvrhost(netclient* netclient, char* host);

void netclient_setsvrport(netclient* netclient, char* port);

/* 连接服务端 */
bool netclient_conn(netclient* netclient);

/* 用于查看是否连接上目标端, 当状态为 INPROCESS 时,检测是否可以转化状态为 CONNECTED */
bool netclient_isconnect(netclient* netclient);

/* 
 * 尝试连接服务端
 *  conn
 *  sleep(1)
 *  is conn ?
 * 
 *  true    连接上
 *  false   未连接上
 */
bool netclient_tryconn(netclient* netclient);

/* 创建连接并发送数据 */
bool netclient_senddata(netclient_protocoltype ptype,
                               char* host,
                               char* port,
                               uint8* data,
                               int datalen);

/* 创建监听事件并等待事件触发,处理触发的事件 */
bool netclient_desc(netclient* netclient);

/*
 * 创建监听事件并等待事件触发, 接收或发送数据,仅接收或发送, 不做业务处理
*/
bool netclient_desc2(netclient* netclient);

bool netclient_addwpacket(netclient* netclient, void* packet);

bool netclient_wpacketisnull(netclient* netclient);

bool netclient_rpacketisnull(netclient* netclient);

bool netclient_default_packets_handler(void* netclient, netpacket* netpacket);

/* 清理描述符/队列 */
void netclient_clear(netclient* netclient);

/* 资源回收 */
void netclient_destroy(netclient* netclient);

#endif
