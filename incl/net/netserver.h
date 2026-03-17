#ifndef _NETSERVER_H
#define _NETSERVER_H

#define NETSERVER_HOSTMAXLEN                         1024
#define NETSERVER_DEFAULTSOCKSIZE                    8

typedef bool (*netserver_handler)(void* netserver, rsocket  sock);

typedef enum NETSERVER_TYPE
{
    NETSERVER_TYPE_NOP               = 0x00,
    NETSERVER_TYPE_XMANAGER          
} netserver_type;

typedef enum NETSERVER_HOSTTYPE
{
    NETSERVER_HOSTTYPE_NOP               = 0x00,
    NETSERVER_HOSTTYPE_UNIXDOMAIN        ,
    NETSERVER_HOSTTYPE_IP
} netserver_hosttype;

typedef struct NETSERVER_HOST
{
    netserver_hosttype           type;
    char                                host[512];               /* 服务端监听地址                    */
} netserver_host;

/* 服务 */
typedef struct NETSERVER
{
    netserver_type               type;                   /* 具体类型                             */
    int                                 fdcnt;
    int                                 fdmax;                  /* 描述符的个数                         */
    int                                 keepalive;              /* 是否启用 keepalive                   */
    int                                 keepaliveidle;          /* 不活跃的链接发送keepalive前的时间    */
    int                                 keepaliveinterval;      /* keepalive 发送的间隔                 */
    int                                 keepalivecount;         /* keepalive发送的次数                  */
    int                                 usertimeout;            /* 发送后等待的时间                     */
    char                                port[128];              /* 服务端监听端口                       */
    dlist*                              hosts;                  /* 监听                                 */
    int*                                pos;                    /* 在模型中的位置下标                   */
    rsocket*                            fd;                     /* 网络描述符                           */
    netiompbase*                 base;                   /* IO复用基础信息                       */
    netiompops*                  ops;                    /* IO 复用模型                          */
    netserver_handler                   callback;               /* 回调函数                             */
} netserver;

/* 初始设置 */
bool netserver_reset(netserver* netserver);

/* 设置netserver svrhost */
bool netserver_host_set(netserver* netserver, char* host, netserver_hosttype hosttype);

/* 设置netserver svrport */
void netserver_port_set(netserver* netserver, int port);

/* 设置类型 */
void netserver_type_set(netserver* netserver, int type);

/* 创建server端 */
bool netserver_create(netserver* netserver);

/* 创建事件并接收描述符,等待触发后调用回掉函数处理 */
bool netserver_desc(netserver* netserver);

/* 资源回收 */
void netserver_free(netserver* netserver);

#endif
