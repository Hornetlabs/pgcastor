#ifndef _RIPPLE_NETSERVER_H
#define _RIPPLE_NETSERVER_H

#define RIPPLE_NETSERVER_HOSTMAXLEN                         1024
#define RIPPLE_NETSERVER_DEFAULTSOCKSIZE                    8

typedef bool (*netserver_handler)(void* netserver, rsocket  sock);

typedef enum RIPPLE_NETSERVER_TYPE
{
    RIPPLE_NETSERVER_TYPE_NOP               = 0x00,
    RIPPLE_NETSERVER_TYPE_XMANAGER          
} ripple_netserver_type;

typedef enum RIPPLE_NETSERVER_HOSTTYPE
{
    RIPPLE_NETSERVER_HOSTTYPE_NOP               = 0x00,
    RIPPLE_NETSERVER_HOSTTYPE_UNIXDOMAIN        ,
    RIPPLE_NETSERVER_HOSTTYPE_IP
} ripple_netserver_hosttype;

typedef struct RIPPLE_NETSERVER_HOST
{
    ripple_netserver_hosttype           type;
    char                                host[512];               /* 服务端监听地址                    */
} ripple_netserver_host;

/* 服务 */
typedef struct RIPPLE_NETSERVER
{
    ripple_netserver_type               type;                   /* 具体类型                             */
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
    ripple_netiompbase*                 base;                   /* IO复用基础信息                       */
    ripple_netiompops*                  ops;                    /* IO 复用模型                          */
    netserver_handler                   callback;               /* 回调函数                             */
} ripple_netserver;

/* 初始设置 */
bool ripple_netserver_reset(ripple_netserver* netserver);

/* 设置netserver svrhost */
bool ripple_netserver_host_set(ripple_netserver* netserver, char* host, ripple_netserver_hosttype hosttype);

/* 设置netserver svrport */
void ripple_netserver_port_set(ripple_netserver* netserver, int port);

/* 设置类型 */
void ripple_netserver_type_set(ripple_netserver* netserver, int type);

/* 创建server端 */
bool ripple_netserver_create(ripple_netserver* netserver);

/* 创建事件并接收描述符,等待触发后调用回掉函数处理 */
bool ripple_netserver_desc(ripple_netserver* netserver);

/* 资源回收 */
void ripple_netserver_free(ripple_netserver* netserver);

#endif
