#ifndef _RIPPLE_NETPOOL_H_
#define _RIPPLE_NETPOOL_H_

#define RIPPLE_NETPOOL_DEFAULTFDSIZE                    128

typedef enum RIPPLE_NETPOOLENTRY_STAT
{
    RIPPLE_NETPOOLENTRY_STAT_NOP                        = 0x00,
    RIPPLE_NETPOOLENTRY_STAT_OK                         ,
    RIPPLE_NETPOOLENTRY_STAT_CLOSEUTILWPACKETNULL       ,

    RIPPLE_NETPOOLENTRY_STAT_MAX
} ripple_netpoolentry_stat;

typedef struct RIPPLE_NETPOOLENTRY
{
    rsocket                             fd;
    ripple_netpoolentry_stat            stat;
    char*                               host;
    char*                               port;
    ripple_queue*                       rpackets;               /* 读到的数据 */
    ripple_queue*                       wpackets;               /* 待发送数据 */
} ripple_netpoolentry;

/* 服务 */
typedef struct RIPPLE_NETPOOL
{
    int                                 fdcnt;
    int                                 fdmax;
    int*                                pos;                    /* 位置 */
    int*                                errorfds;               /* 出现错误的描述符 */
    ripple_netiompbase*                 base;                   /* IO复用基础信息                       */
    ripple_netiompops*                  ops;                    /* IO 复用模型                          */
    ripple_netpoolentry**               fds;
} ripple_netpool;

extern ripple_netpoolentry* ripple_netpoolentry_init(void);

/* 描述符设置 */
extern void ripple_netpoolentry_setfd(ripple_netpoolentry* npoolentry, int fd);

/* 设置主机信息 */
extern bool ripple_netpoolentry_sethost(ripple_netpoolentry* npoolentry, char* host);

/* 设置端口信息 */
extern bool ripple_netpoolentry_setport(ripple_netpoolentry* npoolentry, char* port);

extern void ripple_netpoolentry_destroy(ripple_netpoolentry* npoolentry);

extern ripple_netpool* ripple_netpool_init(void);

/* 添加 */
extern bool ripple_netpool_add(ripple_netpool* npool, ripple_netpoolentry* entry);

/* 删除 */
extern void ripple_netpool_del(ripple_netpool* npool, int fd);

/* 根据 fd 在 netpool 中获取 entry */
extern ripple_netpoolentry* ripple_netpool_getentrybyfd(ripple_netpool* npool, int fd);

/* 创建事件并接收描述符,等待触发后调用回掉函数处理 */
extern bool ripple_netpool_desc(ripple_netpool* npool, int* cnt, int** perrorfds);

/* 销毁 */
extern void ripple_netpool_destroy(ripple_netpool* npool);

#endif
