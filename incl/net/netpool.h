#ifndef _NETPOOL_H_
#define _NETPOOL_H_

#define NETPOOL_DEFAULTFDSIZE                    128

typedef enum NETPOOLENTRY_STAT
{
    NETPOOLENTRY_STAT_NOP                        = 0x00,
    NETPOOLENTRY_STAT_OK                         ,
    NETPOOLENTRY_STAT_CLOSEUTILWPACKETNULL       ,

    NETPOOLENTRY_STAT_MAX
} netpoolentry_stat;

typedef struct NETPOOLENTRY
{
    rsocket                             fd;
    netpoolentry_stat            stat;
    char*                               host;
    char*                               port;
    queue*                       rpackets;               /* 读到的数据 */
    queue*                       wpackets;               /* 待发送数据 */
} netpoolentry;

/* 服务 */
typedef struct NETPOOL
{
    int                                 fdcnt;
    int                                 fdmax;
    int*                                pos;                    /* 位置 */
    int*                                errorfds;               /* 出现错误的描述符 */
    netiompbase*                 base;                   /* IO复用基础信息                       */
    netiompops*                  ops;                    /* IO 复用模型                          */
    netpoolentry**               fds;
} netpool;

extern netpoolentry* netpoolentry_init(void);

/* 描述符设置 */
extern void netpoolentry_setfd(netpoolentry* npoolentry, int fd);

/* 设置主机信息 */
extern bool netpoolentry_sethost(netpoolentry* npoolentry, char* host);

/* 设置端口信息 */
extern bool netpoolentry_setport(netpoolentry* npoolentry, char* port);

extern void netpoolentry_destroy(netpoolentry* npoolentry);

extern netpool* netpool_init(void);

/* 添加 */
extern bool netpool_add(netpool* npool, netpoolentry* entry);

/* 删除 */
extern void netpool_del(netpool* npool, int fd);

/* 根据 fd 在 netpool 中获取 entry */
extern netpoolentry* netpool_getentrybyfd(netpool* npool, int fd);

/* 创建事件并接收描述符,等待触发后调用回掉函数处理 */
extern bool netpool_desc(netpool* npool, int* cnt, int** perrorfds);

/* 销毁 */
extern void netpool_destroy(netpool* npool);

#endif
