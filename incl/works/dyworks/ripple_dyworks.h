#ifndef _RIPPLE_DYWORKS_H
#define _RIPPLE_DYWORKS_H

typedef enum RIPPLE_DYTHREAD_TYPE
{
    RIPPLE_DYTHREAD_TYPE_NOP                                = 0x00,
    RIPPLE_DYTHREAD_TYPE_NETSVR                             ,
    RIPPLE_DYTHREAD_TYPE_REFRESHMGR                         ,
    RIPPLE_DYTHREAD_TYPE_REFRESHMONITOR                     ,
    RIPPLE_DYTHREAD_TYPE_WORK                               ,
    RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE_PARSERWAL    ,
    RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE_SPLITWAL     ,
    RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE_WRITE        ,
    RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE_SERIAL       ,
    RIPPLE_DYTHREAD_TYPE_ONLINEREFRESH_CAPTURE              ,
    RIPPLE_DYTHREAD_TYPE_INTEGRATE_ONLINEREFRESH_MANAGE     ,
    RIPPLE_DYTHREAD_TYPE_MAX                                /* 一定要放在最后 */
} ripple_dythread_type;

/* 线程信息 */
typedef struct RIPPLE_DYWORKS_NODE
{
    ripple_work_status          status;
    pthread_t                   id;
    struct RIPPLE_DYWORKS_NODE* next;
    struct RIPPLE_DYWORKS_NODE* prev;
    void*                       data;
} ripple_dyworks_node;

/* 根据此进行首次区分 */
typedef struct RIPPLE_DYWORKS_NODES
{
    ripple_dythread_type            type;
    pthread_mutex_t                 lock;                       /* 在运行中此锁无用，因为只有主线程维护启动线程 */
    ripple_dyworks_node*            head;
    ripple_dyworks_node*            tail;
} ripple_dyworks_nodes;

typedef struct RIPPLE_DYWORKS
{
    ripple_dyworks_nodes*           nodesrunning;               /* 运行中的尾 */
    ripple_dyworks_nodes*           nodeswait;                  /* 待启动的头 */
} ripple_dyworks;

/* 
 * 设置线程的退出信号
 */
void ripple_dyworks_setterm(void);

/* 用于检测程序是否可以安全退出 */
bool ripple_dyworks_canexit(void);

/* 注册工作线程 */
bool ripple_dyworks_register(ripple_dythread_type type, void* privdata);

/* 启动新线程并回收退出线程 */
void ripple_dyworks_trydestroy(void);

/* 初始化动态线程管理 */
void ripple_dyworks_init(void);


void ripple_dyworks_destroy(int status, void* args);

#endif
