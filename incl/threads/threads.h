#ifndef _THREADS_H_
#define _THREADS_H_

/*
 * 线程类型分为三类, 常驻线程、临时管理线程、临时工作线程
 * 
 *  常驻线程:
 *      业务线程，正常情况下不会退出，当出现错误时,那么可能会退出,只要常驻线程退出,那么整个服务也将退出
 *      常驻线程节点
 *          常驻线程1
 *          常驻线程2
 *          常驻线程3
 *          无管理线程----->临时管理线程
 *          无管理线程----->临时管理线程
 *      常驻线程节点
 *          常驻线程1
 *          常驻线程2
 *          常驻线程3
 *  
 *  临时管理线程
 *      临时管理线程在需要时启动，负责注册启动/停止子线程, 临时管理线程会管理多个子线程，当子线程异常退出时, 临时管理线程走正常退出机制
 * 
 *  临时工作线程
 *      做具体业务的线程，做完业务后就会退出, 退出时临时管理线程负责清理资源，若临时管理线程异常退出，那么清理资源的操作将由主线程完成
 * 
*/

/* 无效的线程编号 */
#define INVALIDTHRID                  0

/* 临时线程编号应大于此值 */
#define THRNODE_NO_NORMAL             4096

typedef enum THRNODE_TYPE
{
    THRNODE_TYPE_NOP                  = 0x00,

    /* 常驻工作线程 */
    THRNODE_TYPE_PERSIST              ,

    /* 临时管理线程 */
    THRNODE_TYPE_TMGR                 ,

    /* 临时工作线程 */
    THRNODE_TYPE_TJOB

} thrnode_type;

typedef enum THRNODE_IDENTITY
{
    THRNODE_IDENTITY_NOP                                     = 0x00,

    /* 落盘线程 */
    THRNODE_IDENTITY_INC_CAPTURE_FLUSH                       ,

    /* 序列化线程 */
    THRNODE_IDENTITY_INC_CAPTURE_SERIAL                      ,

    /* 解析器线程 */
    THRNODE_IDENTITY_INC_CAPTURE_PARSER                      ,

    /* 日志拆分线程 */
    THRNODE_IDENTITY_INC_CAPTURE_LOADRECORD                  ,

    /* 指标线程 */
    THRNODE_IDENTITY_CAPTURE_METRIC                          ,

    /* 大事务管理线程 */
    THRNODE_IDENTITY_INC_CAPTURE_BIGTXNMGR                   ,

    /* 大事务落盘线程 */
    THRNODE_IDENTITY_INC_CAPTURE_BIGTXNFLUSH                 ,

    /* 大事务序列化线程 */
    THRNODE_IDENTITY_INC_CAPTURE_BIGTXNSERIAL                ,

    /* refresh 管理线程 */
    THRNODE_IDENTITY_CAPTURE_REFRESH_MGR                     ,

    /* refresh 工作线程 */
    THRNODE_IDENTITY_CAPTURE_REFRESH_JOB                     ,

    /* onlinerefresh 管理线程 */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_MGR                ,

    /* onlinerefresh 工作线程 */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_JOB                ,

    /* onlinerefresh increment flush 线程 */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_FLUSH          ,

    /* onlinerefresh increment serial 线程 */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_SERIAL         ,

    /* onlinerefresh increment parser 线程 */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_PARSER         ,

    /* onlinerefresh increment loadrecords 线程 */
    THRNODE_IDENTITY_CAPTURE_OLINEREFRESH_INC_LOADRECORDS    ,

    /*------------capture 线程类型在上面添加---------------------*/

    /* 网闸线程 */
    THRNODE_IDENTITY_GAP                                     ,

    /*------------integrate 线程类型在此添加 BEGIN --------------*/
    /* 应用线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_SYNC                      ,

    /* 事务重组线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_REBUILD                   ,

    /* 解析器线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_PARSER                    ,

    /* trail拆分线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_LOADRECORDS               ,

    /* 指标线程 */
    THRNODE_IDENTITY_INTEGRATE_METRIC                        ,

    /* 大事务管理线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNMGR                 ,

    /* 大事务应用线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNSYNC                ,

    /* 大事务重组线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNREBUILD             ,

    /* 大事务解析器线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNPARSER              ,

    /* 大事务trail拆分线程 */
    THRNODE_IDENTITY_INC_INTEGRATE_BIGTXNLOADRECORDS         ,

    /* refresh 管理线程 */
    THRNODE_IDENTITY_INTEGRATE_REFRESH_MGR                   ,

    /* refresh 工作线程 */
    THRNODE_IDENTITY_INTEGRATE_REFRESH_JOB                   ,

    /* onlinerefresh 管理线程 */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_MGR              ,

    /* onlinerefresh 工作线程 */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_JOB              ,

    /* onlinerefresh increment 应用线程 */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_SYNC         ,

    /* onlinerefresh increment 重组线程 */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_REBUILD      ,

    /* onlinerefresh increment 解析器线程 */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_PARSER       ,

    /*onlinerefresh increment trail拆分线程 */
    THRNODE_IDENTITY_INTEGRATE_OLINEREFRESH_INC_LOADRECORDS  ,
    /*------------integrate 线程类型 END------------------------*/

    /*------------xmanager 线程类型 begin-----------------------*/
    THRNODE_IDENTITY_XMANAGER_LISTEN                         ,
    THRNODE_IDENTITY_XMANAGER_AUTH                           ,
    THRNODE_IDENTITY_XMANAGER_METRIC                         ,

    /*------------xmanager 线程类型   end-----------------------*/

    /* 请在此前添加 */
    THRNODE_IDENTITY_MAX
} thrnode_identity;


/*
 * stat 转换逻辑
 *  NOP------------> init -----------> starting -----------------> work ----------> term---------->exit ------------> exited----->free
 *  结构体初始化
 *             |--->加入到启动列表中
 *                                 |---> 主线程启动线程并设置状态为 starting
 *                                                              |------->子线程启动成功,设置为 work
 *                                                                                 |---> 主线程/管理线程设置为 term
 *                                                                                                 |---> 子线程退出后设置为 exit
 *                                                                                                                   |---->主线程检测到退出,设置为 exited
 *                                                                                                                              |---->主/管理线程 回收资源
 * 
*/
typedef enum THRNODE_STAT
{
    THRNODE_STAT_NOP          = 0x00,                     /* 未有用                       */
    THRNODE_STAT_INIT         ,                           /* 增加线程时，初始状态         */
    THRNODE_STAT_STARTING     ,                           /* 启动中                       */
    THRNODE_STAT_WORK         ,                           /* 
                                                                     * 子线程工作时,设置为 WORK
                                                                     * 需要注意的点是: 管理线程, 管理线程在启动完子线程后才设置线程状态为 WORK
                                                                     */
    THRNODE_STAT_IDLE         ,                            /* 子线程没有工作 */
    THRNODE_STAT_TERM         ,                           /* 主线程获取到 SIGTERM,设置线程的状态为退出 */
    THRNODE_STAT_ABORT        ,                           /* 
                                                                     * 线程的主动行为, 线程内部明确出错也捕获到了错误
                                                                     * 异常退出,需要根据线程的类型,退出逻辑不同
                                                                     *  常驻线程, 执行退出逻辑，退出逻辑  工作线程---->管理线程---->常驻线程
                                                                     *  管理线程, 工作线程退出
                                                                     *  工作线程退出, 找到该工作线程对应的管理线程，工作线程---->管理线程
                                                                     *      在管理线程中将异常退出的线程状态设置为 EXIT
                                                                     */
    THRNODE_STAT_EXIT         ,                           /* 子线程正常退出时，设置状态为 EXIT */
    THRNODE_STAT_EXITED       ,                           /* 主线程回收完成后, 设置为 exited */
    THRNODE_STAT_FREE         ,                           /* 用于标识可回收               */
} thrnode_stat;

typedef void (*thrdatafree)(void* args);
typedef bool (*threxitcondition)(void* args);
typedef void* (*thrmain)(void* args);

typedef struct THREADS
{
    /* 其它线程编号,最小值为 THRNODE_NO_NORMAL */
    uint64                      no;

    /* 常驻线程编号,最大值为 THRNODE_NO_NORMAL */
    uint64                      persistno;

    /* 锁 */
    pthread_mutex_t             lock;

    /* 常驻线程 */
    dlist*                      thrpersist;

    /* thrsubmgr, 管理线程 */
    dlist*                      thrsubmgrs;

    /* 线程链表 */
    dlist*                      thrnodes;
} threads;


/* threadnoderef */
typedef struct THRREF
{
    /* 通过 no 在 threads->thrnodes 中获取线程信息 */
    uint64                      no;
} thrref;

/* 管理线程 */
typedef struct THRSUBMGR
{
    /* 归属的常驻线程节点 */
    thrref               persistref;

    /* 管理线程 */
    thrref               submgrref;

    /* 子线程 */
    List*                       childthrrefs;

    /* 管理 */
    threads*             parents;
} thrsubmgr;

typedef struct THRNODE
{
    /* 标识线程的类型, 常驻/动态, see above */
    int                         type;

    /* 线程标识 */
    int                         identity;

    /* 状态, see above */
    int                         stat;

    pthread_t                   thrid;

    /* 线程编号,大于 THRNODE_NO_NORMAL */
    uint64                      no;

    /* 
     * 当为管理线程, thrsubmgr 不为空, 指向自己
     */
    thrsubmgr*           thrsubmgr;
    void*                       data;
    thrdatafree                 free;
    threxitcondition            exitcondition;
    thrmain                     tmain;
} thrnode;

/* 常驻线程 */
typedef struct THRPERSIST
{
    /* 线程编号 */
    uint64                      no;

    /* 名称 */
    char                        name[NAMEDATALEN];

    /* 常驻线程 */
    List*                       thrrefs;
} thrpersist;

/* 线程池初始化 */
threads* threads_init(void);

/* 根据编号获取 node 节点 */
thrnode* threads_getthrnodebyno(threads* thrs, uint64 no);

/* 设置子线程状态为 FREE */
void threads_setsubmgrjobthredsfree(threads* thrs, List* jobthreads, int skipcnt, int scnt);

/* 
 * 设置子线程为 term 状态并退出
 */
void threads_setsubmgrjobthreadterm(threads* thrs, List* jobthreads);

/*
 * 统计子管理线程下的工作线程状态 > 工作状态下的线程个数
*/
bool threads_countsubmgrjobthredsabovework(threads* thrs, List* jobthreads, int* scnt);

/* 
 * 设置子管理线程的子线程为 TERM
 *  1、在 IDLE 状态下为term
 *  2、统计已经退出线程的个数
 * 
 *  skipcnt     跳过的个数
 *  scnt        入参时为统计的个数
 *              出参为已退出的个数
 * 
 *  true        函数执行过程中未遇到逻辑错误
 *  false       函数执行过程中遇到错误的逻辑
 */
bool threads_setsubmgrjobthredstermandcountexit(threads* thrs, List* jobthreads, int skipcnt, int* scnt);

/* 添加一个常驻线程的节点 */
bool threads_addpersist(threads* thrs, uint64* pno, char* name);

/* 
 * 增加常驻线程
 *  identity                线程标识
 *  data                    线程主体结构
 *  
 *  pthrnode                返回值
*/
bool threads_addpersistthread(threads* thrs,
                                        thrnode** pthrnode,
                                        thrnode_identity identity,
                                        uint64 persistno,
                                        void* data,
                                        thrdatafree free,
                                        threxitcondition exitcondition,
                                        thrmain tmain);

/* 增加管理线程 */
bool threads_addsubmanger(threads* thrs,
                                    thrnode_identity identity,
                                    uint64 persistno,
                                    thrsubmgr** pthrsubmgr,
                                    void* data,
                                    thrdatafree free,
                                    threxitcondition exitcondition,
                                    thrmain tmain);


/* 添加工作线程 */
bool threads_addjobthread(threads* thrs,
                                    thrnode_identity identity,
                                    uint64 submgrno,
                                    void* data,
                                    thrdatafree free,
                                    threxitcondition exitcondition,
                                    thrmain tmain);


/* 线程退出,接收到 sigterm 执行此函数 */
bool threads_exit(threads* thrs);

/*
 * 尝试回收线程
 * 
 * 常驻线程退出, 那么设置所有的线程退出, 并等待线程退出
 * 管理线程异常退出, 管理线程和管理线程下的子线程退出
 * 管理线程下的工作线程退出, 只回收工作线程
*/
bool threads_tryjoin(threads* thrs);

/*
 * 启动子线程
*/
void threads_startthread(threads* thrs);

/* 回收节点 */
void threads_thrnoderecycle(threads* thrs);

/* 
 * 是否含有子线程
 *  false       不含有子线程
 *  true        含有子线程
*/
bool threads_hasthrnode(threads* thrs);

/* 内存回收 */
void threads_free(threads* thrs);

#endif
