#ifndef RIPPLE_BIGTXN_PUMPMANAGER_H
#define RIPPLE_BIGTXN_PUMPMANAGER_H

/* 大事务管理线程主函数 */
/* 管理线程状态转换
 * 1.申请ripple_bigtxn_pumpmanager空间加入队列中设置为 INIT
 * 2.遍历队列启动管理线程设置为 STARTING , 此状态下管理线程等待启动net线程
 * 3.当net线程接收到身份验证后设置管理线程为 INPROCESS ，该状态下启动loadrecords/parser/serial线程
 *   并设置管理状态为 WAITDONE 
 * 4.WAITDONE状态下等待解析线程退出 --> 设置loadrecords 线程TERM-->等待net线程退出 --> 设置 serial 线程TERM
 *  --> 等待loadrecords、serial线程退出
 * 5.所有job线程退出设置状态为 DONE ，退出管理线程
 * 
 * 特殊处理：解析到reset
 * 1.解析线程接收到reset事务后回调设置管理线程abandon为 true ，该状态下设置loadrecords/parser/serial/net线程状态为TERM
 *  --> 等待loadrecords/parser/serial/net退出 --> 设置管理线程状态为 ABANDONED
 * 
 * 网络故障：
 * 1.网络出现故障，net线程会设置大事务管理线程状态为 RESET，NET线程会等待管理线程状态不为 RESET
 * 2.大事务管理线程在 RESET 状态下设置 loadrecords/parser/serial 为 TERM --> 清理 txn2filebuffers/parser2txns/recordscache缓存
 *   -->等待loadrecords/parser/serial退出 --> 清理 txn2filebuffers/parser2txns/recordscache缓存 --> 设置管理线程状态为IDENTITY
 * 3.管理线程状态为IDENTITY下，等待网络连接成功接收到身份验证后由net线程设置管理线程状态为 INPROCESS 
 *   --> 管理线程启动loadrecords/parser/serial线程 --> 等待退出
*/

typedef enum RIPPLE_BIGTXN_PUMPMANAGER_STAT
{
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_NOP          = 0x00,             /* 无意义 */
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_INIT         ,                   /* 初始化，加入队列时设置 */
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_STARTING     ,                   /* 启动中，启动线程时设置 */
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_IDENTITY     ,                   /* 等待身份验证，启动net、reset完成设置 */
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_INPROCESS    ,                   /* 运行中 身份验证完成net线程回调设置*/
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_RESET        ,                   /* 重置中，重启loadrecords/parser/序列化，网络故障时设置 */
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_WAITINGDONE  ,                   /* 等待退出 */
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_DONE         ,                   /* 已结束 */
    RIPPLE_BIGTXN_PUMPMANAGER_STAT_ABANDONED                        /* 已放弃 */
} ripple_bigtxn_pumpmanager_stat;

typedef struct ripple_bigtxn_pumpmanager
{
    ripple_bigtxn_pumpmanager_stat          stat;                   /* 标识状态 */
    bool                                    abandon;
    ripple_recpos                           begin;                  /* 开始位置(原增量里面记录的 bigtxn_begin 中trail 文件的 fileid/offset) */
    ripple_recpos                           end;                    /* 结束位置(原增量里面记录的 bigtxn_end 中trail 文件的 fileid/offset) */
    FullTransactionId                       xid;
    char                                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_thrsubmgr*                       thrsmgr;
    char                                    padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue                            *recordscache;
    char                                    padding2[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn                        *parser2txns;
    char                                    padding3[RIPPLE_CACHELINE_SIZE];
    ripple_file_buffers                     *txn2filebuffers;
    char                                    padding4[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                           filegap;
    char                                    padding5[RIPPLE_CACHELINE_SIZE];
} ripple_bigtxn_pumpmanager;

extern int ripple_bigtxn_pumpmanager_compare_recpos(ripple_recpos* src, ripple_recpos* dst);

/* 设置大事务管理线程起点*/
extern void ripple_bigtxn_pumpmanager_set_begin(ripple_bigtxn_pumpmanager *bigtxnmgr, ripple_recpos *pos);

/* 设置大事务管理线程终点*/
extern void ripple_bigtxn_pumpmanager_set_end(ripple_bigtxn_pumpmanager *bigtxnmgr, ripple_recpos *pos);

/* 设置大事务管理线状态*/
extern void ripple_bigtxn_pumpmanager_set_stat(ripple_bigtxn_pumpmanager *bigtxnmgr, int stat);

/* 根据persist 生成大事务链表*/
extern dlist *ripple_bigtxn_pumpmanager_persist2pumpmanager(ripple_bigtxn_persist *persist, ripple_queue* gap);

/* 初始化 */
extern ripple_bigtxn_pumpmanager *ripple_bigtxn_pumpmanager_init(void);

/* 主函数 */
extern void* ripple_bigtxn_pumpmanager_main(void* args);

/* 添加删除网闸目录任务 */
extern void ripple_bigtxn_pumpmanager_gapdeletedir_add(ripple_bigtxn_pumpmanager *bigtxnmgr);

extern void ripple_bigtxn_pumpmanager_destory(void* args);

#endif
