#ifndef _ONLINEREFRESH_INTEGRATE_H
#define _ONLINEREFRESH_INTEGRATE_H

typedef enum ONLINEREFRESH_INTEGRATE_STATE
{
    ONLINEREFRESH_INTEGRATE_NOP          = 0x00,
    ONLINEREFRESH_INTEGRATE_INIT         ,
    ONLINEREFRESH_INTEGRATE_STARTING     ,
    ONLINEREFRESH_INTEGRATE_RUNNING      ,
    ONLINEREFRESH_INTEGRATE_REFRESHDONE  ,
    ONLINEREFRESH_INTEGRATE_DONE         ,
    ONLINEREFRESH_INTEGRATE_ABANDONED    ,
    ONLINEREFRESH_INTEGRATE_FREE         
}onlinerefresh_integrate_state;

typedef struct ONLINEREFRESH_INTEGRATE
{
    int8                                    increment;
    onlinerefresh_integrate_state    stat;
    int                                     parallelcnt;                            /* 存量工作线程, 并行数量 */
    FullTransactionId                       txid;                                   /* 重启时生成过滤集使用 */
    recpos                           begin;
    uuid_t                           no;                                     /* onlinerefresh 编号 */
    char                                    *conninfo;                              /* 连接字串 */
    char                                    data[MAXPATH];                   /* 存放数据的目录 */
    char                                    padding[CACHELINE_SIZE];
    refresh_table_syncstats*         tablesyncstats;
    char                                    padding1[CACHELINE_SIZE];
    queue*                           tqueue;
    char                                    padding2[CACHELINE_SIZE];
    queue*                           recordscache;
    char                                    padding3[CACHELINE_SIZE];
    cache_txn*                       parser2rebuild;
    char                                    padding4[CACHELINE_SIZE];
    cache_txn*                       rebuild2sync;
    char                                    padding5[CACHELINE_SIZE];
    thrsubmgr*                       thrsmgr;
    char                                    padding6[CACHELINE_SIZE];
} onlinerefresh_integrate;


onlinerefresh_integrate *onlinerefresh_integrate_init(bool increment);

void *onlinerefresh_integrate_manage(void* args);

/* 根据persist生成onlinerefresh节点 */
bool onlinerefresh_integrate_persist2onlinerefreshmgr(onlinerefresh_persist *persist, void **onlinerefresh);

/* 检测onlinerefresh与已启动的存量表是否有冲突,有冲突返回 true */
bool onlinerefresh_integrate_isconflict(dlistnode* in_dlnode);

void onlinerefresh_integrate_free(void* in_onlinerefresh);

#endif
