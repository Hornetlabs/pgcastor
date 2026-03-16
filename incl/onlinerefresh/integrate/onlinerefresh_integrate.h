#ifndef _RIPPLE_ONLINEREFRESH_INTEGRATE_H
#define _RIPPLE_ONLINEREFRESH_INTEGRATE_H

typedef enum RIPPLE_ONLINEREFRESH_INTEGRATE_STATE
{
    RIPPLE_ONLINEREFRESH_INTEGRATE_NOP          = 0x00,
    RIPPLE_ONLINEREFRESH_INTEGRATE_INIT         ,
    RIPPLE_ONLINEREFRESH_INTEGRATE_STARTING     ,
    RIPPLE_ONLINEREFRESH_INTEGRATE_RUNNING      ,
    RIPPLE_ONLINEREFRESH_INTEGRATE_REFRESHDONE  ,
    RIPPLE_ONLINEREFRESH_INTEGRATE_DONE         ,
    RIPPLE_ONLINEREFRESH_INTEGRATE_ABANDONED    ,
    RIPPLE_ONLINEREFRESH_INTEGRATE_FREE         
}ripple_onlinerefresh_integrate_state;

typedef struct RIPPLE_ONLINEREFRESH_INTEGRATE
{
    int8                                    increment;
    ripple_onlinerefresh_integrate_state    stat;
    int                                     parallelcnt;                            /* 存量工作线程, 并行数量 */
    FullTransactionId                       txid;                                   /* 重启时生成过滤集使用 */
    ripple_recpos                           begin;
    ripple_uuid_t                           no;                                     /* onlinerefresh 编号 */
    char                                    *conninfo;                              /* 连接字串 */
    char                                    data[RIPPLE_MAXPATH];                   /* 存放数据的目录 */
    char                                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_refresh_table_syncstats*         tablesyncstats;
    char                                    padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                           tqueue;
    char                                    padding2[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                           recordscache;
    char                                    padding3[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                       parser2rebuild;
    char                                    padding4[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                       rebuild2sync;
    char                                    padding5[RIPPLE_CACHELINE_SIZE];
    ripple_thrsubmgr*                       thrsmgr;
    char                                    padding6[RIPPLE_CACHELINE_SIZE];
} ripple_onlinerefresh_integrate;


ripple_onlinerefresh_integrate *ripple_onlinerefresh_integrate_init(bool increment);

void *ripple_onlinerefresh_integrate_manage(void* args);

/* 根据persist生成onlinerefresh节点 */
bool ripple_onlinerefresh_integrate_persist2onlinerefreshmgr(ripple_onlinerefresh_persist *persist, void **onlinerefresh);

/* 检测onlinerefresh与已启动的存量表是否有冲突,有冲突返回 true */
bool ripple_onlinerefresh_integrate_isconflict(dlistnode* in_dlnode);

void ripple_onlinerefresh_integrate_free(void* in_onlinerefresh);

#endif
