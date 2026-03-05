#ifndef _RIPPLE_ONLINEREFRESH_PUMP_H
#define _RIPPLE_ONLINEREFRESH_PUMP_H


/*
 * 1.初始化设置为init
 * 2.启动onlinrefresh时设置为WAITSTART---添加工作线程
 * 3.添加完所有工作线程设置状态STARTING---等待工作线程启动
 *  3.1.没有存量设置状态为WAITINCREMENTDONE---等待增量线程退出
 *  3.2增量退出置状态为DONE，管理线程退出
 * 4.有存量设置状态ADDREFRESH--添加存量任务
 * 5.存量任务添加完成设置状态WAITREFRESHDONE --- 等待存量退出
 * 6.存量退出设置状态为WAITINCREMENTDONE---等待增量线程退出
 * 7.增量退出置状态为DONE，管理线程退出
 *   
 * 网络故障：
 * 1.net线程网路故障设置管理状态为RESET ---- 退出所有工作线程，清理缓存
 * 2.工作线程退出后设置管理状态为WAITSTART --- 添加工作线程
 * 
 * 放弃onlinerefresh
 * 1.parser线程设置管理线程放弃标识为true
 * 2.管理线程检测到要放弃---退出工作线程设置状态为ABANDONED
 */
typedef enum RIPPLE_ONLINEREFRESH_PUMP_STAT
{
    RIPPLE_ONLINEREFRESH_PUMP_NOP                   = 0x00,
    RIPPLE_ONLINEREFRESH_PUMP_INIT                  ,
    RIPPLE_ONLINEREFRESH_PUMP_WAITSTART             ,
    RIPPLE_ONLINEREFRESH_PUMP_STARTING              ,
    RIPPLE_ONLINEREFRESH_PUMP_ADDREFRESH            ,
    RIPPLE_ONLINEREFRESH_PUMP_WAITREFRESHDONE       ,
    RIPPLE_ONLINEREFRESH_PUMP_WAITINCREMENTDONE     ,
    RIPPLE_ONLINEREFRESH_PUMP_RESET                 ,
    RIPPLE_ONLINEREFRESH_PUMP_DONE                  ,
    RIPPLE_ONLINEREFRESH_PUMP_ABANDONED             ,
}ripple_onlinerefresh_pump_stat;

typedef struct RIPPLE_ONLINEREFRESH_PUMP
{
    int8                                    increment;
    bool                                    abandon;
    ripple_recpos                           begin;
    ripple_recpos                           end;
    int                                     parallelcnt;                        /* 存量工作线程, 并行数量 */
    ripple_onlinerefresh_pump_stat          stat;                               /* 标识 onlinerefresh 的状态 */
    ripple_uuid_t                           no;                                 /* onlinerefresh 编号 */
    char                                    data[RIPPLE_MAXPATH];               /* 存放数据的目录 */
    ripple_thrsubmgr*                       thrsmgr;
    char                                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_refresh_table_syncstats*         tablesyncstats;
    char                                    padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                           tqueue;
    char                                    padding2[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                           recordscache;
    char                                    padding3[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                       parser2synctxns;
    char                                    padding4[RIPPLE_CACHELINE_SIZE];
    ripple_file_buffers*                    txn2filebuffer;
    char                                    padding5[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                           filetransfernode;
    char                                    padding6[RIPPLE_CACHELINE_SIZE];
} ripple_onlinerefresh_pump;


ripple_onlinerefresh_pump *ripple_onlinerefresh_pump_init(void);

void ripple_onlinerefresh_pump_free(void* in_onlinerefresh);

void *ripple_onlinerefresh_pump_main(void* args);

/* 设置onlinerefresh结束位置 */
extern void ripple_onlinerefresh_pumpmanager_setend(ripple_onlinerefresh_pump *olrmgr, ripple_recpos *pos);

dlist *ripple_onlinerefresh_pumpmanager_persist2onlinerefreshmgr(ripple_onlinerefresh_persist *persist, ripple_queue* fpt);

#endif
