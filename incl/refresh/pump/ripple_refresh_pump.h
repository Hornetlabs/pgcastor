#ifndef _RIPPLE_REFRESH_PUMP_H_
#define _RIPPLE_REFRESH_PUMP_H_

typedef enum RIPPLE_REFRESH_PUMP_STAT
{
    RIPPLE_REFRESH_PUMP_STAT_NOP             = 0x00,
    RIPPLE_REFRESH_PUMP_STAT_INIT            ,               /* 初始化monitor结构设置为init */
    RIPPLE_REFRESH_PUMP_STAT_STARTING        ,               /* 加入list链表状态 */
    RIPPLE_REFRESH_PUMP_STAT_WORK            ,               /* 执行refresh时设置为工作状态 */
    RIPPLE_REFRESH_PUMP_STAT_DONE                            /* refresh结束时设置为done */
}ripple_refresh_pump_stat;

typedef struct RIPPLE_REFRESH_PUMP
{
    ripple_refresh_pump_stat            stat;
    int                                 parallelcnt;
    ripple_thrsubmgr*                   thrsmgr;
    char                               *refresh_path;       /* refresh文件夹路径 */
    ripple_refresh_table_syncstats     *sync_stats;
    ripple_queue                       *tqueue;
} ripple_refresh_pump;

ripple_refresh_pump *ripple_refresh_pump_init(void);

/* 设置状态为 init */
void ripple_refresh_pump_setstatinit(ripple_refresh_pump* rpump);

/* 设置需要刷新的 refresh 表 */
void ripple_refresh_pump_setsynctablestat(ripple_refresh_pump* rpump, ripple_refresh_table_syncstats* tbsyncstat);

/* 线程处理入口 */
void *ripple_refresh_pump_main(void* args);

/* 真实释放 */
void ripple_refresh_pump_free(ripple_refresh_pump *rpump);

#endif
