#ifndef _RIPPLE_REFRESH_INTEGRATE_H
#define _RIPPLE_REFRESH_INTEGRATE_H

typedef enum RIPPLE_REFRESH_INTEGRATE_STAT
{
    RIPPLE_REFRESH_INTEGRATE_STAT_NOP             = 0x00,
    RIPPLE_REFRESH_INTEGRATE_STAT_INIT            ,               /* 初始化monitor结构设置为init */
    RIPPLE_REFRESH_INTEGRATE_STAT_STARTING        ,               /* 加入list链表状态 */
    RIPPLE_REFRESH_INTEGRATE_STAT_WORK            ,               /* 执行refresh时设置为工作状态 */
    RIPPLE_REFRESH_INTEGRATE_STAT_DONE                            /* refresh结束时设置为done */
}ripple_refresh_integrate_stat;


typedef struct RIPPLE_REFRESH_INTEGRATE
{
    int                                 stat;               /* 管理线程状态 */
    int                                 parallelcnt;
    char*                               conn_info;          /* 保存连接字符串 */
    char                                *refresh_path;       /* refresh文件夹路径 */
    ripple_thrsubmgr*                   thrsmgr;
    ripple_refresh_table_syncstats      *sync_stats;
    ripple_queue                        *tqueue;
} ripple_refresh_integrate;

extern ripple_refresh_integrate *ripple_refresh_integrate_init(void);

extern bool ripple_refresh_integrate_write(ripple_refresh_integrate *rintegrate);

extern bool ripple_refresh_integrate_read(ripple_refresh_integrate** refresh);

extern void *ripple_refresh_integrate_main(void* args);

extern void ripple_refresh_integrate_free(void* args);

extern void ripple_refresh_integrate_listfree(void* args);

#endif
