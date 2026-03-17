#ifndef _REFRESH_INTEGRATE_H
#define _REFRESH_INTEGRATE_H

typedef enum REFRESH_INTEGRATE_STAT
{
    REFRESH_INTEGRATE_STAT_NOP             = 0x00,
    REFRESH_INTEGRATE_STAT_INIT            ,               /* 初始化monitor结构设置为init */
    REFRESH_INTEGRATE_STAT_STARTING        ,               /* 加入list链表状态 */
    REFRESH_INTEGRATE_STAT_WORK            ,               /* 执行refresh时设置为工作状态 */
    REFRESH_INTEGRATE_STAT_DONE                            /* refresh结束时设置为done */
}refresh_integrate_stat;


typedef struct REFRESH_INTEGRATE
{
    int                                 stat;               /* 管理线程状态 */
    int                                 parallelcnt;
    char*                               conn_info;          /* 保存连接字符串 */
    char                                *refresh_path;       /* refresh文件夹路径 */
    thrsubmgr*                   thrsmgr;
    refresh_table_syncstats      *sync_stats;
    queue                        *tqueue;
} refresh_integrate;

extern refresh_integrate *refresh_integrate_init(void);

extern bool refresh_integrate_write(refresh_integrate *rintegrate);

extern bool refresh_integrate_read(refresh_integrate** refresh);

extern void *refresh_integrate_main(void* args);

extern void refresh_integrate_free(void* args);

extern void refresh_integrate_listfree(void* args);

#endif
