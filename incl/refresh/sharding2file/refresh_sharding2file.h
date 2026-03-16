#ifndef _RIPPLE_REFRESH_SHARDING2FILE_H
#define _RIPPLE_REFRESH_SHARDING2FILE_H

typedef struct RIPPLE_TASK_REFRESH_SHARDING2FILE
{
    char*               snap_shot_name;     /* 快照名 */
    char*               conn_info;          /* 连接字符串 */
    char*               refresh_path;       /* refresh文件夹路径, 引用自mgr, 无需释放 */
    PGconn*             conn;               /* libpq连接句柄 */
    ripple_queue*       tqueue;             /* 任务队列 */
} ripple_task_refresh_sharding2file;

void* ripple_refresh_sharding2file_work(void* args);

void ripple_refresh_sharding2file_free(void* args);

ripple_task_refresh_sharding2file * ripple_refresh_sharding2file_init(void);

#endif
