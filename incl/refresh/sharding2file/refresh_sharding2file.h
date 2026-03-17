#ifndef _REFRESH_SHARDING2FILE_H
#define _REFRESH_SHARDING2FILE_H

typedef struct TASK_REFRESH_SHARDING2FILE
{
    char*               snap_shot_name;     /* 快照名 */
    char*               conn_info;          /* 连接字符串 */
    char*               refresh_path;       /* refresh文件夹路径, 引用自mgr, 无需释放 */
    PGconn*             conn;               /* libpq连接句柄 */
    queue*       tqueue;             /* 任务队列 */
} task_refresh_sharding2file;

void* refresh_sharding2file_work(void* args);

void refresh_sharding2file_free(void* args);

task_refresh_sharding2file * refresh_sharding2file_init(void);

#endif
