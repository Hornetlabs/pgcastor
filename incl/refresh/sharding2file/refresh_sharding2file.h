#ifndef _REFRESH_SHARDING2FILE_H
#define _REFRESH_SHARDING2FILE_H

typedef struct TASK_REFRESH_SHARDING2FILE
{
    char*   snap_shot_name; /* Snapshot name */
    char*   conn_info;      /* Connection string */
    char*   refresh_path;   /* Refresh folder path, Referenced from mgr, no need to release */
    PGconn* conn;           /* libpq connection handle */
    queue*  tqueue;         /* Task queue */
} task_refresh_sharding2file;

void* refresh_sharding2file_work(void* args);

void refresh_sharding2file_free(void* args);

task_refresh_sharding2file* refresh_sharding2file_init(void);

#endif
