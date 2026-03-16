#ifndef _RIPPLE_REFRESH_SHARDING2DB_H
#define _RIPPLE_REFRESH_SHARDING2DB_H

/* refresh 线程控制器结构体 */
typedef struct RIPPLE_REFRESH_SHARDING2DB
{
    char*                                   name;
    char                                    *refresh_path;      /* refresh文件夹路径 */
    ripple_refresh_integratesyncstate       *syncstats;
} ripple_refresh_sharding2db;

void* ripple_refresh_sharding2db_work(void* args);

void ripple_refresh_sharding2db_free(void* args);

ripple_refresh_sharding2db* ripple_refresh_sharding2db_init(void);

#endif
