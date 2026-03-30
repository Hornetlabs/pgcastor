#ifndef _REFRESH_SHARDING2DB_H
#define _REFRESH_SHARDING2DB_H

/* Refresh thread controller structure */
typedef struct REFRESH_SHARDING2DB
{
    char*                       name;
    char*                       refresh_path; /* Refresh folder path */
    refresh_integratesyncstate* syncstats;
} refresh_sharding2db;

void* refresh_sharding2db_work(void* args);

void refresh_sharding2db_free(void* args);

refresh_sharding2db* refresh_sharding2db_init(void);

#endif
