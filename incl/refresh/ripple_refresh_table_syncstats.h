#ifndef _RIPPLE_REFRESH_TABLE_SYNCSTATS_H
#define _RIPPLE_REFRESH_TABLE_SYNCSTATS_H

typedef enum RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT
{
    RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_INIT = 0x00,
    RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_SYNCING = 0x01,
    RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_DONE = 0x02
} ripple_refresh_table_syncs_shard_stat;

typedef enum RIPPLE_REFRESH_TABLE_STAT
{
    RIPPLE_REFRESH_TABLE_STAT_WAIT = 0x00,
    RIPPLE_REFRESH_TABLE_STAT_SYNCING = 0x01,
    RIPPLE_REFRESH_TABLE_STAT_DONE = 0x02
} ripple_refresh_table_stat;

typedef struct RIPPLE_REFRESH_TABLE_SYNCSTAT
{
    int         cnt;                /* 分片总个数 */
    int         completecnt;        /* 已完成的数量 */
    int         tablestat;          /* 表的同步状态 */
    Oid         oid;                /* 表的同步oid */
    char        *schema;
    char        *table;
    int8_t      *stat;              /* 每个任务状态 */
    struct RIPPLE_REFRESH_TABLE_SYNCSTAT    *prev;
    struct RIPPLE_REFRESH_TABLE_SYNCSTAT    *next;
} ripple_refresh_table_syncstat;

/*
 * ripple_refresh_table_syncstats 工作原理:
 *  在 初始化时会根据同步表生成 tablesyncall 和 tablesyncing，其中：
 *  tablesyncall 用于标识所有同步的表, tablesyncing 用于标识正在同步的表.
 *  不同的是: tablesyncing 含有状态的变化，初始化时为 RIPPLE_REFRESH_TABLE_STAT_WAIT, 当该表中的一个分片被扫描到会设置tablesyncing->stat 中该分片对应的 bit 位状态为 RIPPLE_REFRESH_TABLE_STAT_SYNCING
 *  在同步过程中, 当 某个表中一个分片同步完成后, 会设置 tablesyncing->stat 中该分片对应的 bit 位状态为 RIPPLE_REFRESH_TABLE_STAT_DONE
 *  当该表中所有的分片都同步完成后, 会将该表在 tablesyncing 中移除并在 tablesyncdone 中增加该表
 * 
 * 当 tablesyncing 位空时就证明存量的同步做完了!!!
 * 
*/
typedef struct RIPPLE_REFRESH_TABLE_SYNCSTATS
{
    pthread_mutex_t                 tablesynclock;
    ripple_refresh_table_syncstat   *tablesyncdone;     /* 已经完成的表 */
    ripple_refresh_table_syncstat   *tablesyncing;      /* 待同步或同步中的表 */
    ripple_refresh_table_syncstat   *tablesyncall;      /* 所有同步的表 */
} ripple_refresh_table_syncstats;

ripple_refresh_table_syncstat* ripple_refresh_table_syncstat_init(void);

void ripple_refresh_table_syncstat_schema_set(char* schema, ripple_refresh_table_syncstat* syncstat);

void ripple_refresh_table_syncstat_table_set(char* table, ripple_refresh_table_syncstat* syncstat);

void ripple_refresh_table_syncstat_oid_set(Oid oid, ripple_refresh_table_syncstat* syncstat);

void ripple_refreshtablesyncstat_cnt_set(int cnt, ripple_refresh_table_syncstat* syncstat);

bool ripple_refresh_table_syncstat_genqueue(ripple_refresh_table_syncstats* tablesyncstats, void* queue, char* refreshdir);

void ripple_refresh_table_syncstat_free(ripple_refresh_table_syncstat* tablesyncstat);

ripple_refresh_table_syncstats* ripple_refresh_table_syncstats_init(void);

bool ripple_refresh_table_syncstats_lock(ripple_refresh_table_syncstats* tablesyncstats);

bool ripple_refresh_table_syncstats_unlock(ripple_refresh_table_syncstats* tablesyncstats);

void ripple_refresh_table_syncstats_tablesyncing_set(ripple_refresh_tables* refreshtables, ripple_refresh_table_syncstats* tablesyncstats);

void ripple_refresh_table_syncstats_tablesyncall_set(ripple_refresh_tables* refreshtables, ripple_refresh_table_syncstats* tablesyncstats);

/* 根据tablesyncing生成 tablesyncall*/
void ripple_refresh_table_syncstats_tablesyncing2tablesyncall(ripple_refresh_table_syncstats* tablesyncstats);

bool ripple_refreshtablesyncstats_markstatdone(ripple_refresh_table_sharding* tablesharding, 
                                                ripple_refresh_table_syncstats* tablesyncstats,
                                                char* refreshdir);

void ripple_refresh_table_syncstats_free(ripple_refresh_table_syncstats* tablesyncstats);

bool ripple_refresh_table_check_in_syncing(ripple_refresh_table_syncstats *tablesyncstats,
                                           ripple_refresh_table_sharding *table_shard,
                                           ripple_refresh_table_syncstat **table_stat);

bool ripple_refresh_table_syncstats_compare(ripple_refresh_table_syncstats* tablesA,
                                           ripple_refresh_table_syncstats* tablesB);

bool ripple_refresh_table_syncstats_truncatetable_fromsyncstats(ripple_refresh_table_syncstats* tablesyncstats, void* in_conn);

bool ripple_refresh_table_syncstats_write(ripple_refresh_table_syncstat *stats, char *refresh_path);

bool ripple_refresh_table_syncstats_read(ripple_refresh_table_syncstat *stats, char *refresh_path);

void ripple_refresh_table_syncstats_tablesyncall_setfromfile(ripple_refresh_tables* refreshtables, ripple_refresh_table_syncstats* tablesyncstats, char* refresh_path);

void ripple_refresh_table_syncstats_tablesyncing_setfromfile(ripple_refresh_tables* refreshtables, ripple_refresh_table_syncstats* tablesyncstats, char* refresh_path);

ripple_refresh_tables* ripple_refresh_table_syncstats_tablesyncing2tables(ripple_refresh_table_syncstats* tablesyncstats);

#endif
