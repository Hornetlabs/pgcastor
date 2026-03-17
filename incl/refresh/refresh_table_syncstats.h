#ifndef _REFRESH_TABLE_SYNCSTATS_H
#define _REFRESH_TABLE_SYNCSTATS_H

typedef enum REFRESH_TABLE_SYNCS_SHARD_STAT
{
    REFRESH_TABLE_SYNCS_SHARD_STAT_INIT = 0x00,
    REFRESH_TABLE_SYNCS_SHARD_STAT_SYNCING = 0x01,
    REFRESH_TABLE_SYNCS_SHARD_STAT_DONE = 0x02
} refresh_table_syncs_shard_stat;

typedef enum REFRESH_TABLE_STAT
{
    REFRESH_TABLE_STAT_WAIT = 0x00,
    REFRESH_TABLE_STAT_SYNCING = 0x01,
    REFRESH_TABLE_STAT_DONE = 0x02
} refresh_table_stat;

typedef struct REFRESH_TABLE_SYNCSTAT
{
    int         cnt;                /* 分片总个数 */
    int         completecnt;        /* 已完成的数量 */
    int         tablestat;          /* 表的同步状态 */
    Oid         oid;                /* 表的同步oid */
    char        *schema;
    char        *table;
    int8_t      *stat;              /* 每个任务状态 */
    struct REFRESH_TABLE_SYNCSTAT    *prev;
    struct REFRESH_TABLE_SYNCSTAT    *next;
} refresh_table_syncstat;

/*
 * refresh_table_syncstats 工作原理:
 *  在 初始化时会根据同步表生成 tablesyncall 和 tablesyncing，其中：
 *  tablesyncall 用于标识所有同步的表, tablesyncing 用于标识正在同步的表.
 *  不同的是: tablesyncing 含有状态的变化，初始化时为 REFRESH_TABLE_STAT_WAIT, 当该表中的一个分片被扫描到会设置tablesyncing->stat 中该分片对应的 bit 位状态为 REFRESH_TABLE_STAT_SYNCING
 *  在同步过程中, 当 某个表中一个分片同步完成后, 会设置 tablesyncing->stat 中该分片对应的 bit 位状态为 REFRESH_TABLE_STAT_DONE
 *  当该表中所有的分片都同步完成后, 会将该表在 tablesyncing 中移除并在 tablesyncdone 中增加该表
 * 
 * 当 tablesyncing 位空时就证明存量的同步做完了!!!
 * 
*/
typedef struct REFRESH_TABLE_SYNCSTATS
{
    pthread_mutex_t                 tablesynclock;
    refresh_table_syncstat   *tablesyncdone;     /* 已经完成的表 */
    refresh_table_syncstat   *tablesyncing;      /* 待同步或同步中的表 */
    refresh_table_syncstat   *tablesyncall;      /* 所有同步的表 */
} refresh_table_syncstats;

refresh_table_syncstat* refresh_table_syncstat_init(void);

void refresh_table_syncstat_schema_set(char* schema, refresh_table_syncstat* syncstat);

void refresh_table_syncstat_table_set(char* table, refresh_table_syncstat* syncstat);

void refresh_table_syncstat_oid_set(Oid oid, refresh_table_syncstat* syncstat);

void refreshtablesyncstat_cnt_set(int cnt, refresh_table_syncstat* syncstat);

bool refresh_table_syncstat_genqueue(refresh_table_syncstats* tablesyncstats, void* queue, char* refreshdir);

void refresh_table_syncstat_free(refresh_table_syncstat* tablesyncstat);

refresh_table_syncstats* refresh_table_syncstats_init(void);

bool refresh_table_syncstats_lock(refresh_table_syncstats* tablesyncstats);

bool refresh_table_syncstats_unlock(refresh_table_syncstats* tablesyncstats);

void refresh_table_syncstats_tablesyncing_set(refresh_tables* refreshtables, refresh_table_syncstats* tablesyncstats);

void refresh_table_syncstats_tablesyncall_set(refresh_tables* refreshtables, refresh_table_syncstats* tablesyncstats);

/* 根据tablesyncing生成 tablesyncall*/
void refresh_table_syncstats_tablesyncing2tablesyncall(refresh_table_syncstats* tablesyncstats);

bool refreshtablesyncstats_markstatdone(refresh_table_sharding* tablesharding, 
                                                refresh_table_syncstats* tablesyncstats,
                                                char* refreshdir);

void refresh_table_syncstats_free(refresh_table_syncstats* tablesyncstats);

bool refresh_table_check_in_syncing(refresh_table_syncstats *tablesyncstats,
                                           refresh_table_sharding *table_shard,
                                           refresh_table_syncstat **table_stat);

bool refresh_table_syncstats_compare(refresh_table_syncstats* tablesA,
                                           refresh_table_syncstats* tablesB);

bool refresh_table_syncstats_truncatetable_fromsyncstats(refresh_table_syncstats* tablesyncstats, void* in_conn);

bool refresh_table_syncstats_write(refresh_table_syncstat *stats, char *refresh_path);

bool refresh_table_syncstats_read(refresh_table_syncstat *stats, char *refresh_path);

void refresh_table_syncstats_tablesyncall_setfromfile(refresh_tables* refreshtables, refresh_table_syncstats* tablesyncstats, char* refresh_path);

void refresh_table_syncstats_tablesyncing_setfromfile(refresh_tables* refreshtables, refresh_table_syncstats* tablesyncstats, char* refresh_path);

refresh_tables* refresh_table_syncstats_tablesyncing2tables(refresh_table_syncstats* tablesyncstats);

#endif
