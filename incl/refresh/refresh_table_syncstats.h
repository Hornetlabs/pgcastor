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
    int                            cnt;         /* total shard count */
    int                            completecnt; /* completed count */
    int                            tablestat;   /* table sync status */
    Oid                            oid;         /* table sync oid */
    char*                          schema;
    char*                          table;
    int8_t*                        stat; /* per-shard task status */
    struct REFRESH_TABLE_SYNCSTAT* prev;
    struct REFRESH_TABLE_SYNCSTAT* next;
} refresh_table_syncstat;

/*
 * refresh_table_syncstats initialization generates tablesyncall and tablesyncing
 * tablesyncall tracks all synced tables, tablesyncing tracks tables being synced
 * When tablesyncing contains status changes during initialization, set REFRESH_TABLE_STAT_WAIT
 * When a table has one shard, set tablesyncing->stat for that shard's bit position to
 * REFRESH_TABLE_STAT_SYNCING When a table shard completes sync, set tablesyncing->stat for that
 * shard's bit position to REFRESH_TABLE_STAT_DONE When all shards complete sync, remove table from
 * tablesyncing and add to tablesyncdone When tablesyncing is empty, sync is complete
 */
typedef struct REFRESH_TABLE_SYNCSTATS
{
    pthread_mutex_t         tablesynclock;
    refresh_table_syncstat* tablesyncdone; /* already complete table */
    refresh_table_syncstat* tablesyncing;  /* pendingsync syncin table */
    refresh_table_syncstat* tablesyncall;  /* hassync table */
} refresh_table_syncstats;

refresh_table_syncstat* refresh_table_syncstat_init(void);

void refresh_table_syncstat_schema_set(char* schema, refresh_table_syncstat* syncstat);

void refresh_table_syncstat_table_set(char* table, refresh_table_syncstat* syncstat);

void refresh_table_syncstat_oid_set(Oid oid, refresh_table_syncstat* syncstat);

void refreshtablesyncstat_cnt_set(int cnt, refresh_table_syncstat* syncstat);

bool refresh_table_syncstat_genqueue(refresh_table_syncstats* tablesyncstats, void* queue,
                                     char* refreshdir);

void refresh_table_syncstat_free(refresh_table_syncstat* tablesyncstat);

refresh_table_syncstats* refresh_table_syncstats_init(void);

bool refresh_table_syncstats_lock(refresh_table_syncstats* tablesyncstats);

bool refresh_table_syncstats_unlock(refresh_table_syncstats* tablesyncstats);

void refresh_table_syncstats_tablesyncing_set(refresh_tables*          refreshtables,
                                              refresh_table_syncstats* tablesyncstats);

void refresh_table_syncstats_tablesyncall_set(refresh_tables*          refreshtables,
                                              refresh_table_syncstats* tablesyncstats);

/* according totablesyncinggenerate tablesyncall*/
void refresh_table_syncstats_tablesyncing2tablesyncall(refresh_table_syncstats* tablesyncstats);

bool refreshtablesyncstats_markstatdone(refresh_table_sharding*  tablesharding,
                                        refresh_table_syncstats* tablesyncstats, char* refreshdir);

void refresh_table_syncstats_free(refresh_table_syncstats* tablesyncstats);

bool refresh_table_check_in_syncing(refresh_table_syncstats* tablesyncstats,
                                    refresh_table_sharding*  table_shard,
                                    refresh_table_syncstat** table_stat);

bool refresh_table_syncstats_compare(refresh_table_syncstats* tablesA,
                                     refresh_table_syncstats* tablesB);

bool refresh_table_syncstats_truncatetable_fromsyncstats(refresh_table_syncstats* tablesyncstats,
                                                         void*                    in_conn);

bool refresh_table_syncstats_write(refresh_table_syncstat* stats, char* refresh_path);

bool refresh_table_syncstats_read(refresh_table_syncstat* stats, char* refresh_path);

void refresh_table_syncstats_tablesyncall_setfromfile(refresh_tables*          refreshtables,
                                                      refresh_table_syncstats* tablesyncstats,
                                                      char*                    refresh_path);

void refresh_table_syncstats_tablesyncing_setfromfile(refresh_tables*          refreshtables,
                                                      refresh_table_syncstats* tablesyncstats,
                                                      char*                    refresh_path);

refresh_tables* refresh_table_syncstats_tablesyncing2tables(
    refresh_table_syncstats* tablesyncstats);

#endif
