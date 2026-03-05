#ifndef _RIPPLE_REFRESH_TABLE_SHARDING_H
#define _RIPPLE_REFRESH_TABLE_SHARDING_H

typedef struct RIPPLE_REFRESH_TABLE_CONDITION
{
    uint32 left_condition;
    uint32 right_condition;
} ripple_refresh_table_condition;

typedef struct RIPPLE_REFRESH_TABLE_SHARDING
{
    char *schema;
    char *table;
    int   shardings;
    int   sharding_no;
    ripple_refresh_table_condition *sharding_condition;
} ripple_refresh_table_sharding;

extern ripple_refresh_table_sharding *ripple_refresh_table_sharding_init(void);
extern void ripple_refresh_table_sharding_set_schema(ripple_refresh_table_sharding *shard, char *table);
extern void ripple_refresh_table_sharding_set_table(ripple_refresh_table_sharding *shard, char *table);
extern void ripple_refresh_table_sharding_set_shardings(ripple_refresh_table_sharding *shard, int num);
extern void ripple_refresh_table_sharding_set_shardno(ripple_refresh_table_sharding *shard, int num);
extern void ripple_refresh_table_sharding_set_condition(ripple_refresh_table_sharding *shard, ripple_refresh_table_condition *cond);
extern ripple_refresh_table_condition *ripple_refresh_table_sharding_condition_init(void);
extern void ripple_refresh_table_sharding_get_info_from_filename(char *filename, ripple_refresh_table_sharding *table_shard);
extern void ripple_refresh_table_sharding_free(ripple_refresh_table_sharding *shard);
extern void ripple_refresh_table_sharding_queuefree(void* data);

#endif
