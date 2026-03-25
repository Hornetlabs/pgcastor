#ifndef _REFRESH_TABLE_SHARDING_H
#define _REFRESH_TABLE_SHARDING_H

typedef struct REFRESH_TABLE_CONDITION
{
    uint32 left_condition;
    uint32 right_condition;
} refresh_table_condition;

typedef struct REFRESH_TABLE_SHARDING
{
    char*                    schema;
    char*                    table;
    int                      shardings;
    int                      sharding_no;
    refresh_table_condition* sharding_condition;
} refresh_table_sharding;

extern refresh_table_sharding* refresh_table_sharding_init(void);
extern void refresh_table_sharding_set_schema(refresh_table_sharding* shard, char* table);
extern void refresh_table_sharding_set_table(refresh_table_sharding* shard, char* table);
extern void refresh_table_sharding_set_shardings(refresh_table_sharding* shard, int num);
extern void refresh_table_sharding_set_shardno(refresh_table_sharding* shard, int num);
extern void refresh_table_sharding_set_condition(refresh_table_sharding*  shard,
                                                 refresh_table_condition* cond);
extern refresh_table_condition* refresh_table_sharding_condition_init(void);
extern void                     refresh_table_sharding_get_info_from_filename(char*                   filename,
                                                                              refresh_table_sharding* table_shard);
extern void                     refresh_table_sharding_free(refresh_table_sharding* shard);
extern void                     refresh_table_sharding_queuefree(void* data);

#endif
