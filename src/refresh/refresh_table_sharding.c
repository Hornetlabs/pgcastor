#include "app_incl.h"
#include "utils/guc/guc.h"
#include "utils/string/stringinfo.h"
#include "port/thread/thread.h"
#include "queue/queue.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "storage/file_buffer.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "refresh/p2csharding/refresh_p2csharding.h"

refresh_table_sharding* refresh_table_sharding_init(void)
{
    refresh_table_sharding* shard = NULL;

    shard = rmalloc0(sizeof(refresh_table_sharding));
    if (!shard)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(shard, 0, 0, sizeof(refresh_table_sharding));

    return shard;
}

void refresh_table_sharding_set_schema(refresh_table_sharding* shard, char* schema)
{
    if (!shard || !schema)
    {
        elog(RLOG_ERROR, "shard or schema is NULL");
    }
    shard->schema = rstrdup(schema);
}

void refresh_table_sharding_set_table(refresh_table_sharding* shard, char* table)
{
    if (!shard || !table)
    {
        elog(RLOG_ERROR, "shard or table is NULL");
    }
    shard->table = rstrdup(table);
}

void refresh_table_sharding_set_shardings(refresh_table_sharding* shard, int num)
{
    if (!shard)
    {
        elog(RLOG_ERROR, "shard is NULL");
    }
    shard->shardings = num;
}

void refresh_table_sharding_set_shardno(refresh_table_sharding* shard, int num)
{
    if (!shard)
    {
        elog(RLOG_ERROR, "shard is NULL");
    }
    shard->sharding_no = num;
}

void refresh_table_sharding_set_condition(refresh_table_sharding* shard, refresh_table_condition* cond)
{
    if (!shard)
    {
        elog(RLOG_ERROR, "shard is NULL");
    }
    shard->sharding_condition = cond;
}

refresh_table_condition* refresh_table_sharding_condition_init(void)
{
    refresh_table_condition* cond = NULL;

    cond = rmalloc0(sizeof(refresh_table_condition));
    cond->left_condition = 0;
    cond->right_condition = 0;

    return cond;
}

void refresh_table_sharding_get_info_from_filename(char* filename, refresh_table_sharding* table_shard)
{
    char* ptr_left = filename;
    char* ptr_right = NULL;
    char  num_char[64] = {'\0'};
    int   len = 0;

    /* calculate schema name length */
    len += strlen(table_shard->schema);

    /* calculate table name length */
    len += strlen(table_shard->table);

    /* skip schema and table */
    ptr_left = ptr_left + len + 2;

    /* shards */
    ptr_right = strstr(ptr_left, "_");
    if (ptr_right == NULL)
    {
        elog(RLOG_ERROR, "invalid file name: %s", filename);
    }
    len = ptr_right - ptr_left;
    rmemcpy1(num_char, 0, ptr_left, len);
    num_char[len] = '\0';
    table_shard->shardings = atoi(num_char);
    ptr_left = ptr_right + 1;

    /* shardno */
    len = strlen(ptr_left);
    rmemcpy1(num_char, 0, ptr_left, len);
    num_char[len] = '\0';
    table_shard->sharding_no = atoi(num_char);
}

void refresh_table_sharding_free(refresh_table_sharding* table_shard)
{
    if (table_shard)
    {
        if (table_shard->schema)
        {
            rfree(table_shard->schema);
        }

        if (table_shard->table)
        {
            rfree(table_shard->table);
        }

        if (table_shard->sharding_condition)
        {
            rfree(table_shard->sharding_condition);
        }

        rfree(table_shard);
    }
}

void refresh_table_sharding_queuefree(void* data)
{
    refresh_table_sharding* table_shard = NULL;

    if (data)
    {
        table_shard = (refresh_table_sharding*)data;
        if (table_shard->schema)
        {
            rfree(table_shard->schema);
        }

        if (table_shard->table)
        {
            rfree(table_shard->table);
        }

        if (table_shard->sharding_condition)
        {
            rfree(table_shard->sharding_condition);
        }

        rfree(table_shard);
    }

    return;
}
