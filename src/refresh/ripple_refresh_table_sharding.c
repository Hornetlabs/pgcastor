#include "ripple_app_incl.h"
#include "utils/guc/guc.h"
#include "utils/string/stringinfo.h"
#include "port/thread/ripple_thread.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "storage/ripple_file_buffer.h"
#include "works/dyworks/ripple_dyworks.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"
#include "refresh/p2csharding/ripple_refresh_p2csharding.h"

ripple_refresh_table_sharding *ripple_refresh_table_sharding_init(void)
{
    ripple_refresh_table_sharding *shard = NULL;

    shard = rmalloc0(sizeof(ripple_refresh_table_sharding));
    if (!shard)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(shard, 0, 0, sizeof(ripple_refresh_table_sharding));

    return shard;
}

void ripple_refresh_table_sharding_set_schema(ripple_refresh_table_sharding *shard, char *schema)
{
    if (!shard || !schema)
    {
        elog(RLOG_ERROR, "shard or schema is NULL");
    }
    shard->schema = rstrdup(schema);
}

void ripple_refresh_table_sharding_set_table(ripple_refresh_table_sharding *shard, char *table)
{
    if (!shard || !table)
    {
        elog(RLOG_ERROR, "shard or table is NULL");
    }
    shard->table = rstrdup(table);
}

void ripple_refresh_table_sharding_set_shardings(ripple_refresh_table_sharding *shard, int num)
{
    if (!shard)
    {
        elog(RLOG_ERROR, "shard is NULL");
    }
    shard->shardings = num;
}

void ripple_refresh_table_sharding_set_shardno(ripple_refresh_table_sharding *shard, int num)
{
    if (!shard)
    {
        elog(RLOG_ERROR, "shard is NULL");
    }
    shard->sharding_no = num;
}

void ripple_refresh_table_sharding_set_condition(ripple_refresh_table_sharding *shard, ripple_refresh_table_condition *cond)
{
    if (!shard)
    {
        elog(RLOG_ERROR, "shard is NULL");
    }
    shard->sharding_condition = cond;
}

ripple_refresh_table_condition *ripple_refresh_table_sharding_condition_init(void)
{
    ripple_refresh_table_condition *cond = NULL;

    cond = rmalloc0(sizeof(ripple_refresh_table_condition));
    cond->left_condition = 0;
    cond->right_condition = 0;

    return cond;
}

void ripple_refresh_table_sharding_get_info_from_filename(char *filename, ripple_refresh_table_sharding *table_shard)
{
    char *ptr_left = filename;
    char *ptr_right = NULL;
    char num_char[64] = {'\0'};
    int len = 0;

    /* 计算 schema name 长度 */
    len += strlen(table_shard->schema);

    /* 计算 table name 长度 */
    len += strlen(table_shard->table);

    /* 跳过schema和table */
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

void ripple_refresh_table_sharding_free(ripple_refresh_table_sharding *table_shard)
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

void ripple_refresh_table_sharding_queuefree(void* data)
{
    ripple_refresh_table_sharding *table_shard = NULL;

    if (data)
    {
        table_shard = (ripple_refresh_table_sharding *)data;
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
