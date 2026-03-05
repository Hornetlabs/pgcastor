#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"

ripple_refresh_table_syncstat* ripple_refresh_table_syncstat_init(void)
{
    ripple_refresh_table_syncstat* tablesyncstat = NULL;

    tablesyncstat = (ripple_refresh_table_syncstat*)rmalloc0(sizeof(ripple_refresh_table_syncstat));
    if (NULL == tablesyncstat)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tablesyncstat, 0, '\0', sizeof(ripple_refresh_table_syncstat));
    tablesyncstat->cnt = 0;
    tablesyncstat->completecnt = 0;
    tablesyncstat->tablestat = RIPPLE_REFRESH_TABLE_STAT_WAIT;
    tablesyncstat->oid = InvalidOid;
    tablesyncstat->schema = NULL;
    tablesyncstat->table = NULL;
    tablesyncstat->stat = NULL;
    tablesyncstat->prev = NULL;
    tablesyncstat->next = NULL;

    return tablesyncstat;
}

void ripple_refresh_table_syncstat_schema_set(char* schema, ripple_refresh_table_syncstat* syncstat)
{
    if (NULL == schema || NULL == syncstat)
    {
        elog(RLOG_ERROR, "syncstat or schema is NULL");
    }

    syncstat->schema = rstrdup(schema);

    return;

}

void ripple_refresh_table_syncstat_table_set(char* table, ripple_refresh_table_syncstat* syncstat)
{
    if (NULL == table || NULL == syncstat)
    {
        elog(RLOG_ERROR, "syncstat or table is NULL");
    }
    
    syncstat->table = rstrdup(table);

    return;
}

void ripple_refresh_table_syncstat_oid_set(Oid oid, ripple_refresh_table_syncstat* syncstat)
{
    if (NULL == syncstat)
    {
        elog(RLOG_ERROR, "syncstat or table is NULL");
    }
    
    syncstat->oid = oid;

    return;
}

void ripple_refreshtablesyncstat_cnt_set(int cnt, ripple_refresh_table_syncstat* syncstat)
{
    if (NULL == syncstat)
    {
        elog(RLOG_ERROR, "syncstat is NULL");
    }

    syncstat->cnt = cnt;

    if (cnt !=0 )
    {
        syncstat->stat = (int8_t*)rmalloc0(cnt * sizeof(int8_t));
        if (NULL == syncstat->stat)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(syncstat->stat, 0, '\0', cnt * sizeof(int8_t));
    }
}

/* 遍历refresh目录根据tablesyncstats, 生成queue */
bool ripple_refresh_table_syncstat_genqueue(ripple_refresh_table_syncstats* tablesyncstats, void* queue, char* refreshdir)
{
    DIR* compdir = NULL;
    struct dirent *entry = NULL;
    StringInfo  path = NULL;
    ripple_refresh_table_syncstat* tables = NULL;

    path = makeStringInfo();

    /* 遍历complete目录生成任务 */
    for (tables = tablesyncstats->tablesyncall; tables != NULL; tables = tables->next)
    {
        ripple_refresh_table_syncstat *temp_table_state = NULL;

        resetStringInfo(path);

        appendStringInfo(path, "%s/%s/%s_%s/%s", refreshdir,
                                                 RIPPLE_REFRESH_REFRESH,
                                                 tables->schema,
                                                 tables->table,
                                                 RIPPLE_REFRESH_COMPLETE);
        compdir = OpenDir(path->data);
        if(NULL == compdir)
        {
            continue;
        }

        while (NULL != (entry = ReadDir(compdir, path->data)))
        {
            ripple_refresh_table_sharding *table_shard = NULL;

            if (0 == strcmp(".", entry->d_name)
            || 0 == strcmp("..", entry->d_name))
            {
                continue;
            }

            table_shard = ripple_refresh_table_sharding_init();

            table_shard->sharding_condition = NULL;

            ripple_refresh_table_sharding_set_schema(table_shard, tables->schema);
            ripple_refresh_table_sharding_set_table(table_shard, tables->table);

            ripple_refresh_table_sharding_get_info_from_filename(entry->d_name, table_shard);

            if (ripple_refresh_table_check_in_syncing(tablesyncstats, table_shard, &temp_table_state))
            {
                /* 释放 */
                ripple_refresh_table_sharding_free(table_shard);

                continue;
            }

            if (NULL == temp_table_state->stat)
            {
                ripple_refreshtablesyncstat_cnt_set(table_shard->shardings, temp_table_state);
            }

            if (0 == table_shard->shardings 
                && RIPPLE_REFRESH_TABLE_STAT_WAIT != temp_table_state->tablestat)
            {
                /* 释放 */
                ripple_refresh_table_sharding_free(table_shard);
                continue;
            }

            temp_table_state->tablestat = RIPPLE_REFRESH_TABLE_STAT_SYNCING;

            if (temp_table_state->stat)
            {
                if (temp_table_state->stat[table_shard->sharding_no - 1] == RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_INIT)
                {
                    temp_table_state->stat[table_shard->sharding_no - 1] = RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_SYNCING;
                }
            }

            elog(RLOG_DEBUG, "pump refresh monitor, queue gen: %s.%s %4d %4d",
                                                               table_shard->schema,
                                                               table_shard->table,
                                                               table_shard->shardings,
                                                               table_shard->sharding_no);
            if (temp_table_state->stat)
            {
                elog(RLOG_DEBUG, "pump refresh monitor, stat [%d]: %d",
                                                               table_shard->sharding_no - 1,
                                                               temp_table_state->stat[table_shard->sharding_no - 1]);
            }

            /* 添加到缓存中 */
            ripple_queue_put((ripple_queue*)queue, (void *)table_shard);
        }

        FreeDir(compdir);

    }
    /* 清理工作 */
    deleteStringInfo(path);

    return true;
}

void ripple_refresh_table_syncstat_free(ripple_refresh_table_syncstat* tablesyncstat)
{
    ripple_refresh_table_syncstat *next = NULL;
    ripple_refresh_table_syncstat *current = tablesyncstat;

    if ( NULL == tablesyncstat)
    {
        return;
    }

    while (current)
    {
        next = current->next;

        if (current->schema)
        {
            rfree(current->schema);
        }

        if (current->table)
        {
            rfree(current->table);
        }

        if (current->stat)
        {
            rfree(current->stat);
        }

        current->prev = NULL;
        current->next = NULL;
        rfree(current);
        current = next;
    }
   tablesyncstat = NULL;
   return;
}
