#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "queue/queue.h"
#include "task/task_slot.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"

refresh_table_syncstat* refresh_table_syncstat_init(void)
{
    refresh_table_syncstat* tablesyncstat = NULL;

    tablesyncstat = (refresh_table_syncstat*)rmalloc0(sizeof(refresh_table_syncstat));
    if (NULL == tablesyncstat)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(tablesyncstat, 0, '\0', sizeof(refresh_table_syncstat));
    tablesyncstat->cnt = 0;
    tablesyncstat->completecnt = 0;
    tablesyncstat->tablestat = REFRESH_TABLE_STAT_WAIT;
    tablesyncstat->oid = INVALIDOID;
    tablesyncstat->schema = NULL;
    tablesyncstat->table = NULL;
    tablesyncstat->stat = NULL;
    tablesyncstat->prev = NULL;
    tablesyncstat->next = NULL;

    return tablesyncstat;
}

void refresh_table_syncstat_schema_set(char* schema, refresh_table_syncstat* syncstat)
{
    if (NULL == schema || NULL == syncstat)
    {
        elog(RLOG_ERROR, "syncstat or schema is NULL");
    }

    syncstat->schema = rstrdup(schema);

    return;
}

void refresh_table_syncstat_table_set(char* table, refresh_table_syncstat* syncstat)
{
    if (NULL == table || NULL == syncstat)
    {
        elog(RLOG_ERROR, "syncstat or table is NULL");
    }

    syncstat->table = rstrdup(table);

    return;
}

void refresh_table_syncstat_oid_set(Oid oid, refresh_table_syncstat* syncstat)
{
    if (NULL == syncstat)
    {
        elog(RLOG_ERROR, "syncstat or table is NULL");
    }

    syncstat->oid = oid;

    return;
}

void refreshtablesyncstat_cnt_set(int cnt, refresh_table_syncstat* syncstat)
{
    if (NULL == syncstat)
    {
        elog(RLOG_ERROR, "syncstat is NULL");
    }

    syncstat->cnt = cnt;

    if (cnt != 0)
    {
        syncstat->stat = (int8_t*)rmalloc0(cnt * sizeof(int8_t));
        if (NULL == syncstat->stat)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(syncstat->stat, 0, '\0', cnt * sizeof(int8_t));
    }
}

/* traverse refresh directory based on tablesyncstats, generate queue */
bool refresh_table_syncstat_genqueue(refresh_table_syncstats* tablesyncstats, void* queue_ptr,
                                     char* refreshdir)
{
    DIR*                    compdir = NULL;
    struct dirent*          entry = NULL;
    StringInfo              path = NULL;
    refresh_table_syncstat* tables = NULL;

    path = makeStringInfo();

    /* traverse complete directory to generate tasks */
    for (tables = tablesyncstats->tablesyncall; tables != NULL; tables = tables->next)
    {
        refresh_table_syncstat* temp_table_state = NULL;

        resetStringInfo(path);

        appendStringInfo(path, "%s/%s/%s_%s/%s", refreshdir, REFRESH_REFRESH, tables->schema,
                         tables->table, REFRESH_COMPLETE);
        compdir = osal_open_dir(path->data);
        if (NULL == compdir)
        {
            continue;
        }

        while (NULL != (entry = osal_read_dir(compdir, path->data)))
        {
            refresh_table_sharding* table_shard = NULL;

            if (0 == strcmp(".", entry->d_name) || 0 == strcmp("..", entry->d_name))
            {
                continue;
            }

            table_shard = refresh_table_sharding_init();

            table_shard->sharding_condition = NULL;

            refresh_table_sharding_set_schema(table_shard, tables->schema);
            refresh_table_sharding_set_table(table_shard, tables->table);

            refresh_table_sharding_get_info_from_filename(entry->d_name, table_shard);

            if (refresh_table_check_in_syncing(tablesyncstats, table_shard, &temp_table_state))
            {
                /* free */
                refresh_table_sharding_free(table_shard);

                continue;
            }

            if (NULL == temp_table_state->stat)
            {
                refreshtablesyncstat_cnt_set(table_shard->shardings, temp_table_state);
            }

            if (0 == table_shard->shardings &&
                REFRESH_TABLE_STAT_WAIT != temp_table_state->tablestat)
            {
                /* free */
                refresh_table_sharding_free(table_shard);
                continue;
            }

            temp_table_state->tablestat = REFRESH_TABLE_STAT_SYNCING;

            if (temp_table_state->stat)
            {
                if (temp_table_state->stat[table_shard->sharding_no - 1] ==
                    REFRESH_TABLE_SYNCS_SHARD_STAT_INIT)
                {
                    temp_table_state->stat[table_shard->sharding_no - 1] =
                        REFRESH_TABLE_SYNCS_SHARD_STAT_SYNCING;
                }
            }

            elog(RLOG_DEBUG, "refresh monitor, queue gen: %s.%s %4d %4d", table_shard->schema,
                 table_shard->table, table_shard->shardings, table_shard->sharding_no);
            if (temp_table_state->stat)
            {
                elog(RLOG_DEBUG, "refresh monitor, stat [%d]: %d", table_shard->sharding_no - 1,
                     temp_table_state->stat[table_shard->sharding_no - 1]);
            }

            /* add to cache */
            queue_put((queue*)queue_ptr, (void*)table_shard);
        }

        osal_free_dir(compdir);
    }
    /* cleanup */
    deleteStringInfo(path);

    return true;
}

void refresh_table_syncstat_free(refresh_table_syncstat* tablesyncstat)
{
    refresh_table_syncstat* next = NULL;
    refresh_table_syncstat* current = tablesyncstat;

    if (NULL == tablesyncstat)
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
