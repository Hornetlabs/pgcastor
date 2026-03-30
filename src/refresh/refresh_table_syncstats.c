#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/string/stringinfo.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/conn.h"
#include "port/thread/thread.h"
#include "queue/queue.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"

refresh_table_syncstats* refresh_table_syncstats_init(void)
{
    refresh_table_syncstats* tablesyncstats = NULL;

    tablesyncstats = (refresh_table_syncstats*)rmalloc0(sizeof(refresh_table_syncstats));
    if (NULL == tablesyncstats)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(tablesyncstats, 0, '\0', sizeof(refresh_table_syncstats));

    /* lock initialization */
    osal_thread_mutex_init(&tablesyncstats->tablesynclock, NULL);
    tablesyncstats->tablesyncall = NULL;
    tablesyncstats->tablesyncdone = NULL;
    tablesyncstats->tablesyncing = NULL;

    return tablesyncstats;
}

bool refresh_table_syncstats_lock(refresh_table_syncstats* tablesyncstats)
{
    int iret = 0;
    if (NULL == tablesyncstats)
    {
        return false;
    }

    iret = osal_thread_lock(&tablesyncstats->tablesynclock);
    if (0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    return true;
}

bool refresh_table_syncstats_unlock(refresh_table_syncstats* tablesyncstats)
{
    int iret = 0;
    if (NULL == tablesyncstats)
    {
        return false;
    }

    iret = osal_thread_unlock(&tablesyncstats->tablesynclock);
    if (0 != iret)
    {
        elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
    }

    return true;
}

void refresh_table_syncstats_tablesyncing_set(refresh_tables* refreshtables, refresh_table_syncstats* tablesyncstats)
{
    refresh_table* cur_table = NULL;

    if (!refreshtables)
    {
        elog(RLOG_WARNING, "refreshtables is NULL");
        tablesyncstats->tablesyncing = NULL;
        return;
    }

    if (!tablesyncstats || 0 == refreshtables->cnt)
    {
        tablesyncstats->tablesyncing = NULL;
        return;
    }

    cur_table = refreshtables->tables;

    refresh_table_syncstats_lock(tablesyncstats);

    while (cur_table)
    {
        /* create a new sync status node */
        refresh_table_syncstat* new_syncstat = refresh_table_syncstat_init();

        /* copy table information */
        refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        /* insert into tablesyncall */
        new_syncstat->next = tablesyncstats->tablesyncing;
        if (tablesyncstats->tablesyncing)
        {
            tablesyncstats->tablesyncing->prev = new_syncstat;
        }

        tablesyncstats->tablesyncing = new_syncstat;

        cur_table = cur_table->next;
    }

    refresh_table_syncstats_unlock(tablesyncstats);
    return;
}

void refresh_table_syncstats_tablesyncall_set(refresh_tables* refreshtables, refresh_table_syncstats* tablesyncstats)
{
    refresh_table* cur_table = NULL;

    if (!refreshtables)
    {
        elog(RLOG_WARNING, "refreshtables is NULL");
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    if (!tablesyncstats || 0 == refreshtables->cnt)
    {
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    cur_table = refreshtables->tables;

    refresh_table_syncstats_lock(tablesyncstats);

    while (cur_table)
    {
        /* create a new sync status node */
        refresh_table_syncstat* new_syncstat = refresh_table_syncstat_init();

        /* copy table information */
        refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        /* insert into tablesyncall */
        new_syncstat->next = tablesyncstats->tablesyncall;
        if (tablesyncstats->tablesyncall)
        {
            tablesyncstats->tablesyncall->prev = new_syncstat;
        }

        tablesyncstats->tablesyncall = new_syncstat;

        cur_table = cur_table->next;
    }

    refresh_table_syncstats_unlock(tablesyncstats);
    return;
}

void refresh_table_syncstats_tablesyncing2tablesyncall(refresh_table_syncstats* tablesyncstats)
{
    refresh_table_syncstat* cur_table = NULL;

    if (NULL == tablesyncstats)
    {
        elog(RLOG_WARNING, "tablesyncstats is NULL");
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    if (!tablesyncstats->tablesyncing)
    {
        elog(RLOG_INFO, "tablesyncstats refreshtables is NULL");
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    refresh_table_syncstats_lock(tablesyncstats);

    cur_table = tablesyncstats->tablesyncing;

    while (cur_table)
    {
        /* create a new sync status node */
        refresh_table_syncstat* new_syncstat = refresh_table_syncstat_init();

        /* copy table information */
        refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        /* insert into tablesyncall */
        new_syncstat->next = tablesyncstats->tablesyncall;
        if (tablesyncstats->tablesyncall)
        {
            tablesyncstats->tablesyncall->prev = new_syncstat;
        }

        tablesyncstats->tablesyncall = new_syncstat;

        cur_table = cur_table->next;
    }

    refresh_table_syncstats_unlock(tablesyncstats);
    return;
}

bool refreshtablesyncstats_markstatdone(refresh_table_sharding*  tablesharding,
                                        refresh_table_syncstats* tablesyncstats,
                                        char*                    refreshdir)
{
    refresh_table_syncstat* current_table = NULL;

    if (!tablesharding || !tablesyncstats || !tablesyncstats->tablesyncing)
    {
        return false;
    }

    refresh_table_syncstats_lock(tablesyncstats);
    current_table = tablesyncstats->tablesyncing;

    /* traverse complete directory to generate tasks */
    while (current_table)
    {
        if (0 == strcmp(current_table->schema, tablesharding->schema) &&
            0 == strcmp(current_table->table, tablesharding->table))
        {
            if (0 != tablesharding->shardings)
            {
                current_table->completecnt += 1;
                current_table->stat[tablesharding->sharding_no - 1] = REFRESH_TABLE_SYNCS_SHARD_STAT_DONE;
            }

            elog(RLOG_DEBUG,
                 "refresh worker, queue: %s.%s %4d %4d, mark done",
                 tablesharding->schema,
                 tablesharding->table,
                 tablesharding->shardings,
                 tablesharding->sharding_no);

            if (current_table->completecnt == current_table->cnt)
            {
                /* remove from tablesyncing queue */
                if (current_table->prev)
                {
                    current_table->prev->next = current_table->next;
                }
                else
                {
                    tablesyncstats->tablesyncing = current_table->next;
                }

                if (current_table->next)
                {
                    current_table->next->prev = current_table->prev;
                }

                /* head insert into tablesyncdone */
                current_table->next = tablesyncstats->tablesyncdone;
                if (tablesyncstats->tablesyncdone)
                {
                    tablesyncstats->tablesyncdone->prev = current_table;
                }
                tablesyncstats->tablesyncdone = current_table;
                current_table->prev = NULL;
                current_table->tablestat = REFRESH_TABLE_STAT_DONE;
            }
            refresh_table_syncstats_write(current_table, refreshdir);
            break;
        }
        current_table = current_table->next;
    }

    refresh_table_syncstats_unlock(tablesyncstats);

    return true;
}

void refresh_table_syncstats_free(refresh_table_syncstats* tablesyncstats)
{
    if (NULL == tablesyncstats)
    {
        return;
    }
    refresh_table_syncstats_lock(tablesyncstats);

    refresh_table_syncstat_free(tablesyncstats->tablesyncall);
    refresh_table_syncstat_free(tablesyncstats->tablesyncing);
    refresh_table_syncstat_free(tablesyncstats->tablesyncdone);

    refresh_table_syncstats_unlock(tablesyncstats);

    rfree(tablesyncstats);
    tablesyncstats = NULL;

    return;
}

bool refresh_table_check_in_syncing(refresh_table_syncstats* tablesyncstats,
                                    refresh_table_sharding*  table_shard,
                                    refresh_table_syncstat** table_stat)
{
    refresh_table_syncstat* table = NULL;

    refresh_table_syncstats_lock(tablesyncstats);
    table = tablesyncstats->tablesyncing;

    /* search in syncing/synced linked list */
    for (; table != NULL; table = table->next)
    {
        if (!strcmp(table->schema, table_shard->schema) && !strcmp(table->table, table_shard->table))
        {
            if (table_stat)
            {
                *table_stat = table;
            }

            /* if status value doesn't exist and table status is SYNCING, sync hasn't started */
            if (!table->stat)
            {
                /* set table sync status*/
                refresh_table_syncstats_unlock(tablesyncstats);
                return false;
            }

            /* when status value exists */
            if (table->stat[table_shard->sharding_no - 1] == REFRESH_TABLE_SYNCS_SHARD_STAT_INIT)
            {
                if (REFRESH_TABLE_STAT_WAIT == table->tablestat)
                {
                    table->tablestat = REFRESH_TABLE_STAT_SYNCING;
                }
                refresh_table_syncstats_unlock(tablesyncstats);
                return false;
            }
            else
            {
                refresh_table_syncstats_unlock(tablesyncstats);
                return true;
            }
        }
    }
    refresh_table_syncstats_unlock(tablesyncstats);

    return true;
}

bool refresh_table_syncstats_compare(refresh_table_syncstats* tablesA, refresh_table_syncstats* tablesB)
{
    refresh_table_syncstat* table = NULL;
    refresh_table_syncstat* currenttable = NULL;

    /* search in pending-sync/syncing linked list */
    for (table = tablesA->tablesyncall; table != NULL; table = table->next)
    {
        for (currenttable = tablesB->tablesyncall; currenttable != NULL; currenttable = currenttable->next)
        {
            if (strcmp(table->schema, currenttable->schema) == 0 && strcmp(table->table, currenttable->table) == 0)
            {
                return true;
            }
        }
    }

    return false;
}

/* cleanup existing tables based on tablesyncstats before applying existing data */
bool refresh_table_syncstats_truncatetable_fromsyncstats(refresh_table_syncstats* tablesyncstats, void* in_conn)
{
    PGconn*                 conn = NULL;
    PGresult*               res = NULL;
    char                    stmtsql[MAX_EXEC_SQL_LEN] = {'\0'};
    refresh_table_syncstat* table = NULL;

    if (NULL == tablesyncstats || NULL == in_conn)
    {
        elog(RLOG_WARNING, "truncate table from syncstats tablesyncstats or in_conn is null ");
        return false;
    }

    conn = (PGconn*)in_conn;

    /* search in syncing list */
    for (table = tablesyncstats->tablesyncing; table != NULL; table = table->next)
    {
        /* don't cleanup tables that already started syncing on restart */
        if (REFRESH_TABLE_STAT_WAIT < table->tablestat)
        {
            continue;
        }

        /* truncate table before writing data */
        rmemset1(stmtsql, 0, 0, MAX_EXEC_SQL_LEN);
        sprintf(stmtsql, "TRUNCATE TABLE \"%s\".\"%s\" ;", table->schema, table->table);

        res = PQexec(conn, stmtsql);
        if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            elog(RLOG_WARNING, "SQL query execution failed: %s", PQerrorMessage(conn));
            PQclear(res);
            return false;
        }
        PQclear(res);
    }
    return true;
}

/* flush stats file to schema_table folder */
bool refresh_table_syncstats_write(refresh_table_syncstat* stats, char* refresh_path)
{
    bool           result = true;
    int            fd = -1;
    char*          bufferbegin = NULL;
    char*          buffer = NULL;
    size_t         input_size = 0;
    int            index_stat = 0;
    StringInfoData path = {'\0'};

    initStringInfo(&path);

    appendStringInfo(&path,
                     "%s/%s/%s_%s/%s",
                     refresh_path,
                     REFRESH_REFRESH,
                     stats->schema,
                     stats->table,
                     REFRESH_STATS);

    fd = osal_basic_open_file(path.data, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_WARNING, "can't openfile: %s, error: %s, please check!", path.data, strerror(errno));
        result = false;
        goto refresh_table_syncstats_write_error;
    }

    /* sizeof(stats->cnt) + sizeof(stats->completecnt) + sizeof(stats->tablestat) + (stats->cnt *
     * sizeof(int8_t)) */
    input_size = sizeof(int) + sizeof(int) + sizeof(int) + (stats->cnt * sizeof(int8_t));
    buffer = rmalloc0(input_size);
    if (!buffer)
    {
        elog(RLOG_WARNING, "oom");
        result = false;
        osal_file_close(fd);
        goto refresh_table_syncstats_write_error;
    }
    rmemset0(buffer, 0, 0, input_size);

    bufferbegin = buffer;

    /* stats->cnt */
    put32bit((uint8_t**)&buffer, (uint32_t)stats->cnt);

    /* stats->completecnt */
    put32bit((uint8_t**)&buffer, (uint32_t)stats->completecnt);

    /* stats->tablestat */
    put32bit((uint8_t**)&buffer, (uint32_t)stats->tablestat);

    /* stats->stat */
    for (index_stat = 0; index_stat < stats->cnt; index_stat++)
    {
        put8bit((uint8_t**)&buffer, (uint8_t)stats->stat[index_stat]);
    }

    if (osal_file_write(fd, bufferbegin, input_size) != input_size)
    {
        elog(RLOG_WARNING, "could not write to file %s, error: %s", path.data, strerror(errno));
        osal_file_close(fd);
        rfree(bufferbegin);
        goto refresh_table_syncstats_write_error;
    }

    if (0 != osal_file_sync(fd))
    {
        elog(RLOG_WARNING, "could not fsync file %s, error: %s", path.data, strerror(errno));
        osal_file_close(fd);
        rfree(bufferbegin);
        goto refresh_table_syncstats_write_error;
    }

    osal_file_close(fd);
    rfree(bufferbegin);

refresh_table_syncstats_write_error:
    if (path.data)
    {
        rfree(path.data);
    }

    return result;
}

/* read stats file from schema_table folder */
bool refresh_table_syncstats_read(refresh_table_syncstat* stats, char* refresh_path)
{
    bool           result = true;
    int            fd = -1;
    char*          buffer_begin = NULL;
    char*          buffer = NULL;
    char*          buffer_stat_begin = NULL;
    char*          buffer_stat = NULL;
    size_t         get_size = 0;
    int            index_stat = 0;
    StringInfoData path = {'\0'};

    initStringInfo(&path);

    appendStringInfo(&path, "%s/%s_%s/%s", refresh_path, stats->schema, stats->table, REFRESH_STATS);

    fd = osal_basic_open_file(path.data, O_RDONLY | BINARY);

    if (fd < 0)
    {
        elog(RLOG_WARNING, "can't openfile: %s, error: %s, please check!", path.data, strerror(errno));
        result = false;
        goto refresh_table_syncstats_read_error;
    }

    /* first get sizeof(stats->cnt) + sizeof(stats->completecnt) + sizeof(stats->tablestat) */
    get_size = sizeof(int) + sizeof(int) + sizeof(int);
    buffer = rmalloc0(get_size);
    if (!buffer)
    {
        elog(RLOG_WARNING, "oom");
        result = false;
        osal_file_close(fd);
        goto refresh_table_syncstats_read_error;
    }
    rmemset0(buffer, 0, 0, get_size);
    buffer_begin = buffer;

    if (osal_file_read(fd, buffer, get_size) != get_size)
    {
        elog(RLOG_WARNING, "could not read file %s, error: %s", path.data, strerror(errno));
        osal_file_close(fd);
        rfree(buffer);
        goto refresh_table_syncstats_read_error;
    }

    /* stats->cnt */
    stats->cnt = (int)get32bit((uint8_t**)&buffer);

    /* stats->completecnt */
    stats->completecnt = (int)get32bit((uint8_t**)&buffer);

    /* stats->tablestat */
    stats->tablestat = (int)get32bit((uint8_t**)&buffer);

    if (0 == stats->cnt && 0 == stats->completecnt)
    {
        goto refresh_table_syncstats_read_error;
    }

    get_size = stats->cnt * sizeof(int8_t);
    buffer_stat = rmalloc0(get_size);
    if (!buffer_stat)
    {
        elog(RLOG_WARNING, "oom");
        result = false;
        rfree(buffer_begin);
        osal_file_close(fd);
        goto refresh_table_syncstats_read_error;
    }
    rmemset0(buffer_stat, 0, 0, get_size);
    buffer_stat_begin = buffer_stat;

    stats->stat = rmalloc0(get_size);
    if (!stats->stat)
    {
        elog(RLOG_WARNING, "oom");
        result = false;
        rfree(buffer_begin);
        rfree(buffer_stat_begin);
        osal_file_close(fd);
        goto refresh_table_syncstats_read_error;
    }
    rmemset0(stats->stat, 0, 0, get_size);

    if (osal_file_read(fd, buffer_stat, get_size) != get_size)
    {
        elog(RLOG_WARNING, "could not read file %s, error: %s", path.data, strerror(errno));
        osal_file_close(fd);
        rfree(buffer_begin);
        rfree(buffer_stat_begin);
        goto refresh_table_syncstats_read_error;
    }

    /* stats->stat */
    for (index_stat = 0; index_stat < stats->cnt; index_stat++)
    {
        stats->stat[index_stat] = (int8_t)get8bit((uint8_t**)&buffer_stat);
        if (REFRESH_TABLE_SYNCS_SHARD_STAT_SYNCING == stats->stat[index_stat])
        {
            stats->stat[index_stat] = REFRESH_TABLE_SYNCS_SHARD_STAT_INIT;
        }
    }

    osal_file_close(fd);
    rfree(buffer_begin);
    rfree(buffer_stat_begin);

refresh_table_syncstats_read_error:
    if (path.data)
    {
        rfree(path.data);
    }

    return result;
}

void refresh_table_syncstats_tablesyncall_setfromfile(refresh_tables*          refreshtables,
                                                      refresh_table_syncstats* tablesyncstats,
                                                      char*                    refresh_path)
{
    refresh_table* cur_table = NULL;

    if (!refreshtables)
    {
        elog(RLOG_WARNING, "refreshtables is NULL");
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    if (!tablesyncstats || 0 == refreshtables->cnt)
    {
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    cur_table = refreshtables->tables;

    while (cur_table)
    {
        /* create a new sync status node */
        refresh_table_syncstat* new_syncstat = refresh_table_syncstat_init();

        /* copy table information */
        refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        refresh_table_syncstats_read(new_syncstat, refresh_path);

        /* insert into tablesyncall */
        new_syncstat->next = tablesyncstats->tablesyncall;
        if (tablesyncstats->tablesyncall)
        {
            tablesyncstats->tablesyncall->prev = new_syncstat;
        }

        tablesyncstats->tablesyncall = new_syncstat;

        cur_table = cur_table->next;
    }
    return;
}

void refresh_table_syncstats_tablesyncing_setfromfile(refresh_tables*          refreshtables,
                                                      refresh_table_syncstats* tablesyncstats,
                                                      char*                    refresh_path)
{
    refresh_table* cur_table = NULL;

    if (!refreshtables)
    {
        elog(RLOG_WARNING, "refreshtables is NULL");
        tablesyncstats->tablesyncing = NULL;
        return;
    }

    if (!tablesyncstats || 0 == refreshtables->cnt)
    {
        tablesyncstats->tablesyncing = NULL;
        return;
    }

    cur_table = refreshtables->tables;

    while (cur_table)
    {
        /* create a new sync status node */
        refresh_table_syncstat* new_syncstat = refresh_table_syncstat_init();

        /* copy table information */
        refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        refresh_table_syncstats_read(new_syncstat, refresh_path);

        /* insert into tablesyncall */
        new_syncstat->next = tablesyncstats->tablesyncing;
        if (tablesyncstats->tablesyncing)
        {
            tablesyncstats->tablesyncing->prev = new_syncstat;
        }

        tablesyncstats->tablesyncing = new_syncstat;

        cur_table = cur_table->next;
    }
    return;
}

/* generate refreshtables from tablesyncing */
refresh_tables* refresh_table_syncstats_tablesyncing2tables(refresh_table_syncstats* tablesyncstats)
{
    refresh_table*          table = NULL;
    refresh_tables*         refreshtables = NULL;
    refresh_table_syncstat* cur_table = NULL;

    if (NULL == tablesyncstats)
    {
        elog(RLOG_WARNING, "tablesyncstats is NULL");
        refreshtables = NULL;
        return refreshtables;
    }

    if (!tablesyncstats->tablesyncing)
    {
        elog(RLOG_INFO, "tablesyncstats refreshtables is NULL");
        refreshtables = NULL;
        return refreshtables;
    }

    refresh_table_syncstats_lock(tablesyncstats);

    cur_table = tablesyncstats->tablesyncing;
    refreshtables = refresh_tables_init();
    if (NULL == refreshtables)
    {
        elog(RLOG_WARNING, "tablesyncstats malloc refreshtables error");
        return NULL;
    }

    while (cur_table)
    {
        /* copy table information */
        table = refresh_table_init();

        refresh_table_setschema(cur_table->schema, table);
        refresh_table_settable(cur_table->table, table);
        refresh_table_setoid(cur_table->oid, table);
        refresh_tables_add(table, refreshtables);

        cur_table = cur_table->next;
    }

    refresh_table_syncstats_unlock(tablesyncstats);
    return refreshtables;
}
