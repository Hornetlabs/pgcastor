#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/string/stringinfo.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/ripple_conn.h"
#include "port/thread/ripple_thread.h"
#include "queue/ripple_queue.h"
#include "task/ripple_task_slot.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "refresh/ripple_refresh_table_syncstats.h"

ripple_refresh_table_syncstats* ripple_refresh_table_syncstats_init(void)
{
    ripple_refresh_table_syncstats* tablesyncstats = NULL;

    tablesyncstats = (ripple_refresh_table_syncstats*)rmalloc0(sizeof(ripple_refresh_table_syncstats));
    if (NULL == tablesyncstats)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(tablesyncstats, 0, '\0', sizeof(ripple_refresh_table_syncstats));
    
    /* 锁初始化 */
    ripple_thread_mutex_init(&tablesyncstats->tablesynclock, NULL);
    tablesyncstats->tablesyncall = NULL;
    tablesyncstats->tablesyncdone = NULL;
    tablesyncstats->tablesyncing = NULL;

    return tablesyncstats;
}

bool ripple_refresh_table_syncstats_lock(ripple_refresh_table_syncstats* tablesyncstats)
{
    int iret = 0;
    if (NULL == tablesyncstats)
    {
        return false;
    }

    iret = ripple_thread_lock(&tablesyncstats->tablesynclock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    return true;
}

bool ripple_refresh_table_syncstats_unlock(ripple_refresh_table_syncstats* tablesyncstats)
{
    int iret = 0;
    if ( NULL == tablesyncstats)
    {
        return false;
    }

    iret = ripple_thread_unlock(&tablesyncstats->tablesynclock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
    }

    return true;
}

void ripple_refresh_table_syncstats_tablesyncing_set(ripple_refresh_tables* refreshtables, ripple_refresh_table_syncstats* tablesyncstats)
{
    ripple_refresh_table *cur_table = NULL;

    if (!refreshtables)
    {
        elog(RLOG_WARNING, "refreshtables is NULL");
        tablesyncstats->tablesyncing = NULL;
        return;
    }

    if ( !tablesyncstats || 0 == refreshtables->cnt)
    {
        tablesyncstats->tablesyncing = NULL;
        return;
    }

    cur_table = refreshtables->tables;

    ripple_refresh_table_syncstats_lock(tablesyncstats);

    while (cur_table)
    {
        // 创建一个新的同步状态节点
        ripple_refresh_table_syncstat *new_syncstat = ripple_refresh_table_syncstat_init();

        // 复制表信息
        ripple_refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        ripple_refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        ripple_refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        // 插入 tablesyncall
        new_syncstat->next = tablesyncstats->tablesyncing;
        if (tablesyncstats->tablesyncing)
        {
            tablesyncstats->tablesyncing->prev = new_syncstat;
        }

        tablesyncstats->tablesyncing = new_syncstat;

        cur_table = cur_table->next;
    }

    ripple_refresh_table_syncstats_unlock(tablesyncstats);
    return;
}

void ripple_refresh_table_syncstats_tablesyncall_set(ripple_refresh_tables* refreshtables, ripple_refresh_table_syncstats* tablesyncstats)
{
    ripple_refresh_table *cur_table = NULL;

    if (!refreshtables )
    {
        elog(RLOG_WARNING, "refreshtables is NULL");
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    if ( !tablesyncstats || 0 == refreshtables->cnt)
    {
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    cur_table = refreshtables->tables;

    ripple_refresh_table_syncstats_lock(tablesyncstats);

    while (cur_table)
    {
        // 创建一个新的同步状态节点
        ripple_refresh_table_syncstat *new_syncstat = ripple_refresh_table_syncstat_init();

        // 复制表信息
        ripple_refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        ripple_refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        ripple_refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        // 插入 tablesyncall
        new_syncstat->next = tablesyncstats->tablesyncall;
        if (tablesyncstats->tablesyncall)
        {
            tablesyncstats->tablesyncall->prev = new_syncstat;
        }

        tablesyncstats->tablesyncall = new_syncstat;

        cur_table = cur_table->next;
    }

    ripple_refresh_table_syncstats_unlock(tablesyncstats);
    return;
}

void ripple_refresh_table_syncstats_tablesyncing2tablesyncall(ripple_refresh_table_syncstats* tablesyncstats)
{
    ripple_refresh_table_syncstat *cur_table = NULL;

    if (NULL == tablesyncstats)
    {
        elog(RLOG_WARNING, "tablesyncstats is NULL");
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    if ( !tablesyncstats->tablesyncing || 0 == tablesyncstats->tablesyncing->cnt)
    {
        elog(RLOG_INFO, "refreshtables is NULL");
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    ripple_refresh_table_syncstats_lock(tablesyncstats);

    cur_table = tablesyncstats->tablesyncing;

    while (cur_table)
    {
        // 创建一个新的同步状态节点
        ripple_refresh_table_syncstat *new_syncstat = ripple_refresh_table_syncstat_init();

        // 复制表信息
        ripple_refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        ripple_refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        ripple_refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        // 插入 tablesyncall
        new_syncstat->next = tablesyncstats->tablesyncall;
        if (tablesyncstats->tablesyncall)
        {
            tablesyncstats->tablesyncall->prev = new_syncstat;
        }

        tablesyncstats->tablesyncall = new_syncstat;

        cur_table = cur_table->next;
    }

    ripple_refresh_table_syncstats_unlock(tablesyncstats);
    return;
}

bool ripple_refreshtablesyncstats_markstatdone(ripple_refresh_table_sharding* tablesharding, ripple_refresh_table_syncstats* tablesyncstats, char* refreshdir)
{
    bool complete = false;
    ripple_refresh_table_syncstat* current_table = NULL;

    if (!tablesharding || !tablesyncstats || !tablesyncstats->tablesyncing)
    {
        return false;
    }

    ripple_refresh_table_syncstats_lock(tablesyncstats);
    current_table = tablesyncstats->tablesyncing;

    /* 遍历complete目录生成任务 */
    while (current_table)
    {
        if (0 == strcmp(current_table->schema, tablesharding->schema)
            && 0 == strcmp(current_table->table, tablesharding->table))
        {
            if (0 != tablesharding->shardings)
            {
                current_table->completecnt += 1;
                current_table->stat[tablesharding->sharding_no - 1] = RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_DONE;
            }

            elog(RLOG_DEBUG, "refresh worker, queue: %s.%s %4d %4d, mark done",
                                                               tablesharding->schema,
                                                               tablesharding->table,
                                                               tablesharding->shardings,
                                                               tablesharding->sharding_no);

            if (current_table->completecnt == current_table->cnt)
            {
                /* 从 tablesyncing 队列中移除 */
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

                /* 头插法加入 tablesyncdone */
                current_table->next = tablesyncstats->tablesyncdone;
                if (tablesyncstats->tablesyncdone)
                {
                    tablesyncstats->tablesyncdone->prev = current_table;
                }
                tablesyncstats->tablesyncdone = current_table;
                current_table->prev = NULL;
                current_table->tablestat = RIPPLE_REFRESH_TABLE_STAT_DONE;
                complete = true;
            }
            ripple_refresh_table_syncstats_write(current_table, refreshdir);
            break;
        }
        current_table = current_table->next;
    }

    ripple_refresh_table_syncstats_unlock(tablesyncstats);

    /* 删除已完成的文件 */
    if (complete)
    {
        StringInfo path = NULL;
        path = makeStringInfo();

        appendStringInfo(path, "%s/%s/%s_%s", refreshdir,
                                              RIPPLE_REFRESH_REFRESH,
                                              tablesharding->schema,
                                              tablesharding->table);
        if (!RemoveDir(path->data))
        {
            elog(RLOG_ERROR, "can't remove dir: %s", path->data);
        }
        deleteStringInfo(path);
    }

    return true;
}

void ripple_refresh_table_syncstats_free(ripple_refresh_table_syncstats* tablesyncstats)
{
    if ( NULL == tablesyncstats)
    {
        return;
    }
    ripple_refresh_table_syncstats_lock(tablesyncstats);

    ripple_refresh_table_syncstat_free(tablesyncstats->tablesyncall);
    ripple_refresh_table_syncstat_free(tablesyncstats->tablesyncing);
    ripple_refresh_table_syncstat_free(tablesyncstats->tablesyncdone);

    ripple_refresh_table_syncstats_unlock(tablesyncstats);

   rfree(tablesyncstats);
   tablesyncstats = NULL;

   return;
}

bool ripple_refresh_table_check_in_syncing(ripple_refresh_table_syncstats* tablesyncstats,
                                           ripple_refresh_table_sharding *table_shard,
                                           ripple_refresh_table_syncstat **table_stat)
{
    ripple_refresh_table_syncstat *table = NULL;

    ripple_refresh_table_syncstats_lock(tablesyncstats);
    table = tablesyncstats->tablesyncing;

    /* 从 待同步/同步中 的链表中查找 */
    for (; table != NULL; table = table->next)
    {
        if (!strcmp(table->schema, table_shard->schema) && !strcmp(table->table, table_shard->table))
        {
            if (table_stat)
            {
                *table_stat = table;
            }

            /* 如果不存在状态值并且表状态为SYNCING, 则没有开始同步 */
            if (!table->stat)
            {
                /* 设置表同步状态*/
                ripple_refresh_table_syncstats_unlock(tablesyncstats);
                return false;
            }

            /* 存在状态值的情况下,  */
            if (table->stat[table_shard->sharding_no - 1] == RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_INIT)
            {
                if (RIPPLE_REFRESH_TABLE_STAT_WAIT == table->tablestat)
                {
                    table->tablestat = RIPPLE_REFRESH_TABLE_STAT_SYNCING;
                }
                ripple_refresh_table_syncstats_unlock(tablesyncstats);
                return false;
            }
            else
            {
                ripple_refresh_table_syncstats_unlock(tablesyncstats);
                return true;
            }
        }
    }
    ripple_refresh_table_syncstats_unlock(tablesyncstats);

    return true;
}

bool ripple_refresh_table_syncstats_compare(ripple_refresh_table_syncstats* tablesA,
                                           ripple_refresh_table_syncstats* tablesB)
{
    ripple_refresh_table_syncstat *table = NULL;
    ripple_refresh_table_syncstat *currenttable = NULL;

    /* 从 待同步/同步中 的链表中查找 */
    for ( table = tablesA->tablesyncall; table != NULL; table = table->next)
    {
        for (currenttable = tablesB->tablesyncall; currenttable != NULL; currenttable = currenttable->next)
        {
            if (strcmp(table->schema, currenttable->schema) == 0
                && strcmp(table->table, currenttable->table) == 0)
            {
                return true;
            }
        }
    }

    return false;
}

/* 在应用存量数据前根据tablesyncstats清理存量表 */
bool ripple_refresh_table_syncstats_truncatetable_fromsyncstats(ripple_refresh_table_syncstats* tablesyncstats, void* in_conn)
{
    PGconn* conn = NULL;
    PGresult* res = NULL;
    char stmtsql[RIPPLE_MAX_EXEC_SQL_LEN] = {'\0'};
    ripple_refresh_table_syncstat *table = NULL;

    if(NULL == tablesyncstats || NULL == in_conn )
    {
        elog(RLOG_WARNING, "truncate table from syncstats tablesyncstats or in_conn is null ");
        return false;
    }

    conn = (PGconn*)in_conn;

    /* 从 同步中 的链表中查找 */
    for (table = tablesyncstats->tablesyncing; table != NULL; table = table->next)
    {
        /* 重启时已开始的表不清理 */
        if (RIPPLE_REFRESH_TABLE_STAT_WAIT < table->tablestat)
        {
            continue;
        }
        
        /* 写入数据前先清空表 */
        rmemset1(stmtsql, 0, 0, RIPPLE_MAX_EXEC_SQL_LEN);
        sprintf(stmtsql, "TRUNCATE TABLE \"%s\".\"%s\" ;", table->schema,
                                                   table->table);

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

/* stats文件落盘到schema_table文件夹中 */
bool ripple_refresh_table_syncstats_write(ripple_refresh_table_syncstat *stats, char *refresh_path)
{
    bool result = true;
    int fd = -1;
    char *bufferbegin = NULL;
    char *buffer = NULL;
    size_t input_size = 0;
    int index_stat = 0;
    StringInfoData path = {'\0'};

    initStringInfo(&path);

    appendStringInfo(&path, "%s/%s/%s_%s/%s", refresh_path, RIPPLE_REFRESH_REFRESH, stats->schema, stats->table, RIPPLE_REFRESH_STATS);

    fd = BasicOpenFile(path.data,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_WARNING, "can't openfile: %s, error: %s, please check!", path.data, strerror(errno));
        result = false;
        goto ripple_refresh_table_syncstats_write_error;
    }

    /* sizeof(stats->cnt) + sizeof(stats->completecnt) + sizeof(stats->tablestat) + (stats->cnt * sizeof(int8_t)) */
    input_size = sizeof(int) + sizeof(int) + sizeof(int) + (stats->cnt * sizeof(int8_t));
    buffer = rmalloc0(input_size);
    if (!buffer)
    {
        elog(RLOG_WARNING, "oom");
        result = false;
        FileClose(fd);
        goto ripple_refresh_table_syncstats_write_error;
    }
    rmemset0(buffer, 0, 0, input_size);

    bufferbegin = buffer;

    /* stats->cnt */
    put32bit((uint8_t **) &buffer, (uint32_t) stats->cnt);

    /* stats->completecnt */
    put32bit((uint8_t **) &buffer, (uint32_t) stats->completecnt);

    /* stats->tablestat */
    put32bit((uint8_t **) &buffer, (uint32_t) stats->tablestat);

    /* stats->stat */
    for (index_stat = 0; index_stat < stats->cnt; index_stat++)
    {
        put8bit((uint8_t **) &buffer, (uint8_t) stats->stat[index_stat]);
    }

    if (FileWrite(fd, bufferbegin, input_size) != input_size)
    {
        elog(RLOG_WARNING, "could not write to file %s, error: %s", path.data, strerror(errno));
        FileClose(fd);
        rfree(bufferbegin);
        goto ripple_refresh_table_syncstats_write_error;
    }

    if(0 != FileSync(fd))
    {
        elog(RLOG_WARNING, "could not fsync file %s, error: %s", path.data, strerror(errno));
        FileClose(fd);
        rfree(bufferbegin);
        goto ripple_refresh_table_syncstats_write_error;
    }

    FileClose(fd);
    rfree(bufferbegin);

ripple_refresh_table_syncstats_write_error:
    if (path.data)
    rfree(path.data);

    return result;
}

/* 读取schema_table文件夹中的stats文件 */
bool ripple_refresh_table_syncstats_read(ripple_refresh_table_syncstat *stats, char *refresh_path)
{
    bool result = true;
    int fd = -1;
    char *buffer_begin = NULL;
    char *buffer = NULL;
    char *buffer_stat_begin = NULL;
    char *buffer_stat = NULL;
    size_t get_size = 0;
    int index_stat = 0;
    StringInfoData path = {'\0'};

    initStringInfo(&path);

    appendStringInfo(&path, "%s/%s_%s/%s", refresh_path, stats->schema, stats->table, RIPPLE_REFRESH_STATS);

    fd = BasicOpenFile(path.data,
                        O_RDONLY | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_WARNING, "can't openfile: %s, error: %s, please check!", path.data, strerror(errno));
        result = false;
        goto ripple_refresh_table_syncstats_read_error;
    }

    /* 先获取sizeof(stats->cnt) + sizeof(stats->completecnt) + sizeof(stats->tablestat) */
    get_size = sizeof(int) + sizeof(int) + sizeof(int);
    buffer = rmalloc0(get_size);
    if (!buffer)
    {
        elog(RLOG_WARNING, "oom");
        result = false;
        FileClose(fd);
        goto ripple_refresh_table_syncstats_read_error;
    }
    rmemset0(buffer, 0, 0, get_size);
    buffer_begin = buffer;

    if (FileRead(fd, buffer, get_size) != get_size)
    {
        elog(RLOG_WARNING, "could not read file %s, error: %s", path.data, strerror(errno));
        FileClose(fd);
        rfree(buffer);
        goto ripple_refresh_table_syncstats_read_error;
    }

    /* stats->cnt */
    stats->cnt = (int) get32bit((uint8_t **) &buffer);

    /* stats->completecnt */
    stats->completecnt = (int) get32bit((uint8_t **) &buffer);

    /* stats->tablestat */
    stats->tablestat = (int) get32bit((uint8_t **) &buffer);

    if (0 == stats->cnt && 0 == stats->completecnt)
    {
        goto ripple_refresh_table_syncstats_read_error;
    }

    get_size = stats->cnt * sizeof(int8_t);
    buffer_stat = rmalloc0(get_size);
    if (!buffer_stat)
    {
        elog(RLOG_WARNING, "oom");
        result = false;
        rfree(buffer_begin);
        FileClose(fd);
        goto ripple_refresh_table_syncstats_read_error;
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
        FileClose(fd);
        goto ripple_refresh_table_syncstats_read_error;
    }
    rmemset0(stats->stat, 0, 0, get_size);

    if (FileRead(fd, buffer_stat, get_size) != get_size)
    {
        elog(RLOG_WARNING, "could not read file %s, error: %s", path.data, strerror(errno));
        FileClose(fd);
        rfree(buffer_begin);
        rfree(buffer_stat_begin);
        goto ripple_refresh_table_syncstats_read_error;
    }

    /* stats->stat */
    for (index_stat = 0; index_stat < stats->cnt; index_stat++)
    {
        stats->stat[index_stat] = (int8_t) get8bit((uint8_t **) &buffer_stat);
        if (RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_SYNCING == stats->stat[index_stat])
        {
            stats->stat[index_stat] = RIPPLE_REFRESH_TABLE_SYNCS_SHARD_STAT_INIT;
        }
    }

    FileClose(fd);
    rfree(buffer_begin);
    rfree(buffer_stat_begin);

ripple_refresh_table_syncstats_read_error:
    if (path.data)
    rfree(path.data);

    return result;
}

void ripple_refresh_table_syncstats_tablesyncall_setfromfile(ripple_refresh_tables* refreshtables, ripple_refresh_table_syncstats* tablesyncstats, char* refresh_path)
{
    ripple_refresh_table *cur_table = NULL;

    if (!refreshtables )
    {
        elog(RLOG_WARNING, "refreshtables is NULL");
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    if ( !tablesyncstats || 0 == refreshtables->cnt)
    {
        tablesyncstats->tablesyncall = NULL;
        return;
    }

    cur_table = refreshtables->tables;

    while (cur_table)
    {
        // 创建一个新的同步状态节点
        ripple_refresh_table_syncstat *new_syncstat = ripple_refresh_table_syncstat_init();

        // 复制表信息
        ripple_refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        ripple_refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        ripple_refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        ripple_refresh_table_syncstats_read(new_syncstat, refresh_path);

        // 插入 tablesyncall
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

void ripple_refresh_table_syncstats_tablesyncing_setfromfile(ripple_refresh_tables* refreshtables, ripple_refresh_table_syncstats* tablesyncstats, char* refresh_path)
{
    ripple_refresh_table *cur_table = NULL;

    if (!refreshtables )
    {
        elog(RLOG_WARNING, "refreshtables is NULL");
        tablesyncstats->tablesyncing = NULL;
        return;
    }

    if ( !tablesyncstats || 0 == refreshtables->cnt)
    {
        tablesyncstats->tablesyncing = NULL;
        return;
    }

    cur_table = refreshtables->tables;

    while (cur_table)
    {
        // 创建一个新的同步状态节点
        ripple_refresh_table_syncstat *new_syncstat = ripple_refresh_table_syncstat_init();

        // 复制表信息
        ripple_refresh_table_syncstat_schema_set(cur_table->schema, new_syncstat);
        ripple_refresh_table_syncstat_table_set(cur_table->table, new_syncstat);
        ripple_refresh_table_syncstat_oid_set(cur_table->oid, new_syncstat);

        ripple_refresh_table_syncstats_read(new_syncstat, refresh_path);

        // 插入 tablesyncall
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

/* 根据tablesyncing生成refreshtables */
ripple_refresh_tables* ripple_refresh_table_syncstats_tablesyncing2tables(ripple_refresh_table_syncstats* tablesyncstats)
{
    ripple_refresh_table* table                 = NULL;
    ripple_refresh_tables* refreshtables        = NULL;
    ripple_refresh_table_syncstat *cur_table    = NULL;

    if (NULL == tablesyncstats)
    {
        elog(RLOG_WARNING, "tablesyncstats is NULL");
        refreshtables = NULL;
        return refreshtables;
    }

    if ( !tablesyncstats->tablesyncing || 0 == tablesyncstats->tablesyncing->cnt)
    {
        elog(RLOG_INFO, "refreshtables is NULL");
        refreshtables = NULL;
        return refreshtables;
    }

    ripple_refresh_table_syncstats_lock(tablesyncstats);

    cur_table = tablesyncstats->tablesyncing;
    refreshtables = ripple_refresh_tables_init();
    if (NULL == refreshtables)
    {
        elog(RLOG_WARNING, "tablesyncstats malloc refreshtables error");
        return NULL;
    }

    while (cur_table)
    {
        // 创建一个新的同步状态节点
        ripple_refresh_table_syncstat *new_syncstat = ripple_refresh_table_syncstat_init();

        // 复制表信息
        table = ripple_refresh_table_init();

        ripple_refresh_table_setschema(new_syncstat->schema, table);
        ripple_refresh_table_settable(new_syncstat->table, table);
        ripple_refresh_table_setoid(new_syncstat->oid, table);
        ripple_refresh_tables_add(table, refreshtables);

        cur_table = cur_table->next;
    }

    ripple_refresh_table_syncstats_unlock(tablesyncstats);
    return refreshtables;
}
