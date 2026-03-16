#include "ripple_app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"

ripple_refresh_table* ripple_refresh_table_init(void)
{
    ripple_refresh_table* refreshtable = NULL;
    refreshtable = (ripple_refresh_table*)rmalloc0(sizeof(ripple_refresh_table));
    if (NULL == refreshtable)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(refreshtable, 0, '\0', sizeof(ripple_refresh_tables));
    refreshtable->oid = InvalidOid;
    refreshtable->schema = NULL;
    refreshtable->table = NULL;
    refreshtable->next = NULL;
    refreshtable->prev = NULL;

    return refreshtable;
}

ripple_refresh_tables* ripple_refresh_tables_init(void)
{
    ripple_refresh_tables* refreshtables = NULL;
    refreshtables = (ripple_refresh_tables*)rmalloc0(sizeof(ripple_refresh_tables));
    if (NULL == refreshtables)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(refreshtables, 0, '\0', sizeof(ripple_refresh_tables));
    refreshtables->cnt = 0;
    refreshtables->tables = NULL;

    return refreshtables;
}

bool ripple_refresh_tables_add(ripple_refresh_table* table, ripple_refresh_tables* tables)
{
    if (NULL == table || NULL == tables)
    {
        elog(RLOG_ERROR, "table or tables is NULL");
    }

    // 插入到链表头部
    table->next = tables->tables;
    table->prev = NULL;
    if (NULL != tables->tables)
    {
        tables->tables->prev = table;
    }
    tables->tables = table;
    tables->cnt++;
    return true;

}

ripple_refresh_table* ripple_refresh_tables_get(ripple_refresh_tables* tables)
{
    ripple_refresh_table* table = NULL;
    if (NULL == tables || NULL == tables->tables)
    {
        elog(RLOG_ERROR, "tables or tables->tables is NULL");
    }
    
    table = tables->tables;
    tables->tables = table->next;
    if(NULL != tables->tables)
    {
        tables->tables->prev = NULL;
    }
    table->prev = NULL;
    table->next = NULL;
    tables->cnt--;
    return table;
}

void ripple_refresh_table_setschema(char* schema, ripple_refresh_table* refreshtable)
{
    if (NULL == schema || NULL == refreshtable)
    {
        elog(RLOG_ERROR, "schema or refreshtable is NULL");
    }
    refreshtable->schema = rstrdup(schema);
    return;
}

void ripple_refresh_table_setoid(Oid oid, ripple_refresh_table* refreshtable)
{
    if (NULL == refreshtable)
    {
        elog(RLOG_ERROR, "setoid refreshtable is NULL");
    }
    refreshtable->oid = oid;
    return;
}

void ripple_refresh_table_settable(char* table, ripple_refresh_table* refreshtable)
{
    if (NULL == table || NULL == refreshtable)
    {
        elog(RLOG_ERROR, "table or refreshtable is NULL");
    }
    refreshtable->table = rstrdup(table);
    return;
}

void ripple_refresh_freetable(ripple_refresh_table *refreshtable)
{
    ripple_refresh_table *current = refreshtable;

    if (NULL == refreshtable)
    {
        return;
    }

    while (NULL != current)
    {
        ripple_refresh_table *next = current->next;

        if (current->schema)
        {
            rfree(current->schema);
        }
        
        if (current->table)
        {
            rfree(current->table);
        }

        rfree(current);
        current = next;
    }

    return;
}

void ripple_refresh_freetables(ripple_refresh_tables *refreshtables)
{
    if (NULL == refreshtables)
    {
        return;
    }
    ripple_refresh_freetable(refreshtables->tables);
    
    rfree(refreshtables);
    refreshtables = NULL;
}

ripple_refresh_tables* ripple_refresh_tables_copy(ripple_refresh_tables* refreshtables)
{
    ripple_refresh_tables* new_tables = NULL;
    ripple_refresh_table* table = NULL;
    ripple_refresh_table* current_table = NULL;

    new_tables = ripple_refresh_tables_init();

    if (!refreshtables || !refreshtables->tables)
    {
        return new_tables;
    }
    
    current_table = refreshtables->tables;

    while (current_table)
    {
        table = ripple_refresh_table_init();

        ripple_refresh_table_setschema(current_table->schema, table);
        ripple_refresh_table_settable(current_table->table, table);
        ripple_refresh_table_setoid(current_table->oid, table);

        ripple_refresh_tables_add(table, new_tables);

        current_table = current_table->next;
    }
    
    return new_tables;

}

bool ripple_refresh_tables_hasrepeat(ripple_refresh_tables* syncdataset,
                                     ripple_refresh_tables* newdataset,
                                     ripple_refresh_table** prepeattable)
{
    ripple_refresh_table *table_new = NULL;
    ripple_refresh_table *table_sync = NULL;

    /* 任何一个为空则没有新表 */
    if (!syncdataset || !newdataset)
    {
        return false;
    }

    table_new = newdataset->tables;
    table_sync = syncdataset->tables;

    /* 任何一个为空则没有新表 */
    if (!table_sync || !table_new)
    {
        return false;
    }

    while (table_new)
    {
        while (table_sync)
        {
            if (table_new->oid == table_sync->oid)
            {
                *prepeattable = table_new;
                return true;
            }
            table_sync = table_sync->next;
        }

        /* 重置 */
        table_sync = syncdataset->tables;

        /* 推进 */
        table_new = table_new->next;
    }
    return false;
}

/* 从同步表哈希中查找是否存在 */
bool ripple_refresh_tables_hasnew(HTAB *syncdataset, ripple_refresh_tables* newdataset)
{
    ripple_refresh_table *oltable = NULL; 

    if (!newdataset || (newdataset->cnt == 0) || !syncdataset)
    {
        return false;
    }
    oltable = newdataset->tables;

    while (oltable)
    {
        bool find = false;
        hash_search(syncdataset, &oltable->oid, HASH_FIND, &find);
        if (!find)
        {
            return true;
        }
        oltable = oltable->next;
    }
    return false;
}

/* 从refresh文件夹中生成refresh tables */
static void ripple_refresh_table_get_table_from_filename(char *filename, ripple_refresh_table *table)
{
    char *ptr_left = filename;
    char *ptr_right = NULL;
    char temp_name[65] = {'\0'};
    int len = 0;

    /* schema */
    ptr_right = strstr(ptr_left, "_");
    if (ptr_right == NULL)
    {
        elog(RLOG_ERROR, "invalid file name: %s", filename);
    }
    len = ptr_right - ptr_left;
    rmemcpy1(temp_name, 0, ptr_left, len);
    temp_name[len] = '\0';
    table->schema = rstrdup(temp_name);
    ptr_left = ptr_right + 1;

    /* name */
    len = strlen(ptr_left);
    rmemcpy1(temp_name, 0, ptr_left, len);
    temp_name[len] = '\0';
    table->table = rstrdup(temp_name);
}

ripple_refresh_tables *ripple_refresh_tables_gen_from_file(char *path)
{
    ripple_refresh_tables *result = NULL;
    ripple_refresh_table *table_prev = NULL;
    DIR* compdir = NULL;
    int table_cnt = 0;
    struct dirent *entry = NULL;

    compdir = OpenDir(path);
    if(NULL == compdir)
    {
        return result;
    }

    result = ripple_refresh_tables_init();

    while (NULL != (entry = ReadDir(compdir, path)))
    {
        ripple_refresh_table *table = NULL;

        if (0 == strcmp(".", entry->d_name)
        || 0 == strcmp("..", entry->d_name))
        {
            continue;
        }

        table_cnt++;

        table = ripple_refresh_table_init();

        if (!result->tables)
        {
            result->tables = table;
        }

        if (table_prev)
        {
            table_prev->next = table;
        }

        table->prev = table_prev;
        table_prev = table;

        ripple_refresh_table_get_table_from_filename(entry->d_name, table);
    }

    FreeDir(compdir);
    result->cnt = table_cnt;

    return result;
}
