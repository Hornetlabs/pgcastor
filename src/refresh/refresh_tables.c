#include "app_incl.h"
#include "port/file/fd.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"

refresh_table* refresh_table_init(void)
{
    refresh_table* refreshtable = NULL;
    refreshtable = (refresh_table*)rmalloc0(sizeof(refresh_table));
    if (NULL == refreshtable)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(refreshtable, 0, '\0', sizeof(refresh_tables));
    refreshtable->oid = INVALIDOID;
    refreshtable->schema = NULL;
    refreshtable->table = NULL;
    refreshtable->next = NULL;
    refreshtable->prev = NULL;

    return refreshtable;
}

refresh_tables* refresh_tables_init(void)
{
    refresh_tables* refreshtables = NULL;
    refreshtables = (refresh_tables*)rmalloc0(sizeof(refresh_tables));
    if (NULL == refreshtables)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(refreshtables, 0, '\0', sizeof(refresh_tables));
    refreshtables->cnt = 0;
    refreshtables->tables = NULL;

    return refreshtables;
}

bool refresh_tables_add(refresh_table* table, refresh_tables* tables)
{
    if (NULL == table || NULL == tables)
    {
        elog(RLOG_ERROR, "table or tables is NULL");
    }

    // insert to list head
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

refresh_table* refresh_tables_get(refresh_tables* tables)
{
    refresh_table* table = NULL;
    if (NULL == tables || NULL == tables->tables)
    {
        elog(RLOG_ERROR, "tables or tables->tables is NULL");
    }

    table = tables->tables;
    tables->tables = table->next;
    if (NULL != tables->tables)
    {
        tables->tables->prev = NULL;
    }
    table->prev = NULL;
    table->next = NULL;
    tables->cnt--;
    return table;
}

void refresh_table_setschema(char* schema, refresh_table* refreshtable)
{
    if (NULL == schema || NULL == refreshtable)
    {
        elog(RLOG_ERROR, "schema or refreshtable is NULL");
    }
    refreshtable->schema = rstrdup(schema);
    return;
}

void refresh_table_setoid(Oid oid, refresh_table* refreshtable)
{
    if (NULL == refreshtable)
    {
        elog(RLOG_ERROR, "setoid refreshtable is NULL");
    }
    refreshtable->oid = oid;
    return;
}

void refresh_table_settable(char* table, refresh_table* refreshtable)
{
    if (NULL == table || NULL == refreshtable)
    {
        elog(RLOG_ERROR, "table or refreshtable is NULL");
    }
    refreshtable->table = rstrdup(table);
    return;
}

void refresh_freetable(refresh_table* refreshtable)
{
    refresh_table* current = refreshtable;

    if (NULL == refreshtable)
    {
        return;
    }

    while (NULL != current)
    {
        refresh_table* next = current->next;

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

void refresh_freetables(refresh_tables* refreshtables)
{
    if (NULL == refreshtables)
    {
        return;
    }
    refresh_freetable(refreshtables->tables);

    rfree(refreshtables);
    refreshtables = NULL;
}

refresh_tables* refresh_tables_copy(refresh_tables* refreshtables)
{
    refresh_tables* new_tables = NULL;
    refresh_table*  table = NULL;
    refresh_table*  current_table = NULL;

    new_tables = refresh_tables_init();

    if (!refreshtables || !refreshtables->tables)
    {
        return new_tables;
    }

    current_table = refreshtables->tables;

    while (current_table)
    {
        table = refresh_table_init();

        refresh_table_setschema(current_table->schema, table);
        refresh_table_settable(current_table->table, table);
        refresh_table_setoid(current_table->oid, table);

        refresh_tables_add(table, new_tables);

        current_table = current_table->next;
    }

    return new_tables;
}

bool refresh_tables_hasrepeat(refresh_tables* syncdataset,
                              refresh_tables* newdataset,
                              refresh_table** prepeattable)
{
    refresh_table* table_new = NULL;
    refresh_table* table_sync = NULL;

    /* if either is empty, there are no new tables */
    if (!syncdataset || !newdataset)
    {
        return false;
    }

    table_new = newdataset->tables;
    table_sync = syncdataset->tables;

    /* if either is empty, there are no new tables */
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

        /* reset */
        table_sync = syncdataset->tables;

        /* advance */
        table_new = table_new->next;
    }
    return false;
}

/* check if exists in sync table hash */
bool refresh_tables_hasnew(HTAB* syncdataset, refresh_tables* newdataset)
{
    refresh_table* oltable = NULL;

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

/* generate refresh tables from refresh folder */
static void refresh_table_get_table_from_filename(char* filename, refresh_table* table)
{
    char* ptr_left = filename;
    char* ptr_right = NULL;
    char  temp_name[65] = {'\0'};
    int   len = 0;

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

refresh_tables* refresh_tables_gen_from_file(char* path)
{
    int             table_cnt = 0;
    refresh_tables* result = NULL;
    refresh_table*  table_prev = NULL;
    DIR*            compdir = NULL;
    struct dirent*  entry = NULL;

    compdir = osal_open_dir(path);
    if (NULL == compdir)
    {
        return result;
    }

    result = refresh_tables_init();

    while (NULL != (entry = osal_read_dir(compdir, path)))
    {
        refresh_table* table = NULL;

        if (0 == strcmp(".", entry->d_name) || 0 == strcmp("..", entry->d_name))
        {
            continue;
        }

        table_cnt++;

        table = refresh_table_init();

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

        refresh_table_get_table_from_filename(entry->d_name, table);
    }

    osal_free_dir(compdir);
    result->cnt = table_cnt;

    return result;
}

/* Generate a refresh file to record the number of shards in the table */
bool refresh_tables_flush(refresh_tables* rtables)
{
    int            fd = -1;
    uint32         totallen = 0;
    uint32         len = 0;
    uint8*         uptr = NULL;
    uint8*         data = NULL;
    refresh_table* rtable = NULL;
    char           filepath[MAXPATH] = {0};

    snprintf(filepath, MAXPATH, "%s/%s", REFRESH_REFRESH, REFRESH_REFRESHTABLES);
    fd = osal_basic_open_file(filepath, O_RDWR | O_CREAT | BINARY);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "open file %s error", filepath);
        return false;
    }
    totallen = 4;
    for (rtable = rtables->tables; NULL != rtable; rtable = rtable->next)
    {
        totallen += 4;
        /* schems */
        totallen += strlen(rtable->schema);
        totallen += 1;
        /* table name */
        totallen += strlen(rtable->table);
        totallen += 1;
    }
    data = rmalloc0(totallen);
    if (NULL == data)
    {
        elog(RLOG_WARNING, "flush refreshtables out of memory");
        return false;
    }
    rmemset0(data, 0, '\0', totallen);
    uptr = data;
    rmemcpy1(uptr, 0, &totallen, 4);
    uptr += 4;

    /* 填充数据 */
    for (rtable = rtables->tables; NULL != rtable; rtable = rtable->next)
    {
        rmemcpy1(uptr, 0, &rtable->oid, 4);
        uptr += 4;
        /* schema */
        len = strlen(rtable->schema);
        rmemcpy1(uptr, 0, rtable->schema, len);
        uptr += len;
        uptr += 1;
        /* table name */
        len = strlen(rtable->table);
        rmemcpy1(uptr, 0, rtable->table, len);
        uptr += len;
        uptr += 1;
    }
    osal_file_pwrite(fd, (char*)data, totallen, 0);
    osal_file_sync(fd);
    osal_file_close(fd);
    return true;
}

/* Load refresh file */
refresh_tables* refresh_tables_load(void)
{
    int             fd = -1;
    uint32          fsize = 0;
    uint32          totallen = 0;
    uint8*          uptr = NULL;
    uint8*          data = NULL;
    refresh_table*  rtable = NULL;
    refresh_tables* rtables = NULL;
    char            filepath[MAXPATH] = {0};

    snprintf(filepath, MAXPATH, "%s/%s", REFRESH_REFRESH, REFRESH_REFRESHTABLES);
    fd = osal_basic_open_file(filepath, O_RDWR | BINARY);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "open file %s error", filepath);
        return false;
    }
    fsize = osal_file_size(fd);
    data = rmalloc0(fsize);
    if (NULL == data)
    {
        osal_file_close(fd);
        fd = -1;
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }
    rmemset0(data, 0, '\0', fsize);
    osal_file_pread(fd, (char*)data, fsize, 0);
    osal_file_close(fd);
    fd = -1;
    uptr = data;
    rmemcpy1(&totallen, 0, uptr, 4);
    uptr += 4;
    totallen -= 4;
    while (0 != totallen)
    {
        if (NULL == rtables)
        {
            rtables = refresh_tables_init();
            if (NULL == rtables)
            {
                elog(RLOG_WARNING, "load refresh tables error");
                return NULL;
            }
        }
        rtable = refresh_table_init();
        if (NULL == rtable)
        {
            elog(RLOG_WARNING, "load refresh tables out of memory");
            refresh_freetables(rtables);
            return NULL;
        }
        refresh_tables_add(rtable, rtables);
        rmemcpy1(&rtable->oid, 0, uptr, 4);
        uptr += 4;
        totallen -= 4;
        /* schema */
        refresh_table_setschema((char*)uptr, rtable);
        totallen -= strlen((char*)uptr);
        uptr += strlen((char*)uptr);
        uptr += 1;
        totallen -= 1;
        /* table */
        refresh_table_settable((char*)uptr, rtable);
        totallen -= strlen((char*)uptr);
        uptr += strlen((char*)uptr);
        uptr += 1;
        totallen -= 1;
    }
    return rtables;
}
