#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/string/strtrim.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/regex/regex.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "catalog/catalog.h"
#include "port/file/fd.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "strategy/filter_dataset.h"

#define FILTER_MAXLINE 129

/* global variable */
extern List* g_table;
extern List* g_tableexclude;
extern List* g_addtablepattern;

/* ----- static function declaration begin ----- */
static bool filter_dataset_byoid(HTAB* oid2datasets, Oid oid);
/* ----- static function declaration end ----- */

static void filter_dataset_str2table(char* line, char* nspname, char* relname)
{
    char*  temp_nspname = line;
    char*  temp_relname = NULL;
    size_t nsp_len = 0;
    size_t rel_len = 0;
    int    index = 0;

    temp_relname = strstr(line, ".");
    nsp_len = temp_relname - temp_nspname;

    /* Skip the dot */
    temp_relname += 1;
    rel_len = strlen(line) - 1 - nsp_len;

    /* Maximum valid length for name is 63, including \\0 it's 64 */
    if (nsp_len > 63 || rel_len > 63)
    {
        elog(RLOG_ERROR, "when dealing filter dataset, invalid input: %s", line);
    }
    for (index = 0; index < nsp_len; index++)
    {
        if (*temp_nspname == ' ' || *temp_nspname == '\r' || *temp_nspname == '\t')
        {
            temp_nspname += 1;
            continue;
        }
        *nspname = *temp_nspname;
        temp_nspname += 1;
        nspname += 1;
    }

    for (index = 0; index < rel_len; index++)
    {
        if (*temp_relname == ' ' || *temp_relname == '\r' || *temp_relname == '\t')
        {
            temp_relname += 1;
            continue;
        }
        *relname = *temp_relname;
        temp_relname += 1;
        relname += 1;
    }
}

void filter_dataset_pairfree(filter_pair* filterpair)
{
    if (NULL == filterpair)
    {
        return;
    }

    if (NULL != filterpair->sch)
    {
        free_regexbase(filterpair->sch);
    }

    if (NULL != filterpair->table)
    {
        free_regexbase(filterpair->table);
    }

    rfree(filterpair);
}

/* Build filter strategy based on refreshtable */
List* filter_dataset_buildpairsbyrefreshtables(refresh_tables* rtables)
{
    List*          filters = NULL;
    ListCell*      lc = NULL;
    refresh_table* rtable = NULL;
    filter_pair*   filterpair = NULL;

    for (rtable = rtables->tables; NULL != rtable; rtable = rtable->next)
    {
        filterpair = (filter_pair*)rmalloc0(sizeof(filter_pair));
        if (NULL == filterpair)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            goto filter_dataset_initpairslistbyrefreshtables_error;
        }
        rmemset0(filterpair, 0, '\0', sizeof(filter_pair));
        filterpair->sch = (regex*)rmalloc0(sizeof(regex));
        if (NULL == filterpair->sch)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            goto filter_dataset_initpairslistbyrefreshtables_error;
        }
        rmemset0(filterpair->sch, 0, '\0', sizeof(regex));
        filterpair->table = (regex*)rmalloc0(sizeof(regex));
        if (NULL == filterpair->table)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            goto filter_dataset_initpairslistbyrefreshtables_error;
        }
        rmemset0(filterpair->table, 0, '\0', sizeof(regex));
        make_regexbase(filterpair->sch, rtable->schema);
        make_regexbase(filterpair->table, rtable->table);
        filters = lappend(filters, filterpair);
    }

    return filters;
filter_dataset_initpairslistbyrefreshtables_error:

    foreach (lc, filters)
    {
        filterpair = (filter_pair*)lfirst(lc);
        filter_dataset_pairfree(filterpair);
    }
    return NULL;
}

/* Rebuild refreshtables from filters */
bool filter_dataset_buildrefreshtablesbyfilters(refresh_tables** prtables, List* filters,
                                                HTAB* hnamespace, HTAB* hclass)
{
    bool                           found = false;
    Oid                            nspoid = INVALIDOID;
    ListCell*                      lc = NULL;
    filter_pair*                   filterpair = NULL;
    refresh_table*                 rtable = NULL;
    refresh_tables*                rtables = NULL;
    catalog_class_value*           classentry = NULL;
    catalog_namespace_value*       nspentry = NULL;
    pg_parser_sysdict_pgclass*     sysdictclass = NULL;
    pg_parser_sysdict_pgnamespace* sysdictnsp = NULL;

    HASH_SEQ_STATUS status;

    hash_seq_init(&status, hclass);
    while (NULL != (classentry = hash_seq_search(&status)))
    {
        found = false;
        nspoid = INVALIDOID;
        sysdictclass = classentry->class;

        /* Only keep 'r' and 'p' */
        if (!(sysdictclass->relkind == 'r' || sysdictclass->relkind == 'p'))
        {
            continue;
        }

        nspoid = sysdictclass->relnamespace;
        nspentry = hash_search(hnamespace, &nspoid, HASH_FIND, &found);
        if (!nspentry)
        {
            elog(RLOG_WARNING, "can't find namespace entry by oid: %u in filter init", nspoid);
            goto filter_dataset_genrefreshtablesbyfilters_error;
        }
        sysdictnsp = nspentry->namespace;

        found = false;
        foreach (lc, filters)
        {
            filterpair = (filter_pair*)lfirst(lc);
            if (cmp_regexbase(filterpair->sch, sysdictnsp->nspname.data) &&
                cmp_regexbase(filterpair->table, sysdictclass->relname.data))
            {
                found = true;
                break;
            }
        }

        if (false == found)
        {
            continue;
        }

        /* If it matches the rules, generate refreshtables */
        if (NULL == rtables)
        {
            rtables = refresh_tables_init();
            if (NULL == rtables)
            {
                elog(RLOG_WARNING, "generate refresh tables, out of memory");
                goto filter_dataset_genrefreshtablesbyfilters_error;
            }
        }

        rtable = refresh_table_init();
        if (NULL == rtable)
        {
            elog(RLOG_WARNING, "generate refresh tables, out of memory");
            goto filter_dataset_genrefreshtablesbyfilters_error;
        }

        refresh_table_setoid(sysdictclass->oid, rtable);
        refresh_table_setschema(sysdictnsp->nspname.data, rtable);
        refresh_table_settable(sysdictclass->relname.data, rtable);

        refresh_tables_add(rtable, rtables);
    }

    *prtables = rtables;
    return true;
filter_dataset_genrefreshtablesbyfilters_error:

    refresh_freetables(rtables);
    return false;
}

/* Build filter strategy */
static List* filter_dataset_initpairslist(List* rulelist)
{
    char*        cptr = NULL;
    char*        include = NULL;
    ListCell*    cell = NULL;
    List*        filter_list = NULL;
    filter_pair* filter_pair_obj = NULL;

    char table[64] = {'\0'};
    char schema[64] = {'\0'};
    char temp[1024] = {'\0'};

    /* Build sync strategy based on input parameters */
    foreach (cell, rulelist)
    {
        include = (char*)lfirst(cell);
        rmemset1(temp, 0, '\0', 1024);
        rmemset1(schema, 0, '\0', 64);
        rmemset1(table, 0, '\0', 64);

        strcpy(temp, include);
        if (strlen(temp) == strlen("*") && 0 == strcmp(temp, "*"))
        {
            rmemcpy1(temp, 0, "*.*", strlen("*.*"));
        }

        /* Clean up invalid characters (spaces, tabs, newlines) */
        cptr = rightstrtrim(temp);
        if ('\0' == cptr[0])
        {
            /* Empty line */
            continue;
        }

        /* Split into schema.table format */
        filter_dataset_str2table(temp, schema, table);

        filter_pair_obj = (filter_pair*)rmalloc0(sizeof(filter_pair));
        if (NULL == filter_pair_obj)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(filter_pair_obj, 0, '\0', sizeof(filter_pair));
        filter_pair_obj->sch = (regex*)rmalloc0(sizeof(regex));
        if (NULL == filter_pair_obj->sch)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(filter_pair_obj->sch, 0, '\0', sizeof(regex));
        filter_pair_obj->table = (regex*)rmalloc0(sizeof(regex));
        if (NULL == filter_pair_obj->table)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(filter_pair_obj->table, 0, '\0', sizeof(regex));
        make_regexbase(filter_pair_obj->sch, schema);
        make_regexbase(filter_pair_obj->table, table);
        filter_list = lappend(filter_list, filter_pair_obj);
    }
    return filter_list;
}

/* Delete filter strategy */
void filter_dataset_listpairsfree(List* rulelist)
{
    ListCell*    lc = NULL;
    filter_pair* filter_pair_obj = NULL;
    if (NULL == rulelist)
    {
        return;
    }

    /* Delete content in rulelist */
    foreach (lc, rulelist)
    {
        filter_pair_obj = (filter_pair*)lfirst(lc);
        free_regexbase(filter_pair_obj->sch);
        free_regexbase(filter_pair_obj->table);
        rfree(filter_pair_obj);
    }
    list_free(rulelist);
    rulelist = NULL;
    return;
}

static bool filter_dataset_match_addtablepattern(List* addtablepattern, char* schema, char* table)
{
    bool         result = false;
    ListCell*    lc = NULL;
    filter_pair* filter_pair_obj = NULL;

    if (NULL == addtablepattern)
    {
        return true;
    }

    foreach (lc, addtablepattern)
    {
        filter_pair_obj = (filter_pair*)lfirst(lc);

        if (cmp_regexbase(filter_pair_obj->sch, schema) &&
            cmp_regexbase(filter_pair_obj->table, table))
        {
            result = true;
            break;
        }
    }

    return result;
}

/* Look up oid in hash table to check if it exists */
static bool filter_dataset_byoid(HTAB* oid2datasets, Oid oid)
{
    bool find = false;

    hash_search(oid2datasets, &oid, HASH_FIND, &find);
    return find;
}

/* Initialize sync dataset */
List* filter_dataset_inittableinclude(List* table)
{
    /* Clear filter rules */
    filter_dataset_listpairsfree(table);

    table = filter_dataset_initpairslist(g_table);
    return table;
}

/* Initialize sync exclusion table */
List* filter_dataset_inittableexclude(List* tableexclude)
{
    /* Clear filter rules */
    filter_dataset_listpairsfree(tableexclude);

    tableexclude = filter_dataset_initpairslist(g_tableexclude);
    return tableexclude;
}

List* filter_dataset_initaddtablepattern(List* tablepattern)
{
    /* Clear filter rules */
    filter_dataset_listpairsfree(tablepattern);

    tablepattern = filter_dataset_initpairslist(g_addtablepattern);

    return tablepattern;
}

/*
 * Generate sync dataset and persist to disk
 * 1. Get filter strategy from GUC parameters
 * 2. Iterate through pg_class hash table, get records, only get records with relkind 'r' and 'p'
 * 3. Based on records, look up pg_namespace table to get nspname
 * 4. Concatenate name nspname.relname for regex matching
 * 5. If matched, persist nspname.relname to disk
 * 6. Write to file after iteration completes
 */
bool filter_dataset_init(List* tbincludes, List* tbexcludes, HTAB* namespace, HTAB* class)
{
    HASH_SEQ_STATUS                status;
    catalog_class_value*           class_value_entry = NULL;
    catalog_namespace_value*       nsp_value_entry = NULL;
    pg_parser_sysdict_pgclass*     temp_class = NULL;
    pg_parser_sysdict_pgnamespace* temp_nsp = NULL;
    char                           filepath_tmp[MAXPATH] = {'\0'};
    char                           filepath[MAXPATH] = {'\0'};
    struct stat                    statbuf;
    FILE*                          fp = NULL;

    /* 64 + 64 + 1 + 1 - 1 */
    /* nspname + relname + . + \\n - 1 null terminator */
    char nsp_relname[129] = {'\0'};

    snprintf(filepath_tmp, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET_TMP);
    snprintf(filepath, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET);

    /* Check if dataset temporary file exists */
    if (0 == stat(filepath_tmp, &statbuf))
    {
        /* Exists, unlink it first */
        osal_durable_unlink(filepath_tmp, RLOG_DEBUG);
    }

    /* Open dataset file */
    fp = osal_allocate_file(filepath_tmp, "w+");

    /* Iterate through pg_class system table to select datasets matching filter conditions */
    hash_seq_init(&status, class);
    while (NULL != (class_value_entry = hash_seq_search(&status)))
    {
        Oid  temp_nsp_oid = INVALIDOID;
        bool temp_nsp_find = false;

        temp_class = class_value_entry->class;

        /* Only keep 'r' and 'p' */
        if (!(temp_class->relkind == 'r' || temp_class->relkind == 'p'))
        {
            continue;
        }

        temp_nsp_oid = temp_class->relnamespace;
        nsp_value_entry = hash_search(namespace, &temp_nsp_oid, HASH_FIND, &temp_nsp_find);
        if (!nsp_value_entry)
        {
            elog(RLOG_ERROR, "can't find namespace entry by oid: %u in filter init", temp_nsp_oid);
        }
        temp_nsp = nsp_value_entry->namespace;

        sprintf(nsp_relname, "%s.%s\n", temp_nsp->nspname.data, temp_class->relname.data);

        if (tbexcludes)
        {
            ListCell* temp_cell = NULL;
            bool      exclude = false;

            foreach (temp_cell, tbexcludes)
            {
                filter_pair* temp_pair = (filter_pair*)lfirst(temp_cell);
                if (cmp_regexbase(temp_pair->sch, temp_nsp->nspname.data) &&
                    cmp_regexbase(temp_pair->table, temp_class->relname.data))
                {
                    exclude = true;
                    break;
                }
            }
            /* Matches exclude regex, skip this record */
            if (exclude)
            {
                continue;
            }
        }

        if (tbincludes)
        {
            ListCell* temp_cell = NULL;
            bool      include = false;

            foreach (temp_cell, tbincludes)
            {
                filter_pair* temp_pair = (filter_pair*)lfirst(temp_cell);
                if (cmp_regexbase(temp_pair->sch, temp_nsp->nspname.data) &&
                    cmp_regexbase(temp_pair->table, temp_class->relname.data))
                {
                    include = true;
                    break;
                }
            }
            /* Does not match include regex, exclude */
            if (!include)
            {
                continue;
            }
        }

        /* Write this record */
        fwrite(nsp_relname, strlen(nsp_relname), 1, fp);

        /* Reset */
        rmemset1(nsp_relname, 0, 0, 129);
    }

    /* Close temporary file */
    osal_free_file(fp);

    /* Rename temporary file */
    if (osal_durable_rename(filepath_tmp, filepath, RLOG_WARNING) != 0)
    {
        elog(RLOG_ERROR, "Error renaming file %s to %s", filepath_tmp, filepath);
    }
    return true;
}

/*
 * Load sync dataset
 * 1. Initialize required variables and structures, initialize regex matching structures, open
 * filter file
 * 2. From file start, use fgets() to read one line at a time
 * 3. Save data to hash table dataset2oid until fgets() returns NULL
 * 4. Iterate through class hash table, query namespace hash table, save matching oids to
 * dataset2oid
 * 5. Iterate through dataset2oid hash table, remove invalid data, save to oid2dataset hash table
 * 6. Cleanup dataset2oid hash table, return oid2dataset hash table
 */
HTAB* filter_dataset_load(HTAB* namespace, HTAB* class)
{
    FILE*                          fp = NULL;
    HASH_SEQ_STATUS                status_class;
    HASH_SEQ_STATUS                status_d2o;
    catalog_class_value*           class_value_entry = NULL;
    catalog_namespace_value*       nsp_value_entry = NULL;
    pg_parser_sysdict_pgclass*     temp_class = NULL;
    pg_parser_sysdict_pgnamespace* temp_nsp = NULL;
    filter_dataset2oidnode*        scan_d2o_entry = NULL;
    HTAB*                          dataset2oid_htab = NULL;
    HTAB*                          oid2dataset_htab = NULL;
    HASHCTL                        hashCtl_d2o = {'\0'};
    HASHCTL                        hashCtl_o2d = {'\0'};
    struct stat                    st;
    char                           filepath[MAXPATH] = {'\0'};
    char                           fline[1024] = {'\0'};
    char                           temp_nspname[64] = {'\0'};
    char                           temp_relname[64] = {'\0'};
    int                            line_num = 0;

    /* Get file path */
    snprintf(filepath, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET);

    /* Check if file exists */
    if (-1 == stat(filepath, &st))
    {
        if (ENOENT != errno)
        {
            elog(RLOG_ERROR, "stat %s error, %s", filepath, strerror(errno));
        }

        /* make compiler happy */
        return NULL;
    }

    fp = osal_file_fopen(filepath, "r");
    if (NULL == fp)
    {
        elog(RLOG_ERROR, "open %s failed", filepath);

        /* make compiler happy */
        return NULL;
    }

    /* First read dataset, create dataset2oid hash */
    hashCtl_d2o.keysize = sizeof(filter_dataset);
    hashCtl_d2o.entrysize = sizeof(filter_dataset2oidnode);

    dataset2oid_htab = hash_create("filter_d2o_htab", 256, &hashCtl_d2o, HASH_ELEM | HASH_BLOBS);

    while (fgets(fline, FILTER_MAXLINE, fp))
    {
        int                     line_len = 0;
        filter_dataset          temp_dataset_key = {{'\0'}};
        filter_dataset2oidnode* temp_d2o_entry = NULL;

        line_len = strlen(fline);
        /* Remove newline character */
        if ('\n' == fline[line_len - 1])
        {
            fline[line_len - 1] = '\0';
            line_len--;
            if (0 == line_len)
            {
                /* Empty line */
                continue;
            }
        }
        /* Windows text remove carriage return */
        if ('\r' == fline[line_len - 1])
        {
            fline[line_len - 1] = '\0';
            line_len--;
            if (0 == line_len)
            {
                /* Empty line */
                continue;
            }
        }
        if (0 == line_len)
        {
            /* Empty line */
            continue;
        }
        filter_dataset_str2table(fline, temp_nspname, temp_relname);
        strcpy(temp_dataset_key.schema, temp_nspname);
        strcpy(temp_dataset_key.table, temp_relname);

        temp_d2o_entry = hash_search(dataset2oid_htab, &temp_dataset_key, HASH_ENTER, NULL);
        if (!temp_d2o_entry)
        {
            elog(RLOG_ERROR, "can't insert dataset2oid_htab hash");
        }
        strcpy(temp_d2o_entry->dataset.schema, temp_dataset_key.schema);
        strcpy(temp_d2o_entry->dataset.table, temp_dataset_key.table);

        /* Oid temporarily assigned as 0 */
        temp_d2o_entry->oid = INVALIDOID;

        /* Reset */
        rmemset1(temp_nspname, 0, 0, 64);
        rmemset1(temp_relname, 0, 0, 64);

        /* Count */
        line_num++;
    }

    /* No dataset */
    if (line_num == 0)
    {
        /* Cleanup, return NULL */
        hash_destroy(dataset2oid_htab);
        return NULL;
    }

    /* Iterate through pg_class system table to fill oid in dataset2oid hash table */
    hash_seq_init(&status_class, class);
    while (NULL != (class_value_entry = hash_seq_search(&status_class)))
    {
        filter_dataset          temp_dataset_key = {{'\0'}};
        filter_dataset2oidnode* temp_d2o_entry = NULL;
        Oid                     temp_nsp_oid = INVALIDOID;
        bool                    temp_nsp_find = false;
        bool                    temp_d2o_find = false;

        temp_class = class_value_entry->class;

        /* Only keep 'r' and 'p' */
        if (!(temp_class->relkind == 'r' || temp_class->relkind == 'p'))
        {
            continue;
        }

        temp_nsp_oid = temp_class->relnamespace;
        nsp_value_entry = hash_search(namespace, &temp_nsp_oid, HASH_FIND, &temp_nsp_find);
        if (!nsp_value_entry)
        {
            elog(RLOG_ERROR, "can't find namespace entry by oid: %u in filter init", temp_nsp_oid);
        }
        temp_nsp = nsp_value_entry->namespace;

        strcpy(temp_dataset_key.schema, temp_nsp->nspname.data);
        strcpy(temp_dataset_key.table, temp_class->relname.data);

        temp_d2o_entry =
            hash_search(dataset2oid_htab, &temp_dataset_key, HASH_FIND, &temp_d2o_find);
        if (temp_d2o_find)
        {
            temp_d2o_entry->oid = temp_class->oid;
        }
    }

    /* Create oid2dataset hash */
    hashCtl_o2d.keysize = sizeof(Oid);
    hashCtl_o2d.entrysize = sizeof(filter_oid2datasetnode);

    oid2dataset_htab = hash_create("filter_o2d_htab", 256, &hashCtl_o2d, HASH_ELEM | HASH_BLOBS);

    /* Iterate through dataset2oid to generate oid2dataset */
    hash_seq_init(&status_d2o, dataset2oid_htab);
    while (NULL != (scan_d2o_entry = hash_seq_search(&status_d2o)))
    {
        Oid                     temp_oid = scan_d2o_entry->oid;
        filter_oid2datasetnode* temp_o2d_entry = NULL;

        if (temp_oid == INVALIDOID)
        {
            elog(RLOG_WARNING, "when convert dataset2oid to oid2dataset, oid invalid, table: %s.%s",
                 scan_d2o_entry->dataset.schema, scan_d2o_entry->dataset.table);
        }

        temp_o2d_entry = hash_search(oid2dataset_htab, &temp_oid, HASH_ENTER, NULL);
        if (!temp_o2d_entry)
        {
            elog(RLOG_ERROR, "can't insert oid2dataset_htab hash");
        }
        temp_o2d_entry->oid = temp_oid;
        strcpy(temp_o2d_entry->dataset.schema, scan_d2o_entry->dataset.schema);
        strcpy(temp_o2d_entry->dataset.table, scan_d2o_entry->dataset.table);
    }

    /* Cleanup work */
    hash_destroy(dataset2oid_htab);
    return oid2dataset_htab;
}

/*
 * Load transaction filter dataset
 * 1. Iterate through namespace hash table to get namespaceoid
 * 2. Iterate through class hash table, find oid of table matching namespaceoid and tablename
 * 3. Generate oid2dataset hash table, return oid2dataset hash table
 */
HTAB* filter_dataset_txnfilterload(HTAB* namespace, HTAB* class)
{
    HASH_SEQ_STATUS                status_class;
    HASH_SEQ_STATUS                status_nap;
    bool                           findnsp = false;
    HASHCTL                        hashCtl_o2d = {'\0'};
    char*                          schema = NULL;
    HTAB*                          oid2dataset_htab = NULL;
    catalog_class_value*           class_entry = NULL;
    catalog_namespace_value*       nsp_entry = NULL;
    pg_parser_sysdict_pgclass*     temp_class = NULL;
    pg_parser_sysdict_pgnamespace* temp_nsp = NULL;
    filter_oid2datasetnode*        o2d_entry = NULL;

    /* Create oid2dataset hash */
    hashCtl_o2d.keysize = sizeof(Oid);
    hashCtl_o2d.entrysize = sizeof(filter_oid2datasetnode);

    oid2dataset_htab = hash_create("filter_o2d_htab", 128, &hashCtl_o2d, HASH_ELEM | HASH_BLOBS);

    schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);
    if (NULL == schema)
    {
        return oid2dataset_htab;
    }

    /* Iterate through pg_namespace system table, get pg_parser_sysdict_pgnamespace based on schema
     */
    hash_seq_init(&status_nap, namespace);
    while (NULL != (nsp_entry = hash_seq_search(&status_nap)))
    {
        temp_nsp = nsp_entry->namespace;
        if (0 == strcmp(temp_nsp->nspname.data, schema))
        {
            findnsp = true;
            break;
        }
    }

    if (false == findnsp)
    {
        elog(RLOG_WARNING, "can't find schema from pg_namespace, %s", schema);
        return oid2dataset_htab;
    }

    /* Iterate through pg_class system table to get table's oid */
    hash_seq_init(&status_class, class);
    while (NULL != (class_entry = hash_seq_search(&status_class)))
    {
        temp_class = class_entry->class;
        if (0 == strcmp(temp_class->relname.data, SYNC_STATUSTABLE_NAME) &&
            temp_nsp->oid == temp_class->relnamespace)
        {
            o2d_entry = hash_search(oid2dataset_htab, &temp_class->oid, HASH_ENTER, NULL);
            if (!o2d_entry)
            {
                elog(RLOG_WARNING, "can't insert oid2dataset_htab hash");
                return NULL;
            }
            o2d_entry->oid = temp_class->oid;
            strcpy(o2d_entry->dataset.schema, temp_nsp->nspname.data);
            strcpy(o2d_entry->dataset.table, temp_class->nspname.data);
            return oid2dataset_htab;
        }
    }
    return oid2dataset_htab;
}

/*
 * Reload dataset
 * 1. Call filter_dataset_free to release original dataset
 * 2. Call filter_dataset_load to load from database
 * 3. Return the loaded dataset hash table
 */
HTAB* filter_dataset_reload(HTAB* namespace, HTAB* class, HTAB* oid2datasets)
{
    filter_dataset_free(oid2datasets);
    return filter_dataset_load(namespace, class);
}

/*
 * Check if DML exists in the table records that need to be captured
 * Calls filter_dataset_byoid
 */
bool filter_dataset_dml(HTAB* oid2datasets, Oid oid)
{
    if (!oid2datasets)
    {
        return false;
    }
    return filter_dataset_byoid(oid2datasets, oid);
}

/*
 * Check if DDL exists in the table records that need to be captured
 * Calls filter_dataset_byoid
 */
bool filter_dataset_ddl(HTAB* oid2datasets, Oid oid)
{
    if (!oid2datasets)
    {
        return false;
    }
    return filter_dataset_byoid(oid2datasets, oid);
}

/*
 * Check if the table in CREATE TABLE statement matches the capture logic
 */
bool filter_dataset_matchforcreate(List* tablepattern, char* schema, char* table)
{
    bool      result = false;
    ListCell* lc = NULL;
    if (NULL == tablepattern)
    {
        return true;
    }

    foreach (lc, tablepattern)
    {
        filter_pair* temp_pair = (filter_pair*)lfirst(lc);
        /* Matches regex, set result to true */
        if (cmp_regexbase(temp_pair->sch, schema) && cmp_regexbase(temp_pair->table, table))
        {
            result = true;
            break;
        }
    }

    return result;
}

/*
 * Add a record to dataset hash
 */
bool filter_dataset_add(HTAB* oid2datasets, Oid oid, char* schema, char* table)
{
    filter_oid2datasetnode* o2d_entry = NULL;
    o2d_entry = hash_search(oid2datasets, &oid, HASH_ENTER, NULL);
    if (!o2d_entry)
    {
        elog(RLOG_WARNING, "can't find entry to oid2datasets hash, oid: %u, %s.%s", oid, schema,
             table);
    }

    o2d_entry->oid = oid;
    strcpy(o2d_entry->dataset.schema, schema);
    strcpy(o2d_entry->dataset.table, table);

    return true;
}

/*
 * Modify a record in dataset hash
 */
bool filter_dataset_modify(HTAB* oid2datasets, Oid oid, char* schema, char* table)
{
    filter_oid2datasetnode* o2d_entry = NULL;
    o2d_entry = hash_search(oid2datasets, &oid, HASH_FIND, NULL);
    if (!o2d_entry)
    {
        elog(RLOG_DEBUG, "can't find entry from oid2datasets hash, oid: %u, %s.%s", oid, schema,
             table);
        return false;
    }

    o2d_entry->oid = oid;
    strcpy(o2d_entry->dataset.schema, schema);
    strcpy(o2d_entry->dataset.table, table);

    return true;
}

/*
 * Delete a record from dataset hash
 */
bool filter_dataset_delete(HTAB* oid2datasets, Oid oid)
{
    bool find = false;
    hash_search(oid2datasets, &oid, HASH_REMOVE, &find);
    if (!find)
    {
        elog(RLOG_DEBUG, "can't elete entry from oid2datasets hash, oid: %u", oid);
        return false;
    }

    return true;
}

/*
 * Persist dataset to disk
 */
bool filter_dataset_flush(HTAB* oid2datasets)
{
    HASH_SEQ_STATUS         status;
    struct stat             statbuf;
    FILE*                   fp = NULL;
    filter_oid2datasetnode* o2d_entry = NULL;
    char                    filepath_tmp[MAXPATH] = {'\0'};
    char                    filepath[MAXPATH] = {'\0'};

    /* 64 + 64 + 1 + 1 - 1 */
    /* nspname + relname + . + \\n - 1 null terminator */
    char nsp_relname[129] = {'\0'};

    snprintf(filepath_tmp, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET_TMP);
    snprintf(filepath, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET);

    /* Check if dataset temporary file exists */
    if (0 == stat(filepath_tmp, &statbuf))
    {
        /* Exists, unlink it first */
        osal_durable_unlink(filepath_tmp, RLOG_DEBUG);
    }

    /* Open dataset file */
    fp = osal_allocate_file(filepath_tmp, "w+");

    /* Iterate through pg_class system table to select datasets matching filter conditions */
    hash_seq_init(&status, oid2datasets);
    while (NULL != (o2d_entry = hash_seq_search(&status)))
    {
        sprintf(nsp_relname, "%s.%s\n", o2d_entry->dataset.schema, o2d_entry->dataset.table);

        /* Write this record */
        fwrite(nsp_relname, strlen(nsp_relname), 1, fp);

        /* Reset */
        rmemset1(nsp_relname, 0, 0, 129);
    }

    /* Close temporary file */
    osal_free_file(fp);

    /* Rename temporary file */
    if (osal_durable_rename(filepath_tmp, filepath, RLOG_WARNING) != 0)
    {
        elog(RLOG_ERROR, "Error renaming file %s to %s", filepath_tmp, filepath);
    }
    return true;
}

/*
 * Free dataset hash
 */
void filter_dataset_free(HTAB* oid2datasets)
{
    hash_destroy(oid2datasets);
}

refresh_tables* filter_dataset_buildrefreshtables(HTAB* hfilters)
{
    HASH_SEQ_STATUS         status;
    filter_oid2datasetnode* entry = NULL;
    refresh_table*          table = NULL;
    refresh_tables*         refreshtables = NULL;

    refreshtables = refresh_tables_init();

    if (NULL == hfilters)
    {
        return refreshtables;
    }

    hash_seq_init(&status, hfilters);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        if (FirstNormalObjectId > entry->oid)
        {
            continue;
        }
        table = refresh_table_init();

        refresh_table_setschema(entry->dataset.schema, table);
        refresh_table_settable(entry->dataset.table, table);
        refresh_table_setoid(entry->oid, table);

        refresh_tables_add(table, refreshtables);
    }
    return refreshtables;
}

bool filter_dataset_updatedatasets(List* addtablepattern, HTAB* namespace, List* sysdicthis,
                                   HTAB* syncdatasets)
{
    bool                     found = false;
    bool                     haschange = false;
    char*                    table = NULL;
    char*                    schema = NULL;
    ListCell*                lc = NULL;
    catalogdata*             catalog_data = NULL;
    catalog_class_value*     classvalue = NULL;
    catalog_namespace_value* nspvalue = NULL;

    if (NULL == syncdatasets)
    {
        return false;
    }

    foreach (lc, sysdicthis)
    {
        catalog_data = (catalogdata*)lfirst(lc);

        if (NULL == catalog_data || NULL == catalog_data->catalog)
        {
            return false;
        }
        if (CATALOG_TYPE_CLASS == catalog_data->type)
        {
            classvalue = (catalog_class_value*)catalog_data->catalog;
            elog(RLOG_DEBUG, "syncdatasets op:%d, classvalue, %s, %u.%u", catalog_data->op,
                 classvalue->class->relname.data, classvalue->class->oid,
                 classvalue->class->relnamespace);

            if ('p' != classvalue->class->relkind && 'r' != classvalue->class->relkind)
            {
                continue;
            }

            table = classvalue->class->relname.data;
            nspvalue =
                hash_search(namespace, &(classvalue->class->relnamespace), HASH_FIND, &found);
            if (found)
            {
                schema = nspvalue->namespace->nspname.data;
            }
            else
            {
                catalog_data->op = CATALOG_OP_DELETE;
            }

            if (CATALOG_OP_INSERT == catalog_data->op)
            {
                if (filter_dataset_match_addtablepattern(addtablepattern, schema, table))
                {
                    if (filter_dataset_add(syncdatasets, classvalue->class->oid, schema, table))
                    {
                        haschange = true;
                    }
                }
            }
            else if (CATALOG_OP_DELETE == catalog_data->op)
            {
                if (filter_dataset_delete(syncdatasets, classvalue->class->oid))
                {
                    haschange = true;
                }
            }
            else if (CATALOG_OP_UPDATE == catalog_data->op)
            {
                if (filter_dataset_modify(syncdatasets, classvalue->class->oid, schema, table))
                {
                    haschange = true;
                }
            }
            else
            {
                elog(RLOG_ERROR, "unknown op:%d", catalog_data->op);
            }
        }
    }
    return haschange;
}

void filter_dataset_updatedatasets_onlinerefresh(HTAB* dataset, List* tables_list)
{
    ListCell* cell = NULL;

    foreach (cell, tables_list)
    {
        refresh_table* table = lfirst(cell);

        filter_dataset_add(dataset, table->oid, table->schema, table->table);
    }
}
