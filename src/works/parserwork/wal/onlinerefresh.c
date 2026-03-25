#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/string/strtrim.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/uuid/uuid.h"
#include "utils/regex/regex.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "catalog/catalog.h"
#include "port/file/fd.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "strategy/filter_dataset.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"

#define ONLINEREFRESH_MAXLINE 129

static bool get_nspname_relname_from_line(char* line, char* nspname, char* relname)
{
    char*  temp_nspname = line;
    char*  temp_relname = NULL;
    size_t nsp_len = 0;
    size_t rel_len = 0;
    int    index = 0;

    temp_relname = strstr(line, ".");
    if (NULL == temp_relname)
    {
        elog(RLOG_WARNING, "%s configure rule: schema.table, %s", ONLINEREFRESH_DAT, line);
        return false;
    }
    nsp_len = temp_relname - temp_nspname;

    /* Skip the dot */
    temp_relname += 1;
    rel_len = strlen(line) - 1 - nsp_len;

    /* Maximum valid length for name is 63, including \0 is 64 */
    if (nsp_len > 63 || rel_len > 63)
    {
        elog(RLOG_WARNING, "when dealing filter dataset, invalid input: %s", line);
        return false;
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

    return true;
}

static void online_refresh_status_write_success(void)
{
    StringInfoData path = {'\0'};
    int            fd = -1;

    initStringInfo(&path);

    appendStringInfo(&path, "%s/%s", guc_getConfigOption(CFG_KEY_DATA), ONLINEREFRESH_STATUS);

    fd = osal_basic_open_file(path.data, O_RDWR | O_CREAT | BINARY);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "open file:%s error, %s", path.data, strerror(errno));
        return;
    }

    osal_file_write(fd, "SUCCESS\n", strlen("SUCCESS\n"));

    osal_file_close(fd);
    rfree(path.data);
}

static void online_refresh_status_write_error_table(List* tables)
{
    ListCell*      cell = NULL;
    StringInfoData str = {'\0'};
    StringInfoData path = {'\0'};
    int            fd = -1;

    if (!tables)
    {
        return;
    }

    initStringInfo(&str);
    initStringInfo(&path);

    appendStringInfo(&str, "ERROR\n");
    foreach (cell, tables)
    {
        char* temp_tables = (char*)lfirst(cell);
        appendStringInfo(&str, "%s\n", temp_tables);
    }

    appendStringInfo(&path, "%s/%s", guc_getConfigOption(CFG_KEY_DATA), ONLINEREFRESH_STATUS);

    fd = osal_basic_open_file(path.data, O_RDWR | O_CREAT | BINARY);
    if (-1 == fd)
    {
        elog(RLOG_WARNING, "open file:%s error, %s", path.data, strerror(errno));
        return;
    }

    osal_file_write(fd, str.data, strlen(str.data));

    osal_file_close(fd);
    rfree(str.data);
    rfree(path.data);
}

List* onlinerefresh_get_newtable(HTAB* dataset, refresh_tables* tables)
{
    refresh_table* table = NULL;
    List*          result = NULL;
    bool           find = false;
    refresh_table* table_list_node = NULL;

    if (!tables)
    {
        return NULL;
    }

    table = tables->tables;

    while (table)
    {
        hash_search(dataset, &table->oid, HASH_FIND, &find);
        if (!find)
        {
            table_list_node = refresh_table_init();
            table_list_node->oid = table->oid;
            table_list_node->schema = rstrdup(table->schema);
            table_list_node->table = rstrdup(table->table);

            result = lappend(result, table_list_node);
            table_list_node = NULL;
        }
        table = table->next;
    }
    return result;
}

/* Populate refreshtables from system catalogs */
bool onlinerefresh_rebuildrefreshtables(refresh_tables* rtables, HTAB* hnamespace, HTAB* hclass,
                                        bool* bmatch)
{
    /*
     * 1. Build filters from rtables
     *    Fuzzy matching may occur, so handle it here
     * 2. Repopulate rtables based on filters
     */
    List*           filters = NULL;
    refresh_tables* nrtables = NULL;

    /* Build filters set */
    filters = filter_dataset_buildpairsbyrefreshtables(rtables);
    if (NULL == filters)
    {
        elog(RLOG_WARNING, "build filters error");
        return false;
    }

    /* Build refresh tables from filters and system catalog */
    if (false == filter_dataset_buildrefreshtablesbyfilters(&nrtables, filters, hnamespace, hclass))
    {
        elog(RLOG_WARNING, "build refresh tables by filters error");
        return false;
    }

    if (NULL == nrtables)
    {
        *bmatch = false;
        return true;
    }

    /* Free tables in rtables */
    refresh_freetable(rtables->tables);
    rtables->cnt = nrtables->cnt;
    rtables->tables = nrtables->tables;
    nrtables->tables = NULL;
    nrtables->cnt = 0;

    refresh_freetables(nrtables);
    return true;
}

/*
 * Load onlinerefresh dataset
 */
refresh_tables* onlinerefresh_data_load(HTAB* namespace, HTAB* class)
{
    int                            line_num = 0;
    FILE*                          fp = NULL;
    char*                          cptr = NULL;
    char*                          data_path = NULL;
    List*                          error_table = NULL;
    HTAB*                          dataset2oid_htab = NULL;
    refresh_tables*                result = NULL;
    refresh_table*                 online_refresh_table = NULL;
    catalog_class_value*           class_value_entry = NULL;
    catalog_namespace_value*       nsp_value_entry = NULL;
    pg_parser_sysdict_pgclass*     temp_class = NULL;
    pg_parser_sysdict_pgnamespace* temp_nsp = NULL;
    filter_dataset2oidnode*        scan_d2o_entry = NULL;
    HASH_SEQ_STATUS                status_class;
    HASH_SEQ_STATUS                status_d2o;
    HASHCTL                        hashCtl_d2o = {'\0'};
    struct stat                    st;
    char                           temp_nspname[NAMEDATALEN] = {'\0'};
    char                           temp_relname[NAMEDATALEN] = {'\0'};
    char                           filepath[MAXPATH] = {'\0'};
    char                           fline[LINESIZE] = {'\0'};

    data_path = guc_getConfigOption(CFG_KEY_DATA);

    /* Get file path */
    snprintf(filepath, MAXPATH, "%s/%s", data_path, ONLINEREFRESH_DAT);

    /* Check if file exists */
    if (-1 == stat(filepath, &st))
    {
        if (ENOENT != errno)
        {
            elog(RLOG_WARNING, "stat %s error, %s", filepath, strerror(errno));
            return NULL;
        }

        elog(RLOG_WARNING, "%s not exist", filepath);
        return NULL;
    }

    fp = osal_file_fopen(filepath, "r");
    if (NULL == fp)
    {
        elog(RLOG_WARNING, "open %s failed", filepath);

        /* make compiler happy */
        return NULL;
    }

    result = rmalloc0(sizeof(refresh_tables));
    if (!result)
    {
        elog(RLOG_WARNING, "oom");
        return NULL;
    }
    rmemset0(result, 0, 0, sizeof(refresh_tables));

    /* Read dataset first, create dataset2oid hash */
    hashCtl_d2o.keysize = sizeof(filter_dataset);
    hashCtl_d2o.entrysize = sizeof(filter_dataset2oidnode);

    dataset2oid_htab =
        hash_create("onlinerefresh_d2o_htab", 256, &hashCtl_d2o, HASH_ELEM | HASH_BLOBS);

    while (fgets(fline, ONLINEREFRESH_MAXLINE, fp))
    {
        filter_dataset          temp_dataset_key = {{'\0'}};
        filter_dataset2oidnode* temp_d2o_entry = NULL;

        /* Comment line, skip */
        cptr = fline;
        if ('#' == *cptr)
        {
            continue;
        }

        cptr = leftstrtrim(fline, LINESIZE);
        if ('\0' == *cptr)
        {
            continue;
        }

        /* Trim trailing spaces and tabs */
        cptr = rightstrtrim(fline);
        if ('\0' == fline[0])
        {
            continue;
        }

        if (false == get_nspname_relname_from_line(fline, temp_nspname, temp_relname))
        {
            elog(RLOG_WARNING, "parse %s -->%s to schema table error", ONLINEREFRESH_DAT, fline);
            return NULL;
        }
        strcpy(temp_dataset_key.schema, temp_nspname);
        strcpy(temp_dataset_key.table, temp_relname);

        temp_d2o_entry = hash_search(dataset2oid_htab, &temp_dataset_key, HASH_ENTER, NULL);
        if (!temp_d2o_entry)
        {
            elog(RLOG_WARNING, "can't insert dataset2oid_htab hash");
            return NULL;
        }
        strcpy(temp_d2o_entry->dataset.schema, temp_dataset_key.schema);
        strcpy(temp_d2o_entry->dataset.table, temp_dataset_key.table);

        /* Initialize oid to INVALIDOID */
        temp_d2o_entry->oid = INVALIDOID;

        /* Reset */
        rmemset1(temp_nspname, 0, 0, 64);

        /* Count */
        line_num++;
    }

    /* No dataset */
    if (line_num == 0)
    {
        /* Cleanup and return NULL */
        hash_destroy(dataset2oid_htab);
        return NULL;
    }

    /* Iterate pg_class system table, populate oid in dataset2oid hash */
    hash_seq_init(&status_class, class);
    while (NULL != (class_value_entry = hash_seq_search(&status_class)))
    {
        filter_dataset          temp_dataset_key = {{'\0'}};
        filter_dataset2oidnode* temp_d2o_entry = NULL;
        Oid                     temp_nsp_oid = INVALIDOID;
        bool                    temp_nsp_find = false;
        bool                    temp_d2o_find = false;

        temp_class = class_value_entry->class;

        /* Only keep r (regular table) and p (partitioned table) */
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

    /* Recount to avoid duplicate lines */
    line_num = 0;

    /* Iterate dataset2oid to generate list */
    hash_seq_init(&status_d2o, dataset2oid_htab);
    while (NULL != (scan_d2o_entry = hash_seq_search(&status_d2o)))
    {
        Oid temp_oid = scan_d2o_entry->oid;

        if (temp_oid == INVALIDOID)
        {
            char temp_table_name[130] = {'\0'};
            elog(RLOG_WARNING, "when load online refresh dat, get oid invalid, table: %s.%s",
                 scan_d2o_entry->dataset.schema, scan_d2o_entry->dataset.table);
            sprintf(temp_table_name, "%s.%s", scan_d2o_entry->dataset.schema,
                    scan_d2o_entry->dataset.table);
            error_table = lappend(error_table, rstrdup(temp_table_name));
            continue;
        }

        /* Increment count */
        line_num++;

        /* Allocate space for refresh table */
        if (!online_refresh_table)
        {
            online_refresh_table = rmalloc0(sizeof(refresh_table));
            if (!online_refresh_table)
            {
                elog(RLOG_ERROR, "oom");
            }
            rmemset0(online_refresh_table, 0, 0, sizeof(refresh_table));

            /* First allocation, save head address */
            result->tables = online_refresh_table;
        }
        else
        {
            online_refresh_table->next = rmalloc0(sizeof(refresh_table));
            if (!online_refresh_table->next)
            {
                elog(RLOG_ERROR, "oom");
            }
            rmemset0(online_refresh_table->next, 0, 0, sizeof(refresh_table));

            /* Second and subsequent allocations, maintain next and prev of next node */
            online_refresh_table->next->prev = online_refresh_table;
            online_refresh_table = online_refresh_table->next;
        }

        /* Assign values */
        online_refresh_table->oid = scan_d2o_entry->oid;
        online_refresh_table->schema = rstrdup(scan_d2o_entry->dataset.schema);
        online_refresh_table->table = rstrdup(scan_d2o_entry->dataset.table);
    }

    result->cnt = line_num;

    if (error_table)
    {
        if (result)
        {
            refresh_freetables(result);
        }
        online_refresh_status_write_error_table(error_table);
        list_free_deep(error_table);
        hash_destroy(dataset2oid_htab);
        return NULL;
    }

    online_refresh_status_write_success();
    /* Cleanup */
    hash_destroy(dataset2oid_htab);
    return result;
}

onlinerefresh* onlinerefresh_init(void)
{
    onlinerefresh* result = NULL;
    result = rmalloc0(sizeof(onlinerefresh));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(onlinerefresh));
    return result;
}

/* Comparison function for onlinerefresh */
int onlinerefresh_cmp(void* s1, void* s2)
{
    onlinerefresh* r1 = NULL;
    onlinerefresh* r2 = NULL;

    r1 = (onlinerefresh*)s1;
    r2 = (onlinerefresh*)s2;

    if (0 == memcmp(r1->no->data, r2->no->data, UUID_LEN))
    {
        return 0;
    }

    return 1;
}

void onlinerefresh_state_setsearchmax(onlinerefresh* refresh)
{
    refresh->state = ONLINEREFRESH_STATE_SEARCHMAX;
}

void onlinerefresh_state_setfullsnapshot(onlinerefresh* refresh)
{
    refresh->state = ONLINEREFRESH_STATE_FULLSNAPSHOT;
}

void onlinerefresh_no_set(onlinerefresh* refresh, uuid_t* no)
{
    refresh->no = no;
}

void onlinerefresh_increment_set(onlinerefresh* refresh, bool increment)
{
    refresh->increment = increment;
}

void onlinerefresh_newtables_set(onlinerefresh* refresh, List* newtables)
{
    refresh->newtables = newtables;
}

void onlinerefresh_txid_set(onlinerefresh* refresh, FullTransactionId txid)
{
    refresh->txid = txid;
}

void onlinerefresh_snapshot_set(onlinerefresh* refresh, snapshot* snapshot)
{
    refresh->snapshot = snapshot;
}

void transcache_make_xids_from_txn(void* in_ctx, onlinerefresh* olnode)
{
    decodingcontext* ctx = NULL;
    txn*             txn_entry = NULL;

    if (!olnode->increment)
    {
        /* Skip when incremental is not needed */
        return;
    }

    ctx = (decodingcontext*)in_ctx;
    txn_entry = ctx->trans_cache->transdlist->head;

    if (!txn_entry)
    {
        return;
    }

    while (txn_entry)
    {
        FullTransactionId xid = txn_entry->xid;

        txn_entry = txn_entry->next;

        if (xid < olnode->snapshot->xmin)
        {
            /* Do not add to xids */
            continue;
        }
        if (xid > olnode->txid)
        {
            continue;
        }
        else
        {
            /* When xmin equals xmax, exclude xid equal to xmin first */
            if (xid != olnode->snapshot->xmin && xid >= olnode->snapshot->xmax)
            {
                onlinerefresh_xids_append(olnode, xid);
            }
        }
    }
}

void onlinerefresh_xids_append(onlinerefresh* refresh, TransactionId xid)
{
    FullTransactionId* xid_p = NULL;
    dlistnode*         xid_dlnode = NULL;

    /* XIDs are unique, check for duplicates here first */
    if (refresh->xids)
    {
        xid_dlnode = refresh->xids->head;
        while (xid_dlnode)
        {
            FullTransactionId* xid_temp = (FullTransactionId*)xid_dlnode->value;
            if (*xid_temp == (FullTransactionId)xid)
            {
                return;
            }
            xid_dlnode = xid_dlnode->next;
        }
    }

    xid_p = rmalloc0(sizeof(FullTransactionId));
    if (!xid_p)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(xid_p, 0, 0, sizeof(FullTransactionId));
    *xid_p = (FullTransactionId)xid;
    refresh->xids = dlist_put(refresh->xids, xid_p);
}

void onlinerefresh_add_xids_from_snapshot(onlinerefresh* refresh, snapshot* snap)
{
    HASH_SEQ_STATUS status;
    snapshot_xid*   entry = NULL;
    TransactionId   xid = InvalidTransactionId;

    hash_seq_init(&status, snap->xids);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        xid = entry->xid;
        onlinerefresh_xids_append(refresh, xid);
    }
}

static void free_xid(void* xid_p)
{
    if (xid_p)
    {
        rfree(xid_p);
    }
}

void onlinerefresh_xids_delete(onlinerefresh* refresh, dlist* dl, dlistnode* dlnode)
{
    refresh->xids = dlist_delete(dl, dlnode, free_xid);
}

bool onlinerefresh_xids_isnull(onlinerefresh* refresh)
{
    return dlist_isnull(refresh->xids);
}

bool onlinerefresh_isxidinsnapshot(onlinerefresh* onlinerefresh, FullTransactionId xid)
{
    TransactionId txid = (TransactionId)xid;
    bool          find = false;
    if (!onlinerefresh || !onlinerefresh->snapshot->xids)
    {
        return false;
    }

    /* Note: key in snapshot's xids hash is actually TransactionId */
    hash_search(onlinerefresh->snapshot->xids, &txid, HASH_FIND, &find);

    return find;
}

void onlinerefresh_destroy(onlinerefresh* olrefresh)
{
    if (NULL == olrefresh)
    {
        return;
    }

    if (olrefresh->no)
    {
        uuid_free(olrefresh->no);
    }
    if (olrefresh->snapshot)
    {
        if (olrefresh->snapshot->name)
        {
            rfree(olrefresh->snapshot->name);
        }
        if (olrefresh->snapshot->xids)
        {
            hash_destroy(olrefresh->snapshot->xids);
        }
        rfree(olrefresh->snapshot);
    }
    /* xids nodes already freed, no need to free again, just free dlist itself */
    if (olrefresh->xids)
    {
        dlist_free(olrefresh->xids, NULL);
    }
    rfree(olrefresh);
}

void onlinerefresh_destroyvoid(void* olrefresh)
{
    onlinerefresh_destroy((onlinerefresh*)olrefresh);
}

dlist* onlinerefresh_refreshdlist_delete(dlist* refresh_dlist, dlistnode* dlnode)
{
    if (!refresh_dlist)
    {
        return NULL;
    }
    refresh_dlist = dlist_delete(refresh_dlist, dlnode, onlinerefresh_destroyvoid);

    return refresh_dlist;
}
