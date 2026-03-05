#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/string/strtrim.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/uuid/ripple_uuid.h"
#include "utils/regex/ripple_regex.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "catalog/ripple_catalog.h"
#include "port/file/fd.h"
#include "refresh/ripple_refresh_tables.h"
#include "refresh/ripple_refresh_table_sharding.h"
#include "strategy/ripple_filter_dataset.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_onlinerefresh.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"

#define RIPPLE_ONLINEREFRESH_MAXLINE 129

static bool ripple_get_nspname_relname_from_line(char *line, char *nspname, char *relname)
{
    char *temp_nspname = line;
    char *temp_relname = NULL;
    size_t nsp_len = 0;
    size_t rel_len = 0;
    int index = 0;

    temp_relname = strstr(line, ".");
    if(NULL == temp_relname)
    {
        elog(RLOG_WARNING, "%s configure rule: schema.table, %s", RIPPLE_ONLINEREFRESH_DAT, line);
        return false;
    }
    nsp_len = temp_relname - temp_nspname;

    /* 跳过 . */
    temp_relname += 1;
    rel_len = strlen(line) - 1 - nsp_len;

    /* name的最大有效长度为63, 包含\0为64 */
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

static void ripple_online_refresh_status_write_success(void)
{
    StringInfoData path = {'\0'};
    int     fd = -1;

    initStringInfo(&path);

    appendStringInfo(&path, "%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA), RIPPLE_ONLINEREFRESH_STATUS);

    fd = BasicOpenFile(path.data, O_RDWR | O_CREAT | RIPPLE_BINARY);
    if(-1 == fd)
    {
        elog(RLOG_WARNING, "open file:%s error, %s", path.data, strerror(errno));
        return;
    }

    FileWrite(fd, "SUCCESS\n", strlen("SUCCESS\n"));

    FileClose(fd);
    rfree(path.data);
}

static void ripple_online_refresh_status_write_error_table(List *tables)
{
    ListCell *cell = NULL;
    StringInfoData str = {'\0'};
    StringInfoData path = {'\0'};
    int     fd = -1;

    if (!tables)
    {
        return;
    }

    initStringInfo(&str);
    initStringInfo(&path);

    appendStringInfo(&str, "ERROR\n");
    foreach(cell, tables)
    {
        char *temp_tables = (char *)lfirst(cell);
        appendStringInfo(&str, "%s\n", temp_tables);
    }

    appendStringInfo(&path, "%s/%s", guc_getConfigOption(RIPPLE_CFG_KEY_DATA), RIPPLE_ONLINEREFRESH_STATUS);

    fd = BasicOpenFile(path.data, O_RDWR | O_CREAT | RIPPLE_BINARY);
    if(-1 == fd)
    {
        elog(RLOG_WARNING, "open file:%s error, %s", path.data, strerror(errno));
        return;
    }

    FileWrite(fd, str.data, strlen(str.data));

    FileClose(fd);
    rfree(str.data);
    rfree(path.data);
}

List *ripple_onlinerefresh_get_newtable(HTAB *dataset, ripple_refresh_tables *tables)
{
    ripple_refresh_table *table = NULL;
    List *result = NULL;
    bool find = false;
    ripple_refresh_table *table_list_node = NULL;

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
            table_list_node = ripple_refresh_table_init();
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

/* 根据 系统表 填充 refreshtables */
bool ripple_onlinerefresh_rebuildrefreshtables(ripple_refresh_tables* rtables,
                                               HTAB* hnamespace,
                                               HTAB* hclass,
                                               bool* bmatch)
{
    /* 
     * 1、根据 rtables 生成 filters
     *  可能会出现模糊匹配, 所以此处需要处理
     * 2、根据 filters 重新填充 rtables
     */
    List* filters = NULL;
    ripple_refresh_tables* nrtables = NULL;

    /* 生成 filters 集合 */
    filters = ripple_filter_dataset_buildpairsbyrefreshtables(rtables);
    if (NULL == filters)
    {
        elog(RLOG_WARNING, "build filters error");
        return false;
    }

    /* 根据 filters 集合和数据字典生成 refresh tables */
    if (false == ripple_filter_dataset_buildrefreshtablesbyfilters(&nrtables, filters, hnamespace, hclass))
    {
        elog(RLOG_WARNING, "build refresh tables by filters error");
        return false;
    }

    if (NULL == nrtables)
    {
        *bmatch = false;
        return true;
    }

    /* 清理掉 rtables 中的表 */
    ripple_refresh_freetable(rtables->tables);
    rtables->cnt = nrtables->cnt;
    rtables->tables = nrtables->tables;
    nrtables->tables = NULL;
    nrtables->cnt = 0;

    ripple_refresh_freetables(nrtables);
    return true;
}

/*
 * 加载onlinerefresh数据集
 */
ripple_refresh_tables *ripple_onlinerefresh_data_load(HTAB* namespace, HTAB* class)
{
    int line_num = 0;
    FILE *fp = NULL;
    char* cptr = NULL;
    char *data_path = NULL;
    List *error_table = NULL;
    HTAB *dataset2oid_htab = NULL;
    ripple_refresh_tables *result = NULL;
    ripple_refresh_table *online_refresh_table = NULL;
    ripple_catalog_class_value *class_value_entry = NULL;
    ripple_catalog_namespace_value *nsp_value_entry = NULL;
    xk_pg_parser_sysdict_pgclass *temp_class = NULL;
    xk_pg_parser_sysdict_pgnamespace *temp_nsp = NULL;
    ripple_filter_dataset2oidnode *scan_d2o_entry = NULL;
    HASH_SEQ_STATUS status_class;
    HASH_SEQ_STATUS status_d2o;
    HASHCTL hashCtl_d2o = {'\0'};
    struct stat st;
    char temp_nspname[RIPPLE_NAMEDATALEN]   = {'\0'};
    char temp_relname[RIPPLE_NAMEDATALEN]   = {'\0'};
    char filepath[RIPPLE_MAXPATH]           = {'\0'};
    char fline[RIPPLE_LINESIZE]             = {'\0'};
    
    data_path = guc_getConfigOption(RIPPLE_CFG_KEY_DATA);

    /* 获取文件路径 */
    snprintf(filepath, RIPPLE_MAXPATH, "%s/%s", data_path, RIPPLE_ONLINEREFRESH_DAT);

    /* 查看文件是否存在 */
    if(-1 == stat(filepath, &st))
    {
        if(ENOENT != errno)
        {
            elog(RLOG_WARNING, "stat %s error, %s", filepath, strerror(errno));
            return NULL;
        }

        elog(RLOG_WARNING, "%s not exist", filepath);
        return NULL;
    }

    fp = FileFOpen(filepath, "r");
    if (NULL == fp)
    {
        elog(RLOG_WARNING, "open %s failed", filepath);

        /* make compiler happy */
        return NULL;
    }

    result = rmalloc0(sizeof(ripple_refresh_tables));
    if (!result)
    {
        elog(RLOG_WARNING, "oom");
        return NULL;
    }
    rmemset0(result, 0, 0, sizeof(ripple_refresh_tables));

    /* 首先读取数据集, 创建dataset2oid hash */
    hashCtl_d2o.keysize = sizeof(ripple_filter_dataset);
    hashCtl_d2o.entrysize = sizeof(ripple_filter_dataset2oidnode);

    dataset2oid_htab = hash_create("onlinerefresh_d2o_htab",
                               256,
                              &hashCtl_d2o,
                               HASH_ELEM | HASH_BLOBS);

    while(fgets(fline, RIPPLE_ONLINEREFRESH_MAXLINE, fp))
    {
        ripple_filter_dataset temp_dataset_key= {{'\0'}};
        ripple_filter_dataset2oidnode *temp_d2o_entry = NULL;

        /* 注释行, 跳过 */
        cptr = fline;
        if('#' == *cptr)
        {
            continue;
        }

        cptr = leftstrtrim(fline, RIPPLE_LINESIZE);
        if('\0' == *cptr)
        {
            continue;
        }

        /* 清空后面的空格制表符 */
        cptr = rightstrtrim(fline);
        if('\0' == fline[0])
        {
            continue;
        }

        if(false == ripple_get_nspname_relname_from_line(fline, temp_nspname, temp_relname))
        {
            elog(RLOG_WARNING, "parse %s -->%s to schema table error", RIPPLE_ONLINEREFRESH_DAT, fline);
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

        /* oid暂时赋值为0 */
        temp_d2o_entry->oid = InvalidOid;

        /* 重置 */
        rmemset1(temp_nspname, 0, 0, 64);
        rmemset1(temp_relname, 0, 0, 64);

        /* 计数 */
        line_num++;
    }

    /* 没有数据集 */
    if (line_num == 0)
    {
        /* 清理, 返回NULL */
        hash_destroy(dataset2oid_htab);
        return NULL;
    }

    /* 遍历pg_class系统表, 填充dataset2oid哈希表的oid */
    hash_seq_init(&status_class, class);
    while (NULL != (class_value_entry = hash_seq_search(&status_class)))
    {
        ripple_filter_dataset temp_dataset_key= {{'\0'}};
        ripple_filter_dataset2oidnode *temp_d2o_entry = NULL;
        Oid temp_nsp_oid = InvalidOid;
        bool temp_nsp_find = false;
        bool temp_d2o_find = false;

        temp_class = class_value_entry->ripple_class;

        /* 只保留r和p */
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
        temp_nsp = nsp_value_entry->ripple_namespace;

        strcpy(temp_dataset_key.schema, temp_nsp->nspname.data);
        strcpy(temp_dataset_key.table, temp_class->relname.data);

        temp_d2o_entry = hash_search(dataset2oid_htab, &temp_dataset_key, HASH_FIND, &temp_d2o_find);
        if (temp_d2o_find)
        {
            temp_d2o_entry->oid = temp_class->oid;
        }
    }

    /* 为了防止重复行, 重新计数 */
    line_num = 0;

    /* 遍历dataset2oid生成list */
    hash_seq_init(&status_d2o, dataset2oid_htab);
    while (NULL != (scan_d2o_entry = hash_seq_search(&status_d2o)))
    {
        Oid temp_oid = scan_d2o_entry->oid;

        if (temp_oid == InvalidOid)
        {
            char temp_table_name[130] = {'\0'};
            elog(RLOG_WARNING, "when load online refresh dat, get oid invalid, table: %s.%s",
                                scan_d2o_entry->dataset.schema,
                                scan_d2o_entry->dataset.table);
            sprintf(temp_table_name, "%s.%s", scan_d2o_entry->dataset.schema, scan_d2o_entry->dataset.table);
            error_table = lappend(error_table, rstrdup(temp_table_name));
            continue;
        }

        /* 计数增加 */
        line_num++;

        /* refresh table 分配空间 */
        if (!online_refresh_table)
        {
            online_refresh_table = rmalloc0(sizeof(ripple_refresh_table));
            if (!online_refresh_table)
            {
                elog(RLOG_ERROR, "oom");
            }
            rmemset0(online_refresh_table, 0, 0, sizeof(ripple_refresh_table));

            /* 第一次分配, 保存首地址 */
            result->tables = online_refresh_table;
        }
        else
        {
            online_refresh_table->next = rmalloc0(sizeof(ripple_refresh_table));
            if (!online_refresh_table->next)
            {
                elog(RLOG_ERROR, "oom");
            }
            rmemset0(online_refresh_table->next, 0, 0, sizeof(ripple_refresh_table));

            /* 第二次及以上分配, 维护next和下一条的prev */
            online_refresh_table->next->prev = online_refresh_table;
            online_refresh_table = online_refresh_table->next;
        }

        /* 赋值 */
        online_refresh_table->oid = scan_d2o_entry->oid;
        online_refresh_table->schema = rstrdup(scan_d2o_entry->dataset.schema);
        online_refresh_table->table = rstrdup(scan_d2o_entry->dataset.table);
    }

    result->cnt = line_num;

    if (error_table)
    {
        if (result)
        {
            ripple_refresh_freetables(result);
        }
        ripple_online_refresh_status_write_error_table(error_table);
        list_free_deep(error_table);
        hash_destroy(dataset2oid_htab);
        return NULL;
    }

    ripple_online_refresh_status_write_success();
    /* 清理工作 */
    hash_destroy(dataset2oid_htab);
    return result;
}

ripple_onlinerefresh *ripple_onlinerefresh_init(void)
{
    ripple_onlinerefresh *result = NULL;
    result = rmalloc0(sizeof(ripple_onlinerefresh));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(ripple_onlinerefresh));
    return result;
}

/* 比较 */
int ripple_onlinerefresh_cmp(void* s1, void* s2)
{
    ripple_onlinerefresh* r1 = NULL;
    ripple_onlinerefresh* r2 = NULL;

    r1 = (ripple_onlinerefresh*)s1;
    r2 = (ripple_onlinerefresh*)s2;

    if (0 == memcmp(r1->no->data, r2->no->data, RIPPLE_UUID_LEN))
    {
        return 0;
    }

    return 1;
}

void ripple_onlinerefresh_state_setsearchmax(ripple_onlinerefresh *refresh)
{
    refresh->state = RIPPLE_ONLINEREFRESH_STATE_SEARCHMAX;
}

void ripple_onlinerefresh_state_setfullsnapshot(ripple_onlinerefresh *refresh)
{
    refresh->state = RIPPLE_ONLINEREFRESH_STATE_FULLSNAPSHOT;
}

void ripple_onlinerefresh_no_set(ripple_onlinerefresh *refresh, ripple_uuid_t *no)
{
    refresh->no = no;
}

void ripple_onlinerefresh_increment_set(ripple_onlinerefresh *refresh, bool increment)
{
    refresh->increment = increment;
}

void ripple_onlinerefresh_newtables_set(ripple_onlinerefresh *refresh, List *newtables)
{
    refresh->newtables = newtables;
}

void ripple_onlinerefresh_txid_set(ripple_onlinerefresh *refresh, FullTransactionId txid)
{
    refresh->txid = txid;
}

void ripple_onlinerefresh_snapshot_set(ripple_onlinerefresh *refresh, ripple_snapshot *snapshot)
{
    refresh->snapshot = snapshot;
}

void ripple_transcache_make_xids_from_txn(void* in_ctx, ripple_onlinerefresh *olnode)
{
    ripple_decodingcontext* ctx = NULL;
    ripple_txn *txn_entry = NULL;

    if (!olnode->increment)
    {
        /* 不需要增量时跳过 */
        return;
    }

    ctx = (ripple_decodingcontext*)in_ctx;
    txn_entry = ctx->transcache->transdlist->head;

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
            /* 无需加入xids */
            continue;
        }
        if (xid > olnode->txid)
        {
            continue;
        }
        else
        {
            /* 存在xmin = xmax的情况, 因此首先排除xid = xmin */
            if (xid != olnode->snapshot->xmin && xid >= olnode->snapshot->xmax)
            {
                ripple_onlinerefresh_xids_append(olnode, xid);
            }
        }
    }
}

void ripple_onlinerefresh_xids_append(ripple_onlinerefresh *refresh, TransactionId xid)
{
    FullTransactionId *xid_p = NULL;
    dlistnode *xid_dlnode = NULL;

    /* 由于xid是不可重复的, 因此在这里先检查是否有重复的xid */
    if (refresh->xids)
    {
        xid_dlnode = refresh->xids->head;
        while (xid_dlnode)
        {
            FullTransactionId *xid_temp = (FullTransactionId *) xid_dlnode->value;
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

void ripple_onlinerefresh_add_xids_from_snapshot(ripple_onlinerefresh *refresh, ripple_snapshot *snap)
{
    HASH_SEQ_STATUS status;
    ripple_snapshot_xid* entry = NULL;
    TransactionId xid = InvalidTransactionId;

    hash_seq_init(&status, snap->xids);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        xid = entry->xid;
        ripple_onlinerefresh_xids_append(refresh, xid);
    }
}

static void free_xid(void *xid_p)
{
    if (xid_p)
    {
        rfree(xid_p);
    }
}

void ripple_onlinerefresh_xids_delete(ripple_onlinerefresh *refresh, dlist *dl, dlistnode *dlnode)
{
    refresh->xids = dlist_delete(dl, dlnode, free_xid);
}

bool ripple_onlinerefresh_xids_isnull(ripple_onlinerefresh* refresh)
{
    return dlist_isnull(refresh->xids);
}

bool ripple_onlinerefresh_isxidinsnapshot(ripple_onlinerefresh* onlinerefresh, FullTransactionId xid)
{
    TransactionId txid = (TransactionId) xid;
    bool find = false;
    if (!onlinerefresh || !onlinerefresh->snapshot->xids)
    {
        return false;
    }

    /* 注意: snapshot的xids哈希里的key实际上是TransactionId */
    hash_search(onlinerefresh->snapshot->xids, &txid, HASH_FIND, &find);

    return find;
}

void ripple_onlinerefresh_destroy(ripple_onlinerefresh* olrefresh)
{
    if (NULL == olrefresh)
    {
        return;
    }

    if (olrefresh->no)
    {
        ripple_uuid_free(olrefresh->no);
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
    /* xids的节点已经释放完毕, 无需再释放, 仅释放dlist本身 */
    if (olrefresh->xids)
    {
        dlist_free(olrefresh->xids, NULL);
    }
    rfree(olrefresh);
}

void ripple_onlinerefresh_destroyvoid(void *olrefresh)
{
    ripple_onlinerefresh_destroy((ripple_onlinerefresh *)olrefresh);
}

dlist *ripple_onlinerefresh_refreshdlist_delete(dlist *refresh_dlist, dlistnode *dlnode)
{
    if (!refresh_dlist)
    {
        return NULL;
    }
    refresh_dlist = dlist_delete(refresh_dlist, dlnode, ripple_onlinerefresh_destroyvoid);

    return refresh_dlist;
}
