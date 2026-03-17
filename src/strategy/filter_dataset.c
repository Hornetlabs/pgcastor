#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/string/strtrim.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/string/stringinfo.h"
#include "utils/regex/regex.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "catalog/catalog.h"
#include "port/file/fd.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "strategy/filter_dataset.h"


#define FILTER_MAXLINE 129

/* 全局变量 */
extern List *g_table;
extern List *g_tableexclude ;
extern List *g_addtablepattern;


/* ----- static 函数声明 begin ----- */
static bool filter_dataset_byoid(HTAB* oid2datasets, Oid oid);
/* ----- static 函数声明 end ----- */

static void filter_dataset_str2table(char *line, char *nspname, char *relname)
{
    char *temp_nspname = line;
    char *temp_relname = NULL;
    size_t nsp_len = 0;
    size_t rel_len = 0;
    int index = 0;

    temp_relname = strstr(line, ".");
    nsp_len = temp_relname - temp_nspname;

    /* 跳过 . */
    temp_relname += 1;
    rel_len = strlen(line) - 1 - nsp_len;

    /* name的最大有效长度为63, 包含\0为64 */
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

/* 根据 refreshtable 生成策略 */
List* filter_dataset_buildpairsbyrefreshtables(refresh_tables* rtables)
{
    List* filters                   = NULL;
    ListCell* lc                    = NULL;
    refresh_table* rtable    = NULL;
    filter_pair* filterpair  = NULL;

    for (rtable = rtables->tables; NULL != rtable; rtable = rtable->next)
    {
        filterpair = (filter_pair*)rmalloc0(sizeof(filter_pair));
        if(NULL == filterpair)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            goto filter_dataset_initpairslistbyrefreshtables_error;
        }
        rmemset0(filterpair, 0, '\0', sizeof(filter_pair));
        filterpair->sch = (regex*)rmalloc0(sizeof(regex));
        if(NULL == filterpair->sch)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            goto filter_dataset_initpairslistbyrefreshtables_error;
        }
        rmemset0(filterpair->sch, 0, '\0', sizeof(regex));
        filterpair->table = (regex*)rmalloc0(sizeof(regex));
        if(NULL == filterpair->table)
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

/* 重新生成 refreshtables */
bool filter_dataset_buildrefreshtablesbyfilters(refresh_tables** prtables,
                                                      List* filters,
                                                      HTAB* hnamespace,
                                                      HTAB* hclass)
{
    bool found                                      = false;
    Oid nspoid                                      = InvalidOid;
    ListCell* lc                                    = NULL;
    filter_pair* filterpair                  = NULL;
    refresh_table* rtable                    = NULL;
    refresh_tables* rtables                  = NULL;
    catalog_class_value *classentry          = NULL;
    catalog_namespace_value *nspentry        = NULL;
    xk_pg_parser_sysdict_pgclass *sysdictclass      = NULL;
    xk_pg_parser_sysdict_pgnamespace *sysdictnsp    = NULL;
    
    HASH_SEQ_STATUS status;

    hash_seq_init(&status, hclass);
    while (NULL != (classentry = hash_seq_search(&status)))
    {
        found = false;
        nspoid = InvalidOid;
        sysdictclass = classentry->class;

        /* 只保留r和p */
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
            filterpair = (filter_pair *) lfirst(lc);
            if (cmp_regexbase(filterpair->sch, sysdictnsp->nspname.data)
                && cmp_regexbase(filterpair->table, sysdictclass->relname.data))
            {
                found = true;
                break;
            }
        }

        if (false == found)
        {
            continue;
        }

        /* 符合规则, 那么生成 refreshtables */
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

/* 生成策略 */
static List* filter_dataset_initpairslist(List* rulelist)
{
    char* cptr                      = NULL;
    char* include                   = NULL;
    ListCell* cell                  = NULL;
    List* filter_list               = NULL;
    filter_pair* filter_pair_obj = NULL;

    char table[64]                  = {'\0'};
    char schema[64]                 = {'\0'};
    char temp[1024]                 = {'\0'};

    /* 根据入参构造 同步策略 */
    foreach(cell, rulelist)
    {
        include = (char*)lfirst(cell);
        rmemset1(temp, 0, '\0', 1024);
        rmemset1(schema, 0, '\0', 64);
        rmemset1(table, 0, '\0', 64);

        strcpy(temp, include);
        if (strlen(temp) == strlen("*") && 0 == strcmp(temp,"*"))
        {
            rmemcpy1(temp, 0, "*.*", strlen("*.*"));
        }

        /* 无效字符清理(空格、制表符、换行符) */
        cptr = rightstrtrim(temp);
        if ('\0' == cptr[0])
        {
            /* 空行 */
            continue;
        }

        /* 拆分为 模式.表名 */
        filter_dataset_str2table(temp, schema, table);

        filter_pair_obj = (filter_pair*)rmalloc0(sizeof(filter_pair));
        if(NULL == filter_pair_obj)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(filter_pair_obj, 0, '\0', sizeof(filter_pair));
        filter_pair_obj->sch = (regex*)rmalloc0(sizeof(regex));
        if(NULL == filter_pair_obj->sch)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(filter_pair_obj->sch, 0, '\0', sizeof(regex));
        filter_pair_obj->table = (regex*)rmalloc0(sizeof(regex));
        if(NULL == filter_pair_obj->table)
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

/* 删除策略 */
void filter_dataset_listpairsfree(List* rulelist)
{
    ListCell* lc = NULL;
    filter_pair* filter_pair_obj = NULL;
    if(NULL == rulelist)
    {
        return;
    }

    /* 删除 rulelist 中的内容 */
    foreach(lc, rulelist)
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
    bool result = false;
    ListCell* lc = NULL;
    filter_pair* filter_pair_obj = NULL;

    if (NULL == addtablepattern)
    {
        return true;
    }

    foreach(lc, addtablepattern)
    {
        filter_pair_obj = (filter_pair*)lfirst(lc);

        if (cmp_regexbase(filter_pair_obj->sch, schema)
             && cmp_regexbase(filter_pair_obj->table, table))
        {
            result = true;
            break;
        }
    }
    
    return result;
}

/* 从根据传入的oid从哈希表中查找是否存在 */
static bool filter_dataset_byoid(HTAB* oid2datasets, Oid oid)
{
    bool find = false;

    hash_search(oid2datasets, &oid, HASH_FIND, &find);
    return find;
}

/* 初始化同步数据集 */
List* filter_dataset_inittableinclude(List* table)
{
    /* 清空过滤规则 */
    filter_dataset_listpairsfree(table);

    table = filter_dataset_initpairslist(g_table);
    return table;
}

/* 初始化同步排除表 */
List* filter_dataset_inittableexclude(List* tableexclude)
{
    /* 清空过滤规则 */
    filter_dataset_listpairsfree(tableexclude);

    tableexclude = filter_dataset_initpairslist(g_tableexclude);
    return tableexclude;
}

List* filter_dataset_initaddtablepattern(List* tablepattern)
{
    /* 清空过滤规则 */
    filter_dataset_listpairsfree(tablepattern);

    tablepattern = filter_dataset_initpairslist(g_addtablepattern);

    return tablepattern;
}

/*
 * 生成同步数据集, 将同步结果落盘
 * 1. 获取guc参数的过滤策略
 * 2. 遍历pg_class哈希表, 获取记录, 只获取relkind为 r 和 p 的记录
 * 3. 根据记录, 查找pg_namespace表, 获取nspname
 * 4. 拼接名称 nspname.relname进行正则匹配
 * 5. 匹配通过, 将nspname.relname落盘
 * 6. 遍历完成后写入文件
 */
bool filter_dataset_init(List* tbincludes, List* tbexcludes, HTAB* namespace, HTAB* class)
{
    HASH_SEQ_STATUS status;
    catalog_class_value *class_value_entry = NULL;
    catalog_namespace_value *nsp_value_entry = NULL;
    xk_pg_parser_sysdict_pgclass *temp_class = NULL;
    xk_pg_parser_sysdict_pgnamespace *temp_nsp = NULL;
    char filepath_tmp[MAXPATH] = {'\0'};
    char filepath[MAXPATH] = {'\0'};
    struct stat statbuf;
    FILE *fp = NULL;

    /* 64 + 64 + 1 + 1 - 1 */
    /* nspname + relname + . + \n - 1个\0 */
    char nsp_relname[129] = {'\0'};

    snprintf(filepath_tmp, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET_TMP);
    snprintf(filepath, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET);

    /* 判断数据集临时文件是否存在 */
    if (0 == stat(filepath_tmp, &statbuf))
    {
        /* 存在, 先unlink掉 */
        osal_durable_unlink(filepath_tmp, RLOG_DEBUG);
    }

    /* 打开数据集文件 */
    fp = osal_allocate_file(filepath_tmp, "w+");

    /* 遍历pg_class系统表, 挑选符合过滤条件的数据集 */
    hash_seq_init(&status, class);
    while (NULL != (class_value_entry = hash_seq_search(&status)))
    {
        Oid temp_nsp_oid = InvalidOid;
        bool temp_nsp_find = false;

        temp_class = class_value_entry->class;

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
        temp_nsp = nsp_value_entry->namespace;

        sprintf(nsp_relname, "%s.%s\n", temp_nsp->nspname.data, temp_class->relname.data);

        if (tbexcludes)
        {
            ListCell *temp_cell = NULL;
            bool exclude = false;

            foreach(temp_cell, tbexcludes)
            {
                filter_pair *temp_pair = (filter_pair *) lfirst(temp_cell);
                if (cmp_regexbase(temp_pair->sch, temp_nsp->nspname.data)
                 && cmp_regexbase(temp_pair->table, temp_class->relname.data))
                {
                    exclude = true;
                    break;
                }

            }
            /* 符合exclude正则表达式, 排除 */
            if (exclude)
            {
                continue;
            }
        }

        if (tbincludes)
        {
            ListCell *temp_cell = NULL;
            bool include = false;

            foreach(temp_cell, tbincludes)
            {
                filter_pair *temp_pair = (filter_pair *) lfirst(temp_cell);
                if (cmp_regexbase(temp_pair->sch, temp_nsp->nspname.data)
                 && cmp_regexbase(temp_pair->table, temp_class->relname.data))
                {
                    include = true;
                    break;
                }

            }
            /* 不符合include正则表达式, 排除 */
            if (!include)
            {
                continue;
            }
        }

        /* 写入该条记录 */
        fwrite(nsp_relname, strlen(nsp_relname), 1, fp);

        /* 重置 */
        rmemset1(nsp_relname, 0, 0, 129);
    }

    /* 关闭临时文件 */
    osal_free_file(fp);

    /* 将临时文件重命名 */
    if (osal_durable_rename(filepath_tmp, filepath, RLOG_WARNING) != 0) 
    {
        elog(RLOG_ERROR, "Error renaming file %s to %s", filepath_tmp, filepath);
    }
    return true;
}

/*
 * 加载同步数据集
 * 1. 初始化所需变量与结构, 初始化正则匹配结构, 打开过滤文件
 * 2. 从文件开始位置, 使用fgets()每次读取一行数据
 * 3. 保存数据到哈希表dataset2oid中, 直到fgets()返回NULL
 * 4. 遍历class哈希表, 查询namespace哈希表, 获取匹配的oid保存在dataset2oid中
 * 5. 遍历dataset2oid哈希表, 去除无效数据, 保存oid2dataset哈希表
 * 6. 清理dataset2oid哈希表, 返回oid2dataset哈希表
 */
HTAB *filter_dataset_load(HTAB* namespace, HTAB* class)
{
    FILE *fp = NULL;
    HASH_SEQ_STATUS status_class;
    HASH_SEQ_STATUS status_d2o;
    catalog_class_value *class_value_entry = NULL;
    catalog_namespace_value *nsp_value_entry = NULL;
    xk_pg_parser_sysdict_pgclass *temp_class = NULL;
    xk_pg_parser_sysdict_pgnamespace *temp_nsp = NULL;
    filter_dataset2oidnode *scan_d2o_entry = NULL;
    HTAB *dataset2oid_htab = NULL;
    HTAB *oid2dataset_htab = NULL;
    HASHCTL hashCtl_d2o = {'\0'};
    HASHCTL hashCtl_o2d = {'\0'};
    struct stat st;
    char filepath[MAXPATH] = {'\0'};
    char fline[1024] = {'\0'};
    char temp_nspname[64] = {'\0'};
    char temp_relname[64] = {'\0'};
    int line_num = 0;

    /* 获取文件路径 */
    snprintf(filepath, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET);

    /* 查看文件是否存在 */
    if(-1 == stat(filepath, &st))
    {
        if(ENOENT != errno)
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

    /* 首先读取数据集, 创建dataset2oid hash */
    hashCtl_d2o.keysize = sizeof(filter_dataset);
    hashCtl_d2o.entrysize = sizeof(filter_dataset2oidnode);

    dataset2oid_htab = hash_create("filter_d2o_htab",
                               256,
                              &hashCtl_d2o,
                               HASH_ELEM | HASH_BLOBS);

    while(fgets(fline, FILTER_MAXLINE, fp))
    {
        int line_len = 0;
        filter_dataset temp_dataset_key= {{'\0'}};
        filter_dataset2oidnode *temp_d2o_entry = NULL;

        line_len = strlen(fline);
        /* 排除换行符 */
        if ('\n' == fline[line_len - 1])
        {
            fline[line_len - 1] = '\0';
            line_len--;
            if (0 == line_len)
            {
                /* 空行 */
                continue;
            }
        }
        /* windos文本排除回车符 */
        if ('\r' == fline[line_len - 1])
        {
            fline[line_len - 1] = '\0';
            line_len--;
            if (0 == line_len)
            {
                /* 空行 */
                continue;
            }
        }
        if (0 == line_len)
        {
            /* 空行 */
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
        filter_dataset temp_dataset_key= {{'\0'}};
        filter_dataset2oidnode *temp_d2o_entry = NULL;
        Oid temp_nsp_oid = InvalidOid;
        bool temp_nsp_find = false;
        bool temp_d2o_find = false;

        temp_class = class_value_entry->class;

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
        temp_nsp = nsp_value_entry->namespace;

        strcpy(temp_dataset_key.schema, temp_nsp->nspname.data);
        strcpy(temp_dataset_key.table, temp_class->relname.data);

        temp_d2o_entry = hash_search(dataset2oid_htab, &temp_dataset_key, HASH_FIND, &temp_d2o_find);
        if (temp_d2o_find)
        {
            temp_d2o_entry->oid = temp_class->oid;
        }
    }

    /* 创建oid2dataset hash */
    hashCtl_o2d.keysize = sizeof(Oid);
    hashCtl_o2d.entrysize = sizeof(filter_oid2datasetnode);

    oid2dataset_htab = hash_create("filter_o2d_htab",
                               256,
                              &hashCtl_o2d,
                               HASH_ELEM | HASH_BLOBS);

    /* 遍历dataset2oid生成oid2dataset */
    hash_seq_init(&status_d2o, dataset2oid_htab);
    while (NULL != (scan_d2o_entry = hash_seq_search(&status_d2o)))
    {
        Oid temp_oid = scan_d2o_entry->oid;
        filter_oid2datasetnode *temp_o2d_entry = NULL;

        if (temp_oid == InvalidOid)
        {
            elog(RLOG_WARNING, "when convert dataset2oid to oid2dataset, oid invalid, table: %s.%s",
                                scan_d2o_entry->dataset.schema,
                                scan_d2o_entry->dataset.table);
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

    /* 清理工作 */
    hash_destroy(dataset2oid_htab);
    return oid2dataset_htab;
}


/*
 * 加载事务过滤数据集
 * 1. 遍历namespace哈希表，获取namespaceoid
 * 2. 遍历class哈希表，查找namespaceoid和tablename匹配的table的oid
 * 3. 生成oid2dataset哈希表, 返回oid2dataset哈希表
 */
HTAB *filter_dataset_txnfilterload(HTAB* namespace, HTAB* class)
{
    HASH_SEQ_STATUS status_class;
    HASH_SEQ_STATUS status_nap;
    bool findnsp                                = false;
    HASHCTL hashCtl_o2d                         = {'\0'};
    char* schema                                = NULL;
    HTAB *oid2dataset_htab                      = NULL;
    catalog_class_value *class_entry     = NULL;
    catalog_namespace_value *nsp_entry   = NULL;
    xk_pg_parser_sysdict_pgclass *temp_class    = NULL;
    xk_pg_parser_sysdict_pgnamespace *temp_nsp  = NULL;
    filter_oid2datasetnode *o2d_entry    = NULL;

    /* 创建oid2dataset hash */
    hashCtl_o2d.keysize = sizeof(Oid);
    hashCtl_o2d.entrysize = sizeof(filter_oid2datasetnode);

    oid2dataset_htab = hash_create("filter_o2d_htab",
                                    128,
                                    &hashCtl_o2d,
                                    HASH_ELEM | HASH_BLOBS);

    schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);
    if (NULL == schema)
    {
        return oid2dataset_htab;
    }

    /* 遍历pg_namespace系统表, 根据schema获取xk_pg_parser_sysdict_pgnamespace */
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

    /* 遍历pg_class系统表, 获取table的oid */
    hash_seq_init(&status_class, class);
    while (NULL != (class_entry = hash_seq_search(&status_class)))
    {
        temp_class = class_entry->class;
        if (0 == strcmp(temp_class->relname.data, SYNC_STATUSTABLE_NAME)
            && temp_nsp->oid == temp_class->relnamespace)
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
 * 重新加载数据集
 * 1. 调用filter_dataset_free释放原有数据集
 * 2. 调用filter_dataset_load加载数据库
 * 3. 返回读取的数据集哈希表
 */
HTAB *filter_dataset_reload(HTAB* namespace, HTAB* class, HTAB* oid2datasets)
{
    filter_dataset_free(oid2datasets);
    return filter_dataset_load(namespace, class);
}

/*
 * 判断DML是否存在于需要捕获的表记录中
 * 调用filter_dataset_byoid
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
 * 判断DDL是否存在于需要捕获的表记录中
 * 调用filter_dataset_byoid
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
 * 判断CREATE TABLE语句的表是否符合捕获逻辑
 */
bool filter_dataset_matchforcreate(List* tablepattern, char* schema, char* table)
{
    bool result = false;
    ListCell *lc = NULL;
    if(NULL == tablepattern)
    {
        return true;
    }

    foreach(lc, tablepattern)
    {
        filter_pair *temp_pair = (filter_pair *) lfirst(lc);
        /* 符合正则表达式 result 赋值为 true */
        if (cmp_regexbase(temp_pair->sch, schema)
            && cmp_regexbase(temp_pair->table, table))
        {
            result = true;
            break;
        }
    }

    return result;
}

/*
 * 向数据集哈希中添加一条记录
 */
bool filter_dataset_add(HTAB* oid2datasets, Oid oid, char* schema, char* table)
{
    filter_oid2datasetnode *o2d_entry = NULL;
    o2d_entry = hash_search(oid2datasets, &oid, HASH_ENTER, NULL);
    if (!o2d_entry)
    {
        elog(RLOG_WARNING, "can't find entry to oid2datasets hash, oid: %u, %s.%s", oid, schema, table);
    }

    o2d_entry->oid = oid;
    strcpy(o2d_entry->dataset.schema, schema);
    strcpy(o2d_entry->dataset.table, table);

    return true;
}

/*
 * 修改数据集哈希中的一条记录
 */
bool filter_dataset_modify(HTAB* oid2datasets, Oid oid, char* schema, char* table)
{
    filter_oid2datasetnode *o2d_entry = NULL;
    o2d_entry = hash_search(oid2datasets, &oid, HASH_FIND, NULL);
    if (!o2d_entry)
    {
        elog(RLOG_DEBUG, "can't find entry from oid2datasets hash, oid: %u, %s.%s", oid, schema, table);
        return false;
    }

    o2d_entry->oid = oid;
    strcpy(o2d_entry->dataset.schema, schema);
    strcpy(o2d_entry->dataset.table, table);

    return true;
}

/*
 * 删除数据集哈希中的一条记录
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
 * 数据集落盘
 */
bool filter_dataset_flush(HTAB* oid2datasets)
{
    HASH_SEQ_STATUS status;
    struct stat statbuf;
    FILE *fp = NULL;
    filter_oid2datasetnode *o2d_entry = NULL;
    char filepath_tmp[MAXPATH] = {'\0'};
    char filepath[MAXPATH] = {'\0'};

    /* 64 + 64 + 1 + 1 - 1 */
    /* nspname + relname + . + \n - 1个\0 */
    char nsp_relname[129] = {'\0'};

    snprintf(filepath_tmp, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET_TMP);
    snprintf(filepath, MAXPATH, "%s/%s", FILTER_DIR, FILTER_DATASET);

    /* 判断数据集临时文件是否存在 */
    if (0 == stat(filepath_tmp, &statbuf))
    {
        /* 存在, 先unlink掉 */
        osal_durable_unlink(filepath_tmp, RLOG_DEBUG);
    }

    /* 打开数据集文件 */
    fp = osal_allocate_file(filepath_tmp, "w+");

    /* 遍历pg_class系统表, 挑选符合过滤条件的数据集 */
    hash_seq_init(&status, oid2datasets);
    while (NULL != (o2d_entry = hash_seq_search(&status)))
    {
        sprintf(nsp_relname, "%s.%s\n", o2d_entry->dataset.schema, o2d_entry->dataset.table);

        /* 写入该条记录 */
        fwrite(nsp_relname, strlen(nsp_relname), 1, fp);

        /* 重置 */
        rmemset1(nsp_relname, 0, 0, 129);
    }

    /* 关闭临时文件 */
    osal_free_file(fp);

    /* 将临时文件重命名 */
    if (osal_durable_rename(filepath_tmp, filepath, RLOG_WARNING) != 0) 
    {
        elog(RLOG_ERROR, "Error renaming file %s to %s", filepath_tmp, filepath);
    }
    return true;
}

/*
 * 数据集哈希释放
 */
void filter_dataset_free(HTAB* oid2datasets)
{
    hash_destroy(oid2datasets);
}

refresh_tables* filter_dataset_buildrefreshtables(HTAB* hfilters)
{
    HASH_SEQ_STATUS status;
    filter_oid2datasetnode *entry = NULL;
    refresh_table *table = NULL;
    refresh_tables *refreshtables = NULL;

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

bool filter_dataset_updatedatasets(List* addtablepattern, HTAB* namespace, List* sysdicthis, HTAB* syncdatasets)
{
    bool found = false;
    bool haschange = false;
    char* table = NULL;
    char* schema = NULL;
    ListCell* lc = NULL;
    catalogdata* catalog_data = NULL;
    catalog_class_value* classvalue = NULL;
    catalog_namespace_value* nspvalue = NULL;

    if (NULL == syncdatasets)
    {
        return false;
    }
    
    foreach(lc, sysdicthis)
    {
        catalog_data = (catalogdata*)lfirst(lc);

        if(NULL == catalog_data || NULL == catalog_data->catalog)
        {
            return false;
        }
        if(CATALOG_TYPE_CLASS == catalog_data->type)
        {
            classvalue = (catalog_class_value*)catalog_data->catalog;
            elog(RLOG_DEBUG, "syncdatasets op:%d, classvalue, %s, %u.%u",
                            catalog_data->op,
                            classvalue->class->relname.data,
                            classvalue->class->oid,
                            classvalue->class->relnamespace);

            if ('p' != classvalue->class->relkind && 'r' != classvalue->class->relkind)
            {
                continue;
            }

            table = classvalue->class->relname.data;
            nspvalue = hash_search(namespace, &(classvalue->class->relnamespace), HASH_FIND, &found);
            if (found)
            {
                schema = nspvalue->namespace->nspname.data;
            }
            else
            {
                catalog_data->op = CATALOG_OP_DELETE;
            }

            if(CATALOG_OP_INSERT == catalog_data->op)
            {
                if (filter_dataset_match_addtablepattern(addtablepattern, schema, table))
                {
                    if(filter_dataset_add(syncdatasets, classvalue->class->oid, schema, table))
                    {
                        haschange = true;
                    }
                }
            }
            else if(CATALOG_OP_DELETE == catalog_data->op)
            {
                if(filter_dataset_delete(syncdatasets, classvalue->class->oid))
                {
                    haschange = true;
                }
            }
            else if(CATALOG_OP_UPDATE == catalog_data->op)
            {
                if(filter_dataset_modify(syncdatasets, classvalue->class->oid, schema, table))
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
    ListCell *cell = NULL;

    foreach(cell, tables_list)
    {
        refresh_table *table = lfirst(cell);

        filter_dataset_add(dataset, table->oid, table->schema, table->table);
    }
}

