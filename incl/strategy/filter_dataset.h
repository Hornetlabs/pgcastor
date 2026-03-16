#ifndef _RIPPLE_FILTER_DATASET_H
#define _RIPPLE_FILTER_DATASET_H

/* 宏定义 */


/* dataset 结构体 */
typedef struct ripple_filter_dataset
{
    char schema[64];
    char table[64];
} ripple_filter_dataset;


typedef struct ripple_filter_dataset2oidnode
{
    ripple_filter_dataset dataset;
    Oid oid;
} ripple_filter_dataset2oidnode;

typedef struct ripple_filter_oidnode2dataset
{
    Oid oid;
    ripple_filter_dataset dataset;
} ripple_filter_oid2datasetnode;

typedef struct RIPPLE_FILTER_PAIR
{
    ripple_regex*   sch;
    ripple_regex*   table;
} ripple_filter_pair;

/* 函数声明 */
extern bool ripple_filter_dataset_init(List* tbincludes, List* tbexcludes, HTAB* namespace, HTAB* class);
extern HTAB *ripple_filter_dataset_load(HTAB* namespace, HTAB* class);
extern HTAB *ripple_filter_dataset_reload(HTAB* namespace, HTAB* class, HTAB* oid2datasets);
extern bool ripple_filter_dataset_dml(HTAB* oid2datasets, Oid oid);
extern bool ripple_filter_dataset_ddl(HTAB* oid2datasets, Oid oid);
extern bool ripple_filter_dataset_matchforcreate(List* tablepattern, char* schema, char* table);
extern bool ripple_filter_dataset_add(HTAB* oid2datasets, Oid oid, char* schema, char* table);
extern bool ripple_filter_dataset_modify(HTAB* oid2datasets, Oid oid, char* schema, char* table);
extern bool ripple_filter_dataset_delete(HTAB* oid2datasets, Oid oid);
extern bool ripple_filter_dataset_flush(HTAB* oid2datasets);
extern void ripple_filter_dataset_free(HTAB* oid2datasets);

/* 双向过滤事务过滤集 */
extern HTAB *ripple_filter_dataset_txnfilterload(HTAB* namespace, HTAB* class);

/* 初始化同步数据集 */
extern List* ripple_filter_dataset_inittableinclude(List* table);

/* 初始化同步排除表 */
extern List* ripple_filter_dataset_inittableexclude(List* tableexclude);

extern List* ripple_filter_dataset_initaddtablepattern(List* tablepattern);

extern void ripple_filter_dataset_listpairsfree(List* rulelist);

extern ripple_refresh_tables *ripple_filter_dataset_buildrefreshtables(HTAB* hfilters);

extern bool ripple_filter_dataset_updatedatasets(List* addtablepattern, HTAB* namespace, List* sysdicthis, HTAB* syncdatasets);

extern void ripple_filter_dataset_updatedatasets_onlinerefresh(HTAB* dataset, List* tables_list);

extern void ripple_filter_dataset_pairfree(ripple_filter_pair* filterpair);

/* 根据 refreshtable 生成策略 */
extern List* ripple_filter_dataset_buildpairsbyrefreshtables(ripple_refresh_tables* rtables);

/* 重新生成 refreshtables */
extern bool ripple_filter_dataset_buildrefreshtablesbyfilters(ripple_refresh_tables** prtables,
                                                              List* filters,
                                                              HTAB* hnamespace,
                                                              HTAB* hclass);

#endif
