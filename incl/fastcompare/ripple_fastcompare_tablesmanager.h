#ifndef RIPPLE_FASTCOMPARE_TABLESMANAGER_H
#define RIPPLE_FASTCOMPARE_TABLESMANAGER_H

typedef struct RIPLE_FASTCOMPARE_CMPARETABLE
{
    char *schema;
    char *table;
} riple_fastcompare_cmparetable;

typedef struct RIPLE_FASTCOMPARE_CMPARETABLES
{
    int cnt;
    riple_fastcompare_cmparetable *table_name;
} riple_fastcompare_cmparetables;

typedef struct RIPPLE_FASTCOMPARE_TABLESMANAGER
{
    int                                     parallelcnt;
    riple_fastcompare_cmparetables          *tables;             /* 待分片表名称 */
    ripple_queue                            *queue;              /* 分片,不关注表,内容为 tableslice */
    ripple_task_slots                       *slots;              /* 工作线程 */
    ripple_fastcompare_tablecomparecatalog  *catalog;
} ripple_fastcompare_tablesmanger;

ripple_fastcompare_tablesmanger* ripple_fastcompare_tablesmanger_init(void);
bool ripple_fastcompare_tablesmanger_load_compare_tables(ripple_fastcompare_tablesmanger* mgr, List *tables);
void ripple_fastcompare_tablesmanager_slice_table(ripple_fastcompare_tablesmanger *mgr);
#endif
