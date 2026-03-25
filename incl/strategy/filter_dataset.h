#ifndef _FILTER_DATASET_H
#define _FILTER_DATASET_H

/* macro definitions */

/* dataset structure */
typedef struct filter_dataset
{
    char schema[64];
    char table[64];
} filter_dataset;

typedef struct filter_dataset2oidnode
{
    filter_dataset dataset;
    Oid            oid;
} filter_dataset2oidnode;

typedef struct filter_oidnode2dataset
{
    Oid            oid;
    filter_dataset dataset;
} filter_oid2datasetnode;

typedef struct FILTER_PAIR
{
    regex* sch;
    regex* table;
} filter_pair;

/* function declarations */
extern bool  filter_dataset_init(List* tbincludes, List* tbexcludes, HTAB* namespace, HTAB* class);
extern HTAB* filter_dataset_load(HTAB* namespace, HTAB* class);
extern HTAB* filter_dataset_reload(HTAB* namespace, HTAB* class, HTAB* oid2datasets);
extern bool  filter_dataset_dml(HTAB* oid2datasets, Oid oid);
extern bool  filter_dataset_ddl(HTAB* oid2datasets, Oid oid);
extern bool  filter_dataset_matchforcreate(List* tablepattern, char* schema, char* table);
extern bool  filter_dataset_add(HTAB* oid2datasets, Oid oid, char* schema, char* table);
extern bool  filter_dataset_modify(HTAB* oid2datasets, Oid oid, char* schema, char* table);
extern bool  filter_dataset_delete(HTAB* oid2datasets, Oid oid);
extern bool  filter_dataset_flush(HTAB* oid2datasets);
extern void  filter_dataset_free(HTAB* oid2datasets);

/* bidirectional filter transaction filter set */
extern HTAB* filter_dataset_txnfilterload(HTAB* namespace, HTAB* class);

/* initialize sync dataset */
extern List* filter_dataset_inittableinclude(List* table);

/* initialize sync exclude tables */
extern List* filter_dataset_inittableexclude(List* tableexclude);

extern List* filter_dataset_initaddtablepattern(List* tablepattern);

extern void filter_dataset_listpairsfree(List* rulelist);

extern refresh_tables* filter_dataset_buildrefreshtables(HTAB* hfilters);

extern bool filter_dataset_updatedatasets(List* addtablepattern, HTAB* namespace, List* sysdicthis,
                                          HTAB* syncdatasets);

extern void filter_dataset_updatedatasets_onlinerefresh(HTAB* dataset, List* tables_list);

extern void filter_dataset_pairfree(filter_pair* filterpair);

/* build strategy from refreshtable */
extern List* filter_dataset_buildpairsbyrefreshtables(refresh_tables* rtables);

/* rebuild refreshtables */
extern bool filter_dataset_buildrefreshtablesbyfilters(refresh_tables** prtables, List* filters,
                                                       HTAB* hnamespace, HTAB* hclass);

#endif
