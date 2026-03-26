#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_search.h"
#include "misc/misc_control.h"
#include "misc/misc_stat.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "catalog/catalog.h"
#include "catalog/class.h"
#include "catalog/attribute.h"
#include "catalog/enum.h"
#include "catalog/namespace.h"
#include "catalog/range.h"
#include "catalog/type.h"
#include "catalog/proc.h"
#include "catalog/constraint.h"
#include "catalog/operator.h"
#include "catalog/authid.h"
#include "catalog/database.h"
#include "catalog/index.h"
#include "catalog/relmapfile.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"

#define CATALOG_TYPE_NUM CATALOG_TYPE_RELMAPFILE - 1

typedef void (*sysdict2cachefunc)(cache_sysdicts* sysdicts, catalogdata* catalogdata);
typedef void (*sysdicthisfree)(catalogdata* catalogdata);
typedef void (*sysdicthiscopy)(catalogdata* src, catalogdata* dst);

typedef struct SYSDICT2CACHENODE
{
    int               type;
    sysdict2cachefunc func;
    sysdicthisfree    freefunc;
    sysdicthiscopy    copyfunc;
} sysdict2cachenode;

static sysdict2cachenode m_sysdict2cache[] = {
    {CATALOG_TYPE_NOP, NULL, NULL, NULL},
    {CATALOG_TYPE_CLASS, class_catalogdata2transcache, class_catalogdatafree, NULL},
    {CATALOG_TYPE_ATTRIBUTE, attribute_catalogdata2transcache, attribute_catalogdatafree, NULL},
    {CATALOG_TYPE_TYPE, type_catalogdata2transcache, type_catalogdatafree, NULL},
    {CATALOG_TYPE_NAMESPACE, namespace_catalogdata2transcache, namespace_catalogdatafree, NULL},
    {CATALOG_TYPE_TABLESPACE, NULL, NULL, NULL},
    {CATALOG_TYPE_ENUM, enum_catalogdata2transcache, enum_catalogdatafree, NULL},
    {CATALOG_TYPE_RANGE, range_catalogdata2transcache, range_catalogdatafree, NULL},
    {CATALOG_TYPE_PROC, proc_catalogdata2transcache, proc_catalogdatafree, NULL},
    {CATALOG_TYPE_CONSTRAINT, constraint_catalogdata2transcache, constraint_catalogdatafree, NULL},
    {CATALOG_TYPE_OPERATOR, NULL, NULL, NULL},
    {CATALOG_TYPE_AUTHID, authid_catalogdata2transcache, authid_catalogdatafree, NULL},
    {CATALOG_TYPE_DATABASE, database_catalogdata2transcache, database_catalogdatafree, NULL},
    {CATALOG_TYPE_INDEX, index_catalogdata2transcache, index_catalogdatafree, NULL},
    {CATALOG_TYPE_RELMAPFILE, relmapfile_catalogdata2transcache, relmapfile_catalogdatafree, NULL}};

static int m_sysdict2cachecnt = (sizeof(m_sysdict2cache)) / (sizeof(sysdict2cachenode));

/* Load system catalog information */
void cache_sysdictsload(void** ref_sysdicts)
{
    int                   r = 0;
    int                   fd = -1;
    char                  buffer[FILE_BLK_SIZE];
    cache_sysdicts*       sysdicts = NULL;
    sysdict_header_array* array = NULL;

    array = (sysdict_header_array*)rmalloc0((CATALOG_TYPE_NUM) * sizeof(sysdict_header_array));
    if (NULL == array)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(array, 0, '\0', (CATALOG_TYPE_NUM) * sizeof(sysdict_header_array));

    rmemset1(buffer, 0, '\0', FILE_BLK_SIZE);

    sysdicts = (cache_sysdicts*)*ref_sysdicts;
    if (NULL == sysdicts)
    {
        sysdicts = (cache_sysdicts*)rmalloc1(sizeof(cache_sysdicts));
        if (NULL == sysdicts)
        {
            elog(RLOG_ERROR, "out of memeory");
        }
        rmemset0(sysdicts, 0, '\0', sizeof(cache_sysdicts));
        *ref_sysdicts = sysdicts;
    }

    /*
     * Read data...
     */
    fd = osal_basic_open_file(SYSDICTS_FILE, O_RDWR | BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", SYSDICTS_FILE);
    }
    if ((r = osal_file_read(fd, buffer, FILE_BLK_SIZE)) > 0)
    {
        rmemcpy0(array,
                 0,
                 buffer + sizeof(sysdict_header),
                 (CATALOG_TYPE_NUM) * sizeof(sysdict_header_array));
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_FILE);
    }

    sysdicts->by_class = classcache_load(array);

    sysdicts->by_attribute = attributecache_load(array);

    sysdicts->by_enum = enumcache_load(array);

    sysdicts->by_namespace = namespacecache_load(array);

    sysdicts->by_proc = proccache_load(array);

    sysdicts->by_range = rangecache_load(array);

    sysdicts->by_type = typecache_load(array);

    sysdicts->by_operator = operatorcache_load(array);

    sysdicts->by_authid = authidcache_load(array);

    sysdicts->by_database = databasecache_load(array);
    sysdicts->by_datname2oid = datname2oid_cache_load(array);

    sysdicts->by_constraint = constraintcache_load(array);

    sysdicts->by_index = indexcache_load(array);

    rfree(array);
    array = NULL;

    return;
}

/* Initialize integrate-side system dictionary */
cache_sysdicts* cache_sysdicts_integrate_init(void)
{
    cache_sysdicts* sysdicts = NULL;
    HASHCTL         hctl = {'\0'};

    if (NULL == sysdicts)
    {
        sysdicts = (cache_sysdicts*)rmalloc0(sizeof(cache_sysdicts));
        if (NULL == sysdicts)
        {
            elog(RLOG_ERROR, "out of memeory");
        }
        rmemset0(sysdicts, 0, '\0', sizeof(cache_sysdicts));
    }

    /* pg_class initialization */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(catalog_class_value);
    sysdicts->by_class = hash_create("integrate_class", 1024, &hctl, HASH_ELEM | HASH_BLOBS);

    /* pg_attribute initialization */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(catalog_attribute_value);
    sysdicts->by_attribute = hash_create("integrate_attr", 2048, &hctl, HASH_ELEM | HASH_BLOBS);

    /* pg_database initialization */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(catalog_database_value);
    sysdicts->by_database = hash_create("integrate_database", 256, &hctl, HASH_ELEM | HASH_BLOBS);

    /* pg_datname2oid initialization */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(pg_parser_NameData);
    hctl.entrysize = sizeof(catalog_datname2oid_value);
    sysdicts->by_datname2oid =
        hash_create("integrate_datname2oid", 256, &hctl, HASH_ELEM | HASH_BLOBS);

    /* pg_index initialization */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(catalog_index_hash_entry);
    sysdicts->by_index = hash_create("integrate_index", 1024, &hctl, HASH_ELEM | HASH_BLOBS);

    /* pg_type initialization */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(catalog_type_value);
    sysdicts->by_type = hash_create("integrate_type", 1024, &hctl, HASH_ELEM | HASH_BLOBS);

    return sysdicts;
}

/* Clean up system dictionary */
void cache_sysdicts_free(void* args)
{
    HASH_SEQ_STATUS status;
    ListCell*       lc = NULL;
    cache_sysdicts* sysdicts = NULL;

    sysdicts = (cache_sysdicts*)args;

    if (NULL == sysdicts)
    {
        return;
    }

    /* Clean up relfilenode cache */
    if (NULL != sysdicts->by_relfilenode)
    {
        hash_destroy(sysdicts->by_relfilenode);
    }

    if (NULL != sysdicts->by_class)
    {
        catalog_class_value* catalogclassentry;
        hash_seq_init(&status, sysdicts->by_class);
        while (NULL != (catalogclassentry = hash_seq_search(&status)))
        {
            if (NULL != catalogclassentry->class)
            {
                rfree(catalogclassentry->class);
            }
        }

        hash_destroy(sysdicts->by_class);
    }

    /* Delete attributes table */
    if (NULL != sysdicts->by_attribute)
    {
        catalog_attribute_value* catalogattrentry = NULL;
        hash_seq_init(&status, sysdicts->by_attribute);
        while (NULL != (catalogattrentry = hash_seq_search(&status)))
        {
            if (NULL != catalogattrentry->attrs)
            {
                foreach (lc, catalogattrentry->attrs)
                {
                    rfree(lfirst(lc));
                }
                list_free(catalogattrentry->attrs);
            }
        }
        hash_destroy(sysdicts->by_attribute);
    }

    /* Delete type table */
    if (NULL != sysdicts->by_type)
    {
        catalog_type_value* catalogtypeentry = NULL;
        hash_seq_init(&status, sysdicts->by_type);
        while (NULL != (catalogtypeentry = hash_seq_search(&status)))
        {
            if (NULL != catalogtypeentry->type)
            {
                rfree(catalogtypeentry->type);
            }
        }

        hash_destroy(sysdicts->by_type);
    }

    /* Delete proc table */
    if (NULL != sysdicts->by_proc)
    {
        catalog_proc_value* catalogprocentry = NULL;
        hash_seq_init(&status, sysdicts->by_proc);
        while (NULL != (catalogprocentry = hash_seq_search(&status)))
        {
            if (NULL != catalogprocentry->proc)
            {
                rfree(catalogprocentry->proc);
            }
        }

        hash_destroy(sysdicts->by_proc);
    }

    /* Delete tablespace table */
    if (NULL != sysdicts->by_tablespace)
    {
        /* tablespace table is not used in current program */
        hash_destroy(sysdicts->by_tablespace);
    }

    /* Delete namespace table */
    if (NULL != sysdicts->by_namespace)
    {
        catalog_namespace_value* catalognamespaceentry = NULL;
        hash_seq_init(&status, sysdicts->by_namespace);
        while (NULL != (catalognamespaceentry = hash_seq_search(&status)))
        {
            if (NULL != catalognamespaceentry->namespace)
            {
                rfree(catalognamespaceentry->namespace);
            }
        }
        hash_destroy(sysdicts->by_namespace);
    }

    /* Delete range table */
    if (NULL != sysdicts->by_range)
    {
        catalog_range_value* catalograngeentry = NULL;
        hash_seq_init(&status, sysdicts->by_range);
        while (NULL != (catalograngeentry = hash_seq_search(&status)))
        {
            if (NULL != catalograngeentry->range)
            {
                rfree(catalograngeentry->range);
            }
        }
        hash_destroy(sysdicts->by_range);
    }

    /* Delete enum table */
    if (NULL != sysdicts->by_enum)
    {
        catalog_enum_value* catalogenumentry = NULL;
        hash_seq_init(&status, sysdicts->by_enum);
        while (NULL != (catalogenumentry = hash_seq_search(&status)))
        {
            if (NULL != catalogenumentry->enums)
            {
                foreach (lc, catalogenumentry->enums)
                {
                    rfree(lfirst(lc));
                }
                list_free(catalogenumentry->enums);
            }
        }
        hash_destroy(sysdicts->by_enum);
    }

    /* Delete operator table */
    if (NULL != sysdicts->by_operator)
    {
        catalog_operator_value* catalogoperatorentry = NULL;
        hash_seq_init(&status, sysdicts->by_operator);
        while (NULL != (catalogoperatorentry = hash_seq_search(&status)))
        {
            if (NULL != catalogoperatorentry->operator)
            {
                rfree(catalogoperatorentry->operator);
            }
        }
        hash_destroy(sysdicts->by_operator);
    }

    /* by_authid */
    if (NULL != sysdicts->by_authid)
    {
        catalog_authid_value* catalogauthidentry = NULL;
        hash_seq_init(&status, sysdicts->by_authid);
        while (NULL != (catalogauthidentry = hash_seq_search(&status)))
        {
            if (NULL != catalogauthidentry->authid)
            {
                rfree(catalogauthidentry->authid);
            }
        }
        hash_destroy(sysdicts->by_authid);
    }

    if (NULL != sysdicts->by_constraint)
    {
        catalog_constraint_value* catalogconentry;
        hash_seq_init(&status, sysdicts->by_constraint);
        while (NULL != (catalogconentry = hash_seq_search(&status)))
        {
            if (NULL != catalogconentry->constraint)
            {
                if (0 != catalogconentry->constraint->conkeycnt)
                {
                    rfree(catalogconentry->constraint->conkey);
                }
                rfree(catalogconentry->constraint);
            }
        }
        hash_destroy(sysdicts->by_constraint);
    }

    /*by_database*/
    if (NULL != sysdicts->by_database)
    {
        catalog_database_value* catalogdatabaseentry = NULL;
        hash_seq_init(&status, sysdicts->by_database);
        while (NULL != (catalogdatabaseentry = hash_seq_search(&status)))
        {
            if (NULL != catalogdatabaseentry->database)
            {
                rfree(catalogdatabaseentry->database);
            }
        }
        hash_destroy(sysdicts->by_database);
    }

    /* by_datname2oid */
    if (NULL != sysdicts->by_datname2oid)
    {
        hash_destroy(sysdicts->by_datname2oid);
        sysdicts->by_datname2oid = NULL;
    }

    /* by_index */
    if (NULL != sysdicts->by_index)
    {
        catalog_index_value*      index = NULL;
        catalog_index_hash_entry* catalogindexentry = NULL;
        hash_seq_init(&status, sysdicts->by_index);
        while (NULL != (catalogindexentry = hash_seq_search(&status)))
        {
            if (NULL != catalogindexentry->index_list)
            {
                foreach (lc, catalogindexentry->index_list)
                {
                    index = (catalog_index_value*)lfirst(lc);
                    if (index->index)
                    {
                        if (index->index->indkey)
                        {
                            rfree(index->index->indkey);
                        }
                        rfree(index->index);
                    }
                    rfree(index);
                }
                list_free(catalogindexentry->index_list);
            }
        }
        hash_destroy(sysdicts->by_index);
    }
    rfree(sysdicts);
    return;
}

/*
 * Clean up related data in system catalog based on class
 * Input: sysdicts dictionary table, lc metadata data
 */
void cache_sysdicts_clearsysdicthisbyclass(cache_sysdicts* sysdicts, ListCell* lc)
{
    bool                      found = false;
    ListCell*                 cell = NULL;
    catalogdata*              catalog_data = NULL;
    pg_sysdict_Form_pg_class  class = NULL;
    catalog_attribute_value*  attrInHash = NULL;
    catalog_index_value*      index_value = NULL;
    catalog_index_hash_entry* indexInHash = NULL;

    catalog_data = (catalogdata*)lfirst(lc);

    if (CATALOG_TYPE_CLASS != catalog_data->type)
    {
        return;
    }

    class = (pg_sysdict_Form_pg_class)catalog_data->catalog;

    /* Clean up attribute */
    attrInHash = hash_search(sysdicts->by_attribute, &class->oid, HASH_FIND, &found);
    if (true == found)
    {
        if (NULL != attrInHash->attrs)
        {
            list_free_deep(attrInHash->attrs);
        }
        cell = NULL;
        hash_search(sysdicts->by_attribute, &attrInHash->attrelid, HASH_REMOVE, NULL);
    }

    /* Clean up index */
    indexInHash = hash_search(sysdicts->by_index, &class->oid, HASH_FIND, &found);
    if (true == found)
    {
        if (NULL != indexInHash->index_list)
        {
            foreach (cell, indexInHash->index_list)
            {
                index_value = (catalog_index_value*)lfirst(cell);
                if (index_value->index)
                {
                    if (index_value->index->indkey)
                    {
                        rfree(index_value->index->indkey);
                    }
                    rfree(index_value->index);
                }
                rfree(index_value);
            }
            list_free(indexInHash->index_list);
        }
        hash_search(sysdicts->by_index, &indexInHash->oid, HASH_REMOVE, NULL);
    }

    return;
}

/*
 * Single system catalog application
 */
void cache_sysdicts_txnsysdicthisitem2cache(cache_sysdicts* sysdicts, ListCell* lc)
{
    catalogdata* catalog_data = NULL;

    catalog_data = (catalogdata*)lfirst(lc);

    /* Process based on different data types */
    if ((m_sysdict2cachecnt - 1) < catalog_data->type)
    {
        elog(RLOG_ERROR,
             "sysdicthis 2 transcache logical error, unknown type:%d",
             catalog_data->type);
    }

    if (NULL == m_sysdict2cache[catalog_data->type].func)
    {
        elog(RLOG_ERROR, "logical error, please check catalog type:%d", catalog_data->type);
    }

    m_sysdict2cache[catalog_data->type].func(sysdicts, catalog_data);
}

/* On transaction commit, apply historical dictionary tables to cache */
void cache_sysdicts_txnsysdicthis2cache(cache_sysdicts* sysdicts, List* sysdicthis)
{
    ListCell* lc = NULL;
    foreach (lc, sysdicthis)
    {
        cache_sysdicts_txnsysdicthisitem2cache(sysdicts, lc);
    }
}

/* Build hash table based on relfilenode structure, for quickly finding relfilenode to table oid
 * during parsing */
HTAB* cache_sysdicts_buildrelfilenode2oid(Oid dbid, void* data)
{
    bool                     found = false;
    HASHCTL                  hctl;
    HASH_SEQ_STATUS          status;
    RelFileNode              relfilenode;
    HTAB*                    by_relfilenode = NULL;
    cache_sysdicts*          sysdicts = NULL;
    catalog_class_value*     entry = NULL;
    pg_sysdict_Form_pg_class class = NULL;
    relfilenode2oid*         relfdentry = NULL;

    sysdicts = (cache_sysdicts*)data;

    /* Create hash */
    rmemset1(&hctl, 0, '\0', sizeof(hctl));
    hctl.keysize = sizeof(RelFileNode);
    hctl.entrysize = sizeof(relfilenode2oid);
    by_relfilenode = hash_create("xsynch_relfilenode2oid", 2048, &hctl, HASH_ELEM | HASH_BLOBS);

    /* Load corresponding relationship */
    hash_seq_init(&status, sysdicts->by_class);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        class = entry->class;

        if (PG_SYSDICT_RELKIND_PARTITIONED_TABLE == class->relkind)
        {
            continue;
        }

        if (0 == class->reltablespace)
        {
            relfilenode.spcNode = PG_DFAULT_TABLESPACE;
        }
        else
        {
            relfilenode.spcNode = class->reltablespace;
        }

        relfilenode.dbNode = dbid;
        relfilenode.relNode = class->relfilenode;
        relfdentry = hash_search(by_relfilenode, &relfilenode, HASH_ENTER, &found);
        if (found)
        {
            elog(RLOG_ERROR, "relfilenode:%u already exist in by_relfilenode", relfdentry->oid);
        }
        relfdentry->relfilenode = relfilenode;
        relfdentry->oid = class->oid;
    }
    return by_relfilenode;
}

/* On transaction commit or rollback, release historical dictionary table structure */
void cache_sysdicts_txnsysdicthisfree(List* sysdicthis)
{
    ListCell*    lc = NULL;
    catalogdata* catalog_data = NULL;

    foreach (lc, sysdicthis)
    {
        catalog_data = (catalogdata*)lfirst(lc);

        if (NULL == catalog_data)
        {
            continue;
        }

        /* Process based on different data types */
        if ((m_sysdict2cachecnt - 1) < catalog_data->type)
        {
            elog(RLOG_ERROR,
                 "sysdicthis 2 transcache logical error, unknown type:%d",
                 catalog_data->type);
        }

        if (NULL == m_sysdict2cache[catalog_data->type].freefunc)
        {
            elog(RLOG_ERROR, "logical error, please check catalog type:%d", catalog_data->type);
        }

        m_sysdict2cache[catalog_data->type].freefunc(catalog_data);
    }
}

/* Release sysdict dictionary table structure */
void cache_sysdicts_catalogdatafreevoid(void* args)
{
    catalogdata* catalog_data = NULL;

    if (NULL == args)
    {
        return;
    }
    catalog_data = (catalogdata*)args;

    if (NULL == catalog_data)
    {
        return;
    }

    /* Process based on different data types */
    if ((m_sysdict2cachecnt - 1) < catalog_data->type)
    {
        elog(RLOG_WARNING,
             "sysdicthis 2 transcache logical error, unknown type:%d",
             catalog_data->type);
        return;
    }

    if (NULL == m_sysdict2cache[catalog_data->type].func)
    {
        elog(RLOG_WARNING, "logical error, please check catalog type:%d", catalog_data->type);
        return;
    }

    m_sysdict2cache[catalog_data->type].freefunc(catalog_data);
}

/* Write modified dictionary table cache information to disk, pass in redlsn*/
void sysdictscache_write(cache_sysdicts* sysdicts, XLogRecPtr redolsn)
{
    int                   fd;
    uint64                offset = 0;
    char                  buffer[FILE_BLK_SIZE];
    sysdict_header*       header = NULL;
    sysdict_header_array* array = NULL;

    array = (sysdict_header_array*)rmalloc0((CATALOG_TYPE_NUM) * sizeof(sysdict_header_array));
    if (NULL == array)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(array, 0, 0, (CATALOG_TYPE_NUM) * sizeof(sysdict_header_array));

    header = (sysdict_header*)rmalloc0(sizeof(sysdict_header));
    if (NULL == header)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(header, 0, 0, sizeof(sysdict_header));
    offset = FILE_BLK_SIZE;

    header->magic = SYSDICT_MAGIC;
    header->compatibility = guc_getConfigOptionInt(CFG_KEY_COMPATIBILITY);
    header->checkpointlsn = redolsn;
    rmemcpy1(buffer, 0, header, sizeof(sysdict_header));

    classcache_write(sysdicts->by_class, &offset, array);

    attributecache_write(sysdicts->by_attribute, &offset, array);

    typecache_write(sysdicts->by_type, &offset, array);

    namespacecache_write(sysdicts->by_namespace, &offset, array);

    enumcache_write(sysdicts->by_enum, &offset, array);

    rangecache_write(sysdicts->by_range, &offset, array);

    proccache_write(sysdicts->by_proc, &offset, array);

    operatorcache_write(sysdicts->by_operator, &offset, array);

    authidcache_write(sysdicts->by_authid, &offset, array);

    databasecache_write(sysdicts->by_database, &offset, array);

    constraintcache_write(sysdicts->by_constraint, &offset, array);

    indexcache_write(sysdicts->by_index, &offset, array);

    rmemcpy1(
        buffer, sizeof(sysdict_header), array, (CATALOG_TYPE_NUM) * sizeof(sysdict_header_array));

    fd = osal_basic_open_file(SYSDICTS_TMP_FILE, O_RDWR | O_CREAT | BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", SYSDICTS_TMP_FILE);
    }

    if (osal_file_pwrite(fd, buffer, FILE_BLK_SIZE, 0) != FILE_BLK_SIZE)
    {
        elog(RLOG_ERROR, "could not write to file %s", SYSDICTS_TMP_FILE);
        osal_file_close(fd);
        return;
    }

    if (0 != osal_file_sync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", SYSDICTS_TMP_FILE);
    }

    if (osal_file_close(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", SYSDICTS_TMP_FILE);
    }

    if (osal_durable_rename(SYSDICTS_TMP_FILE, SYSDICTS_FILE, RLOG_DEBUG) != 0)
    {
        elog(RLOG_WARNING, "Error renaming file %s", SYSDICTS_TMP_FILE);
    }

    rfree(array);
    rfree(header);
    array = NULL;
    header = NULL;
    return;
}
