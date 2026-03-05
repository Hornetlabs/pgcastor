#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_search.h"
#include "misc/ripple_misc_control.h"
#include "misc/ripple_misc_stat.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_ffsmgr.h"
#include "catalog/ripple_catalog.h"
#include "catalog/ripple_class.h"
#include "catalog/ripple_attribute.h"
#include "catalog/ripple_enum.h"
#include "catalog/ripple_namespace.h"
#include "catalog/ripple_range.h"
#include "catalog/ripple_type.h"
#include "catalog/ripple_proc.h"
#include "catalog/ripple_constraint.h"
#include "catalog/ripple_operator.h"
#include "catalog/ripple_authid.h"
#include "catalog/ripple_database.h"
#include "catalog/ripple_index.h"
#include "catalog/ripple_relmapfile.h"
#include "works/ripple_workthreadmgr.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"

#define RIPPLE_CATALOG_TYPE_NUM RIPPLE_CATALOG_TYPE_RELMAPFILE - 1

typedef void (*sysdict2cachefunc)(ripple_cache_sysdicts* sysdicts, ripple_catalogdata* catalogdata);
typedef void (*sysdicthisfree)(ripple_catalogdata* catalogdata);
typedef void (*sysdicthiscopy)(ripple_catalogdata* src, ripple_catalogdata* dst);

typedef struct RIPPLE_SYSDICT2CACHENODE
{
    int                 type;
    sysdict2cachefunc   func;
    sysdicthisfree      freefunc;
    sysdicthiscopy      copyfunc;
} ripple_sysdict2cachenode;

static ripple_sysdict2cachenode m_sysdict2cache[] =
{
    {
        RIPPLE_CATALOG_TYPE_NOP,
        NULL,
        NULL,
        NULL
    },
    { 
        RIPPLE_CATALOG_TYPE_CLASS,
        ripple_class_catalogdata2transcache,
        ripple_class_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_ATTRIBUTE,
        ripple_attribute_catalogdata2transcache,
        ripple_attribute_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_TYPE,
        ripple_type_catalogdata2transcache,
        ripple_type_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_NAMESPACE,
        ripple_namespace_catalogdata2transcache,
        ripple_namespace_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_TABLESPACE,
        NULL,
        NULL,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_ENUM,
        ripple_enum_catalogdata2transcache,
        ripple_enum_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_RANGE,
         ripple_range_catalogdata2transcache,
         ripple_range_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_PROC,
        ripple_proc_catalogdata2transcache,
        ripple_proc_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_CONSTRAINT,
        ripple_constraint_catalogdata2transcache,
        ripple_constraint_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_OPERATOR,
        NULL,
        NULL,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_AUTHID,
        ripple_authid_catalogdata2transcache,
        ripple_authid_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_DATABASE,
        ripple_database_catalogdata2transcache,
        ripple_database_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_INDEX,
        ripple_index_catalogdata2transcache,
        ripple_index_catalogdatafree,
        NULL
    },
    {
        RIPPLE_CATALOG_TYPE_RELMAPFILE,
        ripple_relmapfile_catalogdata2transcache,
        ripple_relmapfile_catalogdatafree,
        NULL
    }
};

static int m_sysdict2cachecnt = (sizeof(m_sysdict2cache))/(sizeof(ripple_sysdict2cachenode));

/* 加载系统表信息 */
void ripple_cache_sysdictsload(void** ref_sysdicts)
{
    int r = 0;
    int fd = -1;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    ripple_cache_sysdicts* sysdicts = NULL;
    ripple_sysdict_header_array* array = NULL;

    array = (ripple_sysdict_header_array*)rmalloc0((RIPPLE_CATALOG_TYPE_NUM) * sizeof(ripple_sysdict_header_array));
    if (NULL == array)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(array, 0, '\0', (RIPPLE_CATALOG_TYPE_NUM) * sizeof(ripple_sysdict_header_array));

    rmemset1(buffer, 0, '\0', RIPPLE_FILE_BLK_SIZE);

    sysdicts = (ripple_cache_sysdicts*)*ref_sysdicts;
    if(NULL == sysdicts)
    {
        sysdicts = (ripple_cache_sysdicts*)rmalloc1(sizeof(ripple_cache_sysdicts));
        if(NULL == sysdicts)
        {
            elog(RLOG_ERROR, "out of memeory");
        }
        rmemset0(sysdicts, 0, '\0', sizeof(ripple_cache_sysdicts));
        *ref_sysdicts = sysdicts;
    }

    /*
    * Read data...
    */
    fd = BasicOpenFile(RIPPLE_SYSDICTS_FILE,
                        O_RDWR | RIPPLE_BINARY);
    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not open file %s", RIPPLE_SYSDICTS_FILE);
    }
    if ((r = FileRead(fd, buffer, RIPPLE_FILE_BLK_SIZE)) > 0) 
    {
        rmemcpy0(array, 0, buffer + sizeof(ripple_sysdict_header), (RIPPLE_CATALOG_TYPE_NUM) * sizeof(ripple_sysdict_header_array));
        
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_FILE);
    }

    sysdicts->by_class = ripple_classcache_load(array);

    sysdicts->by_attribute = ripple_attributecache_load(array);

    sysdicts->by_enum = ripple_enumcache_load(array);

    sysdicts->by_namespace = ripple_namespacecache_load(array);

    sysdicts->by_proc = ripple_proccache_load(array);

    sysdicts->by_range = ripple_rangecache_load(array);

    sysdicts->by_type = ripple_typecache_load(array);

    sysdicts->by_operator = ripple_operatorcache_load(array);

    sysdicts->by_authid = ripple_authidcache_load(array);

    sysdicts->by_database = ripple_databasecache_load(array);
    sysdicts->by_datname2oid = ripple_datname2oid_cache_load(array);

    sysdicts->by_constraint = ripple_constraintcache_load(array);

    sysdicts->by_index = ripple_indexcache_load(array);

    rfree(array);
    array = NULL;


    return;
}

/* 初始化pump端系统字典 */
ripple_cache_sysdicts *ripple_cache_sysdicts_pump_init(void)
{
    ripple_cache_sysdicts* sysdicts = NULL;
    HASHCTL hctl = {'\0'};

    if(NULL == sysdicts)
    {
        sysdicts = (ripple_cache_sysdicts*)rmalloc0(sizeof(ripple_cache_sysdicts));
        if(NULL == sysdicts)
        {
            elog(RLOG_ERROR, "out of memeory");
        }
        rmemset0(sysdicts, 0, '\0', sizeof(ripple_cache_sysdicts));
    }

    /* pg_class初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_class_value);
    sysdicts->by_class = hash_create("pump_class",
                                      1024,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_attribute初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_attribute_value);
    sysdicts->by_attribute = hash_create("pump_attr",
                                      2048,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_database初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_database_value);
    sysdicts->by_database = hash_create("pump_database",
                                      256,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_datname2oid初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(xk_pg_parser_NameData);
    hctl.entrysize = sizeof(ripple_catalog_datname2oid_value);
    sysdicts->by_datname2oid = hash_create("pump_datname2oid",
                                      256,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_index初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_index_hash_entry);
    sysdicts->by_index = hash_create("pump_index",
                                      1024,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_type初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_type_value);
    sysdicts->by_type = hash_create("pump_type",
                                      1024,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    return sysdicts;
}

/* 初始化integrate端系统字典 */
ripple_cache_sysdicts *ripple_cache_sysdicts_integrate_init(void)
{
    ripple_cache_sysdicts* sysdicts = NULL;
    HASHCTL hctl = {'\0'};

    if(NULL == sysdicts)
    {
        sysdicts = (ripple_cache_sysdicts*)rmalloc0(sizeof(ripple_cache_sysdicts));
        if(NULL == sysdicts)
        {
            elog(RLOG_ERROR, "out of memeory");
        }
        rmemset0(sysdicts, 0, '\0', sizeof(ripple_cache_sysdicts));
    }

    /* pg_class初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_class_value);
    sysdicts->by_class = hash_create("pump_class",
                                      1024,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_attribute初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_attribute_value);
    sysdicts->by_attribute = hash_create("pump_attr",
                                      2048,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_database初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_database_value);
    sysdicts->by_database = hash_create("pump_database",
                                      256,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_datname2oid初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(xk_pg_parser_NameData);
    hctl.entrysize = sizeof(ripple_catalog_datname2oid_value);
    sysdicts->by_datname2oid = hash_create("pump_datname2oid",
                                      256,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_index初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_index_hash_entry);
    sysdicts->by_index = hash_create("pump_index",
                                      1024,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    /* pg_type初始化 */
    rmemset1(&hctl, 0, 0, sizeof(hctl));
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(ripple_catalog_type_value);
    sysdicts->by_type = hash_create("pump_type",
                                      1024,
                                     &hctl,
                                      HASH_ELEM | HASH_BLOBS);

    return sysdicts;
}

/* 清理系统字典 */
void ripple_cache_sysdicts_free(void* args)
{
    HASH_SEQ_STATUS status;
    ListCell* lc                    = NULL;
    ripple_cache_sysdicts* sysdicts = NULL;

    sysdicts = (ripple_cache_sysdicts*)args;

    if(NULL == sysdicts)
    {
        return;
    }

    /* relfilenode 缓存清理 */
    if(NULL != sysdicts->by_relfilenode)
    {
        hash_destroy(sysdicts->by_relfilenode);
    }

    if(NULL != sysdicts->by_class)
    {
        ripple_catalog_class_value *catalogclassentry;
        hash_seq_init(&status,sysdicts->by_class);
        while (NULL != (catalogclassentry = hash_seq_search(&status)))
        {
            if(NULL != catalogclassentry->ripple_class)
            {
                rfree(catalogclassentry->ripple_class);
            }
        }

        hash_destroy(sysdicts->by_class);
    }

    /* attributes 表删除 */
    if(NULL != sysdicts->by_attribute)
    {
        ripple_catalog_attribute_value* catalogattrentry = NULL;
        hash_seq_init(&status,sysdicts->by_attribute);
        while(NULL != (catalogattrentry = hash_seq_search(&status)))
        {
            if(NULL != catalogattrentry->attrs)
            {
                foreach(lc, catalogattrentry->attrs)
                {
                    rfree(lfirst(lc));
                }
                list_free(catalogattrentry->attrs);
            }
        }
        hash_destroy(sysdicts->by_attribute);
    }

    /* type 表删除 */
    if(NULL != sysdicts->by_type)
    {
        ripple_catalog_type_value* catalogtypeentry = NULL;
        hash_seq_init(&status,sysdicts->by_type);
        while(NULL != (catalogtypeentry = hash_seq_search(&status)))
        {
            if(NULL != catalogtypeentry->ripple_type)
            {
                rfree(catalogtypeentry->ripple_type);
            }
        }

        hash_destroy(sysdicts->by_type);
    }

    /* proc 表删除 */
    if(NULL != sysdicts->by_proc)
    {
        ripple_catalog_proc_value* catalogprocentry = NULL;
        hash_seq_init(&status,sysdicts->by_proc);
        while(NULL != (catalogprocentry = hash_seq_search(&status)))
        {
            if(NULL != catalogprocentry->ripple_proc)
            {
                rfree(catalogprocentry->ripple_proc);
            }
        }

        hash_destroy(sysdicts->by_proc);
    }

    /* tablespace 表删除 */
    if(NULL != sysdicts->by_tablespace)
    {
        /* tablespace 表在当前程序中没有用到 */
        hash_destroy(sysdicts->by_tablespace);
    }

    /* namespace 表删除 */
    if(NULL != sysdicts->by_namespace)
    {
        ripple_catalog_namespace_value* catalognamespaceentry = NULL;
        hash_seq_init(&status,sysdicts->by_namespace);
        while(NULL != (catalognamespaceentry = hash_seq_search(&status)))
        {
            if(NULL != catalognamespaceentry->ripple_namespace)
            {
                rfree(catalognamespaceentry->ripple_namespace);
            }
        }
        hash_destroy(sysdicts->by_namespace);
    }

    /* range 表删除 */
    if(NULL != sysdicts->by_range)
    {
        ripple_catalog_range_value* catalograngeentry = NULL;
        hash_seq_init(&status,sysdicts->by_range);
        while(NULL != (catalograngeentry = hash_seq_search(&status)))
        {
            if(NULL != catalograngeentry->ripple_range)
            {
                rfree(catalograngeentry->ripple_range);
            }
        }
        hash_destroy(sysdicts->by_range);
    }

    /* enum 表删除 */
    if(NULL != sysdicts->by_enum)
    {
        ripple_catalog_enum_value* catalogenumentry = NULL;
        hash_seq_init(&status,sysdicts->by_enum);
        while(NULL != (catalogenumentry = hash_seq_search(&status)))
        {
            if(NULL != catalogenumentry->enums)
            {
                foreach(lc, catalogenumentry->enums)
                {
                    rfree(lfirst(lc));
                }
                list_free(catalogenumentry->enums);
            }
        }
        hash_destroy(sysdicts->by_enum);
    }

    /* operator 表删除 */
    if(NULL != sysdicts->by_operator)
    {
        ripple_catalog_operator_value* catalogoperatorentry = NULL;
        hash_seq_init(&status,sysdicts->by_operator);
        while(NULL != (catalogoperatorentry = hash_seq_search(&status)))
        {
            if(NULL != catalogoperatorentry->ripple_operator)
            {
                rfree(catalogoperatorentry->ripple_operator);
            }
        }
        hash_destroy(sysdicts->by_operator);
    }

    /* by_authid */
    if(NULL != sysdicts->by_authid)
    {
        ripple_catalog_authid_value* catalogauthidentry = NULL;
        hash_seq_init(&status,sysdicts->by_authid);
        while(NULL != (catalogauthidentry = hash_seq_search(&status)))
        {
            if(NULL != catalogauthidentry->ripple_authid)
            {
                rfree(catalogauthidentry->ripple_authid);
            }
        }
        hash_destroy(sysdicts->by_authid);
    }

    if(NULL != sysdicts->by_constraint)
    {
        ripple_catalog_constraint_value *catalogconentry;
        hash_seq_init(&status,sysdicts->by_constraint);
        while (NULL != (catalogconentry = hash_seq_search(&status)))
        {
            if(NULL != catalogconentry->constraint)
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
    if(NULL != sysdicts->by_database)
    {
        ripple_catalog_database_value* catalogdatabaseentry = NULL;
        hash_seq_init(&status,sysdicts->by_database);
        while(NULL != (catalogdatabaseentry = hash_seq_search(&status)))
        {
            if(NULL != catalogdatabaseentry->ripple_database)
            {
                rfree(catalogdatabaseentry->ripple_database);
            }
        }
        hash_destroy(sysdicts->by_database);
    }

    /* by_datname2oid */
    if(NULL != sysdicts->by_datname2oid)
    {
        hash_destroy(sysdicts->by_datname2oid);
        sysdicts->by_datname2oid = NULL;
    }

    /* by_index */
    if(NULL != sysdicts->by_index)
    {
        ripple_catalog_index_value* index = NULL;
        ripple_catalog_index_hash_entry* catalogindexentry = NULL;
        hash_seq_init(&status,sysdicts->by_index);
        while(NULL != (catalogindexentry = hash_seq_search(&status)))
        {
            if(NULL != catalogindexentry->ripple_index_list)
            {
                foreach(lc, catalogindexentry->ripple_index_list)
                {
                    index = (ripple_catalog_index_value*)lfirst(lc);
                    if (index->ripple_index)
                    {
                        if (index->ripple_index->indkey)
                        {
                            rfree(index->ripple_index->indkey);
                        }
                        rfree(index->ripple_index);
                    }
                    rfree(index);
                }
                list_free(catalogindexentry->ripple_index_list);
            }
        }
        hash_destroy(sysdicts->by_index);
    }
    rfree(sysdicts);
    return;
}

/*
 * 根据class清理系统表中相关数据
 * 入参：sysdicts字典表， lc metadata数据
*/
void ripple_cache_sysdicts_clearsysdicthisbyclass(ripple_cache_sysdicts* sysdicts, ListCell* lc)
{
    bool found                                      = false;
    ListCell* cell                                  = NULL;
    ripple_catalogdata* catalogdata                 = NULL;
    xk_pg_sysdict_Form_pg_class class               = NULL;
    ripple_catalog_attribute_value* attrInHash      = NULL;
    ripple_catalog_index_value* index_value         = NULL;
    ripple_catalog_index_hash_entry* indexInHash    = NULL;

    catalogdata = (ripple_catalogdata*)lfirst(lc);

    if (RIPPLE_CATALOG_TYPE_CLASS != catalogdata->type)
    {
        return;
    }

    class = (xk_pg_sysdict_Form_pg_class)catalogdata->catalog;

    /* 清理attribute */
    attrInHash = hash_search(sysdicts->by_attribute, &class->oid, HASH_FIND, &found);
    if(true == found)
    {
        if(NULL != attrInHash->attrs)
        {
            list_free_deep(attrInHash->attrs);
        }
        cell = NULL;
        hash_search(sysdicts->by_attribute, &attrInHash->attrelid, HASH_REMOVE, NULL);
    }

    /* 清理index */
    indexInHash = hash_search(sysdicts->by_index, &class->oid, HASH_FIND, &found);
    if(true == found)
    {
        if(NULL != indexInHash->ripple_index_list)
        {
            foreach(cell, indexInHash->ripple_index_list)
            {
                index_value = (ripple_catalog_index_value*)lfirst(cell);
                if (index_value->ripple_index)
                {
                    if (index_value->ripple_index->indkey)
                    {
                        rfree(index_value->ripple_index->indkey);
                    }
                    rfree(index_value->ripple_index);
                }
                rfree(index_value);
            }
            list_free(indexInHash->ripple_index_list);
        }
        hash_search(sysdicts->by_index, &indexInHash->oid, HASH_REMOVE, NULL);
    }

    return;
}

/*
 * 单个系统表应用
*/
void ripple_cache_sysdicts_txnsysdicthisitem2cache(ripple_cache_sysdicts* sysdicts, ListCell* lc)
{
    ripple_catalogdata* catalogdata = NULL;

    catalogdata = (ripple_catalogdata*)lfirst(lc);

    /* 根据不同的数据类型处理 */
    if((m_sysdict2cachecnt - 1) < catalogdata->type)
    {
        elog(RLOG_ERROR, "sysdicthis 2 transcache logical error, unknown type:%d", catalogdata->type);
    }

    if(NULL == m_sysdict2cache[catalogdata->type].func)
    {
        elog(RLOG_ERROR, "logical error, please check catalog type:%d", catalogdata->type);
    }

    m_sysdict2cache[catalogdata->type].func(sysdicts, catalogdata);
}

/* 在事务提交, 将历史的字典表应用到缓存中 */
void ripple_cache_sysdicts_txnsysdicthis2cache(ripple_cache_sysdicts* sysdicts, List* sysdicthis)
{
    ListCell* lc = NULL;
    foreach(lc, sysdicthis)
    {
        ripple_cache_sysdicts_txnsysdicthisitem2cache(sysdicts, lc);
    }
}

/* 按照 relfilenode 的结构构建hash表,用于在解析的过程中快速查找relfilenode到表oid */
HTAB* ripple_cache_sysdicts_buildrelfilenode2oid(Oid dbid, void* data)
{
    bool found = false;
    HASHCTL hctl;
    HASH_SEQ_STATUS status;
    RelFileNode ripple_relfilenode;
    HTAB* by_relfilenode = NULL;
    ripple_cache_sysdicts* sysdicts = NULL;
    ripple_catalog_class_value *entry = NULL;
    xk_pg_sysdict_Form_pg_class class = NULL;
    ripple_relfilenode2oid* relfdentry = NULL;

    sysdicts = (ripple_cache_sysdicts*)data;

    /* 创建 hash */
    rmemset1(&hctl, 0, '\0', sizeof(hctl));
    hctl.keysize = sizeof(RelFileNode);
    hctl.entrysize = sizeof(ripple_relfilenode2oid);
    by_relfilenode = hash_create("xsynch_relfilenode2oid", 2048, &hctl,
                                             HASH_ELEM | HASH_BLOBS);

    /* 加载对应关系 */
    hash_seq_init(&status, sysdicts->by_class);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        class = entry->ripple_class;

        if(XK_PG_SYSDICT_RELKIND_PARTITIONED_TABLE == class->relkind)
        {
            continue;
        }

        if(0 == class->reltablespace)
        {
            ripple_relfilenode.spcNode = RIPPLE_PG_DFAULT_TABLESPACE;
        }
        else
        {
            ripple_relfilenode.spcNode = class->reltablespace;
        }

        ripple_relfilenode.dbNode = dbid;
        ripple_relfilenode.relNode = class->relfilenode;
        relfdentry = hash_search(by_relfilenode, &ripple_relfilenode, HASH_ENTER, &found);
        if(found)
        {
            elog(RLOG_ERROR, "relfilenode:%u already exist in by_relfilenode", relfdentry->oid);
        }
        relfdentry->relfilenode = ripple_relfilenode;
        relfdentry->oid = class->oid;
    }
    return by_relfilenode;
}

/* 在事务提交或回滚时，将历史的字典表结构释放 */
void ripple_cache_sysdicts_txnsysdicthisfree(List* sysdicthis)
{
    ListCell* lc = NULL;
    ripple_catalogdata* catalogdata = NULL;

    foreach(lc, sysdicthis)
    {
        catalogdata = (ripple_catalogdata*)lfirst(lc);

        if(NULL == catalogdata)
        {
            continue;
        }

        /* 根据不同的数据类型处理 */
        if((m_sysdict2cachecnt - 1) < catalogdata->type)
        {
            elog(RLOG_ERROR, "sysdicthis 2 transcache logical error, unknown type:%d", catalogdata->type);
        }

        if(NULL == m_sysdict2cache[catalogdata->type].freefunc)
        {
            elog(RLOG_ERROR, "logical error, please check catalog type:%d", catalogdata->type);
        }

        m_sysdict2cache[catalogdata->type].freefunc(catalogdata);
    }
}

/* 将 sysdict 字典表结构释放 */
void ripple_cache_sysdicts_catalogdatafreevoid(void* args)
{
    ripple_catalogdata* catalogdata = NULL;

    if(NULL == args)
    {
        return;
    }
    catalogdata = (ripple_catalogdata*)args;

    if(NULL == catalogdata)
    {
        return;
    }

    /* 根据不同的数据类型处理 */
    if((m_sysdict2cachecnt - 1) < catalogdata->type)
    {
        elog(RLOG_WARNING, "sysdicthis 2 transcache logical error, unknown type:%d", catalogdata->type);
        return;
    }

    if(NULL == m_sysdict2cache[catalogdata->type].func)
    {
        elog(RLOG_WARNING, "logical error, please check catalog type:%d", catalogdata->type);
        return;
    }

    m_sysdict2cache[catalogdata->type].freefunc(catalogdata);
}

/* 将修改后的字典表缓存信息落盘 传入redlsn*/
void ripple_sysdictscache_write(ripple_cache_sysdicts* sysdicts, XLogRecPtr redolsn)
{
    int	 fd;
    uint64 offset = 0;
    char buffer[RIPPLE_FILE_BLK_SIZE];
    ripple_sysdict_header* header = NULL;
    ripple_sysdict_header_array* array = NULL;

    array = (ripple_sysdict_header_array*)rmalloc0((RIPPLE_CATALOG_TYPE_NUM) * sizeof(ripple_sysdict_header_array));
    if (NULL == array)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(array, 0, 0, (RIPPLE_CATALOG_TYPE_NUM) * sizeof(ripple_sysdict_header_array));

    header = (ripple_sysdict_header*)rmalloc0(sizeof(ripple_sysdict_header));
    if (NULL == header)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(header, 0, 0, sizeof(ripple_sysdict_header));
    offset = RIPPLE_FILE_BLK_SIZE;

    header->magic = RIPPLE_SYSDICT_MAGIC;
    header->compatibility = guc_getConfigOptionInt(RIPPLE_CFG_KEY_COMPATIBILITY);
    header->checkpointlsn = redolsn;
    rmemcpy1(buffer, 0, header, sizeof(ripple_sysdict_header));

    ripple_classcache_write(sysdicts->by_class, &offset, array);

    ripple_attributecache_write(sysdicts->by_attribute, &offset, array);

    ripple_typecache_write(sysdicts->by_type, &offset, array);

    ripple_namespacecache_write(sysdicts->by_namespace, &offset, array);

    ripple_enumcache_write(sysdicts->by_enum, &offset, array);

    ripple_rangecache_write(sysdicts->by_range, &offset, array);

    ripple_proccache_write(sysdicts->by_proc, &offset, array);

    ripple_operatorcache_write(sysdicts->by_operator, &offset, array);

    ripple_authidcache_write(sysdicts->by_authid, &offset, array);

    ripple_databasecache_write(sysdicts->by_database, &offset, array);

    ripple_constraintcache_write(sysdicts->by_constraint, &offset, array);

    ripple_indexcache_write(sysdicts->by_index, &offset, array);

    rmemcpy1(buffer, sizeof(ripple_sysdict_header), array, (RIPPLE_CATALOG_TYPE_NUM) * sizeof(ripple_sysdict_header_array));

    fd = BasicOpenFile(RIPPLE_SYSDICTS_TMP_FILE,
                        O_RDWR | O_CREAT | RIPPLE_BINARY);

    if (fd < 0)
    {
        elog(RLOG_ERROR, "could not create file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }

    if (FilePWrite(fd, buffer, RIPPLE_FILE_BLK_SIZE, 0) != RIPPLE_FILE_BLK_SIZE) {
        elog(RLOG_ERROR, "could not write to file %s", RIPPLE_SYSDICTS_TMP_FILE);
        FileClose(fd);
        return;
    }

    if(0 != FileSync(fd))
    {
        elog(RLOG_ERROR, "could not fsync file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }

    if(FileClose(fd))
    {
        elog(RLOG_ERROR, "could not close file %s", RIPPLE_SYSDICTS_TMP_FILE);
    }

    if (durable_rename(RIPPLE_SYSDICTS_TMP_FILE, RIPPLE_SYSDICTS_FILE, RLOG_DEBUG) != 0) 
	{
		elog(RLOG_WARNING, "Error renaming file %s", RIPPLE_SYSDICTS_TMP_FILE);
	}

    rfree(array);
    rfree(header);
    array = NULL;
    header = NULL;
    return;
    
}
