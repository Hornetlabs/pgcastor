#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/conn/conn.h"
#include "utils/list/list_func.h"
#include "utils/guc/guc.h"
#include "port/file/fd.h"
#include "utils/hash/hash_utils.h"
#include "utils/hash/hash_search.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "catalog/control.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
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
#include "works/parserwork/wal/decode_colvalue.h"

#define CATALOG_SYSDICT_SCHEMA "pg_catalog"
#define CATALOG_PG_CLASS       "pg_class"
#define CATALOG_PG_ATTRIBUTE   "pg_attribute"
#define CATALOG_PG_NAMESPACE   "pg_namespace"
#define CATALOG_PG_TYPE        "pg_type"
#define CATALOG_PG_INDEX       "pg_index"

static catalogdata* catalog_copy_class(catalogdata* catalog_src);
static catalogdata* catalog_copy_attribute(catalogdata* catalog_src);
static catalogdata* catalog_copy_type(catalogdata* catalog_src);
static catalogdata* catalog_copy_namespace(catalogdata* catalog_src);
static catalogdata* catalog_copy_enum(catalogdata* catalog_src);
static catalogdata* catalog_copy_range(catalogdata* catalog_src);
static catalogdata* catalog_copy_proc(catalogdata* catalog_src);
static catalogdata* catalog_copy_constraint(catalogdata* catalog_src);
static catalogdata* catalog_copy_authid(catalogdata* catalog_src);
static catalogdata* catalog_copy_database(catalogdata* catalog_src);
static catalogdata* catalog_copy_relmapfile(catalogdata* catalog_src);
static catalogdata* catalog_copy_index(catalogdata* catalog_src);

static catalogdata* catalog_colvalued2catalog_pg12(void* in_colvalues);

static catalogdata* catalog_colvalue_no_filter_conversion_pg12(void* in_colvalues);

static catalogdata* catalog_colvalue_no_filter_conversion(int   dbtype,
                                                          int   dbversion,
                                                          void* in_colvalues);
static catalogdata* catalog_colvalue_no_filter_conversion_postgres(int   dbversion,
                                                                   void* in_colvalues);

typedef catalogdata* (*colvalue2catalog_dbtype_func)(int dbversion, void* in_colvalues);

typedef catalogdata* (*colvalue2catalog_dbversion_func)(void* in_colvalues);

typedef catalogdata* (*catalog_copy_func)(catalogdata* catalog_src);

typedef struct CATALOG_COPY_BY_TYPE
{
    int               type;
    catalog_copy_func func;
} catalog_copy_by_type;

typedef struct COLVALUE2CATALOG_BYDBTYPE
{
    int                          dbtype;
    colvalue2catalog_dbtype_func func;
    colvalue2catalog_dbtype_func no_filter_func;
} colvalue2catalog_bydbtype;

typedef struct COLVALUE2CATALOG_BYDBVERSION
{
    int                             dbversion;
    colvalue2catalog_dbversion_func func;
    colvalue2catalog_dbversion_func no_filter_func;
} colvalue2catalog_bydbversion;

static catalog_copy_by_type m_catalog_copy_fmgr[] = {
    {CATALOG_TYPE_NOP, NULL},
    {CATALOG_TYPE_CLASS, catalog_copy_class},
    {CATALOG_TYPE_ATTRIBUTE, catalog_copy_attribute},
    {CATALOG_TYPE_TYPE, catalog_copy_type},
    {CATALOG_TYPE_NAMESPACE, catalog_copy_namespace},
    {CATALOG_TYPE_TABLESPACE, NULL},
    {CATALOG_TYPE_ENUM, catalog_copy_enum},
    {CATALOG_TYPE_RANGE, catalog_copy_range},
    {CATALOG_TYPE_PROC, catalog_copy_proc},
    {CATALOG_TYPE_CONSTRAINT, catalog_copy_constraint},
    {CATALOG_TYPE_OPERATOR, NULL},
    {CATALOG_TYPE_AUTHID, catalog_copy_authid},
    {CATALOG_TYPE_DATABASE, catalog_copy_database},
    {CATALOG_TYPE_INDEX, catalog_copy_index},
    {CATALOG_TYPE_RELMAPFILE, catalog_copy_relmapfile}};

static int m_catalog_copy_fmgr_cnt = (sizeof(m_catalog_copy_fmgr)) / (sizeof(catalog_copy_by_type));

static colvalue2catalog_bydbversion m_colvalue2catalog_pg_distribute[] = {
    {PGDBVERSION_NOP, NULL, NULL},
    {PGDBVERSION_12, catalog_colvalued2catalog_pg12, catalog_colvalue_no_filter_conversion_pg12}};

static int m_colvalue2catalog_pg_cnt =
    (sizeof(m_colvalue2catalog_pg_distribute)) / (sizeof(colvalue2catalog_bydbversion));

/* colvalue to catalog postgres dispatch entry */
static catalogdata* catalog_colvalued2catalog_postgres(int dbversion, void* in_colvalues)
{
    if ((m_colvalue2catalog_pg_cnt - 1) < dbversion ||
        NULL == m_colvalue2catalog_pg_distribute[dbversion].func)
    {
        return NULL;
    }
    return m_colvalue2catalog_pg_distribute[dbversion].func(in_colvalues);
}

static colvalue2catalog_bydbtype m_colvalue2catalog_dbtype_distribute[] = {
    {DATABASE_TYPE_NOP, NULL, NULL},
    {DATABASE_TYPE_POSTGRESQL,
     catalog_colvalued2catalog_postgres,
     catalog_colvalue_no_filter_conversion_postgres}};

static int m_colvalue2catalog_bydbtype_cnt =
    (sizeof(m_colvalue2catalog_dbtype_distribute)) / (sizeof(colvalue2catalog_bydbtype));

static catalogdata* catalog_copy_class(catalogdata* catalog_src)
{
    catalogdata*               result = NULL;
    catalog_class_value*       classvalue_src = NULL;
    catalog_class_value*       classvalue_dst = NULL;
    pg_parser_sysdict_pgclass* class_src = NULL;
    pg_parser_sysdict_pgclass* class_dst = NULL;

    classvalue_src = (catalog_class_value*)catalog_src->catalog;
    class_src = classvalue_src->class;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for classvalue */
    classvalue_dst = rmalloc0(sizeof(catalog_class_value));
    if (!classvalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(classvalue_dst, 0, 0, sizeof(catalog_class_value));

    /* Allocate memory for class table */
    class_dst = rmalloc0(sizeof(pg_parser_sysdict_pgclass));
    if (!class_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(class_dst, 0, 0, sizeof(pg_parser_sysdict_pgclass));

    /* No pointers, direct copy */
    rmemcpy0(class_dst, 0, class_src, sizeof(pg_parser_sysdict_pgclass));

    classvalue_dst->oid = classvalue_src->oid;
    classvalue_dst->class = class_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)classvalue_dst;

    return result;
}

static catalogdata* catalog_copy_attribute(catalogdata* catalog_src)
{
    catalogdata*                    result = NULL;
    catalog_attribute_value*        attributevalue_src = NULL;
    catalog_attribute_value*        attributevalue_dst = NULL;
    pg_parser_sysdict_pgattributes* attribute_src = NULL;
    pg_parser_sysdict_pgattributes* attribute_dst = NULL;
    List*                           attribute_list_src = NULL;
    List*                           attribute_list_dst = NULL;
    ListCell*                       cell = NULL;

    attributevalue_src = (catalog_attribute_value*)catalog_src->catalog;
    attribute_list_src = attributevalue_src->attrs;

    foreach (cell, attribute_list_src)
    {
        attribute_src = (pg_parser_sysdict_pgattributes*)lfirst(cell);

        /* Allocate memory for attribute table */
        attribute_dst = rmalloc0(sizeof(pg_parser_sysdict_pgattributes));
        if (!attribute_dst)
        {
            elog(RLOG_ERROR, "oom");
        }
        rmemset0(attribute_dst, 0, 0, sizeof(pg_parser_sysdict_pgattributes));

        /* No pointers, direct copy */
        rmemcpy0(attribute_dst, 0, attribute_src, sizeof(pg_parser_sysdict_pgattributes));
        attribute_list_dst = lappend(attribute_list_dst, (void*)attribute_dst);
    }

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for attribute value */
    attributevalue_dst = rmalloc0(sizeof(catalog_attribute_value));
    if (!attributevalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(attributevalue_dst, 0, 0, sizeof(catalog_attribute_value));

    attributevalue_dst->attrelid = attributevalue_src->attrelid;
    attributevalue_dst->attrs = attribute_list_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)attributevalue_dst;

    return result;
}

static catalogdata* catalog_copy_type(catalogdata* catalog_src)
{
    catalogdata*              result = NULL;
    catalog_type_value*       typevalue_src = NULL;
    catalog_type_value*       typevalue_dst = NULL;
    pg_parser_sysdict_pgtype* type_src = NULL;
    pg_parser_sysdict_pgtype* type_dst = NULL;

    typevalue_src = (catalog_type_value*)catalog_src->catalog;
    type_src = typevalue_src->type;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for typevalue */
    typevalue_dst = rmalloc0(sizeof(catalog_type_value));
    if (!typevalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(typevalue_dst, 0, 0, sizeof(catalog_type_value));

    /* Allocate memory for type table */
    type_dst = rmalloc0(sizeof(pg_parser_sysdict_pgtype));
    if (!type_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(type_dst, 0, 0, sizeof(pg_parser_sysdict_pgtype));

    /* No pointers, direct copy */
    rmemcpy0(type_dst, 0, type_src, sizeof(pg_parser_sysdict_pgtype));

    typevalue_dst->oid = typevalue_src->oid;
    typevalue_dst->type = type_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)typevalue_dst;

    return result;
}

static catalogdata* catalog_copy_namespace(catalogdata* catalog_src)
{
    catalogdata*                   result = NULL;
    catalog_namespace_value*       namespacevalue_src = NULL;
    catalog_namespace_value*       namespacevalue_dst = NULL;
    pg_parser_sysdict_pgnamespace* namespace_src = NULL;
    pg_parser_sysdict_pgnamespace* namespace_dst = NULL;

    namespacevalue_src = (catalog_namespace_value*)catalog_src->catalog;
    namespace_src = namespacevalue_src->namespace;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for namespacevalue */
    namespacevalue_dst = rmalloc0(sizeof(catalog_namespace_value));
    if (!namespacevalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(namespacevalue_dst, 0, 0, sizeof(catalog_namespace_value));

    /* Allocate memory for namespace table */
    namespace_dst = rmalloc0(sizeof(pg_parser_sysdict_pgnamespace));
    if (!namespace_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(namespace_dst, 0, 0, sizeof(pg_parser_sysdict_pgnamespace));

    /* No pointers, direct copy */
    rmemcpy0(namespace_dst, 0, namespace_src, sizeof(pg_parser_sysdict_pgnamespace));

    namespacevalue_dst->oid = namespacevalue_src->oid;
    namespacevalue_dst->namespace = namespace_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)namespacevalue_dst;

    return result;
}

static catalogdata* catalog_copy_enum(catalogdata* catalog_src)
{
    catalogdata*              result = NULL;
    catalog_enum_value*       enumvalue_src = NULL;
    catalog_enum_value*       enumvalue_dst = NULL;
    pg_parser_sysdict_pgenum* enum_src = NULL;
    pg_parser_sysdict_pgenum* enum_dst = NULL;
    List*                     enum_list_src = NULL;
    List*                     enum_list_dst = NULL;
    ListCell*                 cell = NULL;

    enumvalue_src = (catalog_enum_value*)catalog_src->catalog;
    enum_list_src = enumvalue_src->enums;

    foreach (cell, enum_list_src)
    {
        enum_src = (pg_parser_sysdict_pgenum*)lfirst(cell);

        /* Allocate memory for enum table */
        enum_dst = rmalloc0(sizeof(pg_parser_sysdict_pgenum));
        if (!enum_dst)
        {
            elog(RLOG_ERROR, "oom");
        }
        rmemset0(enum_dst, 0, 0, sizeof(pg_parser_sysdict_pgenum));

        /* No pointers, direct copy */
        rmemcpy0(enum_dst, 0, enum_src, sizeof(pg_parser_sysdict_pgenum));
        enum_list_dst = lappend(enum_list_dst, (void*)enum_dst);
    }

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for enum value */
    enumvalue_dst = rmalloc0(sizeof(catalog_enum_value));
    if (!enumvalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(enumvalue_dst, 0, 0, sizeof(catalog_enum_value));

    enumvalue_dst->enumtypid = enumvalue_src->enumtypid;
    enumvalue_dst->enums = enum_list_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)enumvalue_dst;

    return result;
}

static catalogdata* catalog_copy_range(catalogdata* catalog_src)
{
    catalogdata*               result = NULL;
    catalog_range_value*       rangevalue_src = NULL;
    catalog_range_value*       rangevalue_dst = NULL;
    pg_parser_sysdict_pgrange* range_src = NULL;
    pg_parser_sysdict_pgrange* range_dst = NULL;

    rangevalue_src = (catalog_range_value*)catalog_src->catalog;
    range_src = rangevalue_src->range;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for rangevalue */
    rangevalue_dst = rmalloc0(sizeof(catalog_range_value));
    if (!rangevalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(rangevalue_dst, 0, 0, sizeof(catalog_range_value));

    /* Allocate memory for range table */
    range_dst = rmalloc0(sizeof(pg_parser_sysdict_pgrange));
    if (!range_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(range_dst, 0, 0, sizeof(pg_parser_sysdict_pgrange));

    /* No pointers, direct copy */
    rmemcpy0(range_dst, 0, range_src, sizeof(pg_parser_sysdict_pgrange));

    rangevalue_dst->rngtypid = rangevalue_src->rngtypid;
    rangevalue_dst->range = range_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)rangevalue_dst;

    return result;
}

static catalogdata* catalog_copy_proc(catalogdata* catalog_src)
{
    catalogdata*              result = NULL;
    catalog_proc_value*       procvalue_src = NULL;
    catalog_proc_value*       procvalue_dst = NULL;
    pg_parser_sysdict_pgproc* proc_src = NULL;
    pg_parser_sysdict_pgproc* proc_dst = NULL;

    procvalue_src = (catalog_proc_value*)catalog_src->catalog;
    proc_src = procvalue_src->proc;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for procvalue */
    procvalue_dst = rmalloc0(sizeof(catalog_proc_value));
    if (!procvalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(procvalue_dst, 0, 0, sizeof(catalog_proc_value));

    /* Allocate memory for proc table */
    proc_dst = rmalloc0(sizeof(pg_parser_sysdict_pgproc));
    if (!proc_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(proc_dst, 0, 0, sizeof(pg_parser_sysdict_pgproc));

    /* No pointers, direct copy */
    rmemcpy0(proc_dst, 0, proc_src, sizeof(pg_parser_sysdict_pgproc));

    procvalue_dst->oid = procvalue_src->oid;
    procvalue_dst->proc = proc_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)procvalue_dst;

    return result;
}

static catalogdata* catalog_copy_constraint(catalogdata* catalog_src)
{
    catalogdata*                    result = NULL;
    catalog_constraint_value*       constraintvalue_src = NULL;
    catalog_constraint_value*       constraintvalue_dst = NULL;
    pg_parser_sysdict_pgconstraint* constraint_src = NULL;
    pg_parser_sysdict_pgconstraint* constraint_dst = NULL;

    constraintvalue_src = (catalog_constraint_value*)catalog_src->catalog;
    constraint_src = constraintvalue_src->constraint;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for constraintvalue */
    constraintvalue_dst = rmalloc0(sizeof(catalog_constraint_value));
    if (!constraintvalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(constraintvalue_dst, 0, 0, sizeof(catalog_constraint_value));

    /* Allocate memory for constraint table */
    constraint_dst = rmalloc0(sizeof(pg_parser_sysdict_pgconstraint));
    if (!constraint_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(constraint_dst, 0, 0, sizeof(pg_parser_sysdict_pgconstraint));

    /* No pointers, direct copy */
    rmemcpy0(constraint_dst, 0, constraint_src, sizeof(pg_parser_sysdict_pgconstraint));

    constraintvalue_dst->conrelid = constraintvalue_src->conrelid;
    constraintvalue_dst->constraint = constraint_dst;

    if (0 != constraint_dst->conkeycnt)
    {
        constraint_dst->conkey = (int16_t*)rmalloc0(constraint_dst->conkeycnt * sizeof(int16_t));
        if (NULL == constraint_dst->conkey)
        {
            elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
            return NULL;
        }
        rmemset0(constraint_dst->conkey, 0, '\0', constraint_dst->conkeycnt * sizeof(int16_t));
        rmemcpy0(constraint_dst->conkey,
                 0,
                 constraint_src->conkey,
                 constraint_dst->conkeycnt * sizeof(int16_t));
    }

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)constraintvalue_dst;

    return result;
}

static catalogdata* catalog_copy_authid(catalogdata* catalog_src)
{
    catalogdata*                result = NULL;
    catalog_authid_value*       authidvalue_src = NULL;
    catalog_authid_value*       authidvalue_dst = NULL;
    pg_parser_sysdict_pgauthid* authid_src = NULL;
    pg_parser_sysdict_pgauthid* authid_dst = NULL;

    authidvalue_src = (catalog_authid_value*)catalog_src->catalog;
    authid_src = authidvalue_src->authid;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for authidvalue */
    authidvalue_dst = rmalloc0(sizeof(catalog_authid_value));
    if (!authidvalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(authidvalue_dst, 0, 0, sizeof(catalog_authid_value));

    /* Allocate memory for authid table */
    authid_dst = rmalloc0(sizeof(pg_parser_sysdict_pgauthid));
    if (!authid_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(authid_dst, 0, 0, sizeof(pg_parser_sysdict_pgauthid));

    /* No pointers, direct copy */
    rmemcpy0(authid_dst, 0, authid_src, sizeof(pg_parser_sysdict_pgauthid));

    authidvalue_dst->oid = authidvalue_src->oid;
    authidvalue_dst->authid = authid_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)authidvalue_dst;

    return result;
}

static catalogdata* catalog_copy_database(catalogdata* catalog_src)
{
    catalogdata*                  result = NULL;
    catalog_database_value*       databasevalue_src = NULL;
    catalog_database_value*       databasevalue_dst = NULL;
    pg_parser_sysdict_pgdatabase* database_src = NULL;
    pg_parser_sysdict_pgdatabase* database_dst = NULL;

    databasevalue_src = (catalog_database_value*)catalog_src->catalog;
    database_src = databasevalue_src->database;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for databasevalue */
    databasevalue_dst = rmalloc0(sizeof(catalog_database_value));
    if (!databasevalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(databasevalue_dst, 0, 0, sizeof(catalog_database_value));

    /* Allocate memory for database table */
    database_dst = rmalloc0(sizeof(pg_parser_sysdict_pgdatabase));
    if (!database_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(database_dst, 0, 0, sizeof(pg_parser_sysdict_pgdatabase));

    /* No pointers, direct copy */
    rmemcpy0(database_dst, 0, database_src, sizeof(pg_parser_sysdict_pgdatabase));

    databasevalue_dst->oid = databasevalue_src->oid;
    databasevalue_dst->database = database_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)databasevalue_dst;

    return result;
}

static catalogdata* catalog_copy_relmapfile(catalogdata* catalog_src)
{
    catalogdata* result = NULL;
    replmapfile* relmapfile_src = NULL;
    replmapfile* relmapfile_dst = NULL;
    relmapping*  mapping_src = NULL;
    relmapping*  mapping_dst = NULL;

    relmapfile_src = catalog_src->catalog;
    mapping_src = relmapfile_src->mapping;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for relmapfile */
    relmapfile_dst = rmalloc0(sizeof(replmapfile));
    if (!relmapfile_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(relmapfile_dst, 0, 0, sizeof(replmapfile));

    /* Allocate memory for mapping_dst */
    mapping_dst = rmalloc0(sizeof(relmapping) * relmapfile_src->num);
    if (!mapping_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(mapping_dst, 0, 0, sizeof(relmapping) * relmapfile_src->num);

    /* No pointers, direct copy */
    rmemcpy0(mapping_dst, 0, mapping_src, sizeof(relmapping) * relmapfile_src->num);

    relmapfile_dst->num = relmapfile_src->num;
    relmapfile_dst->mapping = mapping_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)relmapfile_dst;

    return result;
}

static catalogdata* catalog_copy_index(catalogdata* catalog_src)
{
    catalogdata*               result = NULL;
    catalog_index_value*       indexvalue_src = NULL;
    catalog_index_value*       indexvalue_dst = NULL;
    pg_parser_sysdict_pgindex* index_src = NULL;
    pg_parser_sysdict_pgindex* index_dst = NULL;

    indexvalue_src = (catalog_index_value*)catalog_src->catalog;
    index_src = indexvalue_src->index;

    /* Allocate memory for return value */
    result = rmalloc0(sizeof(catalogdata));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(catalogdata));

    /* Allocate memory for indexvalue */
    indexvalue_dst = rmalloc0(sizeof(catalog_index_value));
    if (!indexvalue_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(indexvalue_dst, 0, 0, sizeof(catalog_index_value));

    /* Allocate memory for index table */
    index_dst = rmalloc0(sizeof(pg_parser_sysdict_pgindex));
    if (!index_dst)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(index_dst, 0, 0, sizeof(pg_parser_sysdict_pgindex));

    /* Has pointers, copy and reallocate */
    rmemcpy0(index_dst, 0, index_src, sizeof(pg_parser_sysdict_pgindex));
    index_dst->indkey = NULL;

    index_dst->indkey = rmalloc0(sizeof(uint32_t) * index_dst->indnatts);
    if (!index_dst->indkey)
    {
        elog(RLOG_ERROR, "oom");
    }

    indexvalue_dst->oid = indexvalue_src->oid;
    indexvalue_dst->index = index_dst;

    result->op = catalog_src->op;
    result->type = catalog_src->type;
    result->catalog = (void*)indexvalue_dst;

    return result;
}

catalogdata* catalog_copy(catalogdata* catalog_in)
{
    if (catalog_in->type > m_catalog_copy_fmgr_cnt || !m_catalog_copy_fmgr[catalog_in->type].func)
    {
        elog(RLOG_ERROR, "invalid catalog type: %d, please check", catalog_in->type);
        return NULL;
    }
    return m_catalog_copy_fmgr[catalog_in->type].func(catalog_in);
}

/* colvalue to catalog dispatch entry */
catalogdata* catalog_colvalued2catalog(int dbtype, int dbversion, void* in_colvalues)
{
    if ((m_colvalue2catalog_bydbtype_cnt - 1) < dbtype ||
        NULL == m_colvalue2catalog_dbtype_distribute[dbtype].func)
    {
        return NULL;
    }
    return m_colvalue2catalog_dbtype_distribute[dbtype].func(dbversion, in_colvalues);
}

/* No-filter system catalog colvalue to catalog structure dispatch function - start */
/* no filter colvalue to catalog dispatch entry */
static catalogdata* catalog_colvalue_no_filter_conversion(int   dbtype,
                                                          int   dbversion,
                                                          void* in_colvalues)
{
    if ((m_colvalue2catalog_bydbtype_cnt - 1) < dbtype ||
        NULL == m_colvalue2catalog_dbtype_distribute[dbtype].no_filter_func)
    {
        return NULL;
    }
    return m_colvalue2catalog_dbtype_distribute[dbtype].no_filter_func(dbversion, in_colvalues);
}

/* No filter colvalue to catalog postgres dispatch entry */
static catalogdata* catalog_colvalue_no_filter_conversion_postgres(int   dbversion,
                                                                   void* in_colvalues)
{
    if ((m_colvalue2catalog_pg_cnt - 1) < dbversion ||
        NULL == m_colvalue2catalog_pg_distribute[dbversion].no_filter_func)
    {
        return NULL;
    }
    return m_colvalue2catalog_pg_distribute[dbversion].no_filter_func(in_colvalues);
}

static catalogdata* catalog_colvalue_no_filter_conversion_pg12(void* in_colvalues)
{
    catalogdata*                     catalog_data = NULL;
    pg_parser_translog_tbcol_values* colvalues = NULL;
    pg_parser_translog_tbcol_value*  colvalue = NULL;

    /* Dispatch processing according to different oid */
    colvalues = (pg_parser_translog_tbcol_values*)in_colvalues;
    colvalue = colvalues->m_new_values;
    switch (colvalues->m_relid)
    {
        case RelationRelationId:
            catalog_data = class_colvalue2class_nofilter(colvalue);
            break;
        case AttributeRelationId:
            catalog_data = class_colvalue2attribute(colvalue);
            break;
        case TypeRelationId:
            catalog_data = type_colvalue2type(colvalue);
            break;
        case EnumRelationId:
            catalog_data = enum_colvalue2enum(colvalue);
            break;
        case NamespaceRelationId:
            catalog_data = namespace_colvalue2namespace(colvalue);
            break;
        case ProcedureRelationId:
            catalog_data = proc_colvalue2proc(colvalue);
            break;
        case RangeRelationId:
            catalog_data = range_colvalue2range(colvalue);
            break;
        case ConstraintRelationId:
            catalog_data = constraint_colvalue2constraint(colvalue);
            break;
        case AuthIdRelationId:
            catalog_data = authid_colvalue2authid(colvalue);
            break;
        case DatabaseRelationId:
            catalog_data = database_colvalue2database(colvalue);
            break;
        case IndexRelationId:
            catalog_data = index_colvalue2index(colvalue);
            break;
        default:
            catalog_data = NULL;
            break;
    }
    if (NULL != catalog_data)
    {
        catalog_data->op = colvalues->m_base.m_dmltype;
    }
    return catalog_data;
}
/* No-filter system catalog colvalue to catalog structure dispatch function - end */

catalogdata* catalog_colvalued2catalog_pg12(void* in_colvalues)
{
    catalogdata*                     catalog_data = NULL;
    pg_parser_translog_tbcol_value*  colvalue = NULL;
    pg_parser_translog_tbcol_values* colvalues = NULL;

    /* Dispatch processing according to different oid */
    colvalues = (pg_parser_translog_tbcol_values*)in_colvalues;
    if (PG_PARSER_TRANSLOG_DMLTYPE_INVALID == colvalues->m_base.m_dmltype)
    {
        return NULL;
    }
    else if (PG_PARSER_TRANSLOG_DMLTYPE_INSERT == colvalues->m_base.m_dmltype ||
             PG_PARSER_TRANSLOG_DMLTYPE_UPDATE == colvalues->m_base.m_dmltype)
    {
        colvalue = colvalues->m_new_values;
    }
    else if (PG_PARSER_TRANSLOG_DMLTYPE_DELETE == colvalues->m_base.m_dmltype)
    {
        colvalue = colvalues->m_old_values;
    }
    else
    {
        elog(RLOG_WARNING, "unknown covalue type");
    }

    switch (colvalues->m_relid)
    {
        case RelationRelationId:
            catalog_data = class_colvalue2class(colvalue);
            break;
        case AttributeRelationId:
            catalog_data = class_colvalue2attribute(colvalue);
            break;
        case TypeRelationId:
            catalog_data = type_colvalue2type(colvalue);
            break;
        case EnumRelationId:
            catalog_data = enum_colvalue2enum(colvalue);
            break;
        case NamespaceRelationId:
            catalog_data = namespace_colvalue2namespace(colvalue);
            break;
        case ProcedureRelationId:
            catalog_data = proc_colvalue2proc(colvalue);
            break;
        case RangeRelationId:
            catalog_data = range_colvalue2range(colvalue);
            break;
        case ConstraintRelationId:
            catalog_data = constraint_colvalue2constraint(colvalue);
            break;
        case AuthIdRelationId:
            catalog_data = authid_colvalue2authid(colvalue);
            break;
        case DatabaseRelationId:
            catalog_data = database_colvalue2database(colvalue);
            break;
        case IndexRelationId:
            catalog_data = index_colvalue2index(colvalue);
            break;
        default:
            break;
    }

    if (NULL != catalog_data)
    {
        catalog_data->op = colvalues->m_base.m_dmltype;
    }
    return catalog_data;
}

static void* catalog_get_sysdict_from_sysdicthash(HTAB* sysdicthash,
                                                  void* search_variable,
                                                  int   dict_type)
{
    switch (dict_type)
    {
        case CATALOG_TYPE_CLASS:
        {
            catalog_class_value* temp_class = NULL;
            temp_class = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_class)
            {
                return (void*)temp_class->class;
            }
            break;
        }
        case CATALOG_TYPE_ATTRIBUTE:
        {
            List*                     temp_attribute_list = NULL;
            catalog_attribute_value*  temp_attribute_value = NULL;

            /* The matching condition for attribute is that oid and attnum match */
            catalog_attribute_search* att_search = (catalog_attribute_search*)search_variable;

            temp_attribute_value = hash_search(sysdicthash, &att_search->attrelid, HASH_FIND, NULL);
            if (temp_attribute_value)
            {
                ListCell* attcell = NULL;

                temp_attribute_list = temp_attribute_value->attrs;
                foreach (attcell, temp_attribute_list)
                {
                    pg_parser_sysdict_pgattributes* temp_attr =
                        (pg_parser_sysdict_pgattributes*)lfirst(attcell);
                    if (temp_attr->attnum == att_search->attnum)
                    {
                        return (void*)temp_attr;
                    }
                }
            }
            break;
        }
        case CATALOG_TYPE_TYPE:
        {
            catalog_type_value* temp_type_value = NULL;
            temp_type_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_type_value)
            {
                return (void*)temp_type_value->type;
            }
            break;
        }
        case CATALOG_TYPE_NAMESPACE:
        {
            catalog_namespace_value* temp_namespace_value = NULL;
            temp_namespace_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_namespace_value)
            {
                return (void*)temp_namespace_value->namespace;
            }
            break;
        }
        case CATALOG_TYPE_TABLESPACE:
        {
            /* Not used */
            break;
        }
        case CATALOG_TYPE_ENUM:
        {
            /* Note: This actually returns a linked list */
            catalog_enum_value* temp_enum_value = NULL;
            temp_enum_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_enum_value)
            {
                return (void*)list_copy(temp_enum_value->enums);
            }
            break;
        }
        case CATALOG_TYPE_RANGE:
        {
            catalog_range_value* temp_range_value = NULL;
            temp_range_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_range_value)
            {
                return (void*)temp_range_value->range;
            }
            break;
        }
        case CATALOG_TYPE_PROC:
        {
            catalog_proc_value* temp_proc_value = NULL;
            temp_proc_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_proc_value)
            {
                return (void*)temp_proc_value->proc;
            }
            break;
        }
        case CATALOG_TYPE_CONSTRAINT:
        {
            catalog_constraint_value* temp_constraint_value = NULL;
            temp_constraint_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_constraint_value)
            {
                return (void*)temp_constraint_value->constraint;
            }
            break;
        }
        case CATALOG_TYPE_OPERATOR:
        {
            catalog_operator_value* temp_operator_value = NULL;
            temp_operator_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_operator_value)
            {
                return (void*)temp_operator_value->operator;
            }
            break;
        }
        case CATALOG_TYPE_AUTHID:
        {
            catalog_authid_value* temp_authid_value = NULL;
            temp_authid_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_authid_value)
            {
                return (void*)temp_authid_value->authid;
            }
            break;
        }
        case CATALOG_TYPE_DATABASE:
        {
            catalog_database_value* temp_database_value = NULL;
            temp_database_value = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (temp_database_value)
            {
                return (void*)temp_database_value->database;
            }
            break;
        }
        /* Do not return index data from here */
        case CATALOG_TYPE_INDEX:
        {
            return NULL;
        }
        default:
        {
            elog(RLOG_WARNING, "unknown catalog type: %d", dict_type);
            break;
        }
    }
    return NULL;
}

/*
 * Currently only index uses this, but written as a general function for extensibility
 * No need to consider delete case in hash
 */
static List* catalog_get_sysdict_list_from_sysdicthash(HTAB* sysdicthash,
                                                       void* search_variable,
                                                       int   dict_type)
{
    List* result = NULL;

    switch (dict_type)
    {
        case CATALOG_TYPE_INDEX:
        {
            catalog_index_hash_entry* entry = NULL;

            entry = hash_search(sysdicthash, search_variable, HASH_FIND, NULL);
            if (entry)
            {
                /* Copy linked list */
                result = list_copy(entry->index_list);
            }
            break;
        }
        default:
        {
            /* No warning */
            break;
        }
    }
    return result;
}

static void* catalog_get_sysdict_from_sysdicthis(List* sysdicthis,
                                                 void* search_variable,
                                                 int   dict_type)
{
    ListCell* hiscell = NULL;
    void*     sysdicthis_result = NULL;

    /* Traverse sysdict his */
    foreach (hiscell, sysdicthis)
    {
        catalogdata* dict = (catalogdata*)lfirst(hiscell);
        if (dict->type == dict_type &&
            ((dict->op == CATALOG_OP_INSERT) || (dict->op == CATALOG_OP_UPDATE)))
        {
            switch (dict_type)
            {
                case CATALOG_TYPE_CLASS:
                {
                    catalog_class_value* temp_class_value = (catalog_class_value*)dict->catalog;
                    if (temp_class_value->oid == *(Oid*)search_variable)
                    {
                        sysdicthis_result = (void*)temp_class_value->class;
                    }
                    break;
                }
                case CATALOG_TYPE_ATTRIBUTE:
                {
                    catalog_attribute_value* temp_attribute_value =
                        (catalog_attribute_value*)dict->catalog;

                    /* The matching condition for att is that oid and attnum correspond */
                    catalog_attribute_search* att_search =
                        (catalog_attribute_search*)search_variable;

                    if (temp_attribute_value->attrelid == att_search->attrelid)
                    {
                        ListCell* attcell = NULL;

                        /* Traverse attrs */
                        foreach (attcell, temp_attribute_value->attrs)
                        {
                            pg_parser_sysdict_pgattributes* temp_attr =
                                (pg_parser_sysdict_pgattributes*)lfirst(attcell);

                            if (temp_attr->attnum == att_search->attnum)
                            {
                                sysdicthis_result = (void*)temp_attr;
                            }
                        }
                    }
                    break;
                }
                case CATALOG_TYPE_TYPE:
                {
                    catalog_type_value* temp_type_value = (catalog_type_value*)dict->catalog;
                    if (temp_type_value->oid == *(Oid*)search_variable)
                    {
                        sysdicthis_result = (void*)temp_type_value->type;
                    }
                    break;
                }
                case CATALOG_TYPE_NAMESPACE:
                {
                    catalog_namespace_value* temp_namespace_value =
                        (catalog_namespace_value*)dict->catalog;
                    if (temp_namespace_value->oid == *(Oid*)search_variable)
                    {
                        sysdicthis_result = (void*)temp_namespace_value->namespace;
                    }
                    break;
                }
                case CATALOG_TYPE_TABLESPACE:
                {
                    /* Not used */
                    break;
                }
                case CATALOG_TYPE_ENUM:
                {
                    /* Note: This actually returns a linked list */
                    catalog_enum_value* temp_enum_value = (catalog_enum_value*)dict->catalog;

                    if (temp_enum_value->enumtypid == *(Oid*)search_variable)
                    {
                        pg_parser_sysdict_pgenum* temp_enum_dict =
                            (pg_parser_sysdict_pgenum*)linitial(temp_enum_value->enums);
                        sysdicthis_result = lappend(sysdicthis_result, temp_enum_dict);
                    }
                    break;
                }
                case CATALOG_TYPE_RANGE:
                {
                    catalog_range_value* temp_range_value = (catalog_range_value*)dict->catalog;
                    if (temp_range_value->rngtypid == *(Oid*)search_variable)
                    {
                        sysdicthis_result = (void*)temp_range_value->range;
                    }
                    break;
                }
                case CATALOG_TYPE_PROC:
                {
                    catalog_proc_value* temp_proc_value = (catalog_proc_value*)dict->catalog;
                    if (temp_proc_value->oid == *(Oid*)search_variable)
                    {
                        sysdicthis_result = (void*)temp_proc_value->proc;
                    }
                    break;
                }
                case CATALOG_TYPE_CONSTRAINT:
                {
                    catalog_constraint_value* temp_constraint_value =
                        (catalog_constraint_value*)dict->catalog;
                    if (temp_constraint_value->conrelid == *(Oid*)search_variable)
                    {
                        sysdicthis_result = (void*)temp_constraint_value->constraint;
                    }
                    break;
                }
                case CATALOG_TYPE_DATABASE:
                {
                    catalog_database_value* temp_database_value =
                        (catalog_database_value*)dict->catalog;
                    if (temp_database_value->oid == *(Oid*)search_variable)
                    {
                        sysdicthis_result = (void*)temp_database_value->database;
                    }
                    break;
                }
                /* index is not returned here */
                case CATALOG_TYPE_INDEX:
                {
                    return NULL;
                }
                default:
                {
                    elog(RLOG_WARNING, "unknown catalog type: %d", dict_type);
                    break;
                }
            }
        }
    }
    return sysdicthis_result;
}

/*
 * Process sysdicthis data for INSERT operation
 * For non-target tables, return the passed-in result
 * For target tables, append data to linked list and return
 */
static List* catalog_get_sysdict_list_from_sysdicthis_opinsert(List*        result,
                                                               catalogdata* dict,
                                                               void*        search_variable,
                                                               int          dict_type)
{
    switch (dict_type)
    {
        case CATALOG_TYPE_INDEX:
        {
            catalog_index_value* temp_index_value = (catalog_index_value*)dict->catalog;
            if (temp_index_value->oid == *(Oid*)search_variable)
            {
                result = lappend(result, temp_index_value);
            }
            break;
        }
        default:
        {
            break;
        }
    }

    return result;
}

/*
 * Process sysdicthis data for UPDATE operation
 * For non-target tables, return the passed-in result
 * For target tables, update data in linked list
 */
static List* catalog_get_sysdict_list_from_sysdicthis_opupdate(List*        result,
                                                               catalogdata* dict,
                                                               void*        search_variable,
                                                               int          dict_type)
{
    switch (dict_type)
    {
        case CATALOG_TYPE_INDEX:
        {
            catalog_index_value* temp_index_value = (catalog_index_value*)dict->catalog;
            if (temp_index_value->oid == *(Oid*)search_variable)
            {
                ListCell* cell = NULL;

                /* If result doesn't exist, simply append but log a warning */
                if (!result)
                {
                    elog(RLOG_WARNING, "search index, do update, index list is null");
                    result = lappend(result, temp_index_value);
                    return result;
                }

                foreach (cell, result)
                {
                    catalog_index_value* search_index_value = (catalog_index_value*)lfirst(cell);

                    /* After finding, just replace the target cell */
                    if (search_index_value->index->indexrelid ==
                        temp_index_value->index->indexrelid)
                    {
                        lfirst(cell) = temp_index_value;
                        break;
                    }
                }
            }
            break;
        }
        default:
        {
            break;
        }
    }

    return result;
}

/*
 * Process sysdicthis data for DELETE operation
 * For non-target tables, return the passed-in result
 * For target tables, delete data from linked list
 */
static List* catalog_get_sysdict_list_from_sysdicthis_opdelete(List*        result,
                                                               catalogdata* dict,
                                                               void*        search_variable,
                                                               int          dict_type)
{
    switch (dict_type)
    {
        case CATALOG_TYPE_INDEX:
        {
            catalog_index_value* temp_index_value = (catalog_index_value*)dict->catalog;
            if (temp_index_value->oid == *(Oid*)search_variable)
            {
                ListCell* cell = NULL;
                ListCell* cell_prev = NULL;

                /* If result doesn't exist, return but log a warning */
                if (!result)
                {
                    elog(RLOG_WARNING, "search index, do delete, index list is null");
                    return result;
                }

                cell = list_head(result);

                while (cell)
                {
                    ListCell*            next_cell = cell->next;
                    catalog_index_value* search_index_value = (catalog_index_value*)lfirst(cell);

                    /* After finding, just replace the target cell */
                    if (search_index_value->index->indexrelid ==
                        temp_index_value->index->indexrelid)
                    {
                        result = list_delete_cell(result, cell, cell_prev);

                        /* After found and processed, break */
                        break;
                    }
                    else
                    {
                        cell_prev = cell;
                    }
                    cell = next_cell;
                }
            }
            break;
        }
        default:
        {
            break;
        }
    }

    return result;
}

/*
 * Currently only index uses this, but written as a general function for extensibility
 * Need to consider delete and update cases, so linked list is passed in
 */
static List* catalog_get_sysdict_list_from_sysdicthis(List* result,
                                                      List* sysdicthis,
                                                      void* search_variable,
                                                      int   dict_type)
{
    ListCell* hiscell = NULL;
    void*     sysdicthis_result = NULL;

    /* Traverse sysdict his */
    foreach (hiscell, sysdicthis)
    {
        catalogdata* dict = (catalogdata*)lfirst(hiscell);
        if (dict->type == dict_type)
        {
            if (dict->op == CATALOG_OP_INSERT)
            {
                sysdicthis_result = catalog_get_sysdict_list_from_sysdicthis_opinsert(
                    sysdicthis_result, dict, search_variable, dict_type);
            }
            else if (dict->op == CATALOG_OP_UPDATE)
            {
                sysdicthis_result = catalog_get_sysdict_list_from_sysdicthis_opupdate(
                    sysdicthis_result, dict, search_variable, dict_type);
            }
            else if (dict->op == CATALOG_OP_DELETE)
            {
                sysdicthis_result = catalog_get_sysdict_list_from_sysdicthis_opdelete(
                    sysdicthis_result, dict, search_variable, dict_type);
            }
            else
            {
                elog(RLOG_WARNING, "unknown op type: %d", dict->op);
                continue;
            }
        }
    }
    return sysdicthis_result;
}

static void* catalog_get_sysdict_from_colvalue(List* sysdict, void* search_variable, int dict_type)
{
    ListCell*    cell = NULL;
    catalogdata* catalog_data = NULL;

    switch (dict_type)
    {
        case CATALOG_TYPE_CLASS:
        {
            catalog_class_value* pgclass_v = NULL;

            foreach (cell, sysdict)
            {
                txn_sysdict*                     dict = (txn_sysdict*)lfirst(cell);
                pg_parser_translog_tbcol_values* col = dict->colvalues;

                if ((col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_INSERT ||
                     col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_UPDATE))
                {
                    if (!strcmp(col->m_base.m_schemaname, CATALOG_SYSDICT_SCHEMA) &&
                        !strcmp(col->m_base.m_tbname, CATALOG_PG_CLASS))
                    {
                        if (dict->convert_colvalues)
                        {
                            pg_sysdict_Form_pg_class temp_class = NULL;
                            catalog_data = (catalogdata*)dict->convert_colvalues;
                            pgclass_v = (catalog_class_value*)catalog_data->catalog;
                            temp_class = (void*)pgclass_v->class;
                            if (temp_class->oid == *(Oid*)search_variable)
                            {
                                return temp_class;
                            }
                        }
                        else
                        {
                            Oid temp_oid = (Oid)atoi((char*)get_class_value_from_colvalue(
                                col->m_new_values, CLASS_MAPNUM_OID, g_idbtype, g_idbversion));
                            if (temp_oid == *(Oid*)search_variable)
                            {
                                dict->convert_colvalues = catalog_colvalue_no_filter_conversion(
                                    g_idbtype, g_idbversion, col);
                                catalog_data = (catalogdata*)dict->convert_colvalues;
                                pgclass_v = (catalog_class_value*)catalog_data->catalog;
                                return (void*)pgclass_v->class;
                            }
                        }
                    }
                }
            }
            break;
        }
        case CATALOG_TYPE_ATTRIBUTE:
        {
            catalog_attribute_value* pgattribute_v = NULL;

            foreach (cell, sysdict)
            {
                txn_sysdict*                     dict = (txn_sysdict*)lfirst(cell);
                pg_parser_translog_tbcol_values* col = dict->colvalues;

                if ((col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_INSERT ||
                     col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_UPDATE))
                {
                    if (!strcmp(col->m_base.m_schemaname, CATALOG_SYSDICT_SCHEMA) &&
                        !strcmp(col->m_base.m_tbname, CATALOG_PG_ATTRIBUTE))
                    {
                        catalog_attribute_search* temp_att_search =
                            (catalog_attribute_search*)search_variable;

                        if (dict->convert_colvalues)
                        {
                            pg_sysdict_Form_pg_attribute temp_att = NULL;
                            catalog_data = (catalogdata*)dict->convert_colvalues;
                            pgattribute_v = (catalog_attribute_value*)catalog_data->catalog;
                            temp_att = linitial(pgattribute_v->attrs);

                            if (temp_att->attrelid == temp_att_search->attrelid &&
                                temp_att->attnum == temp_att_search->attnum)
                            {
                                return temp_att;
                            }
                        }
                        else
                        {
                            Oid     temp_oid = INVALIDOID;
                            int16_t temp_attnum = 0;

                            temp_oid = (Oid)atoi(
                                (char*)get_attribute_value_from_colvalue(col->m_new_values,
                                                                         ATTRIBUTE_MAPNUM_ATTRELID,
                                                                         g_idbtype,
                                                                         g_idbversion));

                            temp_attnum = (int16_t)atoi(
                                (char*)get_attribute_value_from_colvalue(col->m_new_values,
                                                                         ATTRIBUTE_MAPNUM_ATTNUM,
                                                                         g_idbtype,
                                                                         g_idbversion));
                            if (temp_oid == temp_att_search->attrelid &&
                                temp_attnum == temp_att_search->attnum)
                            {
                                dict->convert_colvalues = catalog_colvalue_no_filter_conversion(
                                    g_idbtype, g_idbversion, col);
                                catalog_data = (catalogdata*)dict->convert_colvalues;
                                pgattribute_v = (catalog_attribute_value*)catalog_data->catalog;
                                return linitial(pgattribute_v->attrs);
                            }
                        }
                    }
                }
            }
            break;
        }
        /* Return namespace colvalues record */
        case CATALOG_TYPE_NAMESPACE:
        {
            catalog_namespace_value* pgnamespace_v = NULL;

            foreach (cell, sysdict)
            {
                txn_sysdict*                     dict = (txn_sysdict*)lfirst(cell);
                pg_parser_translog_tbcol_values* col = dict->colvalues;

                if ((col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_INSERT ||
                     col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_UPDATE))
                {
                    if (!strcmp(col->m_base.m_schemaname, CATALOG_SYSDICT_SCHEMA) &&
                        !strcmp(col->m_base.m_tbname, CATALOG_PG_NAMESPACE))
                    {
                        if (dict->convert_colvalues)
                        {
                            pg_sysdict_Form_pg_namespace temp_nsp = NULL;
                            catalog_data = (catalogdata*)dict->convert_colvalues;
                            pgnamespace_v = (catalog_namespace_value*)catalog_data->catalog;
                            temp_nsp = pgnamespace_v->namespace;
                            if (temp_nsp->oid == *(Oid*)search_variable)
                            {
                                return temp_nsp;
                            }
                        }
                        else
                        {
                            Oid temp_oid = INVALIDOID;
                            temp_oid = (Oid)atoi((char*)get_namespace_value_from_colvalue(
                                col->m_new_values, NAMESPACE_MAPNUM_OID, g_idbtype, g_idbversion));
                            if (temp_oid == *(Oid*)search_variable)
                            {
                                dict->convert_colvalues = catalog_colvalue_no_filter_conversion(
                                    g_idbtype, g_idbversion, col);
                                catalog_data = (catalogdata*)dict->convert_colvalues;
                                pgnamespace_v = (catalog_namespace_value*)catalog_data->catalog;
                                return (void*)pgnamespace_v->namespace;
                            }
                        }
                    }
                }
            }
            break;
        }
        case CATALOG_TYPE_TYPE:
        {
            catalog_type_value* pgtype_v = NULL;

            foreach (cell, sysdict)
            {
                txn_sysdict*                     dict = (txn_sysdict*)lfirst(cell);
                pg_parser_translog_tbcol_values* col = dict->colvalues;

                if ((col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_INSERT ||
                     col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_UPDATE))
                {
                    if (!strcmp(col->m_base.m_schemaname, CATALOG_SYSDICT_SCHEMA) &&
                        !strcmp(col->m_base.m_tbname, CATALOG_PG_TYPE))
                    {
                        if (dict->convert_colvalues)
                        {
                            pg_sysdict_Form_pg_type temp_typ = NULL;
                            catalog_data = (catalogdata*)dict->convert_colvalues;
                            pgtype_v = (catalog_type_value*)catalog_data->catalog;
                            temp_typ = pgtype_v->type;
                            if (temp_typ->oid == *(Oid*)search_variable)
                            {
                                return temp_typ;
                            }
                        }
                        else
                        {
                            Oid temp_oid = (Oid)atoi((char*)get_type_value_from_colvalue(
                                col->m_new_values, TYPE_MAPNUM_OID, g_idbtype, g_idbversion));
                            if (temp_oid == *(Oid*)search_variable)
                            {
                                dict->convert_colvalues = catalog_colvalue_no_filter_conversion(
                                    g_idbtype, g_idbversion, col);
                                catalog_data = (catalogdata*)dict->convert_colvalues;
                                pgtype_v = (catalog_type_value*)catalog_data->catalog;
                                return (void*)pgtype_v->type;
                            }
                        }
                    }
                }
            }
            break;
        }
        /* Do not return here */
        case CATALOG_TYPE_INDEX:
        {
            return NULL;
        }
        default:
        {
            break;
        }
    }
    return NULL;
}

static void* catalog_get_sysdict(
    HTAB* sysdict_hash, List* sysdict, List* sysdicthis, void* search_variable, int dict_type)
{
    void* result = NULL;
    /* First search sysdict, and based on whether sysdict exists, determine whether to search
     * sysdict */
    if (sysdict)
    {
        result = catalog_get_sysdict_from_colvalue(sysdict, search_variable, dict_type);
        if (result)
        {
            return result;
        }
    }

    /* Based on whether sysdicthis exists, determine whether to search sysdicthis */
    if (sysdicthis)
    {
        result = catalog_get_sysdict_from_sysdicthis(sysdicthis, search_variable, dict_type);
        if (result)
        {
            return result;
        }
    }

    /* Based on whether sysdict_hash exists, determine whether to search sysdict_hash */
    if (sysdict_hash)
    {
        result = catalog_get_sysdict_from_sysdicthash(sysdict_hash, search_variable, dict_type);
        if (result)
        {
            return result;
        }
    }

    return NULL;
}

/*
 * Get sysdict from cache and assemble into linked list form to return
 * Search order:
 *          sysdict_hash, sysdicthis, sysdict
 * Currently only index uses this, but written as a general function for extensibility
 */
static void* catalog_get_sysdict_list(
    HTAB* sysdict_hash, List* sysdict, List* sysdicthis, void* search_variable, int dict_type)
{
    List* result = NULL;

    /* First search sysdict_hash */
    if (sysdict_hash)
    {
        result =
            catalog_get_sysdict_list_from_sysdicthash(sysdict_hash, search_variable, dict_type);
    }

    /* Then search sysdicthis */
    if (sysdicthis)
    {
        result = catalog_get_sysdict_list_from_sysdicthis(
            result, sysdicthis, search_variable, dict_type);
    }

    /* Finally search sysdict, this is not actually used in index stage, but kept */
    if (sysdict)
    {
        /* now do nothing */
    }

    return (void*)result;
}

static Oid catalog_get_oid_by_relfilenode_from_colvalues(uint32_t relfilenode, List* list)
{
    ListCell* cell = NULL;

    if (!list)
    {
        return INVALIDOID;
    }

    foreach (cell, list)
    {
        txn_sysdict*                     dict = (txn_sysdict*)lfirst(cell);
        pg_parser_translog_tbcol_values* col = dict->colvalues;

        if ((col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_INSERT ||
             col->m_base.m_dmltype == PG_PARSER_TRANSLOG_DMLTYPE_UPDATE) &&
            !strcmp(col->m_base.m_schemaname, CATALOG_SYSDICT_SCHEMA) &&
            !strcmp(col->m_base.m_tbname, CATALOG_PG_CLASS) && col->m_new_values)
        {
            uint32_t temp_relfilenode = 0;
            temp_relfilenode = (uint32_t)atoi((char*)get_class_value_from_colvalue(
                col->m_new_values, CLASS_MAPNUM_RELFILENODE, g_idbtype, g_idbversion));
            if (temp_relfilenode == relfilenode)
            {
                return (Oid)atoi((char*)get_class_value_from_colvalue(
                    col->m_new_values, CLASS_MAPNUM_OID, g_idbtype, g_idbversion));
            }
        }
    }
    return INVALIDOID;
}

Oid catalog_get_oid_by_relfilenode(HTAB*    relfilenode_htab,
                                   List*    sysdicthis,
                                   List*    sysdict,
                                   uint32_t dboid,
                                   uint32_t tbspcoid,
                                   uint32_t relfilenode,
                                   bool     report_error)
{
    RelFileNode      rnode = {'\0'};
    relfilenode2oid* entry = NULL;
    bool             find = false;
    Oid              oid = INVALIDOID;
    rnode.dbNode = dboid;
    rnode.relNode = relfilenode;
    rnode.spcNode = tbspcoid;
    entry = hash_search(relfilenode_htab, &rnode, HASH_FIND, &find);
    if (!find)
    {
        if (sysdicthis)
        {
            ListCell* cell = NULL;

            /* Traverse all contents of the linked list */
            foreach (cell, sysdicthis)
            {
                catalogdata* dict = (catalogdata*)lfirst(cell);
                if (dict->type == CATALOG_TYPE_CLASS &&
                    ((dict->op == CATALOG_OP_INSERT) || (dict->op == CATALOG_OP_UPDATE)))
                {
                    catalog_class_value* temp_pgclass = (catalog_class_value*)dict->catalog;
                    if (temp_pgclass->class->relfilenode == relfilenode)
                    {
                        oid = temp_pgclass->class->oid;
                    }
                }
            }
        }
        if (sysdict)
        {
            /* Not found yet, search from sysdict */
            if (!oid)
            {
                oid = catalog_get_oid_by_relfilenode_from_colvalues(relfilenode, sysdict);
            }
        }
    }
    else
    {
        oid = entry->oid;
    }

    if (!oid)
    {
        if (report_error)
        {
            elog(RLOG_ERROR, "can't find oid by relfilenode: %u", rnode.relNode);
        }
        else
        {
            elog(RLOG_DEBUG,
                 "fpw capture, can't find oid by relfilenode: %u, ignore fpw tuples",
                 rnode.relNode);
        }

        return oid;
    }
    return oid;
}

void* catalog_get_class_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_CLASS);
}

void* catalog_get_database_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_DATABASE);
}

void* catalog_get_namespace_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_NAMESPACE);
}

void* catalog_get_attribute_sysdict(
    HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid attrelid, int16_t attnum)
{
    catalog_attribute_search temp_att_search = {'\0'};

    temp_att_search.attrelid = attrelid;
    temp_att_search.attnum = attnum;

    return catalog_get_sysdict(
        sysdict_hash, sysdict, sysdicthis, &temp_att_search, CATALOG_TYPE_ATTRIBUTE);
}

void* catalog_get_type_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_TYPE);
}

void* catalog_get_range_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_RANGE);
}

void* catalog_get_enum_sysdict_list(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_ENUM);
}

void* catalog_get_proc_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_PROC);
}

void* catalog_get_constraint_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_CONSTRAINT);
}

void* catalog_get_authid_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_AUTHID);
}

void* catalog_get_operator_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_OPERATOR);
}

void* catalog_get_index_sysdict_list(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid)
{
    return catalog_get_sysdict_list(sysdict_hash, sysdict, sysdicthis, &oid, CATALOG_TYPE_INDEX);
}

void catalog_sysdict_getfromdb(void* conn_in, cache_sysdicts* sysdicts)
{
    PGconn* conn = (PGconn*)conn_in;
    if (NULL == sysdicts)
    {
        return;
    }

    class_attribute_getfromdb(conn, sysdicts);

    authid_getfromdb(conn, sysdicts);

    constraint_getfromdb(conn, sysdicts);

    database_getfromdb(conn, sysdicts);

    enum_getfromdb(conn, sysdicts);

    namespace_getfromdb(conn, sysdicts);

    operator_getfromdb(conn, sysdicts);

    proc_getfromdb(conn, sysdicts);

    range_getfromdb(conn, sysdicts);

    type_getfromdb(conn, sysdicts);

    index_getfromdb(conn, sysdicts);

    return;
}

/*
 * Enable FULL mode for tables
 * Only enables for non-system tables that are not already in FULL mode
 */
bool catalog_sysdict_setfullmode(HTAB* hclass)
{
    PGconn*                  conn = NULL;
    PGresult*                res = NULL;
    HASH_SEQ_STATUS          status;
    pg_sysdict_Form_pg_class class;
    catalog_class_value*     entry;
    char                     sql_exec[MAX_EXEC_SQL_LEN] = {'\0'};

    conn = conn_get(guc_getConfigOption("url"));
    /* Connection error, exit */
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "capture connect %s database error", guc_getConfigOption("url"));
        return false;
    }

    hash_seq_init(&status, hclass);
    while ((entry = hash_seq_search(&status)) != NULL)
    {
        class = entry->class;
        if (FirstNormalObjectId > class->oid)
        {
            /*
             * Skip system catalogs
             * In PG database, OIDs less than FirstNormalObjectId are system catalogs
             */
            continue;
        }

        if (PG_SYSDICT_REPLICA_IDENTITY_FULL == class->relreplident)
        {
            /* Skip tables already in FULL mode */
            continue;
        }

        if (PG_SYSDICT_RELKIND_RELATION != class->relkind &&
            PG_SYSDICT_RELKIND_PARTITIONED_TABLE != class->relkind)
        {
            /* Only enable FULL mode for regular tables and partitioned tables */
            continue;
        }

        /* Enable full mode */
        rmemset1(sql_exec, 0, '\0', MAX_EXEC_SQL_LEN);
        snprintf(sql_exec,
                 MAX_EXEC_SQL_LEN,
                 "alter table \"%s\".\"%s\" replica identity full;",
                 class->nspname.data,
                 class->relname.data);
        res = conn_exec(conn, sql_exec);
        if (NULL == res)
        {
            elog(RLOG_WARNING,
                 "capture set table %s.%s replica identity full error",
                 class->nspname.data,
                 class->relname.data);
            PQfinish(conn);
            return false;
        }

        PQclear(res);
    }
    PQfinish(conn);
    return true;
}

/* Filter sysdict data by redolsn
 * Return value dictsprev, data less than redolsn is persisted
 * sysdict greater than redolsn waits for next application
 */
List* catalog_sysdict_filterbylsn(List** sysdict, uint64 redolsn)
{
    ListCell*    lc = NULL;
    List*        sysdicts = NULL;
    List*        dicts = NULL;     /* sysdicts greater than redolsn */
    List*        dictsprev = NULL; /* sysdicts less than redolsn */
    catalogdata* catalog_data = NULL;

    sysdicts = (List*)(*sysdict);

    foreach (lc, sysdicts)
    {
        catalog_data = (catalogdata*)lfirst(lc);

        if (NULL == catalog_data)
        {
            continue;
        }

        if (redolsn >= catalog_data->lsn.wal.lsn)
        {
            dictsprev = lappend(dictsprev, catalog_data);
        }
        else
        {
            dicts = lappend(dicts, catalog_data);
        }
    }

    list_free(sysdicts);
    *sysdict = dicts;
    return dictsprev;
}
