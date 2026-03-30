#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/string/stringinfo.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
#include "works/parserwork/wal/decode_colvalue.h"

/* By database type */
static char* get_value_from_colvalue_postgres(int                             dbversion,
                                              pg_parser_translog_tbcol_value* colvalue,
                                              int                             dtype,
                                              int                             map_num);

static char* set_value_from_colvalue_postgres(int                             dbversion,
                                              char*                           new_value,
                                              pg_parser_translog_tbcol_value* colvalue,
                                              int                             dtype,
                                              int                             map_num);

static char* free_value_from_colvalue_postgres(int                             dbversion,
                                               pg_parser_translog_tbcol_value* colvalue,
                                               int                             dtype,
                                               int                             map_num);

/* By database version */
/* PostgreSQL database */
static char* get_value_from_colvalue_pg12(pg_parser_translog_tbcol_value* colvalue, int dtype, int map_num);
static char* set_value_from_colvalue_pg12(pg_parser_translog_tbcol_value* colvalue,
                                          char*                           new_value,
                                          int                             dtype,
                                          int                             map_num);
static char* free_value_from_colvalue_pg12(pg_parser_translog_tbcol_value* colvalue, int dtype, int map_num);

/* Database type/version dispatch section begin */
typedef char* (*get_value_from_colvalue_dbtype_func)(int                             dbversion,
                                                     pg_parser_translog_tbcol_value* colvalue,
                                                     int                             dtype,
                                                     int                             map_num);

typedef char* (*get_value_from_colvalue_dbversion_func)(pg_parser_translog_tbcol_value* colvalue,
                                                        int                             dtype,
                                                        int                             map_num);

typedef char* (*set_value_from_colvalue_dbtype_func)(int                             dbversion,
                                                     char*                           new_value,
                                                     pg_parser_translog_tbcol_value* colvalue,
                                                     int                             dtype,
                                                     int                             map_num);

typedef char* (*set_value_from_colvalue_dbversion_func)(pg_parser_translog_tbcol_value* colvalue,
                                                        char*                           new_value,
                                                        int                             dtype,
                                                        int                             map_num);

#define free_value_from_colvalue_dbtype_func    get_value_from_colvalue_dbtype_func
#define free_value_from_colvalue_dbversion_func get_value_from_colvalue_dbversion_func

typedef struct DEAL_VALUE_FROM_COLVALUE_BYDBTYPE
{
    int                                  dbtype;
    get_value_from_colvalue_dbtype_func  getfunc;
    set_value_from_colvalue_dbtype_func  setfunc;
    free_value_from_colvalue_dbtype_func freefunc;
} deal_value_from_colvalue_bydbtype;

typedef struct DEAL_VALUE_FROM_COLVALUE_BYDBVERSION
{
    int                                     dbversion;
    get_value_from_colvalue_dbversion_func  getfunc;
    set_value_from_colvalue_dbversion_func  setfunc;
    free_value_from_colvalue_dbversion_func freefunc;
} deal_value_from_colvalue_bydbversion;

static deal_value_from_colvalue_bydbtype m_deal_value_from_colvalue_dbtype_distribute[] = {
    {DATABASE_TYPE_NOP,        NULL,   NULL,                             NULL},
    {DATABASE_TYPE_POSTGRESQL,
     get_value_from_colvalue_postgres, set_value_from_colvalue_postgres,
     free_value_from_colvalue_postgres                                       }
};

static int m_deal_value_from_colvalue_bydbtype_cnt =
    (sizeof(m_deal_value_from_colvalue_dbtype_distribute)) / (sizeof(deal_value_from_colvalue_bydbtype));

static deal_value_from_colvalue_bydbversion m_deal_value_from_colvalue_pg_distribute[] = {
    {PGDBVERSION_NOP, NULL,                         NULL,                         NULL                         },
    {PGDBVERSION_12,  get_value_from_colvalue_pg12, set_value_from_colvalue_pg12, free_value_from_colvalue_pg12}
};

static int m_deal_value_from_colvalue_pg_cnt =
    (sizeof(m_deal_value_from_colvalue_pg_distribute)) / (sizeof(deal_value_from_colvalue_bydbversion));

/* Database type/version dispatch section end */

/* System catalog section begin */

typedef struct GET_VALUE_CATALOG_COLUMN_MAPPING
{
    int map_num;
    int real_num;
} get_value_catalog_column_mapping;

/* ----------pg_class column mapping start---------- */
/* pg12 */
static const get_value_catalog_column_mapping m_pg_class_mapping_pg12[] = {
    {CLASS_MAPNUM_OID,         0},
    {CLASS_MAPNUM_RELNAME,     1},
    {CLASS_MAPNUM_RELNSPOID,   2},
    {CLASS_MAPNUM_RELFILENODE, 7}
};

/* ----------pg_class column mapping end---------- */

/* ----------pg_attribute column mapping start---------- */
/* pg12 */
static const get_value_catalog_column_mapping m_pg_attribute_mapping_pg12[] = {
    {ATTRIBUTE_MAPNUM_ATTRELID, 0},
    {ATTRIBUTE_MAPNUM_ATTNAME,  1},
    {ATTRIBUTE_MAPNUM_ATTNUM,   5}
};
/* ----------pg_attribute column mapping end---------- */

/* ----------pg_namespace column mapping start---------- */
/* pg12 */
static const get_value_catalog_column_mapping m_pg_namespace_mapping_pg12[] = {
    {NAMESPACE_MAPNUM_OID,     0},
    {NAMESPACE_MAPNUM_NSPNAME, 1}
};
/* ----------pg_namespace column mapping end---------- */

/* ----------pg_type column mapping start---------- */
/* pg12 */
static const get_value_catalog_column_mapping m_pg_type_mapping_pg12[] = {
    {TYPE_MAPNUM_OID,     0},
    {TYPE_MAPNUM_TYPNAME, 1}
};

/* ----------pg_type column mapping end---------- */

/* ----------get start---------- */
static char* get_value_from_colvalue_pg12(pg_parser_translog_tbcol_value* colvalue, int dtype, int map_num)
{
    switch (dtype)
    {
        case CATALOG_TYPE_CLASS:
        {
            return (char*)colvalue[m_pg_class_mapping_pg12[map_num].real_num].m_value;
            break;
        }
        case CATALOG_TYPE_ATTRIBUTE:
        {
            return (char*)colvalue[m_pg_attribute_mapping_pg12[map_num].real_num].m_value;
            break;
        }
        case CATALOG_TYPE_NAMESPACE:
        {
            return (char*)colvalue[m_pg_namespace_mapping_pg12[map_num].real_num].m_value;
            break;
        }
        case CATALOG_TYPE_TYPE:
        {
            return (char*)colvalue[m_pg_type_mapping_pg12[map_num].real_num].m_value;
            break;
        }
        default:
        {
            elog(RLOG_ERROR, "try find unknow catalog colvalue, type: %d", dtype);
            break;
        }
    }
    return NULL;
}

static char* get_value_from_colvalue_postgres(int                             dbversion,
                                              pg_parser_translog_tbcol_value* colvalue,
                                              int                             dtype,
                                              int                             map_num)
{
    if ((m_deal_value_from_colvalue_pg_cnt - 1) < dbversion ||
        NULL == m_deal_value_from_colvalue_pg_distribute[dbversion].getfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_pg_distribute[dbversion].getfunc(colvalue, dtype, map_num);
}

static char* get_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                     int                             dtype,
                                     int                             pnum,
                                     int                             dbtype,
                                     int                             dbversion)
{
    if ((m_deal_value_from_colvalue_bydbtype_cnt - 1) < dbtype ||
        NULL == m_deal_value_from_colvalue_dbtype_distribute[dbtype].getfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_dbtype_distribute[dbtype].getfunc(dbversion, colvalue, dtype, pnum);
}

/* ----------get end---------- */

/* ----------set start---------- */

static char* set_value_from_colvalue_pg12(pg_parser_translog_tbcol_value* colvalue,
                                          char*                           new_value,
                                          int                             dtype,
                                          int                             map_num)
{
    switch (dtype)
    {
        case CATALOG_TYPE_CLASS:
        {
            colvalue[m_pg_class_mapping_pg12[map_num].real_num].m_value = new_value;
            colvalue[m_pg_class_mapping_pg12[map_num].real_num].m_valueLen = strlen(new_value) + 1;
            break;
        }
        default:
        {
            elog(RLOG_ERROR, "try find unknow catalog colvalue");
            break;
        }
    }
    return NULL;
}

static char* set_value_from_colvalue_postgres(int                             dbversion,
                                              char*                           new_value,
                                              pg_parser_translog_tbcol_value* colvalue,
                                              int                             dtype,
                                              int                             map_num)
{
    if ((m_deal_value_from_colvalue_pg_cnt - 1) < dbversion ||
        NULL == m_deal_value_from_colvalue_pg_distribute[dbversion].setfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_pg_distribute[dbversion].setfunc(colvalue, new_value, dtype, map_num);
}

static char* set_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                     char*                           new_value,
                                     int                             dtype,
                                     int                             pnum,
                                     int                             dbtype,
                                     int                             dbversion)
{
    if ((m_deal_value_from_colvalue_bydbtype_cnt - 1) < dbtype ||
        NULL == m_deal_value_from_colvalue_dbtype_distribute[dbtype].setfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_dbtype_distribute[dbtype].setfunc(dbversion, new_value, colvalue, dtype, pnum);
}

/* ----------set end---------- */

/* ----------free start---------- */
static char* free_value_from_colvalue_pg12(pg_parser_translog_tbcol_value* colvalue, int dtype, int map_num)
{
    switch (dtype)
    {
        case CATALOG_TYPE_CLASS:
        {
            rfree(colvalue[m_pg_class_mapping_pg12[map_num].real_num].m_value);
            colvalue[m_pg_class_mapping_pg12[map_num].real_num].m_value = NULL;
            break;
        }
        default:
        {
            elog(RLOG_ERROR, "try find unknow catalog colvalue");
            break;
        }
    }
    return NULL;
}

static char* free_value_from_colvalue_postgres(int                             dbversion,
                                               pg_parser_translog_tbcol_value* colvalue,
                                               int                             dtype,
                                               int                             map_num)
{
    if ((m_deal_value_from_colvalue_pg_cnt - 1) < dbversion ||
        NULL == m_deal_value_from_colvalue_pg_distribute[dbversion].freefunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_pg_distribute[dbversion].freefunc(colvalue, dtype, map_num);
}

static char* free_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                      int                             dtype,
                                      int                             pnum,
                                      int                             dbtype,
                                      int                             dbversion)
{
    if ((m_deal_value_from_colvalue_bydbtype_cnt - 1) < dbtype ||
        NULL == m_deal_value_from_colvalue_dbtype_distribute[dbtype].freefunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_dbtype_distribute[dbtype].freefunc(dbversion, colvalue, dtype, pnum);
}

/* ----------free end---------- */

char* get_class_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue, int pnum, int dbtype, int dbversion)
{
    return get_value_from_colvalue(colvalue, CATALOG_TYPE_CLASS, pnum, dbtype, dbversion);
}

char* set_class_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                    char*                           new_value,
                                    int                             pnum,
                                    int                             dbtype,
                                    int                             dbversion)
{
    return set_value_from_colvalue(colvalue, new_value, CATALOG_TYPE_CLASS, pnum, dbtype, dbversion);
}

char* free_class_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue, int pnum, int dbtype, int dbversion)
{
    return free_value_from_colvalue(colvalue, CATALOG_TYPE_CLASS, pnum, dbtype, dbversion);
}

char* get_attribute_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue, int pnum, int dbtype, int dbversion)
{
    return get_value_from_colvalue(colvalue, CATALOG_TYPE_ATTRIBUTE, pnum, dbtype, dbversion);
}

char* get_namespace_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue, int pnum, int dbtype, int dbversion)
{
    return get_value_from_colvalue(colvalue, CATALOG_TYPE_NAMESPACE, pnum, dbtype, dbversion);
}

char* get_type_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue, int pnum, int dbtype, int dbversion)
{
    return get_value_from_colvalue(colvalue, CATALOG_TYPE_TYPE, pnum, dbtype, dbversion);
}

char* get_index_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue, int pnum, int dbtype, int dbversion)
{
    return get_value_from_colvalue(colvalue, CATALOG_TYPE_INDEX, pnum, dbtype, dbversion);
}
