#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/string/stringinfo.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "catalog/ripple_catalog.h"
#include "works/parserwork/wal/ripple_decode_colvalue.h"


/* 按数据库类型 */
static char *ripple_get_value_from_colvalue_postgres(int dbversion,
                                                     xk_pg_parser_translog_tbcol_value *colvalue,
                                                     int dtype,
                                                     int map_num);
static char *ripple_get_value_from_colvalue_highgo(int dbversion,
                                                   xk_pg_parser_translog_tbcol_value *colvalue,
                                                   int dtype,
                                                   int map_num);

static char *ripple_set_value_from_colvalue_postgres(int dbversion,
                                                     char *new_value,
                                                     xk_pg_parser_translog_tbcol_value *colvalue,
                                                     int dtype,
                                                     int map_num);
static char *ripple_set_value_from_colvalue_highgo(int dbversion,
                                                   char *new_value,
                                                   xk_pg_parser_translog_tbcol_value *colvalue,
                                                   int dtype,
                                                   int map_num);

static char *ripple_free_value_from_colvalue_postgres(int dbversion,
                                                      xk_pg_parser_translog_tbcol_value *colvalue,
                                                      int dtype,
                                                      int map_num);
static char *ripple_free_value_from_colvalue_highgo(int dbversion,
                                                    xk_pg_parser_translog_tbcol_value *colvalue,
                                                    int dtype,
                                                    int map_num);

/* 按数据库版本 */
/* pg数据库 */
static char *ripple_get_value_from_colvalue_pg12(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num);
static char *ripple_set_value_from_colvalue_pg12(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 char *new_value,
                                                 int dtype,
                                                 int map_num);
static char *ripple_free_value_from_colvalue_pg12(xk_pg_parser_translog_tbcol_value *colvalue,
                                                  int dtype,
                                                  int map_num);

/* 瀚高数据库 */
#define ripple_get_value_from_colvalue_hg457 ripple_get_value_from_colvalue_pg12
#define ripple_set_value_from_colvalue_hg457 ripple_set_value_from_colvalue_pg12
#define ripple_free_value_from_colvalue_hg457 ripple_free_value_from_colvalue_pg12

#define ripple_get_value_from_colvalue_hg458 ripple_get_value_from_colvalue_pg12
#define ripple_set_value_from_colvalue_hg458 ripple_set_value_from_colvalue_pg12
#define ripple_free_value_from_colvalue_hg458 ripple_free_value_from_colvalue_pg12

static char *ripple_get_value_from_colvalue_hg901(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num);
static char *ripple_set_value_from_colvalue_hg901(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 char *new_value,
                                                 int dtype,
                                                 int map_num);
static char *ripple_free_value_from_colvalue_hg901(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num);

static char *ripple_get_value_from_colvalue_hg902(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num);
static char *ripple_set_value_from_colvalue_hg902(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 char *new_value,
                                                 int dtype,
                                                 int map_num);
static char *ripple_free_value_from_colvalue_hg902(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num);

/* 数据库类型/版本 分发部分 begin */
typedef char* (*get_value_from_colvalue_dbtype_func)(int dbversion,
                                                     xk_pg_parser_translog_tbcol_value *colvalue,
                                                     int dtype,
                                                     int map_num);

typedef char* (*get_value_from_colvalue_dbversion_func)(xk_pg_parser_translog_tbcol_value *colvalue,
                                                        int dtype,
                                                        int map_num);

typedef char* (*set_value_from_colvalue_dbtype_func)(int dbversion,
                                                     char *new_value,
                                                     xk_pg_parser_translog_tbcol_value *colvalue,
                                                     int dtype,
                                                     int map_num);

typedef char* (*set_value_from_colvalue_dbversion_func)(xk_pg_parser_translog_tbcol_value *colvalue,
                                                        char *new_value,
                                                        int dtype,
                                                        int map_num);

#define free_value_from_colvalue_dbtype_func get_value_from_colvalue_dbtype_func
#define free_value_from_colvalue_dbversion_func get_value_from_colvalue_dbversion_func


typedef struct RIPPLE_DEAL_VALUE_FROM_COLVALUE_BYDBTYPE
{
    int                                      dbtype;
    get_value_from_colvalue_dbtype_func      getfunc;
    set_value_from_colvalue_dbtype_func      setfunc;
    free_value_from_colvalue_dbtype_func     freefunc;
} ripple_deal_value_from_colvalue_bydbtype;

typedef struct RIPPLE_DEAL_VALUE_FROM_COLVALUE_BYDBVERSION
{
    int                                         dbversion;
    get_value_from_colvalue_dbversion_func      getfunc;
    set_value_from_colvalue_dbversion_func      setfunc;
    free_value_from_colvalue_dbversion_func     freefunc;
} ripple_deal_value_from_colvalue_bydbversion;

static ripple_deal_value_from_colvalue_bydbtype m_deal_value_from_colvalue_dbtype_distribute[] =
{
    {XK_DATABASE_TYPE_NOP,          NULL, NULL, NULL},
    {XK_DATABASE_TYPE_ORACLE,       NULL, NULL, NULL},
    {XK_DATABASE_TYPE_POSTGRESQL,   ripple_get_value_from_colvalue_postgres,
                                    ripple_set_value_from_colvalue_postgres,
                                    ripple_free_value_from_colvalue_postgres},
    {XK_DATABASE_TYPE_GAUSS,        NULL, NULL, NULL},
    {XK_DATABASE_TYPE_SQLSERVER,    NULL, NULL, NULL},
    {XK_DATABASE_TYPE_VASTBASE,     NULL, NULL, NULL},
    {XK_DATABASE_TYPE_MOGDB,        NULL, NULL, NULL},
    {XK_DATABASE_TYPE_HGDB,         ripple_get_value_from_colvalue_highgo,
                                    ripple_set_value_from_colvalue_highgo,
                                    ripple_free_value_from_colvalue_highgo},
    {XK_DATABASE_TYPE_KINGBASE,     NULL, NULL, NULL},
    {XK_DATABASE_TYPE_UXDB,         NULL, NULL, NULL}
};

static int m_deal_value_from_colvalue_bydbtype_cnt = (sizeof(m_deal_value_from_colvalue_dbtype_distribute))/(sizeof(ripple_deal_value_from_colvalue_bydbtype));

static ripple_deal_value_from_colvalue_bydbversion m_deal_value_from_colvalue_pg_distribute[] =
{
    {RIPPLE_PGDBVERSION_NOP,    NULL, NULL, NULL},
    {RIPPLE_PGDBVERSION_12,     ripple_get_value_from_colvalue_pg12,
                                ripple_set_value_from_colvalue_pg12,
                                ripple_free_value_from_colvalue_pg12}
};

static int m_deal_value_from_colvalue_pg_cnt = (sizeof(m_deal_value_from_colvalue_pg_distribute))/(sizeof(ripple_deal_value_from_colvalue_bydbversion));

/* 添加新的系统表时要仔细对照系统表是否与pg12有所不同 */
static ripple_deal_value_from_colvalue_bydbversion m_deal_value_from_colvalue_hg_distribute[] =
{
    {RIPPLE_HGVERSION_NOP, NULL},
    {RIPPLE_HGVERSION_457, ripple_get_value_from_colvalue_hg457,
                                   ripple_set_value_from_colvalue_hg457,
                                   ripple_free_value_from_colvalue_hg457},
    {RIPPLE_HGVERSION_458, ripple_get_value_from_colvalue_hg458,
                                   ripple_set_value_from_colvalue_hg458,
                                   ripple_free_value_from_colvalue_hg458},
    {RIPPLE_HGVERSION_901, ripple_get_value_from_colvalue_hg901,
                                   ripple_set_value_from_colvalue_hg901,
                                   ripple_free_value_from_colvalue_hg901},
    {RIPPLE_HGVERSION_902, ripple_get_value_from_colvalue_hg902,
                                   ripple_set_value_from_colvalue_hg902,
                                   ripple_free_value_from_colvalue_hg902}
};

static int m_deal_value_from_colvalue_hg_cnt = (sizeof(m_deal_value_from_colvalue_hg_distribute))/(sizeof(ripple_deal_value_from_colvalue_bydbversion));
/* 数据库类型/版本 分发部分 end */

/* 系统表部分 begin */

typedef struct RIPPLE_GET_VALUE_CATALOG_COLUMN_MAPPING
{
    int map_num;
    int real_num;
}ripple_get_value_catalog_column_mapping;


/* ----------pg_class 列映射 开始---------- */
/* pg12 */
static const ripple_get_value_catalog_column_mapping m_pg_class_mapping_pg12[] =
{
    {RIPPLE_CLASS_MAPNUM_OID, 0},
    {RIPPLE_CLASS_MAPNUM_RELNAME, 1},
    {RIPPLE_CLASS_MAPNUM_RELNSPOID, 2},
    {RIPPLE_CLASS_MAPNUM_RELFILENODE, 7}
};

/* hgdbv901 */
static const ripple_get_value_catalog_column_mapping m_pg_class_mapping_hg901[] =
{
    {RIPPLE_CLASS_MAPNUM_OID, 0},
    {RIPPLE_CLASS_MAPNUM_RELNAME, 1},
    {RIPPLE_CLASS_MAPNUM_RELNSPOID, 2},
    {RIPPLE_CLASS_MAPNUM_RELFILENODE, 7}
};

/* hgdbv902 */
static const ripple_get_value_catalog_column_mapping m_pg_class_mapping_hg902[] =
{
    {RIPPLE_CLASS_MAPNUM_OID, 32},
    {RIPPLE_CLASS_MAPNUM_RELNAME, 0},
    {RIPPLE_CLASS_MAPNUM_RELNSPOID, 1},
    {RIPPLE_CLASS_MAPNUM_RELFILENODE, 6},
};
/* ----------pg_class 列映射 结束---------- */

/* ----------pg_attribute 列映射 开始---------- */
/* pg12 */
static const ripple_get_value_catalog_column_mapping m_pg_attribute_mapping_pg12[] =
{
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTRELID, 0},
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTNAME, 1},
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTNUM, 5}
};

/* hgdbv901 */
static const ripple_get_value_catalog_column_mapping m_pg_attribute_mapping_hg901[] =
{
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTRELID, 0},
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTNAME, 1},
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTNUM, 5}
};

/* hgdbv902 */
static const ripple_get_value_catalog_column_mapping m_pg_attribute_mapping_hg902[] =
{
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTRELID, 0},
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTNAME, 1},
    {RIPPLE_ATTRIBUTE_MAPNUM_ATTNUM, 5}
};
/* ----------pg_attribute 列映射 结束---------- */

/* ----------pg_namespace 列映射 开始---------- */
/* pg12 */
static const ripple_get_value_catalog_column_mapping m_pg_namespace_mapping_pg12[] =
{
    {RIPPLE_NAMESPACE_MAPNUM_OID, 0},
    {RIPPLE_NAMESPACE_MAPNUM_NSPNAME, 1}
};

/* hgdbv901 */
static const ripple_get_value_catalog_column_mapping m_pg_namespace_mapping_hg901[] =
{
    {RIPPLE_NAMESPACE_MAPNUM_OID, 0},
    {RIPPLE_NAMESPACE_MAPNUM_NSPNAME, 1}
};

/* hgdbv902 */
static const ripple_get_value_catalog_column_mapping m_pg_namespace_mapping_hg902[] =
{
    {RIPPLE_NAMESPACE_MAPNUM_OID, 3},
    {RIPPLE_NAMESPACE_MAPNUM_NSPNAME, 0}
};
/* ----------pg_namespace 列映射 结束---------- */

/* ----------pg_type 列映射 开始---------- */
/* pg12 */
static const ripple_get_value_catalog_column_mapping m_pg_type_mapping_pg12[] =
{
    {RIPPLE_TYPE_MAPNUM_OID, 0},
    {RIPPLE_TYPE_MAPNUM_TYPNAME, 1}
};

/* hgdbv901 */
static const ripple_get_value_catalog_column_mapping m_pg_type_mapping_hg901[] =
{
    {RIPPLE_TYPE_MAPNUM_OID, 0},
    {RIPPLE_TYPE_MAPNUM_TYPNAME, 1}
};

/* hgdbv902 */
static const ripple_get_value_catalog_column_mapping m_pg_type_mapping_hg902[] =
{
    {RIPPLE_TYPE_MAPNUM_OID, 30},
    {RIPPLE_TYPE_MAPNUM_TYPNAME, 0}
};
/* ----------pg_type 列映射 结束---------- */

/* ----------pg_index 列映射 开始---------- */
// static const ripple_get_value_catalog_column_mapping m_pg_index_mapping_pg12[] =
// {
//     {RIPPLE_INDEX_MAPNUM_INDEXRELID, 0},
//     {, 1}
// };
/* ----------pg_index 列映射 结束---------- */

/* ----------get 开始---------- */
static char *ripple_get_value_from_colvalue_pg12(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
        {
            return (char *)colvalue[m_pg_class_mapping_pg12[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_ATTRIBUTE:
        {
            return (char *)colvalue[m_pg_attribute_mapping_pg12[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_NAMESPACE:
        {
            return (char *)colvalue[m_pg_namespace_mapping_pg12[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_TYPE:
        {
            return (char *)colvalue[m_pg_type_mapping_pg12[map_num].real_num].m_value;
            break;
        }
        // case RIPPLE_CATALOG_TYPE_INDEX:
        // {
        //     return (char *)colvalue[m_pg_type_mapping_pg12[map_num].real_num].m_value;
        //     break;
        // }
        default:
        {
            elog(RLOG_ERROR, "try find unknow catalog colvalue, type: %d", dtype);
            break;
        }
    }
    return NULL;
}

static char *ripple_get_value_from_colvalue_hg901(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
        {
            return (char *)colvalue[m_pg_class_mapping_hg901[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_ATTRIBUTE:
        {
            return (char *)colvalue[m_pg_attribute_mapping_hg901[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_NAMESPACE:
        {
            return (char *)colvalue[m_pg_namespace_mapping_hg901[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_TYPE:
        {
            return (char *)colvalue[m_pg_type_mapping_hg901[map_num].real_num].m_value;
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

static char *ripple_get_value_from_colvalue_hg902(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
        {
            return (char *)colvalue[m_pg_class_mapping_hg902[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_ATTRIBUTE:
        {
            return (char *)colvalue[m_pg_attribute_mapping_hg902[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_NAMESPACE:
        {
            return (char *)colvalue[m_pg_namespace_mapping_hg902[map_num].real_num].m_value;
            break;
        }
        case RIPPLE_CATALOG_TYPE_TYPE:
        {
            return (char *)colvalue[m_pg_type_mapping_hg902[map_num].real_num].m_value;
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

static char *ripple_get_value_from_colvalue_postgres(int dbversion,
                                                     xk_pg_parser_translog_tbcol_value *colvalue,
                                                     int dtype,
                                                     int map_num)
{
    if((m_deal_value_from_colvalue_pg_cnt - 1) < dbversion
        || NULL == m_deal_value_from_colvalue_pg_distribute[dbversion].getfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_pg_distribute[dbversion].getfunc(colvalue,
                                                                       dtype,
                                                                       map_num);
}

static char *ripple_get_value_from_colvalue_highgo(int dbversion,
                                                   xk_pg_parser_translog_tbcol_value *colvalue,
                                                   int dtype,
                                                   int map_num)
{
    if((m_deal_value_from_colvalue_hg_cnt - 1) < dbversion
        || NULL == m_deal_value_from_colvalue_hg_distribute[dbversion].getfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_hg_distribute[dbversion].getfunc(colvalue,
                                                                       dtype,
                                                                       map_num);
}

static char *ripple_get_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                            int dtype,
                                            int pnum,
                                            int dbtype,
                                            int dbversion)
{
    if((m_deal_value_from_colvalue_bydbtype_cnt - 1) < dbtype
        || NULL == m_deal_value_from_colvalue_dbtype_distribute[dbtype].getfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_dbtype_distribute[dbtype].getfunc(dbversion,
                                                                        colvalue,
                                                                        dtype,
                                                                        pnum);
}
/* ----------get 结束---------- */

/* ----------set 开始---------- */

static char *ripple_set_value_from_colvalue_pg12(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 char *new_value,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
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

static char *ripple_set_value_from_colvalue_hg901(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 char *new_value,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
        {
            colvalue[m_pg_class_mapping_hg901[map_num].real_num].m_value = new_value;
            colvalue[m_pg_class_mapping_hg901[map_num].real_num].m_valueLen = strlen(new_value) + 1;
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

static char *ripple_set_value_from_colvalue_hg902(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 char *new_value,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
        {
            colvalue[m_pg_class_mapping_hg902[map_num].real_num].m_value = new_value;
            colvalue[m_pg_class_mapping_hg902[map_num].real_num].m_valueLen = strlen(new_value) + 1;
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

static char *ripple_set_value_from_colvalue_postgres(int dbversion,
                                                     char *new_value,
                                                     xk_pg_parser_translog_tbcol_value *colvalue,
                                                     int dtype,
                                                     int map_num)
{
    if((m_deal_value_from_colvalue_pg_cnt - 1) < dbversion
        || NULL == m_deal_value_from_colvalue_pg_distribute[dbversion].setfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_pg_distribute[dbversion].setfunc(colvalue,
                                                                       new_value,
                                                                       dtype,
                                                                       map_num);
}

static char *ripple_set_value_from_colvalue_highgo(int dbversion,
                                                   char *new_value,
                                                   xk_pg_parser_translog_tbcol_value *colvalue,
                                                   int dtype,
                                                   int map_num)
{
    if((m_deal_value_from_colvalue_hg_cnt - 1) < dbversion
        || NULL == m_deal_value_from_colvalue_hg_distribute[dbversion].setfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_hg_distribute[dbversion].setfunc(colvalue,
                                                                       new_value,
                                                                       dtype,
                                                                       map_num);
}

static char *ripple_set_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                            char *new_value,
                                            int dtype,
                                            int pnum,
                                            int dbtype,
                                            int dbversion)
{
    if((m_deal_value_from_colvalue_bydbtype_cnt - 1) < dbtype
        || NULL == m_deal_value_from_colvalue_dbtype_distribute[dbtype].setfunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_dbtype_distribute[dbtype].setfunc(dbversion,
                                                                        new_value,
                                                                        colvalue,
                                                                        dtype,
                                                                        pnum);
}
/* ----------set 结束---------- */

/* ----------free 开始---------- */
static char *ripple_free_value_from_colvalue_pg12(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
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

static char *ripple_free_value_from_colvalue_hg901(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
        {
            rfree(colvalue[m_pg_class_mapping_hg901[map_num].real_num].m_value);
            colvalue[m_pg_class_mapping_hg901[map_num].real_num].m_value = NULL;
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

static char *ripple_free_value_from_colvalue_hg902(xk_pg_parser_translog_tbcol_value *colvalue,
                                                 int dtype,
                                                 int map_num)
{
    switch(dtype)
    {
        case RIPPLE_CATALOG_TYPE_CLASS:
        {
            rfree(colvalue[m_pg_class_mapping_hg902[map_num].real_num].m_value);
            colvalue[m_pg_class_mapping_hg902[map_num].real_num].m_value = NULL;
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

static char *ripple_free_value_from_colvalue_postgres(int dbversion,
                                                     xk_pg_parser_translog_tbcol_value *colvalue,
                                                     int dtype,
                                                     int map_num)
{
    if((m_deal_value_from_colvalue_pg_cnt - 1) < dbversion
        || NULL == m_deal_value_from_colvalue_pg_distribute[dbversion].freefunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_pg_distribute[dbversion].freefunc(colvalue,
                                                                       dtype,
                                                                       map_num);
}

static char *ripple_free_value_from_colvalue_highgo(int dbversion,
                                                   xk_pg_parser_translog_tbcol_value *colvalue,
                                                   int dtype,
                                                   int map_num)
{
    if((m_deal_value_from_colvalue_hg_cnt - 1) < dbversion
        || NULL == m_deal_value_from_colvalue_hg_distribute[dbversion].freefunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_hg_distribute[dbversion].freefunc(colvalue,
                                                                       dtype,
                                                                       map_num);
}

static char *ripple_free_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                            int dtype,
                                            int pnum,
                                            int dbtype,
                                            int dbversion)
{
    if((m_deal_value_from_colvalue_bydbtype_cnt - 1) < dbtype
        || NULL == m_deal_value_from_colvalue_dbtype_distribute[dbtype].freefunc)
    {
        return NULL;
    }
    return m_deal_value_from_colvalue_dbtype_distribute[dbtype].freefunc(dbversion,
                                                                        colvalue,
                                                                        dtype,
                                                                        pnum);
}
/* ----------free 结束---------- */

char *ripple_get_class_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                           int pnum,
                                           int dbtype,
                                           int dbversion)
{
    return ripple_get_value_from_colvalue(colvalue, RIPPLE_CATALOG_TYPE_CLASS, pnum, dbtype, dbversion);
}

char *ripple_set_class_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                           char *new_value,
                                           int pnum,
                                           int dbtype,
                                           int dbversion)
{
    return ripple_set_value_from_colvalue(colvalue, new_value, RIPPLE_CATALOG_TYPE_CLASS, pnum, dbtype, dbversion);
}

char *ripple_free_class_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                           int pnum,
                                           int dbtype,
                                           int dbversion)
{
    return ripple_free_value_from_colvalue(colvalue, RIPPLE_CATALOG_TYPE_CLASS, pnum, dbtype, dbversion);
}

char *ripple_get_attribute_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                               int pnum,
                                               int dbtype,
                                               int dbversion)
{
    return ripple_get_value_from_colvalue(colvalue, RIPPLE_CATALOG_TYPE_ATTRIBUTE, pnum, dbtype, dbversion);
}

char *ripple_get_namespace_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                               int pnum,
                                               int dbtype,
                                               int dbversion)
{
    return ripple_get_value_from_colvalue(colvalue, RIPPLE_CATALOG_TYPE_NAMESPACE, pnum, dbtype, dbversion);
}

char *ripple_get_type_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                          int pnum,
                                          int dbtype,
                                          int dbversion)
{
    return ripple_get_value_from_colvalue(colvalue, RIPPLE_CATALOG_TYPE_TYPE, pnum, dbtype, dbversion);
}

char *ripple_get_index_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                          int pnum,
                                          int dbtype,
                                          int dbversion)
{
    return ripple_get_value_from_colvalue(colvalue, RIPPLE_CATALOG_TYPE_INDEX, pnum, dbtype, dbversion);
}
