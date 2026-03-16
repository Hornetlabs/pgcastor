#ifndef _RIPPLE_DECODE_COLVALUE_H
#define _RIPPLE_DECODE_COLVALUE_H

typedef enum RIPPLE_CATALOG_MAPNUM_PG_CLASS
{
    RIPPLE_CLASS_MAPNUM_OID         = 0,
    RIPPLE_CLASS_MAPNUM_RELNAME     = 1,
    RIPPLE_CLASS_MAPNUM_RELNSPOID   = 2,
    RIPPLE_CLASS_MAPNUM_RELFILENODE = 3
} ripple_catalog_mapnum_pg_class;

typedef enum RIPPLE_CATALOG_MAPNUM_PG_ATTRIBUTE
{
    RIPPLE_ATTRIBUTE_MAPNUM_ATTRELID    = 0,
    RIPPLE_ATTRIBUTE_MAPNUM_ATTNAME     = 1,
    RIPPLE_ATTRIBUTE_MAPNUM_ATTNUM      = 2
} ripple_catalog_mapnum_pg_attribute;

typedef enum RIPPLE_CATALOG_MAPNUM_PG_NAMESPACE
{
    RIPPLE_NAMESPACE_MAPNUM_OID         = 0,
    RIPPLE_NAMESPACE_MAPNUM_NSPNAME     = 1
} ripple_catalog_mapnum_pg_namespace;

typedef enum RIPPLE_CATALOG_MAPNUM_PG_TYPE
{
    RIPPLE_TYPE_MAPNUM_OID         = 0,
    RIPPLE_TYPE_MAPNUM_TYPNAME     = 1
} ripple_catalog_mapnum_pg_type;

// typedef enum RIPPLE_CATALOG_MAPNUM_PG_INDEX
// {
//     RIPPLE_INDEX_MAPNUM_INDEXRELID  = 0,
//     RIPPLE_INDEX_MAPNUM_INDRELID    = 1,
//     RIPPLE_INDEX_MAPNUM_INDISUNIQUE = 2
// } ripple_catalog_mapnum_pg_index;

extern char *ripple_get_class_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                                  int pnum,
                                                  int dbtype,
                                                  int dbversion);

extern char *ripple_set_class_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                                  char *new_value,
                                                  int pnum,
                                                  int dbtype,
                                                  int dbversion);

extern char *ripple_free_class_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                                  int pnum,
                                                  int dbtype,
                                                  int dbversion);

extern char *ripple_get_attribute_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                                  int pnum,
                                                  int dbtype,
                                                  int dbversion);

extern char *ripple_get_namespace_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                                  int pnum,
                                                  int dbtype,
                                                  int dbversion);

extern char *ripple_get_type_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                          int pnum,
                                          int dbtype,
                                          int dbversion);

extern char *ripple_get_index_value_from_colvalue(xk_pg_parser_translog_tbcol_value *colvalue,
                                          int pnum,
                                          int dbtype,
                                          int dbversion);
#endif
