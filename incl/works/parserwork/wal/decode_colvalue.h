#ifndef _DECODE_COLVALUE_H
#define _DECODE_COLVALUE_H

typedef enum CATALOG_MAPNUM_PG_CLASS
{
    CLASS_MAPNUM_OID = 0,
    CLASS_MAPNUM_RELNAME = 1,
    CLASS_MAPNUM_RELNSPOID = 2,
    CLASS_MAPNUM_RELFILENODE = 3
} catalog_mapnum_pg_class;

typedef enum CATALOG_MAPNUM_PG_ATTRIBUTE
{
    ATTRIBUTE_MAPNUM_ATTRELID = 0,
    ATTRIBUTE_MAPNUM_ATTNAME = 1,
    ATTRIBUTE_MAPNUM_ATTNUM = 2
} catalog_mapnum_pg_attribute;

typedef enum CATALOG_MAPNUM_PG_NAMESPACE
{
    NAMESPACE_MAPNUM_OID = 0,
    NAMESPACE_MAPNUM_NSPNAME = 1
} catalog_mapnum_pg_namespace;

typedef enum CATALOG_MAPNUM_PG_TYPE
{
    TYPE_MAPNUM_OID = 0,
    TYPE_MAPNUM_TYPNAME = 1
} catalog_mapnum_pg_type;

// typedef enum CATALOG_MAPNUM_PG_INDEX
// {
//     INDEX_MAPNUM_INDEXRELID  = 0,
//     INDEX_MAPNUM_INDRELID    = 1,
//     INDEX_MAPNUM_INDISUNIQUE = 2
// } catalog_mapnum_pg_index;

extern char* get_class_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                           int                             pnum,
                                           int                             dbtype,
                                           int                             dbversion);

extern char* set_class_value_from_colvalue(
    pg_parser_translog_tbcol_value* colvalue, char* new_value, int pnum, int dbtype, int dbversion);

extern char* free_class_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                            int                             pnum,
                                            int                             dbtype,
                                            int                             dbversion);

extern char* get_attribute_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                               int                             pnum,
                                               int                             dbtype,
                                               int                             dbversion);

extern char* get_namespace_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                               int                             pnum,
                                               int                             dbtype,
                                               int                             dbversion);

extern char* get_type_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                          int                             pnum,
                                          int                             dbtype,
                                          int                             dbversion);

extern char* get_index_value_from_colvalue(pg_parser_translog_tbcol_value* colvalue,
                                           int                             pnum,
                                           int                             dbtype,
                                           int                             dbversion);
#endif
