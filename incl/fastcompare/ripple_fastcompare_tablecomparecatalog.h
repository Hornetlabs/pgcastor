#ifndef _RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG_H
#define _RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG_H

typedef struct RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG_PGCLASS
{
    uint32                  oid;                            /* class oid */
    char                    relname[NAMEDATALEN];           /* class name */
    char                    nspname[NAMEDATALEN];           /* schema name */
} ripple_fastcompare_tablecomparecatalog_pgclass;

typedef struct RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG_PGATTRIBUTE
{
    uint32                  attrelid;                       /* OID of relation containing this attribute */
    char                    attname[NAMEDATALEN];
    uint32                  atttypid;
    int16                   attlen;
    int16                   attnum;
} ripple_fastcompare_tablecomparecatalog_pgattribute;

typedef struct RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG_PGCONSTRAINT
{
    uint32              conrelid;
    int16               conkeycnt;          /* 主键列个数 */
    List*               conkey;             /* 内容为ColumnDefine */
} ripple_fastcompare_tablecomparecatalog_pgconstraint;

typedef struct RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG_PGATTRIBUTE_VALUE
{
    Oid                     attrelid;
    List*                   attrs;          /* 内容为ripple_fastcompare_tablecomparecatalog_pgattribute */
} ripple_fastcompare_tablecomparecatalog_pgattribute_value;

/* table2oid hash key */
typedef struct RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG_TABLE2OID_KEY
{
    char                tablename[NAMEDATALEN];
    char                schemaname[NAMEDATALEN];
} ripple_fastcompare_tablecomparecatalog_table2oid_key;

typedef struct RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG_TABLE2OID_VALUE
{
    ripple_fastcompare_tablecomparecatalog_table2oid_key     key;
    Oid                                                     oid;
} ripple_fastcompare_tablecomparecatalog_table2oid_value;

typedef struct RIPPLE_FASTCOMPARE_TABLECOMPARECATALOG
{
    HTAB*                   pg_class;       /* key:reloid  value pg_class */
    HTAB*                   pg_attribute;   /* key:oid value: list */
    HTAB*                   pg_constraint;  /* key: pg_class->oid value:pg_constraint */
    HTAB*                   table2oid;      /* key:schema.table value: pg_class->oid */
} ripple_fastcompare_tablecomparecatalog;

ripple_fastcompare_tablecomparecatalog* ripple_fastcompare_tablecomparecatalog_init(void);

bool ripple_fastcompare_tablecomparecatalog_gettablebyoid(HTAB* class, Oid oid, char* table, char* schema);

Oid ripple_fastcompare_tablecomparecatalog_getoidbytable(HTAB* table2oid, char* table, char* schema);

List* ripple_fastcompare_tablecomparecatalog_getcoldefinebyoid(HTAB* attrhash, Oid oid);

List* ripple_fastcompare_tablecomparecatalog_getpkcoldefinebyoid(HTAB* conhash, Oid oid);

void ripple_fastcompare_tablecomparecatalog_destroy(ripple_fastcompare_tablecomparecatalog* sysdicts);

#endif
