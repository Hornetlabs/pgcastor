#ifndef _REFRESH_TABLES_H
#define _REFRESH_TABLES_H

typedef struct REFRESH_TABLE
{
    Oid                   oid;
    char*                 schema;
    char*                 table;
    struct REFRESH_TABLE* prev;
    struct REFRESH_TABLE* next;
} refresh_table;

typedef struct REFRESH_TABLES
{
    int            cnt;
    refresh_table* tables;
} refresh_tables;

extern refresh_table* refresh_table_init(void);

extern void refresh_table_setschema(char* schema, refresh_table* refreshtable);

extern void refresh_table_setoid(Oid oid, refresh_table* refreshtable);

extern void refresh_table_settable(char* table, refresh_table* refreshtable);

extern void refresh_freetable(refresh_table* refreshtable);

extern refresh_tables* refresh_tables_init(void);

extern bool refresh_tables_add(refresh_table* table, refresh_tables* tables);

extern refresh_table* refresh_tables_get(refresh_tables* tables);

extern void refresh_freetables(refresh_tables* refreshtables);

extern refresh_tables* refresh_tables_copy(refresh_tables* refreshtables);

extern bool refresh_tables_hasrepeat(refresh_tables* syncdataset,
                                     refresh_tables* newdataset,
                                     refresh_table** prepeattable);

extern bool refresh_tables_hasnew(HTAB* syncdataset, refresh_tables* newdataset);

extern refresh_tables* refresh_tables_gen_from_file(char* path);

extern bool refresh_tables_flush(refresh_tables* rtables);

extern refresh_tables* refresh_tables_load(void);

#endif
