#ifndef _RIPPLE_REFRESH_TABLES_H
#define _RIPPLE_REFRESH_TABLES_H

typedef struct RIPPLE_REFRESH_TABLE
{
    Oid   oid;
    char *schema;
    char *table;
    struct RIPPLE_REFRESH_TABLE* prev;
    struct RIPPLE_REFRESH_TABLE* next;
} ripple_refresh_table;

typedef struct RIPPLE_REFRESH_TABLES
{
    int cnt;
    ripple_refresh_table *tables;
} ripple_refresh_tables;


extern ripple_refresh_table* ripple_refresh_table_init(void);

extern void ripple_refresh_table_setschema(char* schema, ripple_refresh_table* refreshtable);

extern void ripple_refresh_table_setoid(Oid oid, ripple_refresh_table* refreshtable);

extern void ripple_refresh_table_settable(char* table, ripple_refresh_table* refreshtable);

extern void ripple_refresh_freetable(ripple_refresh_table *refreshtable);

extern ripple_refresh_tables* ripple_refresh_tables_init(void);

extern bool ripple_refresh_tables_add(ripple_refresh_table* table, ripple_refresh_tables* tables);

extern ripple_refresh_table* ripple_refresh_tables_get(ripple_refresh_tables* tables);

extern void ripple_refresh_freetables(ripple_refresh_tables *refreshtables);

extern ripple_refresh_tables* ripple_refresh_tables_copy(ripple_refresh_tables* refreshtables);

extern bool ripple_refresh_tables_hasrepeat(ripple_refresh_tables* syncdataset,
                                            ripple_refresh_tables* newdataset,
                                            ripple_refresh_table** prepeattable);

extern bool ripple_refresh_tables_hasnew(HTAB *syncdataset, ripple_refresh_tables* newdataset);

extern ripple_refresh_tables *ripple_refresh_tables_gen_from_file(char *path);

#endif
