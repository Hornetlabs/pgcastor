#ifndef _CATALOG_H
#define _CATALOG_H

typedef enum CATALOG_TYPE
{
    CATALOG_TYPE_NOP = 0x00,
    CATALOG_TYPE_CLASS = 0x01,
    CATALOG_TYPE_ATTRIBUTE,
    CATALOG_TYPE_TYPE,
    CATALOG_TYPE_NAMESPACE,
    CATALOG_TYPE_TABLESPACE,
    CATALOG_TYPE_ENUM,
    CATALOG_TYPE_RANGE,
    CATALOG_TYPE_PROC,
    CATALOG_TYPE_CONSTRAINT,
    CATALOG_TYPE_OPERATOR,
    CATALOG_TYPE_AUTHID,
    CATALOG_TYPE_DATABASE,
    CATALOG_TYPE_INDEX,
    CATALOG_TYPE_RELMAPFILE /* Add new system table before this */
} catalog_type;

typedef enum CATALOG_OP
{
    CATALOG_OP_NOP = 0x00,
    CATALOG_OP_INSERT = 0x01,
    CATALOG_OP_DELETE = 0x02,
    CATALOG_OP_UPDATE = 0x03
} catalog_op;

typedef struct CATALOGDATA
{
    catalog_type type;
    catalog_op   op;
    recpos       lsn; /* Transaction starting lsn, for filtering dictionary table */
    void*        catalog;
} catalogdata;

typedef struct CLASS_VALUE
{
    Oid oid;
    pg_sysdict_Form_pg_class class;
} catalog_class_value;

typedef struct TYPE_VALUE
{
    Oid                     oid;
    pg_sysdict_Form_pg_type type;
} catalog_type_value;

typedef struct PROC_VALUE
{
    Oid                     oid;
    pg_sysdict_Form_pg_proc proc;
} catalog_proc_value;

typedef struct NAMESPACE_VALUE
{
    Oid oid;
    pg_sysdict_Form_pg_namespace namespace;
} catalog_namespace_value;

typedef struct RANGE_VALUE
{
    Oid                      rngtypid;
    pg_sysdict_Form_pg_range range;
} catalog_range_value;

typedef struct ATTRIBUTE_VALUE
{
    Oid   attrelid;
    List* attrs;
} catalog_attribute_value;

typedef struct ENUM_VALUE
{
    Oid   enumtypid;
    List* enums;
} catalog_enum_value;

typedef struct CONSTRAINT_VALUE
{
    Oid                           conrelid;
    pg_sysdict_Form_pg_constraint constraint;
} catalog_constraint_value;

typedef struct OPERATOR_VALUE
{
    Oid                         oid;
    pg_sysdict_Form_pg_operator operator;
} catalog_operator_value;

typedef struct AUTHID_VALUE
{
    Oid                       oid;
    pg_sysdict_Form_pg_authid authid;
} catalog_authid_value;

typedef struct DATABASE_VALUE
{
    Oid                         oid;
    pg_sysdict_Form_pg_database database;
} catalog_database_value;

typedef struct INDEX_VALUE
{
    Oid                      oid;
    pg_sysdict_Form_pg_index index;
} catalog_index_value;

typedef struct CATALOG_INDEX_HASH_ENTRY
{
    Oid   oid;
    List* index_list; /* catalog_index_value */
} catalog_index_hash_entry;

// database datname mapping oid
typedef struct DATNAME2OID_VALUE
{
    pg_parser_NameData datname;
    Oid                oid;
} catalog_datname2oid_value;

typedef struct RELMAPPING
{
    Oid mapoid;      /* OID of a catalog */
    Oid mapfilenode; /* its filenode number */
} relmapping;

typedef struct RELMAPFILE
{
    int32       num;
    relmapping* mapping;
} replmapfile;

typedef struct SYSDICT_HEADER_ARRAY
{
    uint16 type;   /* Dictionary table type */
    uint64 offset; /* Offset within file */
    uint64 len;    /* End position */
} sysdict_header_array;

typedef struct SYSDICT_HEADER
{
    uint32     magic;
    uint32     compatibility;
    XLogRecPtr checkpointlsn;
} sysdict_header;

typedef struct CATALOG_ATTRIBUTE_SEARCH
{
    Oid     attrelid;
    int16_t attnum;
} catalog_attribute_search;

void catalog_sysdict_getfromdb(void* conn_in, cache_sysdicts* sysdicts);

bool catalog_sysdict_setfullmode(HTAB* hclass);

List* catalog_sysdict_filterbylsn(List** sysdict, uint64 redolsn);

catalogdata* catalog_copy(catalogdata* catalog_in);

catalogdata* catalog_colvalued2catalog(int dbtype, int dbversion, void* in_colvalues);

Oid catalog_get_oid_by_relfilenode(HTAB* relfilenode_htab, List* sysdicthis, List* sysdict,
                                   uint32_t dboid, uint32_t tbspcoid, uint32_t relfilenode,
                                   bool report_error);

void* catalog_get_class_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_database_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_attribute_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis,
                                    Oid attrelid, int16_t attnum);

void* catalog_get_namespace_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_type_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_range_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_enum_sysdict_list(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_proc_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_constraint_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_authid_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_operator_sysdict(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

void* catalog_get_index_sysdict_list(HTAB* sysdict_hash, List* sysdict, List* sysdicthis, Oid oid);

#endif
