#ifndef _RIPPLE_CATALOG_H
#define _RIPPLE_CATALOG_H

typedef enum RIPPLE_CATALOG_TYPE
{
    RIPPLE_CATALOG_TYPE_NOP   = 0x00,
    RIPPLE_CATALOG_TYPE_CLASS = 0x01,
    RIPPLE_CATALOG_TYPE_ATTRIBUTE,
    RIPPLE_CATALOG_TYPE_TYPE,
    RIPPLE_CATALOG_TYPE_NAMESPACE,
    RIPPLE_CATALOG_TYPE_TABLESPACE,
    RIPPLE_CATALOG_TYPE_ENUM,
    RIPPLE_CATALOG_TYPE_RANGE,
    RIPPLE_CATALOG_TYPE_PROC,
    RIPPLE_CATALOG_TYPE_CONSTRAINT,
    RIPPLE_CATALOG_TYPE_OPERATOR,
    RIPPLE_CATALOG_TYPE_AUTHID,
    RIPPLE_CATALOG_TYPE_DATABASE,
    RIPPLE_CATALOG_TYPE_INDEX,
    RIPPLE_CATALOG_TYPE_RELMAPFILE      /* 在此之前添加新系统表 */
} ripple_catalog_type;

typedef enum RIPPLE_CATALOG_OP
{
    RIPPLE_CATALOG_OP_NOP    = 0x00,
    RIPPLE_CATALOG_OP_INSERT = 0x01,
    RIPPLE_CATALOG_OP_DELETE = 0x02,
    RIPPLE_CATALOG_OP_UPDATE = 0x03
} ripple_catalog_op;

typedef struct RIPPLE_CATALOGDATA
{
    ripple_catalog_type type;
    ripple_catalog_op   op;
    ripple_recpos       lsn;            /* 事务开始的lsn，用于过滤字典表 */
    void*               catalog;
} ripple_catalogdata;

typedef struct RIPPLE_CLASS_VALUE
{
    Oid                             oid;
    xk_pg_sysdict_Form_pg_class    ripple_class;
} ripple_catalog_class_value;

typedef struct RIPPLE_TYPE_VALUE
{
    Oid                             oid;
    xk_pg_sysdict_Form_pg_type    ripple_type;
} ripple_catalog_type_value;

typedef struct RIPPLE_PROC_VALUE
{
    Oid                             oid;
    xk_pg_sysdict_Form_pg_proc    ripple_proc;
} ripple_catalog_proc_value;

typedef struct RIPPLE_NAMESPACE_VALUE
{
    Oid                                 oid;
    xk_pg_sysdict_Form_pg_namespace     ripple_namespace;
} ripple_catalog_namespace_value;

typedef struct RIPPLE_RANGE_VALUE
{
    Oid                             rngtypid;
    xk_pg_sysdict_Form_pg_range    ripple_range;
} ripple_catalog_range_value;

typedef struct RIPPLE_ATTRIBUTE_VALUE
{
    Oid                     attrelid;
    List*                   attrs;
} ripple_catalog_attribute_value;

typedef struct RIPPLE_ENUM_VALUE
{
    Oid                     enumtypid;
    List*                   enums;
} ripple_catalog_enum_value;

typedef struct RIPPLE_CONSTRAINT_VALUE
{
    Oid                                    conrelid;
    xk_pg_sysdict_Form_pg_constraint       constraint;
} ripple_catalog_constraint_value;

typedef struct RIPPLE_OPERATOR_VALUE
{
    Oid                                oid;
    xk_pg_sysdict_Form_pg_operator     ripple_operator;
} ripple_catalog_operator_value;

typedef struct RIPPLE_AUTHID_VALUE
{
    Oid                              oid;
    xk_pg_sysdict_Form_pg_authid     ripple_authid;
} ripple_catalog_authid_value;

typedef struct RIPPLE_DATABASE_VALUE
{
    Oid                              oid;
    xk_pg_sysdict_Form_pg_database   ripple_database;
} ripple_catalog_database_value;

typedef struct RIPPLE_INDEX_VALUE
{
    Oid                              oid;
    xk_pg_sysdict_Form_pg_index      ripple_index;
} ripple_catalog_index_value;

typedef struct RIPPLE_CATALOG_INDEX_HASH_ENTRY
{
    Oid                              oid;
    List*                            ripple_index_list; /* ripple_catalog_index_value */
} ripple_catalog_index_hash_entry;

//database datname映射oid
typedef struct RIPPLE_DATNAME2OID_VALUE
{
    xk_pg_parser_NameData            datname;
    Oid                              oid;
} ripple_catalog_datname2oid_value;

typedef struct RIPPLE_RELMAPPING
{
	Oid			mapoid;			/* OID of a catalog */
	Oid			mapfilenode;	/* its filenode number */
} ripple_relmapping;

typedef struct RIPPLE_RELMAPFILE
{
    int32               num;
    ripple_relmapping*  mapping;
} ripple_replmapfile;

typedef struct RIPPLE_SYSDICT_HEADER_ARRAY
{
    uint16               type;                /* 字典表类型 */
    uint64               offset;              /* 文件内的偏移 */
    uint64               len;                 /* 结束位置 */
} ripple_sysdict_header_array;

typedef struct RIPPLE_SYSDICT_HEADER
{
    uint32               magic;
    uint32               compatibility;
    XLogRecPtr           checkpointlsn;
} ripple_sysdict_header;

typedef struct RIPPLE_CATALOG_ATTRIBUTE_SEARCH
{
    Oid     attrelid;
    int16_t attnum;
} ripple_catalog_attribute_search;


void ripple_catalog_sysdict_getfromdb(void* conn_in, ripple_cache_sysdicts* sysdicts);

bool ripple_catalog_sysdict_setfullmode(HTAB* hclass);

List* ripple_catalog_sysdict_filterbylsn(List **sysdict, uint64 redolsn);

ripple_catalogdata *ripple_catalog_copy(ripple_catalogdata *catalog_in);

ripple_catalogdata* ripple_catalog_colvalued2catalog(int dbtype, int dbversion, void* in_colvalues);

Oid ripple_catalog_get_oid_by_relfilenode(HTAB *relfilenode_htab,
                                          List *sysdicthis,
                                          List *sysdict,
                                          uint32_t dboid,
                                          uint32_t tbspcoid,
                                          uint32_t relfilenode,
                                          bool report_error);

void *ripple_catalog_get_class_sysdict(HTAB *sysdict_hash,
                                      List *sysdict,
                                      List *sysdicthis,
                                      Oid   oid);

void *ripple_catalog_get_database_sysdict(HTAB *sysdict_hash,
                                          List *sysdict,
                                          List *sysdicthis,
                                          Oid   oid);

void *ripple_catalog_get_attribute_sysdict(HTAB *sysdict_hash,
                                         List *sysdict,
                                         List *sysdicthis,
                                         Oid   attrelid,
                                         int16_t attnum);

void *ripple_catalog_get_namespace_sysdict(HTAB *sysdict_hash,
                                       List *sysdict,
                                       List *sysdicthis,
                                       Oid   oid);

void *ripple_catalog_get_type_sysdict(HTAB *sysdict_hash,
                                       List *sysdict,
                                       List *sysdicthis,
                                       Oid   oid);

void *ripple_catalog_get_range_sysdict(HTAB *sysdict_hash,
                                       List *sysdict,
                                       List *sysdicthis,
                                       Oid   oid);

void *ripple_catalog_get_enum_sysdict_list(HTAB *sysdict_hash,
                                           List *sysdict,
                                           List *sysdicthis,
                                           Oid   oid);

void *ripple_catalog_get_proc_sysdict(HTAB *sysdict_hash,
                                      List *sysdict,
                                      List *sysdicthis,
                                      Oid   oid);

void *ripple_catalog_get_constraint_sysdict(HTAB *sysdict_hash,
                                            List *sysdict,
                                            List *sysdicthis,
                                            Oid   oid);

void *ripple_catalog_get_authid_sysdict(HTAB *sysdict_hash,
                                        List *sysdict,
                                        List *sysdicthis,
                                        Oid   oid);

void *ripple_catalog_get_operator_sysdict(HTAB *sysdict_hash,
                                          List *sysdict,
                                          List *sysdicthis,
                                          Oid   oid);

void *ripple_catalog_get_index_sysdict_list(HTAB *sysdict_hash,
                                            List *sysdict,
                                            List *sysdicthis,
                                            Oid   oid);

#endif
