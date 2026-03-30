#ifndef PG_PARSER_TRANSLOG_H
#define PG_PARSER_TRANSLOG_H

#include "common/pg_parser_sysdict.h"

/* WAL log level definition */
#define PG_PARSER_WALLEVEL_REPLICA 0
#define PG_PARSER_WALLEVEL_LOGICAL 1

/* OriginId define */
#define PG_PARSER_TRANSLOG_InvalidRepOriginId 0

/* Pre-parse interface return type definition */
#define PG_PARSER_TRANSLOG_INVALID             (uint8_t)0x00
#define PG_PARSER_TRANSLOG_HEAP_INSERT         (uint8_t)0x01
#define PG_PARSER_TRANSLOG_HEAP_UPDATE         (uint8_t)0x02
#define PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE     (uint8_t)0x03
#define PG_PARSER_TRANSLOG_HEAP_DELETE         (uint8_t)0x04
#define PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT  (uint8_t)0x05
#define PG_PARSER_TRANSLOG_XACT_COMMIT         (uint8_t)0x06
#define PG_PARSER_TRANSLOG_XACT_ABORT          (uint8_t)0x07
#define PG_PARSER_TRANSLOG_XLOG_SWITCH         (uint8_t)0x08
#define PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE     (uint8_t)0x09
#define PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN   (uint8_t)0x0A
#define PG_PARSER_TRANSLOG_FPW_TUPLE           (uint8_t)0x0B
#define PG_PARSER_TRANSLOG_RELMAP              (uint8_t)0x0C
#define PG_PARSER_TRANSLOG_RUNNING_XACTS       (uint8_t)0x0D
#define PG_PARSER_TRANSLOG_XLOG_RECOVERY       (uint8_t)0x0E
#define PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE (uint8_t)0x0F
#define PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE  (uint8_t)0x10
#define PG_PARSER_TRANSLOG_XACT_ASSIGNMENT     (uint8_t)0x11
#define PG_PARSER_TRANSLOG_XACT_PREPARE        (uint8_t)0x12
#define PG_PARSER_TRANSLOG_HEAP_TRUNCATE       (uint8_t)0x13
#define PG_PARSER_TRANSLOG_SEQ                 (uint8_t)0x14

/* Secondary parse type definition */
#define PG_PARSER_TRANSLOG_RETURN_INVALID    (uint8_t)0x00
#define PG_PARSER_TRANSLOG_RETURN_WITH_DATA  (uint8_t)0x01
#define PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE (uint8_t)0x02

/* Secondary parse DML type definition */
#define PG_PARSER_TRANSLOG_DMLTYPE_INVALID     (uint8_t)0x00
#define PG_PARSER_TRANSLOG_DMLTYPE_INSERT      (uint8_t)0x01
#define PG_PARSER_TRANSLOG_DMLTYPE_DELETE      (uint8_t)0x02
#define PG_PARSER_TRANSLOG_DMLTYPE_UPDATE      (uint8_t)0x03
#define PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT (uint8_t)0x04

/* Secondary parse DML table type definition */
#define PG_PARSER_TRANSLOG_TABLETYPE_NORMAL (uint8_t)0x00
#define PG_PARSER_TRANSLOG_TABLETYPE_SYS    (uint8_t)0x01
#define PG_PARSER_TRANSLOG_TABLETYPE_DICT   (uint8_t)0x02

/* DDL type definition */
#define PG_PARSER_DDLTYPE_CREATE  (uint8_t)0x01
#define PG_PARSER_DDLTYPE_ALTER   (uint8_t)0x02
#define PG_PARSER_DDLTYPE_DROP    (uint8_t)0x03
#define PG_PARSER_DDLTYPE_SPECIAL (uint8_t)0x04

/* DDL info definition */

/* DDL create statement info definition */
#define PG_PARSER_DDLINFO_CREATE_TABLE     (uint8_t)0x01
#define PG_PARSER_DDLINFO_CREATE_NAMESPACE (uint8_t)0x02
#define PG_PARSER_DDLINFO_CREATE_DATABASE  (uint8_t)0x03
#define PG_PARSER_DDLINFO_CREATE_INDEX     (uint8_t)0x04
#define PG_PARSER_DDLINFO_CREATE_SEQUENCE  (uint8_t)0x05
#define PG_PARSER_DDLINFO_CREATE_VIEW      (uint8_t)0x06
#define PG_PARSER_DDLINFO_CREATE_FUNCTION  (uint8_t)0x07
#define PG_PARSER_DDLINFO_CREATE_TRIGGER   (uint8_t)0x08
#define PG_PARSER_DDLINFO_CREATE_TYPE      (uint8_t)0x09

/* DDL alter statement info definition */
#define PG_PARSER_DDLINFO_ALTER_COLUMN_RENAME            (uint8_t)0x01
#define PG_PARSER_DDLINFO_ALTER_COLUMN_NOTNULL           (uint8_t)0x02
#define PG_PARSER_DDLINFO_ALTER_COLUMN_NULL              (uint8_t)0x03
#define PG_PARSER_DDLINFO_ALTER_COLUMN_TYPE              (uint8_t)0x04
#define PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT           (uint8_t)0x05
#define PG_PARSER_DDLINFO_ALTER_COLUMN_DROP_DEFAULT      (uint8_t)0x06
#define PG_PARSER_DDLINFO_ALTER_TABLE_ADD_COLUMN         (uint8_t)0x07
#define PG_PARSER_DDLINFO_ALTER_TABLE_RENAME             (uint8_t)0x08
#define PG_PARSER_DDLINFO_ALTER_TABLE_DROP_COLUMN        (uint8_t)0x09
#define PG_PARSER_DDLINFO_ALTER_TABLE_ADD_CONSTRAINT     (uint8_t)0x0A
#define PG_PARSER_DDLINFO_ALTER_TABLE_DROP_CONSTRAINT    (uint8_t)0x0B
#define PG_PARSER_DDLINFO_ALTER_TABLE_NAMESPACE          (uint8_t)0x0C
#define PG_PARSER_DDLINFO_ALTER_TABLE_SET_LOGGED         (uint8_t)0x0D
#define PG_PARSER_DDLINFO_ALTER_TABLE_SET_UNLOGGED       (uint8_t)0x0E
#define PG_PARSER_DDLINFO_ALTER_TABLE_OWNER              (uint8_t)0x0F
#define PG_PARSER_DDLINFO_ALTER_SEQ_RESTART              (uint8_t)0x10
#define PG_PARSER_DDLINFO_ALTER_TABLE_REPLICA_IDENTIFITY (uint8_t)0x11

/* DDL drop statement info definition */
#define PG_PARSER_DDLINFO_DROP_NAMESPACE (uint8_t)0x01
#define PG_PARSER_DDLINFO_DROP_DATABASE  (uint8_t)0x02
#define PG_PARSER_DDLINFO_DROP_VIEW      (uint8_t)0x03
#define PG_PARSER_DDLINFO_DROP_TABLE     (uint8_t)0x04
#define PG_PARSER_DDLINFO_DROP_INDEX     (uint8_t)0x05
#define PG_PARSER_DDLINFO_DROP_SEQUENCE  (uint8_t)0x06
#define PG_PARSER_DDLINFO_DROP_FUNCTION  (uint8_t)0x07
#define PG_PARSER_DDLINFO_DROP_TRIGGER   (uint8_t)0x08
#define PG_PARSER_DDLINFO_DROP_TYPE      (uint8_t)0x09

/* DDL special statement info definition */
#define PG_PARSER_DDLINFO_TRUNCATE (uint8_t)0x01
#define PG_PARSER_DDLINFO_REINDEX  (uint8_t)0x02

/* Page size definition */
#define PG_PARSER_PAGESIZE_8K  (uint32_t)8192
#define PG_PARSER_PAGESIZE_16K (uint32_t)16384
#define PG_PARSER_PAGESIZE_32K (uint32_t)32786
#define PG_PARSER_PAGESIZE_64K (uint32_t)65536

typedef enum PG_PARSER_TRANSLOG_COLINFO
{
    INFO_NOTHING = 0x00,
    INFO_COL_IS_NULL = 0x01,   /* Null value marker during insert */
    INFO_COL_MAY_NULL = 0x02,  /* Data not recorded in WAL during update/delete old data retrieval */
    INFO_COL_IS_TOAST = 0x03,  /* Out-of-line storage */
    INFO_COL_IS_CUSTOM = 0x04, /* Custom type */
    INFO_COL_IS_ARRAY = 0x05,  /* ARRAY type */
    INFO_COL_IS_BYTEA = 0x06,  /* Column is binary data */
    INFO_COL_IS_DROPED = 0x07, /* Column was dropped */
    INFO_COL_IS_NODE = 0x08    /* Column is binary data */
} pg_parser_translog_colinfo;

#ifndef database_type_define
typedef enum PG_PARSER_DATABASE_TYPE
{
    DATABASE_TYPE_NOP = 0x00,
    DATABASE_TYPE_POSTGRESQL
} pg_parser_database_type;

#define DATABASE_PG127       "pg127"
#define DATABASE_PG1410      "pg1410"

#define database_type_define 1

#endif
/*-------------------   Pre-parse interface input parameters  begin --------------------*/
typedef struct PG_PARSER_TRANSLOG_PRE
{
    int8_t   m_walLevel;   /* WAL log level */
    int8_t   m_debugLevel; /* Debug information level */
    int16_t  m_dbtype;     /* Database type */
    uint32_t m_pagesize;   /* Page size */
    char*    m_dbversion;  /* Database version */
    uint8_t* m_record;     /* record */
} pg_parser_translog_pre;

/*-------------------   Pre-parse interface input parameters  end --------------------*/

/*-------------------   Pre-parse interface output parameters  begin --------------------*/
typedef struct PG_PARSER_TRANSLOG_PRE_BASE
{
    uint8_t  m_type; /* record type */
    uint32_t m_xid;  /* Transaction ID */
    uint16_t m_originid;
} pg_parser_translog_pre_base;

/* Transaction status: COMMIT, ABORT, COMMIT PREPARE, ABORT PREPARE */
typedef struct PG_PARSER_TRANSLOG_PRE_TRANS
{
    pg_parser_translog_pre_base m_base;
    uint8_t                     m_status; /* 0x01 abort, 0x02 commit */
    int64                       m_time;
    void*                       m_transdata; /* xl_xact_parsed_commit */
} pg_parser_translog_pre_trans;

typedef struct PG_PARSER_TRANSLOG_PRE_ASSIGNMENT
{
    pg_parser_translog_pre_base m_base;
    void*                       m_assignment;
} pg_parser_translog_pre_assignment;

/* heap */
typedef struct PG_PARSER_TRANSLOG_PRE_HEAP
{
    pg_parser_translog_pre_base m_base;
    uint8_t                     m_needtuple;   /* Whether page is needed */
    uint32_t                    m_tuplecnts;   /* Number of tuples */
    uint32_t                    m_tupitemoff;  /* Offset of tuple item */
    uint32_t                    m_transid;     /* Transaction ID */
    uint32_t                    m_relfilenode; /* Physical file ID */
    uint32_t                    m_dboid;       /* Database OID */
    uint32_t                    m_tbspcoid;    /* Tablespace OID */
    uint32_t                    m_pagenos;     /* Page number */
} pg_parser_translog_pre_heap;

typedef struct PG_PARSER_TRANSLOG_PRE_HEAP_TRUNCATE
{
    pg_parser_translog_pre_base m_base;
    bool                        cascade;
    bool                        reseq;
    uint32_t                    dbid;
    uint32_t                    nrelids;
    uint32_t*                   relids;
} pg_parser_translog_pre_heap_truncate;

/* Column parse output parameter structure for storing tuple update information */
typedef struct PG_PARSER_TRANSLOG_TUPLECACHE
{
    uint32_t m_tuplelen;   /* Tuple length */
    uint32_t m_itemoffnum; /* Tuple item ID offset */
    uint32_t m_pageno;     /* Page number */
    uint8_t* m_tupledata;  /* Tuple data */
} pg_parser_translog_tuplecache;

/* During pre-parse, return full page write when non-parse type exists in full page write */
typedef struct PG_PARSER_TRANSLOG_PRE_IMAGE_TUPLE
{
    pg_parser_translog_pre_base    m_base;
    uint32_t                       m_relfilenode; /* Physical file ID */
    uint32_t                       m_dboid;
    uint32_t                       m_tbspcoid;
    uint32_t                       m_tuplecnt; /* Number of tuples */
    uint32_t                       m_transid;
    pg_parser_translog_tuplecache* m_tuples;
} pg_parser_translog_pre_image_tuple;

/* checkpoint */
typedef struct PG_PARSER_TRANSLOG_PRE_TRANSCHKP
{
    pg_parser_translog_pre_base m_base;
    uint32_t                    m_this_timeline;
    uint32_t                    m_prev_timeline;
    uint64_t                    m_nextid;   /* Next transaction ID */
    uint64_t                    m_redo_lsn; /* Redo start LSN, full page write will be flushed after this LSN
                                               (including this LSN) */
} pg_parser_translog_pre_transchkp;

typedef struct PG_PARSER_TRANSLOG_PRE_ENDRECOVERY
{
    pg_parser_translog_pre_base m_base;
    uint32_t                    m_this_timeline;
    uint32_t                    m_prev_timeline;
} pg_parser_translog_pre_endrecovery;

/* switch */
typedef struct PG_PARSER_TRANSLOG_PRE_SWITCH
{
    pg_parser_translog_pre_base m_base;
} pg_parser_translog_pre_switch;

typedef struct PG_PARSER_TRANSLOG_PRE_RELMAP
{
    pg_parser_translog_pre_base m_base;
    uint64_t                    m_dboid;
    uint16_t                    m_count;
    void*                       m_mapping;
} pg_parser_translog_pre_relmap;

typedef struct PG_PARSER_TRANSLOG_PRE_RUNNING_XACT
{
    pg_parser_translog_pre_base m_base;
    void*                       m_standby;
} pg_parser_translog_pre_running_xact;

/* seq */
typedef struct PG_PARSER_TRANSLOG_PRE_SEQ
{
    pg_parser_translog_pre_base m_base;
    uint32_t                    m_dboid;
    uint32_t                    m_tbspcoid;
    uint32_t                    m_relfilenode;
    int64_t                     m_last_value;
} pg_parser_translog_pre_seq;

/*-------------------   Pre-parse interface output parameters    end --------------------*/

/*-------------------   Data dictionary structure begin -----------------------*/

typedef struct PG_PARSER_SYSDICTS
{
    pg_parser_sysdict_pgclass_dict      m_pg_class;
    pg_parser_sysdict_pgattributes_dict m_pg_attribute;
    pg_parser_sysdict_pgnamespace_dict  m_pg_namespace;
    pg_parser_sysdict_pgtype_dict       m_pg_type;
    pg_parser_sysdict_pgrange_dict      m_pg_range;
    pg_parser_sysdict_pgenum_dict       m_pg_enum;
    pg_parser_sysdict_pgproc_dict       m_pg_proc;
} pg_parser_sysdicts;

/*-------------------   Data dictionary structure   end -----------------------*/

/*-------------------   Column parse interface     begin -----------------------*/

typedef struct PG_PARSER_TRANSLOG_CONVERTINFO
{
    char* m_tzname;         /* Timezone name of source database */
    char* m_monetary;       /* lc_monetary of source database */
    char* m_numeric;        /* lc_numeric of source database */
    char* m_dbcharset;      /* Database encoding */
    char* m_tartgetcharset; /* Target encoding */
} pg_parser_translog_convertinfo;

typedef struct PG_PARSER_TRANSLOG_CONVERTINFO_WITH_ZIC
{
    bool                            istoast;
    uint8_t                         debuglevel;
    int32_t                         ziclen;
    char*                           zicdata;
    char*                           dbversion;
    int16_t                         dbtype;
    int32_t*                        errorno;
    pg_parser_translog_convertinfo* convertinfo;
} pg_parser_translog_convertinfo_with_zic;

/* Column parse interface input parameters */
typedef struct PG_PARSER_TRANSLOG_TRANSLOG2COL
{
    uint8_t                         m_iscatalog;  /* Whether it is a system catalog */
    uint8_t                         m_walLevel;   /* WAL level */
    uint8_t                         m_debugLevel; /* Debug information level */
    int16_t                         m_dbtype;     /* Database type */
    uint32_t                        m_pagesize;   /* Page size */
    uint32_t                        m_tuplecnt;   /* Tuple count */
    char*                           m_dbversion;  /* Database version */
    uint8_t*                        m_record;     /* Record data */
    pg_parser_translog_convertinfo* m_convert;    /* Conversion information */
    pg_parser_translog_tuplecache*  m_tuples;     /* Tuple data */
    pg_parser_sysdicts*             m_sysdicts;   /* System catalogs */
} pg_parser_translog_translog2col;

/* Column parse output basic element */
typedef struct PG_PARSER_TRANSLOG_TBCOLBASE
{
    uint8_t  m_type;       /* Type identifier */
    uint8_t  m_dmltype;    /* DML type */
    uint8_t  m_tabletype;  /* Table type, see secondary parse DML table type definition */
    uint16_t m_originid;   /* originid */
    char*    m_schemaname; /* Schema name */
    char*    m_tbname;     /* Table name */
} pg_parser_translog_tbcolbase;

/* Type used for pg_parser_translog_tbcol_value->m_value */
typedef struct PG_PARSER_TRANSLOG_TBCOL_VALUETYPE_EXTERNAL
{
    int32_t  m_rawsize;    /* Raw data size (including header) */
    int32_t  m_extsize;    /* Out-of-line storage size */
    uint32_t m_valueid;    /* Corresponding data record ID in toast table */
    uint32_t m_toastrelid; /* Toast table OID */
} pg_parser_translog_tbcol_valuetype_external;

/* Column parse output value structure */
typedef struct PG_PARSER_TRANSLOG_TBCOL_VALUE
{
    bool     m_freeFlag; /* Flag to release colName */
    uint8_t  m_info;     /* Compare with pg_parser_translog_colinfo */
    uint32_t m_coltype;  /* Column type OID */
    uint32_t m_valueLen; /* Data length */
    char*    m_colName;  /* Column name */

    void*    m_value; /* Column data, converted structure based on m_info, m_coltype
                       * When INFO_COL_IS_TOAST is set in m_info
                       * Stores out-of-line storage structure
                       * pg_parser_translog_tbcol_valuetype_external
                       */
} pg_parser_translog_tbcol_value;

/* Out-of-line storage secondary parse interface */
typedef struct PG_PARSER_TRANSLOG_EXTERNAL
{
    uint8_t                         m_debuglevel;  /* Debug level */
    int16_t                         m_dbtype;      /* Database type */
    uint32_t                        m_typeid;      /* Type OID */
    uint32_t                        m_datalen;     /* Data length */
    char*                           m_dbversion;   /* Database version */
    char*                           m_colName;     /* Column name */
    char*                           m_typout;      /* Output function */
    pg_parser_translog_convertinfo* m_convertInfo; /* Column type OID */
    char*                           m_chunkdata;   /* Out-of-line storage data */
} pg_parser_translog_external;

/* Custom column */
typedef struct PG_PARSER_TRANSLOG_TBCOL_VALUETYPE_CUSTOMER
{
    pg_parser_translog_tbcol_value*                     m_value; /* Column value */

    /* next */
    struct PG_PARSER_TRANSLOG_TBCOL_VALUETYPE_CUSTOMER* m_next; /* Next column value */
} pg_parser_translog_tbcol_valuetype_customer;

/* Column parse output */
typedef struct PG_PARSER_TRANSLOG_TBCOL_VALUES
{
    pg_parser_translog_tbcolbase    m_base;
    bool                            m_haspkey;
    uint16_t                        m_valueCnt;    /* Column count */
    uint32_t                        m_tupleCnt;    /* Tuple count */
    uint32_t                        m_relfilenode; /* Physical file ID */
    uint32_t                        m_relid;       /* Table OID */
    pg_parser_translog_tbcol_value* m_new_values;  /* New columns */
    pg_parser_translog_tbcol_value* m_old_values;  /* Old columns */
    pg_parser_translog_tuplecache*  m_tuple;       /* Tuple update */
} pg_parser_translog_tbcol_values;

typedef struct PG_PARSER_TRANSLOG_TBCOL_ROWS
{
    pg_parser_translog_tbcol_value* m_new_values; /* New columns */
} pg_parser_translog_tbcol_rows;

typedef struct PG_PARSER_TRANSLOG_TBCOL_NVALUES
{
    pg_parser_translog_tbcolbase   m_base;
    bool                           m_haspkey;
    uint16_t                       m_valueCnt;    /* Column count */
    uint16_t                       m_rowCnt;      /* Row count */
    uint32_t                       m_tupleCnt;    /* Tuple count */
    uint32_t                       m_relfilenode; /* Physical file ID */
    uint32_t                       m_relid;       /* Table OID */
    pg_parser_translog_tbcol_rows* m_rows;
    pg_parser_translog_tuplecache* m_tuple; /* Tuple update */
} pg_parser_translog_tbcol_nvalues;

/*-------------------   Column parse interface     end -----------------------*/

/*-------------------   DDL parse interface begin -----------------------*/

/*-------------------      DDL parse input parameters    -----------------------*/
typedef struct PG_PARSER_TRANSLOG_SYSTB2DLL_RECORD
{
    pg_parser_translog_tbcol_values*            m_record; /* Pointer after secondary parse */
    struct PG_PARSER_TRANSLOG_SYSTB2DLL_RECORD* m_next;
} pg_parser_translog_systb2dll_record;

typedef struct PG_PARSER_TRANSLOG_SYSTB2DDL
{
    int8_t                               m_debugLevel; /* Debug information level */
    int16_t                              m_dbtype;     /* Database type */
    char*                                m_dbversion;  /* Database version */
    pg_parser_translog_systb2dll_record* m_record;
    pg_parser_translog_convertinfo*      m_convert;
} pg_parser_translog_systb2ddl;

/* DDL type base */
typedef struct PG_PARSER_TRANSLOG_DDLSTMTBASE
{
    uint8_t m_ddltype; /* Used to identify create/alter/drop etc. */
    uint8_t m_ddlinfo; /* Used to identify specific DDL statements, create table/alter table/drop
                          table etc. */
} pg_parser_translog_ddltstmtbase;

#define PG_PARSER_DDL_COLUMN_NOTNULL (uint8_t)0x01

#define PG_PARSER_NODETYPE_VAR       (uint8_t)0X01
#define PG_PARSER_NODETYPE_CONST     (uint8_t)0X02
#define PG_PARSER_NODETYPE_FUNC      (uint8_t)0X03
#define PG_PARSER_NODETYPE_OP        (uint8_t)0X04
#define PG_PARSER_NODETYPE_CHAR      (uint8_t)0X05
#define PG_PARSER_NODETYPE_TYPE      (uint8_t)0X06
#define PG_PARSER_NODETYPE_SEPARATOR (uint8_t)0X07
#define PG_PARSER_NODETYPE_BOOL      (uint8_t)0X08

typedef struct pg_parser_nodetree
{
    uint8_t                    m_node_type;
    void*                      m_node;
    struct pg_parser_nodetree* m_next;
} pg_parser_nodetree;

typedef struct pg_parser_node_var
{
    uint16_t m_attno;
} pg_parser_node_var;

typedef struct pg_parser_node_const
{
    uint32_t m_typid;
    char*    m_char_value;
} pg_parser_node_const;

typedef struct pg_parser_node_func
{
    uint16_t m_argnum;
    uint32_t m_funcid;
    char*    m_funcname;
} pg_parser_node_func;

typedef struct pg_parser_node_op
{
    uint32_t m_opid;
    char*    m_opname;
} pg_parser_node_op;

typedef struct pg_parser_node_type
{
    uint32_t m_typeid;
} pg_parser_node_type;

#define PG_PARSER_BOOLEXPR_AND (uint8_t)0X01
#define PG_PARSER_BOOLEXPR_OR  (uint8_t)0X02
#define PG_PARSER_BOOLEXPR_NOT (uint8_t)0X03

typedef struct pg_parser_node_bool
{
    uint8_t m_booltype;
} pg_parser_node_bool;

/* Column definition in table */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_COL
{
    uint8_t             m_flag; /* notnull, hasdefault */
    uint32_t            m_coltypid;
    int32_t             m_length;
    int32_t             m_precision;
    int32_t             m_scale;
    int32_t             m_typemod;
    char*               m_colname;
    pg_parser_nodetree* m_default;
} pg_parser_translog_ddlstmt_col;

typedef struct PG_PARSER_OPTION
{
    char*                    m_key;
    char*                    m_value;
    struct PG_PARSER_OPTION* m_next;
} pg_parser_option;

/*----------- ddlstmt  create table begin---------------*/

#define PG_PARSER_DDL_PARTITION_TABLE_HASH  (uint8_t)0x01
#define PG_PARSER_DDL_PARTITION_TABLE_LIST  (uint8_t)0x02
#define PG_PARSER_DDL_PARTITION_TABLE_RANGE (uint8_t)0x03

/* Partition table */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_CREATETABLE_PARTITIONBY
{
    uint8_t             m_partition_type;
    uint16_t            m_column_num;
    uint16_t*           m_column;  /*
                                    *Here stores an array, sequentially pointing to columns used as partitions,
                                    *starting from 1            If 0, it means the corresponding partition key
                                    *column is            an expression rather than a simple column reference
                                    *Therefore,            the            corresponding expression needs to be
                                    *obtained            from m_colnode
                                    */
    pg_parser_nodetree* m_colnode; /* Store expression */
} pg_parser_translog_ddlstmt_createtable_partitionby;

typedef struct PG_PARSER_TRANSLOG_DDLSTMT_CREATETABLE_PARTITIONSUB
{
    uint32_t            m_partitionof_table_oid;
    pg_parser_nodetree* m_partitionof_node;

} pg_parser_translog_ddlstmt_createtable_partitionsub;

/* Default value */

typedef struct PG_PARSER_TRANSLOG_DDLSTMT_DEFAULT
{
    bool                m_att_default;
    uint32_t            m_relid;        /* Table OID */
    char*               m_colname;      /* Column name */
    pg_parser_nodetree* m_default_node; /* Default value definition */
} pg_parser_translog_ddlstmt_default;

#define PG_PARSER_DDL_CONSTRAINT_PRIMARYKEY (uint8_t)0x00
#define PG_PARSER_DDL_CONSTRAINT_FOREIGNKEY (uint8_t)0x01
#define PG_PARSER_DDL_CONSTRAINT_UNIQUE     (uint8_t)0x02
#define PG_PARSER_DDL_CONSTRAINT_CHECK      (uint8_t)0x03

/* Primary key constraint */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_TBCONSTRAINT_KEY
{
    uint16_t                        m_colcnt;  /* Column count */
    pg_parser_translog_ddlstmt_col* m_concols; /* Column structure */
} pg_parser_translog_ddlstmt_tbconstraint_key;

/* Foreign key constraint */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_TBCONSTRAINT_FKEY
{
    uint16_t  m_colcnt;            /* Column count */
    uint32_t  m_consfkeyid;        /* Table OID that the foreign key depends on */
    uint16_t* m_concols_position;  /* Array of columns in this table that are foreign keys */
    uint16_t* m_fkeycols_position; /* Array of columns in the foreign key table's primary key */
} pg_parser_translog_ddlstmt_tbconstraint_fkey;

/* Constraint */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_TBCONSTRAINT
{
    uint8_t  m_type;            /* Constraint type, pkey 1, fkey 2, check 3, unique 4 */
    uint32_t m_consnspoid;      /* namespace oid */
    uint32_t m_relid;           /* Table OID */
    char*    m_consname;        /* Constraint name */
    void*    m_constraint_stmt; /* Constraint structure */
} pg_parser_translog_ddlstmt_tbconstraint;

/* Check constraint */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_TBCONSTRAINT_CHECK
{
    pg_parser_nodetree* m_check_node;
} pg_parser_translog_ddlstmt_tbconstraint_check;

#define PG_PARSER_DDL_INDEX_UNIQUE (uint8_t)0x01

/* Index */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_INDEX
{
    uint8_t                         m_option;
    uint16_t                        m_colcnt;
    uint16_t*                       m_column; /*
                                               *Here stores an array, sequentially pointing to columns used as partitions,
                                               *starting from 1 If 0, it means the corresponding partition key column is
                                               *an expression rather than a simple column reference Therefore, the
                                               *corresponding expression needs to be obtained from m_colnode
                                               */
    uint32_t                        m_indtype;
    uint32_t                        m_indnspoid;
    uint32_t                        m_relid;
    char*                           m_indname;
    /* Column order is also the column order in multi-column index */
    pg_parser_translog_ddlstmt_col* m_includecols;
    pg_parser_nodetree*             m_colnode; /* Store expression */
} pg_parser_translog_ddlstmt_index;

#define PG_PARSER_DDL_TABLE_TYPE_NORMAL         (uint8_t)0x01
#define PG_PARSER_DDL_TABLE_TYPE_PARTITION      (uint8_t)0x02
#define PG_PARSER_DDL_TABLE_TYPE_PARTITION_SUB  (uint8_t)0x03
#define PG_PARSER_DDL_TABLE_TYPE_PARTITION_BOTH (uint8_t)0x04

#define PG_PARSER_DDL_TABLE_LOG_LOGGED          (uint8_t)0x00
#define PG_PARSER_DDL_TABLE_LOG_TEMP            (uint8_t)0x01
#define PG_PARSER_DDL_TABLE_LOG_UNLOGGED        (uint8_t)0x02

#define PG_PARSER_DDL_TABLE_FLAG_NORMAL         (uint8_t)0x00
#define PG_PARSER_DDL_TABLE_FLAG_EMPTY          (uint8_t)0x01

/* create table main statement */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_CREATETABLE
{
    /* todo Other identifiers for table creation can be placed here */
    uint8_t                                              m_tabletype;
    uint8_t                                              m_logtype;
    uint8_t                                              m_tableflag;
    uint16_t                                             m_colcnt;
    uint32_t                                             m_relid;
    uint32_t                                             m_nspoid;
    uint32_t                                             m_inherits_cnt;
    uint32_t*                                            m_inherits;
    uint32_t                                             m_owner;
    char*                                                m_tabname;
    pg_parser_translog_ddlstmt_col*                      m_cols;
    pg_parser_translog_ddlstmt_createtable_partitionby*  m_partitionby;
    pg_parser_translog_ddlstmt_createtable_partitionsub* m_partitionof;
} pg_parser_translog_ddlstmt_createtable;

/* drop table and truncate table */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_DROP_BASE
{
    uint32_t m_namespace_oid;
    uint32_t m_relid;
    char*    m_name;
} pg_parser_translog_ddlstmt_drop_base;

typedef struct PG_PARSER_TRANSLOG_DDLSTMT_DROP_CONSTRAINT
{
    bool     m_islocal;
    uint32_t m_namespace_oid;
    uint32_t m_relid;
    char*    m_consname;
} pg_parser_translog_ddlstmt_drop_constraint;

/* Character type return value */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_VALUEBASE
{
    uint32_t m_valuelen;
    uint32_t m_owner;
    char*    m_value;
} pg_parser_translog_ddlstmt_valuebase;

/* alter table */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_ALTERTABLE
{
    uint32_t m_relid;
    uint32_t m_relnamespaceid_new; /* Table's new namespace OID */
    uint32_t m_relnamespaceid;     /* Table's namespace OID */
    char*    m_relname_new;        /* Table's new name */
    char*    m_relname;            /* Table name */
} pg_parser_translog_ddlstmt_altertable;

/* alter table add column */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_ADDCOLUMN
{
    uint32_t                        m_relid;
    uint32_t                        m_relnamespace;
    char*                           m_relname;
    pg_parser_translog_ddlstmt_col* m_addcolumn;
} pg_parser_translog_ddlstmt_addcolumn;

typedef struct PG_PARSER_TRANSLOG_DDLSTMT_SETLOG
{
    uint32_t m_relid;
    uint32_t m_relnamespace;
    char*    m_relname;
} pg_parser_translog_ddlstmt_setlog;

/* alter table alter column */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_ALTERCOLUMN
{
    bool     m_notnull;   /* Whether NOT NULL */
    uint32_t m_type;      /* Column type OID */
    uint32_t m_type_new;  /* New column type OID */
    uint32_t m_relid;     /* Table OID */
    uint32_t m_relnspid;  /* Table's namespace OID */
    int32_t  m_length;    /* Type length */
    int32_t  m_precision; /* Type precision */
    int32_t  m_scale;     /* Type scale */
    int32_t  m_typemod;
    char*    m_relname;     /* Table name */
    char*    m_colname_new; /* Table's new name */
    char*    m_colname;     /* Column name */
} pg_parser_translog_ddlstmt_altercolumn;

/* type DDL begin*/
#define PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_COMPOSITE (uint8_t)0X01
#define PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_ENUM      (uint8_t)0X02
#define PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_RANGE     (uint8_t)0X03
#define PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_DOMAIN    (uint8_t)0X04
#define PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_NULL      (uint8_t)0X05

/* ddl create composite type, range type definition */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_TYPRANGE
{
    uint32_t m_subtype;         /* Type OID */
    uint32_t m_subtype_opclass; /* Operator class OID */
    uint32_t m_collation;       /* Collation OID for range comparison */
} pg_parser_translog_ddlstmt_typrange;

/* ddl create composite type, sub-type definition in composite type */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_TYPCOL
{
    uint32_t m_coltypid; /* Type OID */
    char*    m_colname;  /* Sub-type column name */
} pg_parser_translog_ddlstmt_typcol;

/* DDL type main */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_TYPE
{
    uint8_t  m_typtype;   /* Type (composite 1, enum 2, range 3, domain 4, null 5) */
    uint16_t m_typvalcnt; /* Sub-type count (composite), enum value count (enum), 1 for others */
    uint32_t m_typnspid;  /* Type's namespace OID */
    uint32_t m_owner;
    char*    m_type_name; /* Type name */
    void*    m_typptr;    /* Type sub-structure (composite, enum, range, domain) */
} pg_parser_translog_ddlstmt_type;

/* type DDL end*/

/* sequence */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT_SEQUENCE
{
    bool     m_seqcycle;     /* Whether cycle */
    uint32_t m_seqnspid;     /* namespace OID */
    uint32_t m_seqtypid;     /* Sequence type OID (int2, int4, int8) */
    uint64_t m_seqincrement; /* Sequence increment value */
    uint64_t m_seqmax;       /* Sequence maximum value */
    uint64_t m_seqmin;       /* Sequence minimum value */
    uint64_t m_seqstart;     /* Sequence start value */
    uint64_t m_seqcache;     /* Sequence cache size */
    char*    m_seqname;      /* Sequence name */
} pg_parser_translog_ddlstmt_sequence;

/* Generic structure */
typedef struct PG_PARSER_TRANSLOG_DDLSTMT
{
    pg_parser_translog_ddltstmtbase    m_base;
    void*                              m_ddlstmt;
    struct PG_PARSER_TRANSLOG_DDLSTMT* m_next;
} pg_parser_translog_ddlstmt;

/*-------------------   DDL parse interface   end -----------------------*/

typedef struct pg_parser_RelFileNode
{
    uint32_t spcNode; /* tablespace */
    uint32_t dbNode;  /* database */
    uint32_t relNode; /* relation */
} pg_parser_RelFileNode;

extern bool pg_parser_trans_preTrans(pg_parser_translog_pre*       pg_parser_pre_data,
                                     pg_parser_translog_pre_base** pg_parser_result,
                                     int32_t*                      pg_parser_errno);
extern bool pg_parser_trans_TransRecord(pg_parser_translog_translog2col* pg_parser_transData,
                                        pg_parser_translog_tbcolbase**   pg_parser_trans_result,
                                        int32_t*                         pg_parser_errno);
extern bool pg_parser_trans_TransRecord_GetTuple(pg_parser_translog_translog2col* pg_parser_transData,
                                                 pg_parser_translog_tbcolbase**   pg_parser_trans_result,
                                                 int32_t*                         pg_parser_errno);
extern bool pg_parser_trans_DDLtrans(pg_parser_translog_systb2ddl* pg_parser_ddl,
                                     pg_parser_translog_ddlstmt**  pg_parser_ddl_result,
                                     int32_t*                      pg_parser_errno);
extern bool pg_parser_trans_external_trans(pg_parser_translog_external*     pg_parser_exdata,
                                           pg_parser_translog_tbcol_value** pg_parser_trans_result,
                                           int32_t*                         pg_parser_errno);
extern void pg_parser_trans_preTrans_free(pg_parser_translog_pre_base* pg_parser_result);
extern void pg_parser_trans_TransRecord_free(pg_parser_translog_translog2col* pg_parser_trans_pre,
                                             pg_parser_translog_tbcolbase*    pg_parser_trans);
extern void pg_parser_trans_TransRecord_Minsert_free(pg_parser_translog_translog2col* pg_parser_trans_pre,
                                                     pg_parser_translog_tbcolbase*    pg_parser_trans);
extern void pg_parser_trans_external_free(pg_parser_translog_external* ext, pg_parser_translog_tbcol_value* result);
extern void pg_parser_trans_ddl_free(pg_parser_translog_systb2ddl* ddl, pg_parser_translog_ddlstmt* result);

extern bool pg_parser_trans_matchmissing(pg_parser_translog_tbcol_value* value1,
                                         pg_parser_translog_tbcol_value* value2,
                                         uint16_t                        valuecnt);

extern void pg_parser_free_value_ext(pg_parser_translog_tbcol_value* value);
#endif
