#ifndef XK_PG_PARSER_TRANSLOG_H
#define XK_PG_PARSER_TRANSLOG_H

#include "common/xk_pg_parser_sysdict.h"

/* WAL日志级别定义 */
#define XK_PG_PARSER_WALLEVEL_REPLICA 0
#define XK_PG_PARSER_WALLEVEL_LOGICAL 1

/* OriginId define */
#define XK_PG_PARSER_TRANSLOG_InvalidRepOriginId 0

/* 预解析接口返回类型定义 */
#define XK_PG_PARSER_TRANSLOG_INVALID               (uint8_t) 0x00
#define XK_PG_PARSER_TRANSLOG_HEAP_INSERT           (uint8_t) 0x01
#define XK_PG_PARSER_TRANSLOG_HEAP_UPDATE           (uint8_t) 0x02
#define XK_PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE       (uint8_t) 0x03
#define XK_PG_PARSER_TRANSLOG_HEAP_DELETE           (uint8_t) 0x04
#define XK_PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT    (uint8_t) 0x05
#define XK_PG_PARSER_TRANSLOG_XACT_COMMIT           (uint8_t) 0x06
#define XK_PG_PARSER_TRANSLOG_XACT_ABORT            (uint8_t) 0x07
#define XK_PG_PARSER_TRANSLOG_XLOG_SWITCH           (uint8_t) 0x08
#define XK_PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE       (uint8_t) 0x09
#define XK_PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN     (uint8_t) 0x0A
#define XK_PG_PARSER_TRANSLOG_FPW_TUPLE             (uint8_t) 0x0B
#define XK_PG_PARSER_TRANSLOG_RELMAP                (uint8_t) 0x0C
#define XK_PG_PARSER_TRANSLOG_RUNNING_XACTS         (uint8_t) 0x0D
#define XK_PG_PARSER_TRANSLOG_XLOG_RECOVERY         (uint8_t) 0x0E
#define XK_PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE   (uint8_t) 0x0F
#define XK_PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE    (uint8_t) 0x10
#define XK_PG_PARSER_TRANSLOG_XACT_ASSIGNMENT       (uint8_t) 0x11
#define XK_PG_PARSER_TRANSLOG_XACT_PREPARE          (uint8_t) 0x12
#define XK_PG_PARSER_TRANSLOG_HEAP_TRUNCATE         (uint8_t) 0x13
#define XK_PG_PARSER_TRANSLOG_SEQ                   (uint8_t) 0x14
#if 0
#define XK_PG_PARSER_TRANSLOG_HEAP_INPLACE          (uint8_t) 0x14
#define XK_PG_PARSER_TRANSLOG_HEAP_CONFIRM          (uint8_t) 0x15
#endif


/* 二次解析type定义 */
#define XK_PG_PARSER_TRANSLOG_RETURN_INVALID        (uint8_t) 0x00
#define XK_PG_PARSER_TRANSLOG_RETURN_WITH_DATA      (uint8_t) 0x01
#define XK_PG_PARSER_TRANSLOG_RETURN_WITH_TUPLE     (uint8_t) 0x02

/* 二次解析DML type定义 */
#define XK_PG_PARSER_TRANSLOG_DMLTYPE_INVALID       (uint8_t) 0x00
#define XK_PG_PARSER_TRANSLOG_DMLTYPE_INSERT        (uint8_t) 0x01
#define XK_PG_PARSER_TRANSLOG_DMLTYPE_DELETE        (uint8_t) 0x02
#define XK_PG_PARSER_TRANSLOG_DMLTYPE_UPDATE        (uint8_t) 0x03
#define XK_PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT   (uint8_t) 0x04

/* 二次解析DML table type定义 */
#define XK_PG_PARSER_TRANSLOG_TABLETYPE_NORMAL      (uint8_t) 0x00
#define XK_PG_PARSER_TRANSLOG_TABLETYPE_SYS         (uint8_t) 0x01
#define XK_PG_PARSER_TRANSLOG_TABLETYPE_DICT        (uint8_t) 0x02

/* DDL type 定义 */
#define XK_PG_PARSER_DDLTYPE_CREATE                 (uint8_t) 0x01
#define XK_PG_PARSER_DDLTYPE_ALTER                  (uint8_t) 0x02
#define XK_PG_PARSER_DDLTYPE_DROP                   (uint8_t) 0x03
#define XK_PG_PARSER_DDLTYPE_SPECIAL                (uint8_t) 0x04

/* DDL info 定义 */

/* DDL create语句 info 定义 */
#define XK_PG_PARSER_DDLINFO_CREATE_TABLE           (uint8_t) 0x01
#define XK_PG_PARSER_DDLINFO_CREATE_NAMESPACE       (uint8_t) 0x02
#define XK_PG_PARSER_DDLINFO_CREATE_DATABASE        (uint8_t) 0x03
#define XK_PG_PARSER_DDLINFO_CREATE_INDEX           (uint8_t) 0x04
#define XK_PG_PARSER_DDLINFO_CREATE_SEQUENCE        (uint8_t) 0x05
#define XK_PG_PARSER_DDLINFO_CREATE_VIEW            (uint8_t) 0x06
#define XK_PG_PARSER_DDLINFO_CREATE_FUNCTION        (uint8_t) 0x07
#define XK_PG_PARSER_DDLINFO_CREATE_TRIGGER         (uint8_t) 0x08
#define XK_PG_PARSER_DDLINFO_CREATE_TYPE            (uint8_t) 0x09

/* DDL alter语句 info 定义 */
#define XK_PG_PARSER_DDLINFO_ALTER_COLUMN_RENAME         (uint8_t) 0x01
#define XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NOTNULL        (uint8_t) 0x02
#define XK_PG_PARSER_DDLINFO_ALTER_COLUMN_NULL           (uint8_t) 0x03
#define XK_PG_PARSER_DDLINFO_ALTER_COLUMN_TYPE           (uint8_t) 0x04
#define XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DEFAULT        (uint8_t) 0x05
#define XK_PG_PARSER_DDLINFO_ALTER_COLUMN_DROP_DEFAULT   (uint8_t) 0x06
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_ADD_COLUMN      (uint8_t) 0x07
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_RENAME          (uint8_t) 0x08
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_COLUMN     (uint8_t) 0x09
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_ADD_CONSTRAINT  (uint8_t) 0x0A
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_DROP_CONSTRAINT (uint8_t) 0x0B
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_NAMESPACE       (uint8_t) 0x0C
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_SET_LOGGED      (uint8_t) 0x0D
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_SET_UNLOGGED    (uint8_t) 0x0E
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_OWNER           (uint8_t) 0x0F
#define XK_PG_PARSER_DDLINFO_ALTER_SEQ_RESTART           (uint8_t) 0x10
#define XK_PG_PARSER_DDLINFO_ALTER_TABLE_REPLICA_IDENTIFITY (uint8_t) 0x11

/* DDL drop语句 info 定义 */
#define XK_PG_PARSER_DDLINFO_DROP_NAMESPACE             (uint8_t) 0x01
#define XK_PG_PARSER_DDLINFO_DROP_DATABASE              (uint8_t) 0x02
#define XK_PG_PARSER_DDLINFO_DROP_VIEW                  (uint8_t) 0x03
#define XK_PG_PARSER_DDLINFO_DROP_TABLE                 (uint8_t) 0x04
#define XK_PG_PARSER_DDLINFO_DROP_INDEX                 (uint8_t) 0x05
#define XK_PG_PARSER_DDLINFO_DROP_SEQUENCE              (uint8_t) 0x06
#define XK_PG_PARSER_DDLINFO_DROP_FUNCTION              (uint8_t) 0x07
#define XK_PG_PARSER_DDLINFO_DROP_TRIGGER               (uint8_t) 0x08
#define XK_PG_PARSER_DDLINFO_DROP_TYPE                  (uint8_t) 0x09

/* DDL 特殊语句 info 定义 */
#define XK_PG_PARSER_DDLINFO_TRUNCATE                   (uint8_t) 0x01
#define XK_PG_PARSER_DDLINFO_REINDEX                    (uint8_t) 0x02

/* 页大小定义 */
#define XK_PG_PARSER_PAGESIZE_8K                        (uint32_t) 8192
#define XK_PG_PARSER_PAGESIZE_16K                       (uint32_t) 16384
#define XK_PG_PARSER_PAGESIZE_32K                       (uint32_t) 32786
#define XK_PG_PARSER_PAGESIZE_64K                       (uint32_t) 65536
typedef enum XK_PG_PARSER_TRANSLOG_COLINFO
{
    INFO_NOTHING = 0x00,
    INFO_COL_IS_NULL = 0x01,        /* insert时的空值标记 */
    INFO_COL_MAY_NULL = 0x02,       /* update和delete取旧数据时, 在WAL中未记录的数据 */
    INFO_COL_IS_TOAST = 0x03,       /* 行外存储 */
    INFO_COL_IS_CUSTOM = 0x04,      /* 自定义类型 */
    INFO_COL_IS_ARRAY = 0x05,       /* ARRAY类型 */
    INFO_COL_IS_BYTEA = 0x06,       /* 列为二进制数据 */
    INFO_COL_IS_DROPED = 0x07,      /* 列被drop */
    INFO_COL_IS_NODE = 0x08         /* 列为二进制数据 */
} xk_pg_parser_translog_colinfo;

#ifndef xk_database_type_define
typedef enum XK_PG_PARSER_DATABASE_TYPE
{
    XK_DATABASE_TYPE_NOP = 0x00,
    XK_DATABASE_TYPE_POSTGRESQL
} xk_pg_parser_database_type;

#define XK_DATABASE_PG127    "pg127"
#define XK_DATABASE_PG1410   "pg1410"

#define xk_database_type_define 1

#endif
/*-------------------   预解析接口入参  begin --------------------*/
typedef struct XK_PG_PARSER_TRANSLOG_PRE
{
    int8_t                       m_walLevel;            /* wal日志级别 */
    int8_t                       m_debugLevel;         /* debug信息级别 */
    int16_t                      m_dbtype;             /* 数据库类型 */
    uint32_t                     m_pagesize;           /* 页大小 */
    char                         *m_dbversion;         /* 数据库版本 */
    uint8_t                      *m_record;            /* record */
} xk_pg_parser_translog_pre;
/*-------------------   预解析接口入参  end --------------------*/

/*-------------------   预解析接口出参  begin --------------------*/
typedef struct XK_PG_PARSER_TRANSLOG_PRE_BASE
{
    uint8_t                      m_type;               /* record类型 */
    uint32_t                     m_xid;                /* 事务id */
    uint16_t                     m_originid;
} xk_pg_parser_translog_pre_base;

/* 事务状态, COMMIT, ABORT, COMMIT PREPARE, ABORT PREPARE */
typedef struct XK_PG_PARSER_TRANSLOG_PRE_TRANS
{
    xk_pg_parser_translog_pre_base m_base;
    uint8_t                        m_status;        /* 0x01 abort, 0x02 commit */
    int64                          m_time;
    void                          *m_transdata;     /* xl_xact_parsed_commit */
} xk_pg_parser_translog_pre_trans;

typedef struct XK_PG_PARSER_TRANSLOG_PRE_ASSIGNMENT
{
    xk_pg_parser_translog_pre_base m_base;
    void                          *m_assignment;
} xk_pg_parser_translog_pre_assignment;

/* heap */
typedef struct XK_PG_PARSER_TRANSLOG_PRE_HEAP
{
    xk_pg_parser_translog_pre_base m_base;
    uint8_t                        m_needtuple;        /* 是否需要页 */
    uint32_t                       m_tuplecnts;        /* tuple的数量 */
    uint32_t                       m_tupitemoff;       /* tuple的item的偏移 */
    uint32_t                       m_transid;          /* 事务id */
    uint32_t                       m_relfilenode;      /* 物理文件id */
    uint32_t                       m_dboid;            /* 数据库oid */
    uint32_t                       m_tbspcoid;         /* tablespace oid */
    uint32_t                       m_pagenos;          /* 页号 */
} xk_pg_parser_translog_pre_heap;

typedef struct XK_PG_PARSER_TRANSLOG_PRE_HEAP_TRUNCATE
{
    xk_pg_parser_translog_pre_base m_base;
    bool            cascade;
    bool            reseq;
    uint32_t        dbid;
    uint32_t        nrelids;
    uint32_t       *relids;
} xk_pg_parser_translog_pre_heap_truncate;

#if 0
typedef struct XK_PG_PARSER_TRANSLOG_PAGE
{
    uint32_t                    m_relfilenode;      /* 物理文件id */
    uint32_t                    m_pageno;           /* 页号 */
    uint8_t*                    m_page;             /* 页数据 */
} xk_pg_parser_translog_page;
#endif

/* 列解析出参保存tuple更新信息的结构体 */
typedef struct XK_PG_PARSER_TRANSLOG_TUPLECACHE
{
    uint32_t                          m_tuplelen;               /* tuple长度 */
    uint32_t                          m_itemoffnum;             /* tuple itemid偏移 */
    uint32_t                          m_pageno;                 /* 页号 */
    uint8_t*                          m_tupledata;              /* tuple数据 */
} xk_pg_parser_translog_tuplecache;

/* 预解析时当非解析类型存于全页写时, 返回全页写 */
typedef struct XK_PG_PARSER_TRANSLOG_PRE_IMAGE_TUPLE
{
    xk_pg_parser_translog_pre_base     m_base;
    uint32_t                           m_relfilenode;       /* 物理文件id */
    uint32_t                           m_dboid;
    uint32_t                           m_tbspcoid;
    uint32_t                           m_tuplecnt;          /* tuple的数量 */
    uint32_t                           m_transid;
    xk_pg_parser_translog_tuplecache  *m_tuples;
} xk_pg_parser_translog_pre_image_tuple;

/* checkpoint */
typedef struct XK_PG_PARSER_TRANSLOG_PRE_TRANSCHKP
{
    xk_pg_parser_translog_pre_base m_base;
    uint32_t                       m_this_timeline;
    uint32_t                       m_prev_timeline;
    uint64_t                       m_nextid; /* 下一个事务id */
    uint64_t                       m_redo_lsn; /* redo开始的lsn, 在此lsn之后(包括该lsn)才会刷新全页写 */
} xk_pg_parser_translog_pre_transchkp;

typedef struct XK_PG_PARSER_TRANSLOG_PRE_ENDRECOVERY
{
    xk_pg_parser_translog_pre_base m_base;
    uint32_t                       m_this_timeline;
    uint32_t                       m_prev_timeline;
} xk_pg_parser_translog_pre_endrecovery;

/* switch */
typedef struct XK_PG_PARSER_TRANSLOG_PRE_SWITCH
{
    xk_pg_parser_translog_pre_base m_base;
} xk_pg_parser_translog_pre_switch;

typedef struct XK_PG_PARSER_TRANSLOG_PRE_RELMAP
{
    xk_pg_parser_translog_pre_base m_base;
    uint64_t                       m_dboid;
    uint16_t                       m_count;
    void                          *m_mapping;
} xk_pg_parser_translog_pre_relmap;

typedef struct XK_PG_PARSER_TRANSLOG_PRE_RUNNING_XACT
{
    xk_pg_parser_translog_pre_base m_base;
    void                          *m_standby;
} xk_pg_parser_translog_pre_running_xact;

/* seq */
typedef struct XK_PG_PARSER_TRANSLOG_PRE_SEQ
{
    xk_pg_parser_translog_pre_base m_base;
    uint32_t    m_dboid;
    uint32_t    m_tbspcoid;
    uint32_t    m_relfilenode;
    int64_t     m_last_value;
} xk_pg_parser_translog_pre_seq;

/*-------------------   预解析接口出参    end --------------------*/



/*-------------------   数据字典结构 begin -----------------------*/

typedef struct XK_PG_PARSER_SYSDICTS
{
    xk_pg_parser_sysdict_pgclass_dict              m_pg_class;
    xk_pg_parser_sysdict_pgattributes_dict         m_pg_attribute;
    xk_pg_parser_sysdict_pgnamespace_dict          m_pg_namespace;
    xk_pg_parser_sysdict_pgtype_dict               m_pg_type;
    xk_pg_parser_sysdict_pgrange_dict              m_pg_range;
    xk_pg_parser_sysdict_pgenum_dict               m_pg_enum;
    xk_pg_parser_sysdict_pgproc_dict               m_pg_proc;
} xk_pg_parser_sysdicts;

/*-------------------   数据字典结构   end -----------------------*/


/*-------------------   列解析接口     begin -----------------------*/

typedef struct XK_PG_PARSER_TRANSLOG_CONVERTINFO
{
    char *m_tzname;             /* 源数据库的时区名 */
    char *m_monetary;           /* 源数据库的lc_monetary */
    char *m_numeric;            /* 源数据库的lc_numeric */
    char *m_dbcharset;          /* 数据库编码 */
    char *m_tartgetcharset;     /* 目标编码 */
}xk_pg_parser_translog_convertinfo;

typedef struct XK_PG_PARSER_TRANSLOG_CONVERTINFO_WITH_ZIC
{
    bool     istoast;
    uint8_t  debuglevel;
    int32_t  ziclen;
    char    *zicdata;
    char    *dbversion;
    int16_t  dbtype;
    int32_t *errorno;
    xk_pg_parser_translog_convertinfo *convertinfo;
}xk_pg_parser_translog_convertinfo_with_zic;


/* 列解析接口入参 */
typedef struct XK_PG_PARSER_TRANSLOG_TRANSLOG2COL
{
    uint8_t                            m_iscatalog;          /* 是否是系统表 */
    uint8_t                            m_walLevel;           /* wal级别 */
    uint8_t                            m_debugLevel;         /* debug信息级别 */
    int16_t                            m_dbtype;             /* 数据库类型 */
    uint32_t                           m_pagesize;           /* 页大小 */
    uint32_t                           m_tuplecnt;           /* tuple数量 */
    char                              *m_dbversion;          /* 数据库版本 */
    uint8_t*                           m_record;             /* record数据 */
    xk_pg_parser_translog_convertinfo* m_convert;            /* 保存有转换信息 */
    xk_pg_parser_translog_tuplecache*  m_tuples;             /* tuple数据 */
    xk_pg_parser_sysdicts*             m_sysdicts;           /* 系统表 */
} xk_pg_parser_translog_translog2col;

/* 列解析出参基本元素 */
typedef struct XK_PG_PARSER_TRANSLOG_TBCOLBASE
{
    uint8_t                      m_type;               /* 类型标识 */
    uint8_t                      m_dmltype;            /* dml类型 */
    uint8_t                      m_tabletype;          /* 表类型, 见二次解析DML table type定义 */
    uint16_t                     m_originid;           /* originid */
    char*                        m_schemaname;         /* 模式名    */
    char*                        m_tbname;             /* 表名      */
} xk_pg_parser_translog_tbcolbase;

/* 对应的 xk_pg_parser_translog_tbcol_value->m_value 使用的类型 */
typedef struct XK_PG_PARSER_TRANSLOG_TBCOL_VALUETYPE_EXTERNAL
{
    int32_t                      m_rawsize;               /* 原数据大小 (包括头部) */
    int32_t                      m_extsize;               /* 行外存储大小 */
    uint32_t                     m_valueid;               /* toast表中对应的数据记录id */
    uint32_t                     m_toastrelid;            /* toast表的oid */
} xk_pg_parser_translog_tbcol_valuetype_external;

/* 列解析出参列值结构体 */
typedef struct XK_PG_PARSER_TRANSLOG_TBCOL_VALUE
{
    bool                         m_freeFlag;           /* 释放colName的标记 */
    uint8_t                      m_info;               /* 比较xk_pg_parser_translog_colinfo */
    uint32_t                     m_coltype;            /* 列类型oid */
    uint32_t                     m_valueLen;           /* 数据长度 */
    char*                        m_colName;            /* 列名称 */

    void*                        m_value;              /* 列数据，根据m_info, m_coltype转换结构
                                                        * 在m_info设置了INFO_COL_IS_TOAST时
                                                        * 存放行外存储结构体
                                                        * xk_pg_parser_translog_tbcol_valuetype_external
                                                        */
} xk_pg_parser_translog_tbcol_value;

/* 行外存储二次调用解析接口 */
typedef struct XK_PG_PARSER_TRANSLOG_EXTERNAL
{
    uint8_t                            m_debuglevel;       /* debug等级 */
    int16_t                            m_dbtype;           /* 数据库类型 */
    uint32_t                           m_typeid;           /* 类型oid */
    uint32_t                           m_datalen;          /* 数据长度 */
    char                              *m_dbversion;        /* 数据库版本 */
    char                              *m_colName;          /* 列名称 */
    char                              *m_typout;           /* 输出函数 */
    xk_pg_parser_translog_convertinfo *m_convertInfo;      /* 列类型oid */
    char                              *m_chunkdata;        /* 行外存储数据 */
} xk_pg_parser_translog_external;

/* 自定义列 */
typedef struct XK_PG_PARSER_TRANSLOG_TBCOL_VALUETYPE_CUSTOMER
{
    xk_pg_parser_translog_tbcol_value*        m_value;                 /* 列值 */

    /* next */
    struct XK_PG_PARSER_TRANSLOG_TBCOL_VALUETYPE_CUSTOMER* m_next;     /* 相临列值 */
} xk_pg_parser_translog_tbcol_valuetype_customer;

/* 列解析出参 */
typedef struct XK_PG_PARSER_TRANSLOG_TBCOL_VALUES
{
    xk_pg_parser_translog_tbcolbase          m_base;
    bool                                     m_haspkey;
    uint16_t                                 m_valueCnt;        /* 列数量 */
    uint32_t                                 m_tupleCnt;        /* tuple数量 */
    uint32_t                                 m_relfilenode;     /* 物理文件id */
    uint32_t                                 m_relid;           /* 表oid */
    xk_pg_parser_translog_tbcol_value*       m_new_values;      /* 新列相关 */
    xk_pg_parser_translog_tbcol_value*       m_old_values;      /* 旧列相关 */
    xk_pg_parser_translog_tuplecache*        m_tuple;           /* tuple更新相关 */
} xk_pg_parser_translog_tbcol_values;

typedef struct XK_PG_PARSER_TRANSLOG_TBCOL_ROWS
{
    xk_pg_parser_translog_tbcol_value*       m_new_values;      /* 新列相关 */
} xk_pg_parser_translog_tbcol_rows;

typedef struct XK_PG_PARSER_TRANSLOG_TBCOL_NVALUES
{
    xk_pg_parser_translog_tbcolbase          m_base;
    bool                                     m_haspkey;
    uint16_t                                 m_valueCnt;        /* 列数量 */
    uint16_t                                 m_rowCnt;          /* 行数量 */
    uint32_t                                 m_tupleCnt;        /* tuple数量 */
    uint32_t                                 m_relfilenode;     /* 物理文件id */
    uint32_t                                 m_relid;           /* 表oid */
    xk_pg_parser_translog_tbcol_rows        *m_rows;
    xk_pg_parser_translog_tuplecache        *m_tuple;           /* tuple更新相关 */
} xk_pg_parser_translog_tbcol_nvalues;

/*-------------------   列解析接口     end -----------------------*/



/*-------------------   DDL解析接口 begin -----------------------*/

/*-------------------      DDL解析入参    -----------------------*/
typedef struct XK_PG_PARSER_TRANSLOG_SYSTB2DLL_RECORD
{
    xk_pg_parser_translog_tbcol_values            *m_record;          /* 二次解析后的指针 */
    struct XK_PG_PARSER_TRANSLOG_SYSTB2DLL_RECORD *m_next;
} xk_pg_parser_translog_systb2dll_record;

/* DDL所需要的系统表 */
#if 0
typedef struct XK_PG_PARSER_TRANSLOG_SYSTB2DLL_SYSTBS
{
    xk_pg_parser_sysdict_pgclass_dict              m_pg_class;    /* todo sysdicts */
    xk_pg_parser_sysdict_pgnamespace_dict          m_pg_namespace;
    xk_pg_parser_sysdict_pgtype_dict               m_pg_type;
} xk_pg_parser_translog_systb2dll_systbs;
#endif

typedef struct XK_PG_PARSER_TRANSLOG_SYSTB2DDL
{
    int8_t                                         m_debugLevel;    /* debug信息级别 */
    int16_t                                        m_dbtype;             /* 数据库类型 */
    char                                          *m_dbversion;          /* 数据库版本 */
    xk_pg_parser_translog_systb2dll_record        *m_record;
    xk_pg_parser_translog_convertinfo             *m_convert;
} xk_pg_parser_translog_systb2ddl;

/* DDL类型基础 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMTBASE
{
    uint8_t                  m_ddltype;                 /* 用于标识 create/alter/drop 等 */
    uint8_t                  m_ddlinfo;                 /* 用于标识 具体的类型DLL 语句, create table/alter table/drop table 等 */
} xk_pg_parser_translog_ddltstmtbase;

#define XK_PG_PARSER_DDL_COLUMN_NOTNULL (uint8_t) 0x01

#define XK_PG_PARSER_NODETYPE_VAR       (uint8_t)0X01
#define XK_PG_PARSER_NODETYPE_CONST     (uint8_t)0X02
#define XK_PG_PARSER_NODETYPE_FUNC      (uint8_t)0X03
#define XK_PG_PARSER_NODETYPE_OP        (uint8_t)0X04
#define XK_PG_PARSER_NODETYPE_CHAR      (uint8_t)0X05
#define XK_PG_PARSER_NODETYPE_TYPE      (uint8_t)0X06
#define XK_PG_PARSER_NODETYPE_SEPARATOR (uint8_t)0X07
#define XK_PG_PARSER_NODETYPE_BOOL      (uint8_t)0X08

typedef struct xk_pg_parser_nodetree
{
    uint8_t         m_node_type;
    void           *m_node;
    struct xk_pg_parser_nodetree *m_next;
}xk_pg_parser_nodetree;

typedef struct xk_pg_parser_node_var
{
    uint16_t m_attno;
}xk_pg_parser_node_var;

typedef struct xk_pg_parser_node_const
{
    uint32_t m_typid;
    char    *m_char_value;
}xk_pg_parser_node_const;

typedef struct xk_pg_parser_node_func
{
    uint16_t m_argnum;
    uint32_t m_funcid;
    char    *m_funcname;
}xk_pg_parser_node_func;

typedef struct xk_pg_parser_node_op
{
    uint32_t m_opid;
    char    *m_opname;
}xk_pg_parser_node_op;

typedef struct xk_pg_parser_node_type
{
    uint32_t m_typeid;
}xk_pg_parser_node_type;

#define XK_PG_PARSER_BOOLEXPR_AND   (uint8_t)0X01
#define XK_PG_PARSER_BOOLEXPR_OR    (uint8_t)0X02
#define XK_PG_PARSER_BOOLEXPR_NOT   (uint8_t)0X03

typedef struct xk_pg_parser_node_bool
{
    uint8_t m_booltype;
}xk_pg_parser_node_bool;

/* table中的列定义 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_COL
{
    uint8_t                   m_flag;                    /* notnull, hasdefault */
    uint32_t                  m_coltypid;
    int32_t                   m_length;
    int32_t                   m_precision;
    int32_t                   m_scale;
    int32_t                   m_typemod;
    char*                     m_colname;
    xk_pg_parser_nodetree    *m_default;
} xk_pg_parser_translog_ddlstmt_col;

typedef struct XK_PG_PARSER_OPTION
{
    char*                        m_key;
    char*                        m_value;
    struct XK_PG_PARSER_OPTION*  m_next;
} xk_pg_parser_option;  

/*----------- ddlstmt  create table begin---------------*/

#define XK_PG_PARSER_DDL_PARTITION_TABLE_HASH  (uint8_t) 0x01
#define XK_PG_PARSER_DDL_PARTITION_TABLE_LIST  (uint8_t) 0x02
#define XK_PG_PARSER_DDL_PARTITION_TABLE_RANGE (uint8_t) 0x03

/* 分区表 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_CREATETABLE_PARTITIONBY
{
    uint8_t   m_partition_type;
    uint16_t  m_column_num;
    uint16_t* m_column;                 /* 
                                         *这里存储一个数组, 顺序地指向作为分区的列, 从1开始
                                         * 如果为0, 表示对应的分区键列是一个表达式而不是简单的列引用
                                         * 因此需要从m_colnode中获取对应的表达式
                                         */
    xk_pg_parser_nodetree *m_colnode;   /* 存储表达式 */
} xk_pg_parser_translog_ddlstmt_createtable_partitionby;

typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_CREATETABLE_PARTITIONSUB
{
    uint32_t               m_partitionof_table_oid;
    xk_pg_parser_nodetree* m_partitionof_node;

} xk_pg_parser_translog_ddlstmt_createtable_partitionsub;


/* 默认值 */

typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_DEFAULT
{
    bool                                        m_att_default;
    uint32_t                                    m_relid;                 /* 表oid */
    char                                       *m_colname;               /* 列名 */
    xk_pg_parser_nodetree                      *m_default_node;          /* 默认值定义 */
} xk_pg_parser_translog_ddlstmt_default;

#define XK_PG_PARSER_DDL_CONSTRAINT_PRIMARYKEY (uint8_t) 0x00
#define XK_PG_PARSER_DDL_CONSTRAINT_FOREIGNKEY (uint8_t) 0x01
#define XK_PG_PARSER_DDL_CONSTRAINT_UNIQUE     (uint8_t) 0x02
#define XK_PG_PARSER_DDL_CONSTRAINT_CHECK      (uint8_t) 0x03

/* 主键约束 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_TBCONSTRAINT_KEY
{
    uint16_t                                    m_colcnt;               /* 列数 */
    xk_pg_parser_translog_ddlstmt_col          *m_concols;              /* 列结构体 */
} xk_pg_parser_translog_ddlstmt_tbconstraint_key;

/* 外键约束 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_TBCONSTRAINT_FKEY
{
    uint16_t                                    m_colcnt;               /* 列数 */
    uint32_t                                    m_consfkeyid;           /* 外键依赖的表oid */
    uint16_t                                   *m_concols_position;     /* 该表作为外键的列的数组 */
    uint16_t                                   *m_fkeycols_position;    /* 外键表的主键的列的数组 */
} xk_pg_parser_translog_ddlstmt_tbconstraint_fkey;

/* 约束 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_TBCONSTRAINT
{
    uint8_t                                     m_type;                 /* 约束的类型, pkey 1, fkey 2, check 3, unique 4 */
    uint32_t                                    m_consnspoid;           /* namespace oid */
    uint32_t                                    m_relid;                /* 表的oid */
    char                                       *m_consname;             /* 约束名 */
    void                                       *m_constraint_stmt;      /* 约束结构体 */
} xk_pg_parser_translog_ddlstmt_tbconstraint;

/* 检查约束 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_TBCONSTRAINT_CHECK
{
    xk_pg_parser_nodetree                      *m_check_node;
} xk_pg_parser_translog_ddlstmt_tbconstraint_check;

#define XK_PG_PARSER_DDL_INDEX_UNIQUE          (uint8_t) 0x01
/* 索引 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_INDEX
{
    uint8_t                                     m_option;
    uint16_t                                    m_colcnt;
    uint16_t                                   *m_column;   /* 
                                                             *这里存储一个数组, 顺序地指向作为分区的列, 从1开始
                                                             * 如果为0, 表示对应的分区键列是一个表达式而不是简单的列引用
                                                             * 因此需要从m_colnode中获取对应的表达式
                                                             */
    uint32_t                                    m_indtype;
    uint32_t                                    m_indnspoid;
    uint32_t                                    m_relid;
    char*                                       m_indname;
    /* 列的先后关系也是多列索引中列的先后关系 */
    xk_pg_parser_translog_ddlstmt_col*          m_includecols;
    xk_pg_parser_nodetree                      *m_colnode;   /* 存储表达式 */
} xk_pg_parser_translog_ddlstmt_index;

#define XK_PG_PARSER_DDL_TABLE_TYPE_NORMAL          (uint8_t) 0x01
#define XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION       (uint8_t) 0x02
#define XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION_SUB   (uint8_t) 0x03
#define XK_PG_PARSER_DDL_TABLE_TYPE_PARTITION_BOTH  (uint8_t) 0x04

#define XK_PG_PARSER_DDL_TABLE_LOG_LOGGED           (uint8_t) 0x00
#define XK_PG_PARSER_DDL_TABLE_LOG_TEMP             (uint8_t) 0x01
#define XK_PG_PARSER_DDL_TABLE_LOG_UNLOGGED         (uint8_t) 0x02

#define XK_PG_PARSER_DDL_TABLE_FLAG_NORMAL          (uint8_t) 0x00
#define XK_PG_PARSER_DDL_TABLE_FLAG_EMPTY           (uint8_t) 0x01

/* create table 主体语句 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_CREATETABLE
{
    /* todo 创建表其它的标识可以放到此处 */
    uint8_t                                                   m_tabletype;
    uint8_t                                                   m_logtype;
    uint8_t                                                   m_tableflag;
    uint16_t                                                  m_colcnt;
    uint32_t                                                  m_relid;
    uint32_t                                                  m_nspoid;
    uint32_t                                                  m_inherits_cnt;
    uint32_t                                                 *m_inherits;
    uint32_t                                                  m_owner;
    char*                                                     m_tabname;
    xk_pg_parser_translog_ddlstmt_col*                        m_cols;
    xk_pg_parser_translog_ddlstmt_createtable_partitionby*    m_partitionby;
    xk_pg_parser_translog_ddlstmt_createtable_partitionsub*   m_partitionof;
} xk_pg_parser_translog_ddlstmt_createtable;

/* drop table 和 trncate table */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_DROP_BASE
{
    uint32_t    m_namespace_oid;
    uint32_t    m_relid;
    char       *m_name;
} xk_pg_parser_translog_ddlstmt_drop_base;

typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_DROP_CONSTRAINT
{
    bool        m_islocal;
    uint32_t    m_namespace_oid;
    uint32_t    m_relid;
    char       *m_consname;
} xk_pg_parser_translog_ddlstmt_drop_constraint;

/* 字符型返回值 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_VALUEBASE
{
    uint32_t    m_valuelen;
    uint32_t    m_owner;
    char       *m_value;
}xk_pg_parser_translog_ddlstmt_valuebase;

/* alter table alter table */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_ALTERTABLE
{
    uint32_t m_relid;
    uint32_t m_relnamespaceid_new;          /* 表的新namespace oid */
    uint32_t m_relnamespaceid;              /* 表的namespace oid */
    char    *m_relname_new;                 /* 表的新名 */
    char    *m_relname;                     /* 表名 */
}xk_pg_parser_translog_ddlstmt_altertable;

/* alter table add column */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_ADDCOLUMN
{
    uint32_t m_relid;
    uint32_t m_relnamespace;
    char    *m_relname;
    xk_pg_parser_translog_ddlstmt_col *m_addcolumn;
}xk_pg_parser_translog_ddlstmt_addcolumn;

typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_SETLOG
{
    uint32_t m_relid;
    uint32_t m_relnamespace;
    char    *m_relname;
}xk_pg_parser_translog_ddlstmt_setlog;

/* alter table alter column */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_ALTERCOLUMN
{
    bool     m_notnull;             /* 是否非空 */
    uint32_t m_type;                /* 列类型oid */
    uint32_t m_type_new;            /* 新的列类型oid */
    uint32_t m_relid;               /* 表的oid */
    uint32_t m_relnspid;            /* 表的namespace oid */
    int32_t  m_length;              /* 类型长度 */
    int32_t  m_precision;           /* 类型精度 */
    int32_t  m_scale;               /* 类型刻度 */
    int32_t  m_typemod;
    char     *m_relname;            /* 表名 */
    char     *m_colname_new;        /* 表的新名 */
    char     *m_colname;            /* 列名 */
}xk_pg_parser_translog_ddlstmt_altercolumn;

/* type DDL begin*/
#define XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_COMPOSITE (uint8_t)0X01
#define XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_ENUM      (uint8_t)0X02
#define XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_RANGE     (uint8_t)0X03
#define XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_DOMAIN    (uint8_t)0X04
#define XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE_TYPTYPE_NULL      (uint8_t)0X05

/* ddl 创建组合type类型, 范围类型定义 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPRANGE
{
    uint32_t m_subtype;             /* 类型oid */
    uint32_t m_subtype_opclass;     /* 操作符类oid */
    uint32_t m_collation;           /* 范围比较的排序规则的OID */
}xk_pg_parser_translog_ddlstmt_typrange;

/* ddl 创建组合type类型, 组合类型中的子类型定义 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPCOL
{
    uint32_t m_coltypid;    /* 类型oid */
    char *m_colname;        /* 子类型列名 */
}xk_pg_parser_translog_ddlstmt_typcol;

/* DDL type主体 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_TYPE
{
    uint8_t  m_typtype;             /* 类型(组合1,枚举2,范围3,域4, 空5) */
    uint16_t m_typvalcnt;           /* 类型的子类(组合), 枚举值个数(枚举), 其他为1 */
    uint32_t m_typnspid;            /* 类型的namespace oid */
    uint32_t m_owner;
    char    *m_type_name;           /* 类型名 */
    void    *m_typptr;              /* 类型子结构体(组合, 枚举, 范围，域) */
}xk_pg_parser_translog_ddlstmt_type;
/* type DDL end*/

/* sequence 序列 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT_SEQUENCE
{
    bool     m_seqcycle;        /* 是否循环 */
    uint32_t m_seqnspid;        /* namespace oid */
    uint32_t m_seqtypid;        /* 序列的类型oid(int2, int4, int8) */
    uint64_t m_seqincrement;    /* 序列增量值 */
    uint64_t m_seqmax;          /* 序列最大值 */
    uint64_t m_seqmin;          /* 序列最小值 */
    uint64_t m_seqstart;        /* 序列起始值 */
    uint64_t m_seqcache;        /* 序列缓冲尺寸 */
    char    *m_seqname;         /* 序列名 */
}xk_pg_parser_translog_ddlstmt_sequence;

/* 通用结构体 */
typedef struct XK_PG_PARSER_TRANSLOG_DDLSTMT
{
    xk_pg_parser_translog_ddltstmtbase    m_base;
    void                                 *m_ddlstmt;
    struct XK_PG_PARSER_TRANSLOG_DDLSTMT *m_next;
} xk_pg_parser_translog_ddlstmt;

/*-------------------   DDL解析接口   end -----------------------*/

typedef struct xk_pg_parser_RelFileNode
{
    uint32_t        spcNode;    /* tablespace */
    uint32_t        dbNode;     /* database */
    uint32_t        relNode;    /* relation */
} xk_pg_parser_RelFileNode;

extern bool xk_pg_parser_trans_preTrans(xk_pg_parser_translog_pre *xk_pg_parser_pre_data,
                                        xk_pg_parser_translog_pre_base **xk_pg_parser_result,
                                        int32_t *xk_pg_parser_errno);
extern bool xk_pg_parser_trans_TransRecord(xk_pg_parser_translog_translog2col *xk_pg_parser_transData,
                                    xk_pg_parser_translog_tbcolbase **xk_pg_parser_trans_result,
                                    int32_t *xk_pg_parser_errno);
extern bool xk_pg_parser_trans_TransRecord_GetTuple(xk_pg_parser_translog_translog2col *xk_pg_parser_transData,
                                    xk_pg_parser_translog_tbcolbase **xk_pg_parser_trans_result,
                                    int32_t *xk_pg_parser_errno);
extern bool xk_pg_parser_trans_DDLtrans(xk_pg_parser_translog_systb2ddl *xk_pg_parser_ddl,
                                 xk_pg_parser_translog_ddlstmt **xk_pg_parser_ddl_result,
                                 int32_t *xk_pg_parser_errno);
extern bool xk_pg_parser_trans_external_trans(xk_pg_parser_translog_external *xk_pg_parser_exdata,
                                       xk_pg_parser_translog_tbcol_value **xk_pg_parser_trans_result,
                                       int32_t *xk_pg_parser_errno);
extern void xk_pg_parser_trans_preTrans_free(xk_pg_parser_translog_pre_base *xk_pg_parser_result);
extern void xk_pg_parser_trans_TransRecord_free(xk_pg_parser_translog_translog2col *xk_pg_parser_trans_pre,
                                    xk_pg_parser_translog_tbcolbase *xk_pg_parser_trans);
extern void xk_pg_parser_trans_TransRecord_Minsert_free(xk_pg_parser_translog_translog2col *xk_pg_parser_trans_pre,
                                    xk_pg_parser_translog_tbcolbase *xk_pg_parser_trans);
extern void xk_pg_parser_trans_external_free(xk_pg_parser_translog_external *ext,
                                      xk_pg_parser_translog_tbcol_value *result);
extern void xk_pg_parser_trans_ddl_free(xk_pg_parser_translog_systb2ddl *ddl,
                                 xk_pg_parser_translog_ddlstmt *result);

extern bool xk_pg_parser_trans_matchmissing(xk_pg_parser_translog_tbcol_value *value1,
                                            xk_pg_parser_translog_tbcol_value *value2,
                                            uint16_t valuecnt);

extern void xk_pg_parser_free_value_ext(xk_pg_parser_translog_tbcol_value *value);
#endif
