#ifndef _RIPPLE_FF_DETAIL_H
#define _RIPPLE_FF_DETAIL_H


/* 文件内容分类 */
typedef enum RIPPLE_FF_CXT_TYPE
{
    RIPPLE_FFTRAIL_CXT_TYPE_NOP        = 0x00,                 /* 无效                   */
    RIPPLE_FFTRAIL_CXT_TYPE_FHEADER    = 0x01,                 /* 文件头                  */
    RIPPLE_FFTRAIL_CXT_TYPE_DATA       = 0x02,                 /* 文件中的 data 信息       */
    RIPPLE_FFTRAIL_CXT_TYPE_RESET      = 0x03,                 /* 事务重置，标识未完成的事务清理 */
    RIPPLE_FFTRAIL_CXT_TYPE_FTAIL      = 0x04,                 /* 文件尾信息               */
} ripple_ff_cxt_type;

typedef struct RIPPLE_FF_HEADER
{
    uint32              magic;
    char*               version;
    uint32              compatibility;
    uint32              encryption;
    FullTransactionId   startxid;
    FullTransactionId   endxid;
    uint32              dbtype;
    char*               dbversion;
    uint64              redolsn;
    uint64              restartlsn;
    uint64              confirmlsn;
    char*               filename;
    uint64              filesize;
} ripple_ff_header;

typedef struct RIPPLE_FF_TAIL
{
    uint64               nexttrailno;
} ripple_ff_tail;


typedef ripple_ff_tail          ripple_ff_reset;

typedef enum RIPPLE_FF_DATA_TYPE
{
    RIPPLE_FF_DATA_TYPE_NOP                             = 0x00,
    RIPPLE_FF_DATA_TYPE_DBMETADATA                      = 0x01,                 /* 数据库信息 */
    RIPPLE_FF_DATA_TYPE_TBMETADATA                      = 0x02,                 /* 表信息    */
    RIPPLE_FF_DATA_TYPE_TXN                             = 0x03,                 /* 无用，填充使用 */
    RIPPLE_FF_DATA_TYPE_DML_INSERT                      = 0x04,                 /* DML INSERT */
    RIPPLE_FF_DATA_TYPE_DML_UPDATE                      = 0x05,                 /* DML UPDATE */
    RIPPLE_FF_DATA_TYPE_DML_DELETE                      = 0x06,                 /* DML DELETE */
    RIPPLE_FF_DATA_TYPE_DDL_STMT                        = 0x07,                 /* DDL 语句 */
    RIPPLE_FF_DATA_TYPE_DDL_STRUCT                      = 0x08,                 /* DDL 语句 */
    RIPPLE_FF_DATA_TYPE_REC_CONTRECORD                  = 0x09,                 /* 上个 record 的延续 */
    RIPPLE_FF_DATA_TYPE_DML_MULTIINSERT                 = 0x0A,                 /* DML MULTIINSERT */
    RIPPLE_FF_DATA_TYPE_TXNCOMMIT                       = 0x0B,                 /* 事务提交 */
    RIPPLE_FF_DATA_TYPE_REFRESH                         = 0x0C,                 /* 存量数据 */
    RIPPLE_FF_DATA_TYPE_TXNBEGIN                        = 0x0D,                 /* 事务开始 */
    RIPPLE_FF_DATA_TYPE_ONLINE_REFRESH_BEGIN            = 0x0E,                 /* online refresh begin */
    RIPPLE_FF_DATA_TYPE_ONLINE_REFRESH_END              = 0x0F,                 /* online refresh end   */
    RIPPLE_FF_DATA_TYPE_ONLINE_REFRESH_INCREMENT_END    = 0x10,                 /* onlinerefresh 增量end   */
    RIPPLE_FF_DATA_TYPE_BIGTXN_BEGIN                    ,                       /* 大事务开始标识   */
    RIPPLE_FF_DATA_TYPE_BIGTXN_END                      ,                       /* 大事务结束标识   */
    RIPPLE_FF_DATA_TYPE_ONLINEREFRESH_ABANDON                                   /* 放弃的onlinerefresh */
} ripple_ff_data_type;

/* formattype */
typedef enum RIPPLE_FF_DATA_FORMATTYPE
{
    RIPPLE_FF_DATA_FORMATTYPE_SQL           = 0x01,                 /* 通过 sql 查询获取    */
    RIPPLE_FF_DATA_FORMATTYPE_WAL           = 0x02                  /* 事务日志中获取       */
} ripple_ff_data_formattype;

/* 处于事务的位置 */
typedef enum RIPPLE_FF_DATA_TRANSIND
{
    RIPPLE_FF_DATA_TRANSIND_START           = 0x01,
    RIPPLE_FF_DATA_TRANSIND_IN              = 0x02,
    RIPPLE_FF_DATA_TRANSIND_END             = 0x04
} ripple_ff_data_tranind;

typedef struct RIPPLE_FF_DATA
{
    ripple_ff_data_type         type;
    uint16                      dbmdno;
    uint32                      tbmdno;
    uint64                      transid;
    uint8                       transind;                   /* 标识事务内位置 */
    uint64                      totallength;                /* 组成一条完整数据的长度 */
    uint16                      reclength;                  /* 当前 record 的数据长度，不包含 rectail 的长度和 header 的内容   */
    uint16                      reccount;                   /* 当前 record 记录的总条数 */
    uint8                       formattype;                 /* 数据来源 */
    uint16                      subtype;                    /* 用于区分子类型，当没有子类型时为 type */
    uint64                      orgpos;                     /* 记录语句结束位置的lsn偏移量 */
    r_crc32c                    crc32;
} ripple_ff_data;

/*
 * 数据库的编号
*/
typedef struct RIPPLE_FF_DBMETADATA
{
    ripple_ff_data      header;
    uint16              dbmdno;
    Oid                 oid;
    char*               dbname;
    char*               charset;
    char*               timezone;
    char*               money;
} ripple_ff_dbmetadata;

/*
 * 表的编号信息
*/
typedef struct RIPPLE_FF_COLUMN
{
    Oid                 typid;
    uint16              flag;                                       /* 列类型标识,自定义列、自定义列的子列 */
    uint16              num;                                        /* 列在表中的顺序 */
    int32               length;                                     /* 列类型长度 */
    int32               precision;                                  /* 列类型精度 */
    int32               scale;                                      /* 列类型刻度 */
    char                typename[RIPPLE_NAMEDATALEN];               /* 列类型名称 */
    char                column[RIPPLE_NAMEDATALEN];                 /* 列名 */
} ripple_ff_column;

/*
 * 表源数据信息flag标识
 * 添加时按位进行或操作添加
 */
typedef enum RIPPLE_FF_TBMETADATA_FLAG
{
    RIPPLE_FF_TBMETADATA_FLAG_NOP = 0x00
} RIPPLE_FF_TBMETADATA_FLAG;

/* 表索引信息 */
typedef enum RIPPLE_FF_TBINDEX_TYPE
{
    RIPPLE_FF_TBINDEX_TYPE_NOP = 0x00,
    RIPPLE_FF_TBINDEX_TYPE_PKEY,
    RIPPLE_FF_TBINDEX_TYPE_UNIQUE
} ripple_ff_tbindex_type;

typedef struct RIPPLE_FF_TBINDEX
{
    ripple_ff_tbindex_type  index_type;
    bool                    index_identify;
    uint32                  index_oid;
    uint32                  index_key_num;
    uint32*                 index_key;
} ripple_ff_tbindex;

/* 表元数据信息 */
typedef struct RIPPLE_FF_TBMETADATA
{
    ripple_ff_data      header;
    uint32              tbmdno;
    Oid                 oid;
    uint16              flag;                   /* 含有主键/唯一约束等信息 */
    uint16              colcnt;                 /* 列个数               */
    char                identify;               /* replica identify */
    char*               schema;
    char*               table;
    ripple_ff_column*   columns;
    List*               index;                  /* 索引信息 ripple_ff_tbindex */
} ripple_ff_tbmetadata;

//flag标识
typedef enum RIPPLE_FF_FLAG
{
    RIPPLE_FF_COL_IS_NORMAL     = 0x00,         /* 正常的列 */
    RIPPLE_FF_COL_IS_NULL       = 0x01,         /* 有列信息且 length 为 0  */
    RIPPLE_FF_COL_IS_MISSING    = 0x02,         /* update和delete取旧数据时, 在WAL中未记录的数据 */
    RIPPLE_FF_COL_IS_DROPED     = 0x03,         /* 列被drop */
    RIPPLE_FF_COL_IS_CUSTOM     = 0x04          /* 自定义列类型 */
} ripple_ff_flag;

/* 事务数据 */
typedef struct RIPPLE_FF_TXNDATA
{
    ripple_ff_data      header;
    void*               data;
} ripple_ff_txndata;

/* 数据块位置信息 */
typedef struct RIPPLE_FF_FILEINFO
{
    uint64              fileid;
    uint64              blknum;                 /* 起始为1 */
    FullTransactionId   xid;
} ripple_ff_fileinfo;

extern ripple_ff_tbindex* ripple_ff_tbindex_init(int type, uint32_t keynum);
extern void ripple_ff_tbindex_free(ripple_ff_tbindex* index);
extern void ripple_ff_tbmetadata_free(ripple_ff_tbmetadata *data);
#endif
