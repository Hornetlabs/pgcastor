#ifndef _FF_DETAIL_H
#define _FF_DETAIL_H


/* 文件内容分类 */
typedef enum FF_CXT_TYPE
{
    FFTRAIL_CXT_TYPE_NOP        = 0x00,                 /* 无效                   */
    FFTRAIL_CXT_TYPE_FHEADER    = 0x01,                 /* 文件头                  */
    FFTRAIL_CXT_TYPE_DATA       = 0x02,                 /* 文件中的 data 信息       */
    FFTRAIL_CXT_TYPE_RESET      = 0x03,                 /* 事务重置，标识未完成的事务清理 */
    FFTRAIL_CXT_TYPE_FTAIL      = 0x04,                 /* 文件尾信息               */
} ff_cxt_type;

typedef struct FF_HEADER
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
} ff_header;

typedef struct FF_TAIL
{
    uint64               nexttrailno;
} ff_tail;


typedef ff_tail          ff_reset;

typedef enum FF_DATA_TYPE
{
    FF_DATA_TYPE_NOP                             = 0x00,
    FF_DATA_TYPE_DBMETADATA                      = 0x01,                 /* 数据库信息 */
    FF_DATA_TYPE_TBMETADATA                      = 0x02,                 /* 表信息    */
    FF_DATA_TYPE_TXN                             = 0x03,                 /* 无用，填充使用 */
    FF_DATA_TYPE_DML_INSERT                      = 0x04,                 /* DML INSERT */
    FF_DATA_TYPE_DML_UPDATE                      = 0x05,                 /* DML UPDATE */
    FF_DATA_TYPE_DML_DELETE                      = 0x06,                 /* DML DELETE */
    FF_DATA_TYPE_DDL_STMT                        = 0x07,                 /* DDL 语句 */
    FF_DATA_TYPE_DDL_STRUCT                      = 0x08,                 /* DDL 语句 */
    FF_DATA_TYPE_REC_CONTRECORD                  = 0x09,                 /* 上个 record 的延续 */
    FF_DATA_TYPE_DML_MULTIINSERT                 = 0x0A,                 /* DML MULTIINSERT */
    FF_DATA_TYPE_TXNCOMMIT                       = 0x0B,                 /* 事务提交 */
    FF_DATA_TYPE_REFRESH                         = 0x0C,                 /* 存量数据 */
    FF_DATA_TYPE_TXNBEGIN                        = 0x0D,                 /* 事务开始 */
    FF_DATA_TYPE_ONLINE_REFRESH_BEGIN            = 0x0E,                 /* online refresh begin */
    FF_DATA_TYPE_ONLINE_REFRESH_END              = 0x0F,                 /* online refresh end   */
    FF_DATA_TYPE_ONLINE_REFRESH_INCREMENT_END    = 0x10,                 /* onlinerefresh 增量end   */
    FF_DATA_TYPE_BIGTXN_BEGIN                    ,                       /* 大事务开始标识   */
    FF_DATA_TYPE_BIGTXN_END                      ,                       /* 大事务结束标识   */
    FF_DATA_TYPE_ONLINEREFRESH_ABANDON                                   /* 放弃的onlinerefresh */
} ff_data_type;

/* formattype */
typedef enum FF_DATA_FORMATTYPE
{
    FF_DATA_FORMATTYPE_SQL           = 0x01,                 /* 通过 sql 查询获取    */
    FF_DATA_FORMATTYPE_WAL           = 0x02                  /* 事务日志中获取       */
} ff_data_formattype;

/* 处于事务的位置 */
typedef enum FF_DATA_TRANSIND
{
    FF_DATA_TRANSIND_START           = 0x01,
    FF_DATA_TRANSIND_IN              = 0x02,
    FF_DATA_TRANSIND_END             = 0x04
} ff_data_tranind;

typedef struct FF_DATA
{
    ff_data_type         type;
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
} ff_data;

/*
 * 数据库的编号
*/
typedef struct FF_DBMETADATA
{
    ff_data      header;
    uint16              dbmdno;
    Oid                 oid;
    char*               dbname;
    char*               charset;
    char*               timezone;
    char*               money;
} ff_dbmetadata;

/*
 * 表的编号信息
*/
typedef struct FF_COLUMN
{
    Oid                 typid;
    uint16              flag;                                       /* 列类型标识,自定义列、自定义列的子列 */
    uint16              num;                                        /* 列在表中的顺序 */
    int32               length;                                     /* 列类型长度 */
    int32               precision;                                  /* 列类型精度 */
    int32               scale;                                      /* 列类型刻度 */
    char                typename[NAMEDATALEN];               /* 列类型名称 */
    char                column[NAMEDATALEN];                 /* 列名 */
} ff_column;

/*
 * 表源数据信息flag标识
 * 添加时按位进行或操作添加
 */
typedef enum FF_TBMETADATA_FLAG
{
    FF_TBMETADATA_FLAG_NOP = 0x00
} FF_TBMETADATA_FLAG;

/* 表索引信息 */
typedef enum FF_TBINDEX_TYPE
{
    FF_TBINDEX_TYPE_NOP = 0x00,
    FF_TBINDEX_TYPE_PKEY,
    FF_TBINDEX_TYPE_UNIQUE
} ff_tbindex_type;

typedef struct FF_TBINDEX
{
    ff_tbindex_type  index_type;
    bool                    index_identify;
    uint32                  index_oid;
    uint32                  index_key_num;
    uint32*                 index_key;
} ff_tbindex;

/* 表元数据信息 */
typedef struct FF_TBMETADATA
{
    ff_data      header;
    uint32              tbmdno;
    Oid                 oid;
    uint16              flag;                   /* 含有主键/唯一约束等信息 */
    uint16              colcnt;                 /* 列个数               */
    char                identify;               /* replica identify */
    char*               schema;
    char*               table;
    ff_column*   columns;
    List*               index;                  /* 索引信息 ff_tbindex */
} ff_tbmetadata;

//flag标识
typedef enum FF_FLAG
{
    FF_COL_IS_NORMAL     = 0x00,         /* 正常的列 */
    FF_COL_IS_NULL       = 0x01,         /* 有列信息且 length 为 0  */
    FF_COL_IS_MISSING    = 0x02,         /* update和delete取旧数据时, 在WAL中未记录的数据 */
    FF_COL_IS_DROPED     = 0x03,         /* 列被drop */
    FF_COL_IS_CUSTOM     = 0x04          /* 自定义列类型 */
} ff_flag;

/* 事务数据 */
typedef struct FF_TXNDATA
{
    ff_data      header;
    void*               data;
} ff_txndata;

/* 数据块位置信息 */
typedef struct FF_FILEINFO
{
    uint64              fileid;
    uint64              blknum;                 /* 起始为1 */
    FullTransactionId   xid;
} ff_fileinfo;

extern ff_tbindex* ff_tbindex_init(int type, uint32_t keynum);
extern void ff_tbindex_free(ff_tbindex* index);
extern void ff_tbmetadata_free(ff_tbmetadata *data);
#endif
