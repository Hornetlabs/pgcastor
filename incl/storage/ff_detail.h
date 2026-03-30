#ifndef _FF_DETAIL_H
#define _FF_DETAIL_H

/* File content classification */
typedef enum FF_CXT_TYPE
{
    FFTRAIL_CXT_TYPE_NOP = 0x00,     /* Invalid                   */
    FFTRAIL_CXT_TYPE_FHEADER = 0x01, /* File header                  */
    FFTRAIL_CXT_TYPE_DATA = 0x02,    /* Data information in file       */
    FFTRAIL_CXT_TYPE_RESET = 0x03,   /* Transaction reset, indicates unfinished transaction cleanup */
    FFTRAIL_CXT_TYPE_FTAIL = 0x04,   /* File tail information               */
} ff_cxt_type;

typedef struct FF_HEADER
{
    uint32            magic;
    char*             version;
    uint32            compatibility;
    uint32            encryption;
    FullTransactionId startxid;
    FullTransactionId endxid;
    uint32            dbtype;
    char*             dbversion;
    uint64            redolsn;
    uint64            restartlsn;
    uint64            confirmlsn;
    char*             filename;
    uint64            filesize;
} ff_header;

typedef struct FF_TAIL
{
    uint64 nexttrailno;
} ff_tail;

typedef ff_tail ff_reset;

typedef enum FF_DATA_TYPE
{
    FF_DATA_TYPE_NOP = 0x00,
    FF_DATA_TYPE_DBMETADATA = 0x01,                   /* Database information */
    FF_DATA_TYPE_TBMETADATA = 0x02,                   /* Table information    */
    FF_DATA_TYPE_TXN = 0x03,                          /* Unused, for padding */
    FF_DATA_TYPE_DML_INSERT = 0x04,                   /* DML INSERT */
    FF_DATA_TYPE_DML_UPDATE = 0x05,                   /* DML UPDATE */
    FF_DATA_TYPE_DML_DELETE = 0x06,                   /* DML DELETE */
    FF_DATA_TYPE_DDL_STMT = 0x07,                     /* DDL statement */
    FF_DATA_TYPE_DDL_STRUCT = 0x08,                   /* DDL statement */
    FF_DATA_TYPE_REC_CONTRECORD = 0x09,               /* Continuation of previous record */
    FF_DATA_TYPE_DML_MULTIINSERT = 0x0A,              /* DML MULTIINSERT */
    FF_DATA_TYPE_TXNCOMMIT = 0x0B,                    /* Transaction commit */
    FF_DATA_TYPE_REFRESH = 0x0C,                      /* Stock data */
    FF_DATA_TYPE_TXNBEGIN = 0x0D,                     /* Transaction begin */
    FF_DATA_TYPE_ONLINE_REFRESH_BEGIN = 0x0E,         /* online refresh begin */
    FF_DATA_TYPE_ONLINE_REFRESH_END = 0x0F,           /* online refresh end   */
    FF_DATA_TYPE_ONLINE_REFRESH_INCREMENT_END = 0x10, /* onlinerefresh incremental end   */
    FF_DATA_TYPE_BIGTXN_BEGIN,                        /* Big transaction begin flag   */
    FF_DATA_TYPE_BIGTXN_END,                          /* Big transaction end flag   */
    FF_DATA_TYPE_ONLINEREFRESH_ABANDON                /* Abandoned onlinerefresh */
} ff_data_type;

/* formattype */
typedef enum FF_DATA_FORMATTYPE
{
    FF_DATA_FORMATTYPE_SQL = 0x01, /* Obtained through sql query    */
    FF_DATA_FORMATTYPE_WAL = 0x02  /* Obtained from transaction log       */
} ff_data_formattype;

/* Position in transaction */
typedef enum FF_DATA_TRANSIND
{
    FF_DATA_TRANSIND_START = 0x01,
    FF_DATA_TRANSIND_IN = 0x02,
    FF_DATA_TRANSIND_END = 0x04
} ff_data_tranind;

typedef struct FF_DATA
{
    ff_data_type type;
    uint16       dbmdno;
    uint32       tbmdno;
    uint64       transid;
    uint8        transind;    /* Indicate position within transaction */
    uint64       totallength; /* Length of composing complete data */
    uint16       reclength;   /* Data length of current record, excluding rectail length and header content */
    uint16       reccount;    /* Total count of records in current record */
    uint8        formattype;  /* Data source */
    uint16       subtype;     /* Used to distinguish sub type, when no sub type is type */
    uint64       orgpos;      /* Lsn offset recording statement end position */
    r_crc32c     crc32;
} ff_data;

/*
 * Database number
 */
typedef struct FF_DBMETADATA
{
    ff_data header;
    uint16  dbmdno;
    Oid     oid;
    char*   dbname;
    char*   charset;
    char*   timezone;
    char*   money;
} ff_dbmetadata;

/*
 * Table number information
 */
typedef struct FF_COLUMN
{
    Oid    typid;
    uint16 flag;                /* Column type flag, custom column, sub column of custom column */
    uint16 num;                 /* Column order in table */
    int32  length;              /* Column type length */
    int32  precision;           /* Column type precision */
    int32  scale;               /* Column type scale */
    char typename[NAMEDATALEN]; /* Column typeName */
    char column[NAMEDATALEN];   /* Column name */
} ff_column;

/*
 * Table source data information flag
 * When adding, perform bitwise OR operation
 */
typedef enum FF_TBMETADATA_FLAG
{
    FF_TBMETADATA_FLAG_NOP = 0x00
} FF_TBMETADATA_FLAG;

/* Table index information */
typedef enum FF_TBINDEX_TYPE
{
    FF_TBINDEX_TYPE_NOP = 0x00,
    FF_TBINDEX_TYPE_PKEY,
    FF_TBINDEX_TYPE_UNIQUE
} ff_tbindex_type;

typedef struct FF_TBINDEX
{
    ff_tbindex_type index_type;
    bool            index_identify;
    uint32          index_oid;
    uint32          index_key_num;
    uint32*         index_key;
} ff_tbindex;

/* Table metadata information */
typedef struct FF_TBMETADATA
{
    ff_data    header;
    uint32     tbmdno;
    Oid        oid;
    uint16     flag;     /* Contains primary key/unique constraint etc information */
    uint16     colcnt;   /* Column count               */
    char       identify; /* replica identify */
    char*      schema;
    char*      table;
    ff_column* columns;
    List*      index; /* Index information ff_tbindex */
} ff_tbmetadata;

/* Flag identifier */
typedef enum FF_FLAG
{
    FF_COL_IS_NORMAL = 0x00,  /* Normal column */
    FF_COL_IS_NULL = 0x01,    /* Has column info and length is 0  */
    FF_COL_IS_MISSING = 0x02, /* When update and delete get old data, data not recorded in WAL */
    FF_COL_IS_DROPED = 0x03,  /* Column is dropped */
    FF_COL_IS_CUSTOM = 0x04   /* Custom column type */
} ff_flag;

/* Transaction data */
typedef struct FF_TXNDATA
{
    ff_data header;
    void*   data;
} ff_txndata;

/* Data block position information */
typedef struct FF_FILEINFO
{
    uint64            fileid;
    uint64            blknum; /* Starting from 1 */
    FullTransactionId xid;
} ff_fileinfo;

extern ff_tbindex* ff_tbindex_init(int type, uint32_t keynum);
extern void ff_tbindex_free(ff_tbindex* index);
extern void ff_tbmetadata_free(ff_tbmetadata* data);
#endif
