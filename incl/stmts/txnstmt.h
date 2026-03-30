#ifndef _TXNSTMTS_H
#define _TXNSTMTS_H

typedef enum TXNSTMT_TYPE
{
    TXNSTMT_TYPE_NOP = 0x00,
    TXNSTMT_TYPE_DML = 0x01,                  /* DML statement type */
    TXNSTMT_TYPE_DDL = 0x02,                  /* DDL statement type */
    TXNSTMT_TYPE_METADATA,                    /* metadata information */
    TXNSTMT_TYPE_SHIFTFILE,                   /* file shift information */
    TXNSTMT_TYPE_REFRESH,                     /* refresh type, stores table information */
    TXNSTMT_TYPE_ONLINEREFRESH_BEGIN,         /* online refresh begin type, stores table info, uuid, xid */
    TXNSTMT_TYPE_ONLINEREFRESH_END,           /* online refresh end type, marks end of full load (uuid) */
    TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END, /* online refresh increment
                                                 end type, marks end of incremental sync (uuid) */
    TXNSTMT_TYPE_ONLINEREFRESH_DATASET,       /* online refresh
                                                 dataset type, updates filter set from dictionary */
    TXNSTMT_TYPE_UPDATESYNCTABLE,             /* update destination status table */
    TXNSTMT_TYPE_PREPARED,                    /* marks assembled prepared statements, rebuild thread converts DML to
                                                 prepared */
    TXNSTMT_TYPE_BURST,                /* marks assembled burst statements, rebuild thread converts DML to burst */
    TXNSTMT_TYPE_BIGTXN_BEGIN,         /* big transaction begin */
    TXNSTMT_TYPE_BIGTXN_END,           /* big transaction end */
    TXNSTMT_TYPE_ABANDON,              /* abandoned transaction, clean up specified big transaction */
    TXNSTMT_TYPE_RESET,                /* file shift transaction, clean up incomplete transactions */
    TXNSTMT_TYPE_UPDATEREWIND,         /* update status table rewind */
    TXNSTMT_TYPE_SYSDICTHIS,           /* used only by capture incremental update for big transaction
                                          dictionary */
    TXNSTMT_TYPE_ONLINEREFRESHABANDON, /* abandoned online refresh information */
    TXNSTMT_TYPE_COMMIT,               /* transaction commit */
    TXNSTMT_TYPE_MAX
} txnstmt_type;

typedef struct RIPPLETXNSTMT_DDL
{
    uint16 type;
    uint16 subtype;
    char*  ddlstmt;
} txnstmt_ddl;

typedef struct TXNSTMT_METADATA
{
    ListCell* begin;
    ListCell* end;
} txnstmt_metadata;

typedef struct TXNSTMT_SHIFTFILE
{
    XLogRecPtr redolsn;
    XLogRecPtr restartlsn;
    XLogRecPtr confirmlsn;
} txnstmt_shiftfile;

typedef struct TXNSTMT_UPDATESYNCTABLE
{
    FullTransactionId xid;
    uint64            trailno;
    uint64            offset;
    XLogRecPtr        lsn;
} txnstmt_updatesynctable;

typedef struct TXNSTMT_UPDATEREWIND
{
    recpos rewind;
} txnstmt_updaterewind;

typedef struct TXNSTMT_PREPARED
{
    uint8  optype;
    uint32 valuecnt;
    uint64 number;
    char*  preparedsql;
    char*  preparedname;
    char** values;
    void*  row;
} txnstmt_prepared;

typedef struct BIGTXN_END_STMT
{
    FullTransactionId xid;    /* transaction id */
    bool              commit; /* serialization structure */
} bigtxn_end_stmt;

typedef struct COMMIT_STMT
{
    int64 endtimestamp;
} commit_stmt;

typedef struct TXNSTMT
{
    txnstmt_type type;
    recpos       start;
    recpos       end;
    recpos       extra0; /* records extra position info (statement end position) */
    Oid          database;
    uint32       len;
    void*        stmt;
} txnstmt;

txnstmt* txnstmt_init(void);

void txnstmt_free(txnstmt* txnstmt);

#endif
