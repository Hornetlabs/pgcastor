#ifndef _RIPPLE_TXNSTMTS_H
#define _RIPPLE_TXNSTMTS_H


typedef enum RIPPLE_TXNSTMT_TYPE
{
    RIPPLE_TXNSTMT_TYPE_NOP                         = 0x00,
    RIPPLE_TXNSTMT_TYPE_DML                         = 0x01,     /* DML类型语句 */
    RIPPLE_TXNSTMT_TYPE_DDL                         = 0x02,     /* DDL类型语句 */
    RIPPLE_TXNSTMT_TYPE_METADATA                    ,           /* 元数据信息 */
    RIPPLE_TXNSTMT_TYPE_SHIFTFILE                   ,           /* 切换文件信息 */
    RIPPLE_TXNSTMT_TYPE_REFRESH                     ,           /* refresh类型保存用到的表信息 */
    RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_BEGIN         ,           /* onlinerefresh begin类型保存用到的表信息，uuid，事务号 */
    RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_END           ,           /* onlinerefresh end类型标识onlinerefresh存量结束（uuid） */
    RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END ,           /* onlinerefresh increment end类型标识onlinerefresh增量结束（uuid） */
    RIPPLE_TXNSTMT_TYPE_ONLINEREFRESH_DATASET       ,           /* onlinerefresh dataset类型onlinerefresh字典表信息更新过滤集 */
    RIPPLE_TXNSTMT_TYPE_UPDATESYNCTABLE             ,           /* 更新目的端状态表 */
    RIPPLE_TXNSTMT_TYPE_PREPARED                    ,           /* 标识已经组装完 prepared语句，在rebuil线程将 DML处理为 prepared */
    RIPPLE_TXNSTMT_TYPE_BURST                       ,           /* 标识已经组装完 burst语句，在rebuil线程将 DML处理为 burst */
    RIPPLE_TXNSTMT_TYPE_BIGTXN_BEGIN                ,           /* 大事务开始 */
    RIPPLE_TXNSTMT_TYPE_BIGTXN_END                  ,           /* 大事务结束 */
    RIPPLE_TXNSTMT_TYPE_ABANDON                     ,           /* 要放弃的事务，清理指定大事务 */
    RIPPLE_TXNSTMT_TYPE_RESET                       ,           /* 文件切换事务，清理未完成的事务 */
    RIPPLE_TXNSTMT_TYPE_UPDATEREWIND                ,           /* 更新状态表rewind */
    RIPPLE_TXNSTMT_TYPE_SYSDICTHIS                  ,           /* 只用于capture原增量更新大事务中字典表 */
    RIPPLE_TXNSTMT_TYPE_ONLINEREFRESHABANDON        ,           /* 放弃的onlinerefresh信息 */
    RIPPLE_TXNSTMT_TYPE_COMMIT                      ,           /* 事务提交 */
    RIPPLE_TXNSTMT_TYPE_MAX
} ripple_txnstmt_type;

typedef struct RIPPLETXNSTMT_DDL
{
    uint16      type;
    uint16      subtype;
    char*       ddlstmt;
} ripple_txnstmt_ddl;

typedef struct RIPPLE_TXNSTMT_METADATA
{
    ListCell *begin;
    ListCell *end;
}ripple_txnstmt_metadata;

typedef struct RIPPLE_TXNSTMT_SHIFTFILE
{
    XLogRecPtr      redolsn;
    XLogRecPtr      restartlsn;
    XLogRecPtr      confirmlsn;
}ripple_txnstmt_shiftfile;

typedef struct RIPPLE_TXNSTMT_UPDATESYNCTABLE
{
    FullTransactionId   xid;
    uint64              trailno;
    uint64              offset;
    XLogRecPtr          lsn;
}ripple_txnstmt_updatesynctable;

typedef struct RIPPLE_TXNSTMT_UPDATEREWIND
{
    ripple_recpos           rewind;
}ripple_txnstmt_updaterewind;

typedef struct RIPPLE_TXNSTMT_PREPARED
{
    uint8               optype;
    uint32              valuecnt;
    uint64              number;
    char*               preparedsql;
    char*               preparedname;
    char**              values;
    void*               row;
} ripple_txnstmt_prepared;

typedef struct RIPPLE_BIGTXN_END_STMT
{
    FullTransactionId   xid;        /* 事务号 */
    bool                commit;     /* 序列化结构 */
} ripple_bigtxn_end_stmt;

typedef struct RIPPLE_COMMIT_STMT
{
    int64                   endtimestamp;
} ripple_commit_stmt;

typedef struct RIPPLE_TXNSTMT
{
    ripple_txnstmt_type     type;
    ripple_recpos           start;
    ripple_recpos           end;
    ripple_recpos           extra0; /* 记录额外的位置信息(语句结束位置) */
    Oid                     database;
    uint32                  len;
    void*                   stmt;
} ripple_txnstmt;

ripple_txnstmt* ripple_txnstmt_init(void);

void ripple_txnstmt_free(ripple_txnstmt* txnstmt);

#endif
