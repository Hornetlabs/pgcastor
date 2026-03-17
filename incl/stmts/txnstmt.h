#ifndef _TXNSTMTS_H
#define _TXNSTMTS_H


typedef enum TXNSTMT_TYPE
{
    TXNSTMT_TYPE_NOP                         = 0x00,
    TXNSTMT_TYPE_DML                         = 0x01,     /* DML类型语句 */
    TXNSTMT_TYPE_DDL                         = 0x02,     /* DDL类型语句 */
    TXNSTMT_TYPE_METADATA                    ,           /* 元数据信息 */
    TXNSTMT_TYPE_SHIFTFILE                   ,           /* 切换文件信息 */
    TXNSTMT_TYPE_REFRESH                     ,           /* refresh类型保存用到的表信息 */
    TXNSTMT_TYPE_ONLINEREFRESH_BEGIN         ,           /* onlinerefresh begin类型保存用到的表信息，uuid，事务号 */
    TXNSTMT_TYPE_ONLINEREFRESH_END           ,           /* onlinerefresh end类型标识onlinerefresh存量结束（uuid） */
    TXNSTMT_TYPE_ONLINEREFRESH_INCREMENT_END ,           /* onlinerefresh increment end类型标识onlinerefresh增量结束（uuid） */
    TXNSTMT_TYPE_ONLINEREFRESH_DATASET       ,           /* onlinerefresh dataset类型onlinerefresh字典表信息更新过滤集 */
    TXNSTMT_TYPE_UPDATESYNCTABLE             ,           /* 更新目的端状态表 */
    TXNSTMT_TYPE_PREPARED                    ,           /* 标识已经组装完 prepared语句，在rebuil线程将 DML处理为 prepared */
    TXNSTMT_TYPE_BURST                       ,           /* 标识已经组装完 burst语句，在rebuil线程将 DML处理为 burst */
    TXNSTMT_TYPE_BIGTXN_BEGIN                ,           /* 大事务开始 */
    TXNSTMT_TYPE_BIGTXN_END                  ,           /* 大事务结束 */
    TXNSTMT_TYPE_ABANDON                     ,           /* 要放弃的事务，清理指定大事务 */
    TXNSTMT_TYPE_RESET                       ,           /* 文件切换事务，清理未完成的事务 */
    TXNSTMT_TYPE_UPDATEREWIND                ,           /* 更新状态表rewind */
    TXNSTMT_TYPE_SYSDICTHIS                  ,           /* 只用于capture原增量更新大事务中字典表 */
    TXNSTMT_TYPE_ONLINEREFRESHABANDON        ,           /* 放弃的onlinerefresh信息 */
    TXNSTMT_TYPE_COMMIT                      ,           /* 事务提交 */
    TXNSTMT_TYPE_MAX
} txnstmt_type;

typedef struct RIPPLETXNSTMT_DDL
{
    uint16      type;
    uint16      subtype;
    char*       ddlstmt;
} txnstmt_ddl;

typedef struct TXNSTMT_METADATA
{
    ListCell *begin;
    ListCell *end;
}txnstmt_metadata;

typedef struct TXNSTMT_SHIFTFILE
{
    XLogRecPtr      redolsn;
    XLogRecPtr      restartlsn;
    XLogRecPtr      confirmlsn;
}txnstmt_shiftfile;

typedef struct TXNSTMT_UPDATESYNCTABLE
{
    FullTransactionId   xid;
    uint64              trailno;
    uint64              offset;
    XLogRecPtr          lsn;
}txnstmt_updatesynctable;

typedef struct TXNSTMT_UPDATEREWIND
{
    recpos           rewind;
}txnstmt_updaterewind;

typedef struct TXNSTMT_PREPARED
{
    uint8               optype;
    uint32              valuecnt;
    uint64              number;
    char*               preparedsql;
    char*               preparedname;
    char**              values;
    void*               row;
} txnstmt_prepared;

typedef struct BIGTXN_END_STMT
{
    FullTransactionId   xid;        /* 事务号 */
    bool                commit;     /* 序列化结构 */
} bigtxn_end_stmt;

typedef struct COMMIT_STMT
{
    int64                   endtimestamp;
} commit_stmt;

typedef struct TXNSTMT
{
    txnstmt_type     type;
    recpos           start;
    recpos           end;
    recpos           extra0; /* 记录额外的位置信息(语句结束位置) */
    Oid                     database;
    uint32                  len;
    void*                   stmt;
} txnstmt;

txnstmt* txnstmt_init(void);

void txnstmt_free(txnstmt* txnstmt);

#endif
