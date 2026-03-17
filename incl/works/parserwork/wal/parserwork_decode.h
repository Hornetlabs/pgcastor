#ifndef _PARSERWORK_DECODE_H
#define _PARSERWORK_DECODE_H

typedef enum DECODINGCONTEXT_STAT
{
    DECODINGCONTEXT_INIT             = 0x00,
    DECODINGCONTEXT_REWIND           ,
    DECODINGCONTEXT_RUNNING          ,
    DECODINGCONTEXT_SET_PAUSE        ,
    DECODINGCONTEXT_PAUSE            ,
    DECODINGCONTEXT_RESUME
} decodingcontext_stat;

typedef pg_parser_translog_pre decodewalpre;

typedef struct DECODINGCONTEXT_PRIVDATACALLBACK
{
    /* 设置splitwork的拆分的起点和终点 */
    void (*setloadlsn)(void* privdata, XLogRecPtr startlsn, XLogRecPtr endlsn);

    /* 设置capturestate解析到的结束lsn */
    void (*setmetricparselsn)(void* privdata, XLogRecPtr pareslsn);

    /* rewind结束设置redo, restart, confirm lsn */
    void (*setparserlsn)(void* privdata, XLogRecPtr confirm, XLogRecPtr restart, XLogRecPtr redo);

    /* capturestate设置redo, restart, confirm lsn */
    void (*setmetricsynclsn)(void* privdata, XLogRecPtr redolsn, XLogRecPtr restartlsn, XLogRecPtr confirmlsn);

    /* 设置capturestate时间戳 */
    void (*setmetricparsetimestamp)(void* privdata, TimestampTz parsetimestamp);

} decodingctx_privdatacallback;

typedef struct DECODINGCONTEXT
{
    /* 用于标识是否需要过滤大事务, true过滤, false不过滤 */
    bool                                filterbigtrans;

    /* 状态值, rewind, normal */
    int32                               stat;
    capturebase                  base;
    decodewalpre                 walpre;

    /* 解析的数据库标识 */
    Oid                                 database;

    /* 解析到的 lsn, 默认值为 2, refresh 事务的 lsn 为 1 */
    XLogRecPtr                          parselsn;

    /* 转换信息,numercic(show lc_numeric),money(show lc_monetary )、源端目标端字符集(pg_database->encoding)、时区信息(show timezone) */
    char*                               tzname;
    char*                               monetary;
    char*                               numeric;
    char*                               orgdbcharset;
    char*                               tgtdbcharset;
    rewind_info*                      rewind_ptr;
    txn*                         refreshtxn;
    dlist*                              onlinerefresh;
    transcache*                  trans_cache;

    /* 解析的 record 来源 */
    record*                      decode_record;
    queue*                       recordqueue;

    /* 保存完整事务的缓存 */
    cache_txn*                   parser2txns;

    /* 大事务缓存 */
    cache_txn*                   parser2bigtxns;

    /* 上层结构 */
    void*                               privdata;                   /* 内容为: increment_capture*/
    decodingctx_privdatacallback callback;
} decodingcontext;

/* 是否要兑换ddl true兑换 */
static inline bool decodingcontext_isddlfilter(bool filter, bool redo)
{
    return (false == filter && false == redo);
}

/* 是否过滤stmt true过滤 */
static inline bool decodingcontext_isstmtsfilter(bool filter, bool redo)
{
    return (true == redo || true == filter);
}

void parserwork_waldecode(decodingcontext* decodingctx);

void decode_getparserlsn(XLogRecPtr* plsn);

#endif
