#ifndef _RIPPLE_PARSERWORK_DECODE_H
#define _RIPPLE_PARSERWORK_DECODE_H

typedef enum RIPPLE_DECODINGCONTEXT_STAT
{
    RIPPLE_DECODINGCONTEXT_INIT             = 0x00,
    RIPPLE_DECODINGCONTEXT_REWIND           ,
    RIPPLE_DECODINGCONTEXT_RUNNING          ,
    RIPPLE_DECODINGCONTEXT_SET_PAUSE        ,
    RIPPLE_DECODINGCONTEXT_PAUSE            ,
    RIPPLE_DECODINGCONTEXT_RESUME
} ripple_decodingcontext_stat;

typedef xk_pg_parser_translog_pre ripple_decodewalpre;

typedef struct RIPPLE_DECODINGCONTEXT_PRIVDATACALLBACK
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

} ripple_decodingctx_privdatacallback;

typedef struct RIPPLE_DECODINGCONTEXT
{
    /* 用于标识是否需要过滤大事务, true过滤, false不过滤 */
    bool                                filterbigtrans;

    /* 状态值, rewind, normal */
    int32                               stat;
    ripple_capturebase                  base;
    ripple_decodewalpre                 walpre;

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
    ripple_rewind*                      rewind;
    ripple_txn*                         refreshtxn;
    dlist*                              onlinerefresh;
    ripple_transcache*                  transcache;

    /* 解析的 record 来源 */
    ripple_record*                      decode_record;
    ripple_queue*                       recordqueue;

    /* 保存完整事务的缓存 */
    ripple_cache_txn*                   parser2txns;

    /* 大事务缓存 */
    ripple_cache_txn*                   parser2bigtxns;

    /* 上层结构 */
    void*                               privdata;                   /* 内容为: ripple_increment_capture*/
    ripple_decodingctx_privdatacallback callback;
} ripple_decodingcontext;

/* 是否要兑换ddl true兑换 */
static inline bool ripple_decodingcontext_isddlfilter(bool filter, bool redo)
{
    return (false == filter && false == redo);
}

/* 是否过滤stmt true过滤 */
static inline bool ripple_decodingcontext_isstmtsfilter(bool filter, bool redo)
{
    return (true == redo || true == filter);
}

void ripple_parserwork_waldecode(ripple_decodingcontext* decodingctx);

void ripple_decode_getparserlsn(XLogRecPtr* plsn);

#endif
