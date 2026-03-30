#ifndef _PARSERWORK_DECODE_H
#define _PARSERWORK_DECODE_H

typedef enum DECODINGCONTEXT_STAT
{
    DECODINGCONTEXT_INIT = 0x00,
    DECODINGCONTEXT_REWIND,
    DECODINGCONTEXT_RUNNING,
    DECODINGCONTEXT_SET_PAUSE,
    DECODINGCONTEXT_PAUSE,
    DECODINGCONTEXT_RESUME
} decodingcontext_stat;

typedef pg_parser_translog_pre decodewalpre;

typedef struct DECODINGCONTEXT_PRIVDATACALLBACK
{
    /* Set splitwork split start and end points */
    void (*setloadlsn)(void* privdata, XLogRecPtr startlsn, XLogRecPtr endlsn);

    /* Set end lsn parsed by capturestate */
    void (*setmetricparselsn)(void* privdata, XLogRecPtr pareslsn);

    /* Set redo, restart, confirm lsn when rewind ends */
    void (*setparserlsn)(void* privdata, XLogRecPtr confirm, XLogRecPtr restart, XLogRecPtr redo);

    /* capturestate set redo, restart, confirm lsn */
    void (*setmetricsynclsn)(void* privdata, XLogRecPtr redolsn, XLogRecPtr restartlsn, XLogRecPtr confirmlsn);

    /* Set capturestate timestamp */
    void (*setmetricparsetimestamp)(void* privdata, TimestampTz parsetimestamp);

} decodingctx_privdatacallback;

typedef struct DECODINGCONTEXT
{
    /* Used to indicate whether to filter big transactions, true filter, false not filter */
    bool                         filterbigtrans;

    /* Status value, rewind, normal */
    int32                        stat;
    capturebase                  base;
    decodewalpre                 walpre;

    /* Parsed database identifier */
    Oid                          database;

    /* Parsed lsn, default value is 2, refresh transaction lsn is 1 */
    XLogRecPtr                   parselsn;

    /* Conversion information, numeric(show lc_numeric), money(show lc_monetary
     * ), source/target charset(pg_database->encoding), timezone information(show timezone) */
    char*                        tzname;
    char*                        monetary;
    char*                        numeric;
    char*                        orgdbcharset;
    char*                        tgtdbcharset;
    rewind_info*                 rewind_ptr;
    txn*                         refreshtxn;
    dlist*                       onlinerefresh;
    transcache*                  trans_cache;

    /* Source of parsed record */
    record*                      decode_record;
    queue*                       recordqueue;

    /* Cache for saving complete transactions */
    cache_txn*                   parser2txns;

    /* Big transaction cache */
    cache_txn*                   parser2bigtxns;

    /* Upper layer structure */
    void*                        privdata; /* Content is: increment_capture*/
    decodingctx_privdatacallback callback;
} decodingcontext;

/* Whether to convert ddl true convert */
static inline bool decodingcontext_isddlfilter(bool filter, bool redo)
{
    return (false == filter && false == redo);
}

/* Whether to filter stmt true filter */
static inline bool decodingcontext_isstmtsfilter(bool filter, bool redo)
{
    return (true == redo || true == filter);
}

void parserwork_waldecode(decodingcontext* decodingctx);

void decode_getparserlsn(XLogRecPtr* plsn);

#endif
