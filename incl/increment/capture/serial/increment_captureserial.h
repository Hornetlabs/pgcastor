#ifndef _INCREMENT_CAPTURESERIAL_H
#define _INCREMENT_CAPTURESERIAL_H


typedef enum INCREMENT_CAPTURESERIAL_STATE
{
    INCREMENT_CAPTURESERIAL_STATE_NOP         = 0x00,
    INCREMENT_CAPTURESERIAL_STATE_WORKING     = 0x01,
    INCREMENT_CAPTURESERIAL_STATE_TERM        = 0x02
} increment_captureserial_state;

typedef struct INCREMENT_CAPTURESERIAL_CALLBACK
{
    /* capture 获取timeline */
    TimeLineID (*parserstat_curtlid_get)(void* privdata);

} increment_captureserial_callback;

typedef struct INCREMENT_CAPTURESERIALSTATE
{
    serialstate                          base;
    int                                         state;
    TimeLineID                                  curtlid;
    XLogRecPtr                                  redolsn;
    XLogRecPtr                                  restartlsn;
    XLogRecPtr                                  confirmlsn;
    cache_txn*                           parser2serialtxns;
    transcache*                          dictcache;
    void*                                       privdata;
    List*                                       redosysdicts;                   /* 两个 checkpoint 之间的系统表变更 */
    List*                                       onlinerefreshdataset;
    increment_captureserial_callback     callback;
} increment_captureserialstate;

void* increment_captureserial_main(void *args);

increment_captureserialstate* increment_captureserial_init(void);

char* increment_captureserial_getdbname(void* captureserial, Oid oid);

Oid increment_captureserial_getdboid(void* captureserial);

void* increment_captureserial_getnamespace(void* captureserial, Oid oid);

void* increment_captureserial_getclass(void* captureserial, Oid oid);

void* increment_captureserial_getattributes(void* captureserial, Oid oid);

void* increment_captureserial_gettype(void* captureserial, Oid oid);

void increment_captureserial_transcatalog2transcache(void* captureserial, void* catalog);

void increment_captureserial_ffsmgr_setcallback(increment_captureserialstate* wstate);

void  increment_captureserial_destroy(increment_captureserialstate* captureserialstate);

#endif
