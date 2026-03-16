#ifndef _RIPPLE_INCREMENT_CAPTURESERIAL_H
#define _RIPPLE_INCREMENT_CAPTURESERIAL_H


typedef enum RIPPLE_INCREMENT_CAPTURESERIAL_STATE
{
    RIPPLE_INCREMENT_CAPTURESERIAL_STATE_NOP         = 0x00,
    RIPPLE_INCREMENT_CAPTURESERIAL_STATE_WORKING     = 0x01,
    RIPPLE_INCREMENT_CAPTURESERIAL_STATE_TERM        = 0x02
} ripple_increment_captureserial_state;

typedef struct RIPPLE_INCREMENT_CAPTURESERIAL_CALLBACK
{
    /* capture 获取timeline */
    TimeLineID (*parserstat_curtlid_get)(void* privdata);

} ripple_increment_captureserial_callback;

typedef struct RIPPLE_INCREMENT_CAPTURESERIALSTATE
{
    ripple_serialstate                          base;
    int                                         state;
    TimeLineID                                  curtlid;
    XLogRecPtr                                  redolsn;
    XLogRecPtr                                  restartlsn;
    XLogRecPtr                                  confirmlsn;
    ripple_cache_txn*                           parser2serialtxns;
    ripple_transcache*                          dictcache;
    void*                                       privdata;
    List*                                       redosysdicts;                   /* 两个 checkpoint 之间的系统表变更 */
    List*                                       onlinerefreshdataset;
    ripple_increment_captureserial_callback     callback;
} ripple_increment_captureserialstate;

void* ripple_increment_captureserial_main(void *args);

ripple_increment_captureserialstate* ripple_increment_captureserial_init(void);

char* ripple_increment_captureserial_getdbname(void* captureserial, Oid oid);

Oid ripple_increment_captureserial_getdboid(void* captureserial);

void* ripple_increment_captureserial_getnamespace(void* captureserial, Oid oid);

void* ripple_increment_captureserial_getclass(void* captureserial, Oid oid);

void* ripple_increment_captureserial_getattributes(void* captureserial, Oid oid);

void* ripple_increment_captureserial_gettype(void* captureserial, Oid oid);

void ripple_increment_captureserial_transcatalog2transcache(void* captureserial, void* catalog);

void ripple_increment_captureserial_ffsmgr_setcallback(ripple_increment_captureserialstate* wstate);

void  ripple_increment_captureserial_destroy(ripple_increment_captureserialstate* captureserialstate);

#endif
