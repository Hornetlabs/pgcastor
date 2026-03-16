#ifndef RIPPLE_ONLINEREFRESH_CAPTURESERIAL_H
#define RIPPLE_ONLINEREFRESH_CAPTURESERIAL_H

typedef struct RIPPLE_TASK_ONLINEREFRESHCAPTURESERIAL_CALLBACK
{
    /* capture 获取timeline */
    bool (*parserstat_curtlid_get)(void* privdata, TimeLineID* tlid);

} ripple_onlinerefresh_captureserial_callback;

typedef struct RIPPLE_ONLINEREFRESH_CAPTURESERIAL
{
    ripple_serialstate*                                 serialstate;
    ripple_cache_txn*                                   parser2serialtxns;
    ripple_transcache*                                  dictcache;
    void*                                               privdata;
    ripple_onlinerefresh_captureserial_callback         callback;
} ripple_onlinerefresh_captureserial;

extern ripple_onlinerefresh_captureserial *ripple_onlinerefresh_captureserial_init(void);

extern void *ripple_onlinerefresh_captureserial_main(void *args);
extern void ripple_onlinerefresh_captureserial_free(void *args);
#endif
