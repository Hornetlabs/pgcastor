#ifndef ONLINEREFRESH_CAPTURESERIAL_H
#define ONLINEREFRESH_CAPTURESERIAL_H

typedef struct TASK_ONLINEREFRESHCAPTURESERIAL_CALLBACK
{
    /* Capture get timeline */
    bool (*parserstat_curtlid_get)(void* privdata, TimeLineID* tlid);

} onlinerefresh_captureserial_callback;

typedef struct ONLINEREFRESH_CAPTURESERIAL
{
    serialstate*                         serialstate;
    cache_txn*                           parser2serialtxns;
    transcache*                          dictcache;
    void*                                privdata;
    onlinerefresh_captureserial_callback callback;
} onlinerefresh_captureserial;

extern onlinerefresh_captureserial* onlinerefresh_captureserial_init(void);

extern void* onlinerefresh_captureserial_main(void* args);
extern void  onlinerefresh_captureserial_free(void* args);
#endif
