#ifndef _INCREMENT_CAPTURE_H_
#define _INCREMENT_CAPTURE_H_

typedef void (*increment_callback)(decodingcontext* ctx);

typedef struct INCREMENT_CAPTURE
{
    uint64                        persistno;
    char                          padding[CACHELINE_SIZE];
    splitwalcontext*              splitwalctx;
    char                          padding1[CACHELINE_SIZE];
    queue*                        recordsqueue;
    char                          padding2[CACHELINE_SIZE];
    decodingcontext*              decodingctx;
    char                          padding3[CACHELINE_SIZE];
    cache_txn*                    parser2serialtxns;
    char                          padding4[CACHELINE_SIZE];
    increment_captureserialstate* serialstate;
    char                          padding5[CACHELINE_SIZE];
    file_buffers*                 txn2filebuffer;
    char                          padding6[CACHELINE_SIZE];
    increment_captureflush*       writestate;
    char                          padding7[CACHELINE_SIZE];
    pthread_mutex_t               olrefreshlock;
    dlist*                        olrefreshing;
    /* Tables to do onlinerefresh */
    dlist*                        olrefreshtables;
    char                          padding8[CACHELINE_SIZE];
    metric_capture*               metric;
    char                          padding9[CACHELINE_SIZE];
    bigtxn_captureserial*         bigtxnserialstate;
    char                          padding10[CACHELINE_SIZE];
    increment_captureflush*       bigtxnwritestate;
    char                          padding11[CACHELINE_SIZE];
    threads*                      threads; /* Thread management */
    char                          padding12[CACHELINE_SIZE];
} increment_capture;

void increment_capture_parserwal_rewindingstat_setemiting(void* privdata);

void increment_capture_splitwal_lsn_set(void* privdata, XLogRecPtr startlsn, XLogRecPtr endlsn);

void increment_capture_writestate_set_trail(void* privdata, char* trail);

increment_capture* increment_capture_init(void);

void increment_capture_destroy(increment_capture* capturestate);

TimeLineID increment_capture_parserstat_curtlid_get(void* privdata);

#endif
