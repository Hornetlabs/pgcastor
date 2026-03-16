#ifndef _RIPPLE_INCREMENT_CAPTURE_H_
#define _RIPPLE_INCREMENT_CAPTURE_H_

typedef void (*ripple_increment_callback)(ripple_decodingcontext* ctx);

typedef struct RIPPLE_INCREMENT_CAPTURE
{
    uint64                                  persistno;
    char                                    padding[RIPPLE_CACHELINE_SIZE];
    ripple_splitwalcontext*                 splitwalctx;
    char                                    padding1[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                           recordsqueue;
    char                                    padding2[RIPPLE_CACHELINE_SIZE];
    ripple_decodingcontext*                 decodingctx;
    char                                    padding3[RIPPLE_CACHELINE_SIZE];
    ripple_cache_txn*                       parser2serialtxns;
    char                                    padding4[RIPPLE_CACHELINE_SIZE];
    ripple_increment_captureserialstate*    serialstate;
    char                                    padding5[RIPPLE_CACHELINE_SIZE];
    ripple_file_buffers*                    txn2filebuffer;
    char                                    padding6[RIPPLE_CACHELINE_SIZE];
    ripple_increment_captureflush*          writestate;
    char                                    padding7[RIPPLE_CACHELINE_SIZE];
    pthread_mutex_t                         olrefreshlock;
    dlist*                                  olrefreshing;
    /* 待做 onlinerefresh 的表 */
    dlist*                                  olrefreshtables;
    char                                    padding8[RIPPLE_CACHELINE_SIZE];
    ripple_metric_capture*                  metric;
    char                                    padding9[RIPPLE_CACHELINE_SIZE];
    ripple_bigtxn_captureserial*            bigtxnserialstate;
    char                                    padding10[RIPPLE_CACHELINE_SIZE];
    ripple_increment_captureflush*          bigtxnwritestate;
    char                                    padding11[RIPPLE_CACHELINE_SIZE];
    ripple_threads*                         threads;                                /* 线程管理 */
    char                                    padding12[RIPPLE_CACHELINE_SIZE];
} ripple_increment_capture;

void ripple_increment_capture_parserwal_rewindingstat_setemiting(void* privdata);

void ripple_increment_capture_splitwal_lsn_set(void* privdata, XLogRecPtr startlsn, XLogRecPtr endlsn);

void ripple_increment_capture_writestate_set_trail(void* privdata, char* trail);

ripple_increment_capture* ripple_increment_capture_init(void);

void ripple_increment_capture_destroy(ripple_increment_capture* capturestate);

TimeLineID ripple_increment_capture_parserstat_curtlid_get(void* privdata);

#endif
