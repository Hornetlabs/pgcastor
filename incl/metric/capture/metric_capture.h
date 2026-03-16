#ifndef _RIPPLE_METRIC_CAPTURE_H
#define _RIPPLE_METRIC_CAPTURE_H


typedef struct RIPPLE_METRIC_CAPTURE
{
    XLogRecPtr          redolsn;
    XLogRecPtr          restartlsn;
    XLogRecPtr          confirmlsn;
    XLogRecPtr          loadlsn;
    XLogRecPtr          parselsn;
    XLogRecPtr          flushlsn;
    uint64              trailno;
    uint64              trailstart;
    uint64              parsetimestamp;
    uint64              flushtimestamp;

    pthread_mutex_t     dlpacketslock;
    dlist*              dlpackets;
    char                padding[RIPPLE_CACHELINE_SIZE];
    void                (*addonlinerefresh)(void* privdata, void* rtables);
    void*               privdata;
    char                padding1[RIPPLE_CACHELINE_SIZE];
} ripple_metric_capture;

/* 状态主线程 */
extern void* ripple_metric_capture_main(void *args);


extern ripple_metric_capture* ripple_metric_capture_init(void);

extern void ripple_metric_capture_addpackets(ripple_metric_capture* mcapture, ripple_netpacket* npacket);


/* 缓存清理 */
extern void ripple_metric_capture_destroy(ripple_metric_capture* state);

#endif
