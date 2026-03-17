#ifndef _METRIC_CAPTURE_H
#define _METRIC_CAPTURE_H


typedef struct METRIC_CAPTURE
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
    char                padding[CACHELINE_SIZE];
    void                (*addonlinerefresh)(void* privdata, void* rtables);
    void*               privdata;
    char                padding1[CACHELINE_SIZE];
} metric_capture;

/* 状态主线程 */
extern void* metric_capture_main(void *args);


extern metric_capture* metric_capture_init(void);

extern void metric_capture_addpackets(metric_capture* mcapture, netpacket* npacket);


/* 缓存清理 */
extern void metric_capture_destroy(metric_capture* state);

#endif
