#ifndef RIPPLE_INCREMENT_COLLECTOR_H
#define RIPPLE_INCREMENT_COLLECTOR_H

typedef struct RIPPLE_INCREMENT_COLLECTORNETBUFFERNODE
{
    char                        name[128];
    ripple_file_buffers*        netbuffer;
} ripple_increment_collectornetbuffernode;


typedef struct RIPPLE_INCREMENT_COLLECTOR
{
    uint64                                      persistno;
    char                                        padding[RIPPLE_CACHELINE_SIZE];
    ripple_increment_collectornetsvr*           netsvr;
    char                                        padding1[RIPPLE_CACHELINE_SIZE];
    pthread_mutex_t                             flushlock;
    char                                        padding2[RIPPLE_CACHELINE_SIZE];
    List*                                       netbuffers;
    char                                        padding3[RIPPLE_CACHELINE_SIZE];
    List*                                       flushthreads;
    char                                        padding4[RIPPLE_CACHELINE_SIZE];
    ripple_queue*                               filetransfernode;
    char                                        padding5[RIPPLE_CACHELINE_SIZE];
    ripple_filetransfer_collector*              ftptransfer;
    char                                        padding6[RIPPLE_CACHELINE_SIZE];
    ripple_threads*                             threads;                                /* 线程管理 */
    char                                        padding7[RIPPLE_CACHELINE_SIZE];
    ripple_metric_collector_state*               collectorstate;
    char                                        padding8[RIPPLE_CACHELINE_SIZE];
} ripple_increment_collector;

ripple_increment_collector* ripple_increment_collector_init(void);

ripple_file_buffers* ripple_increment_collector_netsvr_netbuffer_get(void* privdata, char* pumpname);

void ripple_increment_collector_cflush_fileid_get(void* privdata, char* name, uint64* pfileid, uint64* cfileid);

void ripple_increment_collector_destroy(ripple_increment_collector* collectorstate);


#endif
