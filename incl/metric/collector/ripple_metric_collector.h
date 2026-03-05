#ifndef _RIPPLE_METRIC_COLLECTOR_H
#define _RIPPLE_METRIC_COLLECTOR_H

typedef struct RIPPLE_METRIC_COLLECTORNODE
{
    char                jobname[128];
    XLogRecPtr          recvlsn;                /* 接收到 pump 发送的 lsn */
    XLogRecPtr          flushlsn;               /* 持久到 trail 文件中的 lsn */
    uint64              recvtrailno;            /* 接收到的 trail 文件编号 */
    uint64              recvtrailstart;         /* 接收到的 trail 文件内的偏移 */
    uint64              flushtrailno;           /* 持久化到磁盘的 trail 文件编号 */
    uint64              flushtrailstart;        /* 持久化到磁盘的 trail 文件内的偏移 */
    TimestampTz         recvtimestamp;          /* 接收到 pump 发送的时间戳 */
    TimestampTz         flushtimestamp;         /* 持久化到 trail 文件中的时间戳 */
} ripple_metric_collectornode;

typedef struct RIPPLE_METRIC_COLLECTOR_STATE
{
    List*               pumps;
} ripple_metric_collector_state;


void* ripple_metric_collector_main(void *args);

ripple_metric_collector_state* ripple_metric_collector_init(void);

ripple_metric_collectornode* ripple_metric_collectornode_init(char* name);

bool ripple_state_collector_checkjobname(char* name);

void ripple_metric_collector_destroy(void* args);

#endif