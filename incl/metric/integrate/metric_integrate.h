#ifndef _METRIC_INTEGRATE_H
#define _METRIC_INTEGRATE_H


typedef struct METRIC_INTEGRATE
{
    XLogRecPtr              loadlsn;
    XLogRecPtr              synclsn;
    uint64                  loadtrailno;
    uint64                  loadtrailstart;
    uint64                  synctrailno;
    uint64                  synctrailstart;
    TimestampTz             loadtimestamp;
    TimestampTz             synctimestamp;
} metric_integrate;


void* metric_integrate_main(void *args);

metric_integrate* metric_integrate_init(void);

void metric_integrate_destroy(void* args);

#endif