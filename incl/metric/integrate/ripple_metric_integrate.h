#ifndef _RIPPLE_METRIC_INTEGRATE_H
#define _RIPPLE_METRIC_INTEGRATE_H


typedef struct RIPPLE_METRIC_INTEGRATE
{
    XLogRecPtr              loadlsn;
    XLogRecPtr              synclsn;
    uint64                  loadtrailno;
    uint64                  loadtrailstart;
    uint64                  synctrailno;
    uint64                  synctrailstart;
    TimestampTz             loadtimestamp;
    TimestampTz             synctimestamp;
} ripple_metric_integrate;


void* ripple_metric_integrate_main(void *args);

ripple_metric_integrate* ripple_metric_integrate_init(void);

void ripple_metric_integrate_destroy(void* args);

#endif