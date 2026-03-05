#ifndef _RIPPLE_INCREMENT_COLLECTORFILETRANSFER_H
#define _RIPPLE_INCREMENT_COLLECTORFILETRANSFER_H


typedef struct RIPPLE_INCREMENT_COLLECTORFILETRANSFER
{
    ripple_filetransfer_ftp*                    filetransfer;
    ripple_queue*                               filetransfernode;
} ripple_filetransfer_collector;

ripple_filetransfer_collector* ripple_filetransfer_collector_init(void);

void* ripple_filetransfer_collector_main(void *args);

void ripple_filetransfer_collector_free(void* args);

#endif
