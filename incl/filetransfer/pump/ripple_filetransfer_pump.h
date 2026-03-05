#ifndef _RIPPLE_INCREMENT_PUMPFILETRANSFER_H
#define _RIPPLE_INCREMENT_PUMPFILETRANSFER_H


typedef struct RIPPLE_FILETRANSFER_PUMP
{
    ripple_filetransfer_ftp*                    filetransfer;
    ripple_queue*                               filetransfernode;
} ripple_filetransfer_pump;

ripple_filetransfer_pump* ripple_filetransfer_pump_init(void);

void* ripple_filetransfer_pump_main(void *args);

void ripple_filetransfer_pump_free(ripple_filetransfer_pump* ftransfer);

#endif
