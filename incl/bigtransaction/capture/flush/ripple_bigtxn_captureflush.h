#ifndef _RIPPLE_BIGTXN_CAPTUREFLUSH_H
#define _RIPPLE_BIGTXN_CAPTUREFLUSH_H

typedef struct RIPPLE_BIGTXN_CAPTUREFLUSH_file
{
    FullTransactionId               xid;
    int                             fd;
    uint64                          fileid;
} ripple_bigtxn_captureflush_file;

/* 写数据 */
void* ripple_bigtxn_captureflush_main(void *args);

ripple_increment_captureflush* ripple_bigtxn_captureflush_init(void);

void ripple_bigtxn_captureflush_destroy(ripple_increment_captureflush* wstates);

#endif
