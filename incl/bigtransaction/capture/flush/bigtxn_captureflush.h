#ifndef _BIGTXN_CAPTUREFLUSH_H
#define _BIGTXN_CAPTUREFLUSH_H

typedef struct BIGTXN_CAPTUREFLUSH_file
{
    FullTransactionId               xid;
    int                             fd;
    uint64                          fileid;
} bigtxn_captureflush_file;

/* 写数据 */
void* bigtxn_captureflush_main(void *args);

increment_captureflush* bigtxn_captureflush_init(void);

void bigtxn_captureflush_destroy(increment_captureflush* wstates);

#endif
