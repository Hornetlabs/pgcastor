#ifndef RIPPLE_ONLINEREFRESH_CAPTUREFLUSH_H
#define RIPPLE_ONLINEREFRESH_CAPTUREFLUSH_H

typedef struct RIPPLE_ONLINEREFRESH_CAPTUREFLUSH
{
    int                     fd;
    uint64                  maxsize;
    uint64                  trailno;
    char                   *trail;      /* trail 文件存储位置 */
    ripple_file_buffers    *txn2filebuffer;
} ripple_onlinerefresh_captureflush;

extern ripple_onlinerefresh_captureflush *ripple_onlinerefresh_captureflush_init(void);

extern void *ripple_onlinerefresh_captureflush_main(void *args);
extern void ripple_onlinerefresh_captureflush_free(void *args);

#endif
