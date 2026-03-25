#ifndef ONLINEREFRESH_CAPTUREFLUSH_H
#define ONLINEREFRESH_CAPTUREFLUSH_H

typedef struct ONLINEREFRESH_CAPTUREFLUSH
{
    int           fd;
    uint64        maxsize;
    uint64        trailno;
    char*         trail; /* Trail file storage position */
    file_buffers* txn2filebuffer;
} onlinerefresh_captureflush;

extern onlinerefresh_captureflush* onlinerefresh_captureflush_init(void);

extern void* onlinerefresh_captureflush_main(void* args);
extern void  onlinerefresh_captureflush_free(void* args);

#endif
