#ifndef _INCREMENT_CAPTUREFLUSH_H
#define _INCREMENT_CAPTUREFLUSH_H

typedef struct INCREMENT_CAPTUREFLUSH_CALLBACK
{
    /* Set capturestate lsn written to file */
    void (*setmetricflushlsn)(void* privdata, XLogRecPtr flushlsn);

    /* Set status thread trail file number */
    void (*setmetrictrailno)(void* privdata, uint64 trailno);

    /* Set status thread trail offset within file */
    void (*setmetrictrailstart)(void* privdata, uint64 trailstart);

    /* Set status thread timestamp */
    void (*setmetricflushtimestamp)(void* privdata, TimestampTz flushtimestamp);

} increment_captureflush_callback;

typedef struct INCREMENT_CAPTUREFLUSH
{
    int          fd;
    int          basefd; /* File descriptor corresponding to base */
    uint64       maxsize;
    uint64       fileid; /* File number written to      */
    char         path[MAXPATH];
    txnscontext* txnsctx; /*
                           * System dictionary
                           *  Only use system dictionary
                           */
    capturebase                     base;
    file_buffers*                   txn2filebuffer;
    void*                           privdata; /* Content is: increment_capture*/
    increment_captureflush_callback callback;
} increment_captureflush;

/* Write data */
void* increment_captureflush_main(void* args);

increment_captureflush* increment_captureflush_init(void);

void increment_captureflush_destroy(increment_captureflush* wstates);

#endif
