#ifndef _STORAGE_FILE_BUFFER_H
#define _STORAGE_FILE_BUFFER_H

typedef enum FILE_BUFFER_FLAG
{
    FILE_BUFFER_FLAG_NOP = 0x00,
    FILE_BUFFER_FLAG_DATA = 0x01,   /* contains data, meaning data field has valid content */
    FILE_BUFFER_FLAG_REDO = 0x02,   /* buffer contains redolsn; extra data contains all
                                       system table changes from last checkpoint to current */
    FILE_BUFFER_FLAG_REWIND = 0x04, /* buffer contains restartlsn and confirmlsn; extra
                                       data contains restartlsn and confirmlsn */
    FILE_BUFFER_FLAG_ONLINREFRESHEND = 0x08,
    FILE_BUFFER_FLAG_ONLINREFRESH_DATASET = 0x10,
    FILE_BUFFER_FLAG_BIGTXNEND = 0x20, /* big transaction commit flag */
} file_buffer_flag;

#define INVALID_BUFFERID 0

typedef struct FILE_BUFFER_EXTRA_CHECKPOINT
{
    recpos redolsn;  /* redo LSN              */
    recpos orgaddr;  /* trail file info       */
    recpos segno;    /* segment number        */
    List*  sysdicts; /* system tables to apply */
} file_buffer_extra_checkpoint;

typedef struct FILE_BUFFER_EXTRA_REWIND
{
    recpos     restartlsn; /* LSN where parsing starts                          */
    recpos     confirmlsn; /* LSN where application starts                      */
    recpos     flushlsn;   /* LSN flushed to file                                */
    recpos     fileaddr;   /* file number / offset from file header for writing */
    TimeLineID curtlid;
} file_buffer_extra_rewind;

typedef struct FILE_BUFFER_EXTRA_ONLINEDATASET
{
    List* dataset; /* stores refresh_table */
} file_buffer_extra_onlinedataset;

typedef struct FILE_BUFFER_EXTRA
{
    file_buffer_extra_rewind        rewind;
    file_buffer_extra_checkpoint    chkpoint;
    file_buffer_extra_onlinedataset dataset;
    TimestampTz                     timestamp;
} file_buffer_extra;

typedef struct FILE_BUFFER
{
    bool                used;     /* flag indicating whether this buffer is in use */
    int                 bufid;    /* buffer identifier          */
    int                 flag;     /* flag information           */
    uint64              maxsize;  /* available space for data   */
    uint64              start;    /* usable offset in data      */
                                  /* flush thread checks if start is 0; if so, skips flush */
    file_buffer_extra   extra;    /* extra information          */
    void*               privdata; /* private data               */
                                  /* currently stores ff_fileinfo */
    uint8*              data;     /* data buffer                */
    struct FILE_BUFFER* next;     /* next node                  */
    struct FILE_BUFFER* tail;     /* last node                  */
} file_buffer;

typedef struct FILE_BUFFERS
{
    bool            flwsignal;  /* free list wait signal */
    bool            wflwsignal; /* flush list wait signal */
    int             maxbufid;   /* number of file_buffer entries in buffers */
    pthread_cond_t  flcond;
    pthread_cond_t  wflcond;
    pthread_mutex_t fllock;     /* free list lock */
    pthread_mutex_t wfllock;    /* flush list lock */
    file_buffer*    freelist;   /* free list                                  */
    file_buffer*    wflushlist; /* pending flush list                         */
    file_buffer*    buffers;    /* buffer pool                                */
} file_buffers;

/* initialize buffer pool */
file_buffers* file_buffer_init(void);

/* get buffer by bufid */
file_buffer* file_buffer_getbybufid(file_buffers* filebuffers, int bufid);

/* get an available buffer */
int file_buffer_get(file_buffers* filebuffers, int* timeout);

/* return buffer to free list */
void file_buffer_free(file_buffers* filebuffers, file_buffer* rfbuffer);

/*
 * copy buffer
 *  note: src->privdata will be set to NULL
 */
void file_buffer_copy(file_buffer* src, file_buffer* dst);

/* add buffer to pending flush queue */
void file_buffer_waitflush_add(file_buffers* filebuffers, file_buffer* fbuffer);

/* get buffer from pending flush queue */
file_buffer* file_buffer_waitflush_get(file_buffers* filebuffers, int* timeout);

void file_buffer_clean(file_buffers* filebuffers);

void file_buffer_destroy(file_buffers* filebuffers);

/* clean waitflush list */
void riple_file_buffer_clean_waitflush(file_buffers* filebuffers);

#endif
