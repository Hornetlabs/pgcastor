#ifndef _SPLITWAL_WORK_H
#define _SPLITWAL_WORK_H

typedef enum splitwork_wal_status
{
    SPLITWORK_WAL_STATUS_INIT = 0,
    SPLITWORK_WAL_STATUS_REWIND,
    SPLITWORK_WAL_STATUS_NORMAL
} splitwork_wal_status;

// typedef struct SPLITWAL_INCOMPLETERECORD
// {
//     uint32      len;                /* Save original length */
//     uint32      incomplete_len;     /* Stored length */
//     XLogRecPtr  startlsn;           /* Starting lsn, first incomplete record at wal file start is
//     recorded as record end lsn */ char       *record;             /* record data */
// }splitwal_incompleteRecord;

// typedef struct SPLITWAL_PAGEBUFFER
// {
//     uint32              size;                   /* block size */
//     XLogRecPtr          startptr;               /* lsn where block starts */
//     char               *buf;                    /* Data */
//     splitwal_incompleteRecord   *incomplete;             /* (If exists) Last incomplete record of
//     block */
// }splitwal_pageBuffer;

typedef struct TimeLine2lsn
{
    TimeLineID timeline;
    XLogRecPtr lsn;
} TimeLine2lsn;

typedef struct timelineMAP
{
    uint32_t size;
    List*    map; /* Save TimeLine2lsn */
} timelineMAP;

// typedef struct SPLITWAL_WALREADCTL
// {
//     bool        wait;           /* Wait flag */
//     bool        change;         /* Status change flag */
//     bool        need_decrypt;
//     int         status;         /* Status information, rewind or normal */
//     int         fd;             /* File descriptor */
//     TimeLineID  timeline;       /* Timeline */
//     char       *inpath;         /* wal folder path */
//     XLogRecPtr  change_startptr; /* Starting lsn after state switch */
//     XLogRecPtr  startptr;       /* Starting lsn */
//     XLogRecPtr  endptr;         /* Ending lsn */
//     uint32      blcksz;         /* Block size of wal file */
//     uint32      walsz;          /* wal file size */
//     XLogRecPtr  prev;           /* lsn of last divided record */
//     XLogSegNo sendSegNo;
//     uint32 sendOff;
//     splitwal_incompleteRecord *seg_first_incomplete;      /* (If exists) First incomplete record
//     of current file*
//     */ splitwal_incompleteRecord *seg_first_incomplete_next; /* (If exists)
//     First incomplete record of next wal file*/
// } splitwal_WalReadCtl;

typedef struct SPLITWALCTX_PRIVDATACALLBACK
{
    /* Set parser status to emiting */
    void (*parserwal_rewindstat_setemiting)(void* privdata);

    /* Set capturestate split lsn */
    void (*capturestate_loadlsn_set)(void* privdata, XLogRecPtr splitls);

} splitwalctx_privdatacallback;

typedef struct SPLITWALCONTEXT
{
    void*                        privdata;        /* increment_capture */
    bool                         change;          /* Status change flag */
    int                          status;          /* Status information, rewind or normal */
    XLogRecPtr                   change_startptr; /* Starting lsn after state switch */
    queue*                       recordqueue;     /* Divided record cache */
    loadwalrecords*              loadrecords;     /* Controller for reading records */
    XLogRecPtr                   rewind_start;    /* rewind starting point */
    splitwalctx_privdatacallback callback;
} splitwalcontext;

extern void*            splitwork_wal_main(void* args);
extern splitwalcontext* splitwal_init(void);
extern void             splitwal_destroy(splitwalcontext* split_wal_ctx);
extern void*            onlinerefresh_captureloadrecord_main(void* args);
extern void             onlinerefresh_captureloadrecord_free(void* args);

#endif
