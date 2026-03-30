#ifndef _SPLITWAL_WORK_H
#define _SPLITWAL_WORK_H

typedef enum splitwork_wal_status
{
    SPLITWORK_WAL_STATUS_INIT = 0,
    SPLITWORK_WAL_STATUS_REWIND,
    SPLITWORK_WAL_STATUS_NORMAL
} splitwork_wal_status;

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

extern void* splitwork_wal_main(void* args);
extern splitwalcontext* splitwal_init(void);
extern void splitwal_destroy(splitwalcontext* split_wal_ctx);
extern void* onlinerefresh_captureloadrecord_main(void* args);
extern void onlinerefresh_captureloadrecord_free(void* args);

#endif
