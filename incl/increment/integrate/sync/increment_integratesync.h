#ifndef _INCREMENT_INTEGRATESYNC_H
#define _INCREMENT_INTEGRATESYNC_H

typedef enum INCREMENT_INTEGRATESYNC_STATE
{
    INCREMENT_INTEGRATESYNC_STATE_NOP = 0x00,
    INCREMENT_INTEGRATESYNC_STATE_IDLE, /* sync cannot get transaction, idle state
                                           onlinerefresh needs to wait for sync to be idle state
                                           when starting (to ensure previous transactions are
                                           completed)
                                         */
    INCREMENT_INTEGRATESYNC_STATE_WORK  /* sync gets transaction, working status, working status does
                                           not start onlinerefresh */
} increment_integratesync_state;

typedef struct INCREMENT_INTEGRATESYNCSTATE_PRIVDATACALLBACK
{
    /* Set splittrail fileid and working status */
    void (*splittrail_fileid_emitoffse_set)(void* privdata, uint64 fileid, uint64 emitoffset);

    /* Set integratestate lsn synced to database */
    void (*setmetricsynclsn)(void* privdata, XLogRecPtr synclsn);

    /* Set integratestate trail file number synced to database*/
    void (*setmetricsynctrailno)(void* privdata, uint64 fileid);

    /* Set integratestate trail file offset synced to database */
    void (*setmetricsynctrailstart)(void* privdata, uint64 fileid);

    /* Set integratestate timestamp synced to database */
    void (*setmetricsynctimestamp)(void* privdata, TimestampTz synctimestamp);

    /* Whether refresh is complete, start refresh check if refresh is running */
    bool (*integratestate_isrefreshdown)(void* privdata);

    /* Set rebuild thread filterlsn */
    void (*integratestate_rebuildfilter_set)(void* privdata, XLogRecPtr lsn);

} increment_integratesyncstate_privdatacallback;

typedef struct INCREMENT_INTEGRATESYNCSTATE
{
    syncstate                                     base;
    int                                           state;
    uint64                                        lsn;
    uint64                                        trailno;
    recpos                                        rewind;
    cache_txn*                                    rebuild2sync;
    void*                                         privdata; /* Content is: intergratestate*/
    increment_integratesyncstate_privdatacallback callback;
} increment_integratesyncstate;

increment_integratesyncstate* increment_integratesync_init(void);

void increment_integratesync_destroy(increment_integratesyncstate* syncworkstate);

void* increment_integratesync_main(void* args);

#endif
