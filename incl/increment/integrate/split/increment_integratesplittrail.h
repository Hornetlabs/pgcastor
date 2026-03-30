#ifndef _INCREMENT_INTEGRATESPLITTRAIL_H
#define _INCREMENT_INTEGRATESPLITTRAIL_H

typedef struct INCREMENT_INTEGRATESPLITTRAIL_CALLBACK
{
    /* Set integratestate lsn read from trail file*/
    void (*setmetricloadtrailno)(void* privdata, uint64 loadtrailno);

    /* Set integratestate lsn read from trail file*/
    void (*setmetricloadtrailstart)(void* privdata, uint64 loadtrailstart);

} increment_integratesplittrail_callback;

typedef struct INCREMENT_INTEGRATESPLITTRAIL
{
    loadtrailrecords*                      loadrecords;
    bool                                   filter; /* Flag whether to filter record */
    int                                    state;
    uint64                                 emitoffset; /* Application starting point */
    char*                                  capturedata;
    void*                                  privdata;
    queue*                                 recordscache;
    increment_integratesplittrail_callback callback;
} increment_integratesplittrail;

void increment_integratesplittrail_state_set(increment_integratesplittrail* splittrail, int state);

void increment_integratesplittrail_emit_set(increment_integratesplittrail* splittrail,
                                            uint64                         fileid,
                                            uint64                         emitoffset);

increment_integratesplittrail* increment_integratesplittrail_init(void);

void* increment_integratesplitrail_main(void* args);

void increment_integratesplittrail_free(increment_integratesplittrail* splitrail);

#endif
