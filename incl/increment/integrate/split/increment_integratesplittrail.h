#ifndef _INCREMENT_INTEGRATESPLITTRAIL_H
#define _INCREMENT_INTEGRATESPLITTRAIL_H


typedef struct INCREMENT_INTEGRATESPLITTRAIL_CALLBACK
{
    /* 设置integratestate在trail文件中读取的lsn*/
    void (*setmetricloadtrailno)(void* privdata, uint64 loadtrailno);

    /* 设置integratestate在trail文件中读取的lsn*/
    void (*setmetricloadtrailstart)(void* privdata, uint64 loadtrailstart);

} increment_integratesplittrail_callback;

typedef struct INCREMENT_INTEGRATESPLITTRAIL
{
    loadtrailrecords*                        loadrecords;
    bool                                            filter;                 /* 标识是否过滤record */
    int                                             state;
    uint64                                          emitoffset;             /* 应用的起点 */
    char*                                           capturedata;
    void*                                           privdata;
    queue*                                   recordscache;
    increment_integratesplittrail_callback   callback;
}increment_integratesplittrail;

void increment_integratesplittrail_state_set(increment_integratesplittrail* splittrail, int state);

void increment_integratesplittrail_emit_set(increment_integratesplittrail* splittrail, uint64 fileid, uint64 emitoffset);

increment_integratesplittrail* increment_integratesplittrail_init(void);

void* increment_integratesplitrail_main(void* args);

void increment_integratesplittrail_free(increment_integratesplittrail* splitrail);


#endif
