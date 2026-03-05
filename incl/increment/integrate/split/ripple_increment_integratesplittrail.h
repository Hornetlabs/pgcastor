#ifndef _RIPPLE_INCREMENT_INTEGRATESPLITTRAIL_H
#define _RIPPLE_INCREMENT_INTEGRATESPLITTRAIL_H


typedef struct RIPPLE_INCREMENT_INTEGRATESPLITTRAIL_CALLBACK
{
    /* 设置integratestate在trail文件中读取的lsn*/
    void (*setmetricloadtrailno)(void* privdata, uint64 loadtrailno);

    /* 设置integratestate在trail文件中读取的lsn*/
    void (*setmetricloadtrailstart)(void* privdata, uint64 loadtrailstart);

} ripple_increment_integratesplittrail_callback;

typedef struct RIPPLE_INCREMENT_INTEGRATESPLITTRAIL
{
    ripple_loadtrailrecords*                        loadrecords;
    bool                                            filter;                 /* 标识是否过滤record */
    int                                             state;
    uint64                                          emitoffset;             /* 应用的起点 */
    char*                                           capturedata;
    void*                                           privdata;
    ripple_queue*                                   recordscache;
    ripple_increment_integratesplittrail_callback   callback;
}ripple_increment_integratesplittrail;

void ripple_increment_integratesplittrail_state_set(ripple_increment_integratesplittrail* splittrail, int state);

void ripple_increment_integratesplittrail_emit_set(ripple_increment_integratesplittrail* splittrail, uint64 fileid, uint64 emitoffset);

ripple_increment_integratesplittrail* ripple_increment_integratesplittrail_init(void);

void* ripple_increment_integratesplitrail_main(void* args);

void ripple_increment_integratesplittrail_free(ripple_increment_integratesplittrail* splitrail);


#endif
