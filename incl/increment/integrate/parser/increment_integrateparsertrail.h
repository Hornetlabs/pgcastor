#ifndef _INCREMENT_INTEGRATEPARSERTRAIL_H
#define _INCREMENT_INTEGRATEPARSERTRAIL_H

typedef struct INCREMENT_INTEGRATEPARSERTRAIL_CALLBACK
{
    /* Parse thread callback addrefresh */
    void (*integratestate_addrefresh)(void* privdata, void* refresh);

    /* Whether refresh is complete, start refresh check if refresh is running */
    bool (*integratestate_isrefreshdown)(void* privdata);

    /* Set integratestate lsn after transaction reorganization */
    void (*setmetricloadlsn)(void* privdata, XLogRecPtr loadlsn);

    /* Set integratestate timestamp after transaction reorganization */
    void (*setmetricloadtimestamp)(void* privdata, TimestampTz loadtimestamp);

} increment_integrateparsertrail_callback;

typedef struct INCREMENT_INTEGRATEPARSERTRAIL
{
    parsertrail                             parsertrail;
    int                                     state;
    queue*                                  recordscache;
    void*                                   privdata;
    increment_integrateparsertrail_callback callback;
} increment_integrateparsertrail;

void* increment_integrateparsertrail_main(void* args);

increment_integrateparsertrail* increment_integrateparsertrail_init(void);

void increment_integrateparsertrail_free(increment_integrateparsertrail* parsertrail);

#endif
