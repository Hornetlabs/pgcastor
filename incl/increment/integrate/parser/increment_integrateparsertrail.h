#ifndef _INCREMENT_INTEGRATEPARSERTRAIL_H
#define _INCREMENT_INTEGRATEPARSERTRAIL_H

typedef struct INCREMENT_INTEGRATEPARSERTRAIL_CALLBACK
{
    /* 解析线程回调添加refresh */
    void (*integratestate_addrefresh)(void* privdata, void* refresh);

    /* refresh 是否完成，启动refresh检测是否有refresh在运行 */
    bool (*integratestate_isrefreshdown)(void* privdata);

    /* 设置integratestate重组后事务的 lsn */
    void (*setmetricloadlsn)(void* privdata, XLogRecPtr loadlsn);

    /* 设置integratestate重组后事务 timestamp */
    void (*setmetricloadtimestamp)(void* privdata, TimestampTz loadtimestamp);

} increment_integrateparsertrail_callback;

typedef struct INCREMENT_INTEGRATEPARSERTRAIL
{
    parsertrail                              parsertrail;
    int                                             state;
    queue*                                   recordscache;
    void*                                           privdata;
    increment_integrateparsertrail_callback  callback;
}increment_integrateparsertrail;

void* increment_integrateparsertrail_main(void *args);

increment_integrateparsertrail* increment_integrateparsertrail_init(void);

void increment_integrateparsertrail_free(increment_integrateparsertrail* parsertrail);

#endif
