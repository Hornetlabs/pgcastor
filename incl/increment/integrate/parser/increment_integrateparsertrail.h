#ifndef _RIPPLE_INCREMENT_INTEGRATEPARSERTRAIL_H
#define _RIPPLE_INCREMENT_INTEGRATEPARSERTRAIL_H

typedef struct RIPPLE_INCREMENT_INTEGRATEPARSERTRAIL_CALLBACK
{
    /* 解析线程回调添加refresh */
    void (*integratestate_addrefresh)(void* privdata, void* refresh);

    /* refresh 是否完成，启动refresh检测是否有refresh在运行 */
    bool (*integratestate_isrefreshdown)(void* privdata);

    /* 设置integratestate重组后事务的 lsn */
    void (*setmetricloadlsn)(void* privdata, XLogRecPtr loadlsn);

    /* 设置integratestate重组后事务 timestamp */
    void (*setmetricloadtimestamp)(void* privdata, TimestampTz loadtimestamp);

} ripple_increment_integrateparsertrail_callback;

typedef struct RIPPLE_INCREMENT_INTEGRATEPARSERTRAIL
{
    ripple_parsertrail                              parsertrail;
    int                                             state;
    ripple_queue*                                   recordscache;
    void*                                           privdata;
    ripple_increment_integrateparsertrail_callback  callback;
}ripple_increment_integrateparsertrail;

void* ripple_increment_integrateparsertrail_main(void *args);

ripple_increment_integrateparsertrail* ripple_increment_integrateparsertrail_init(void);

void ripple_increment_integrateparsertrail_free(ripple_increment_integrateparsertrail* parsertrail);

#endif
