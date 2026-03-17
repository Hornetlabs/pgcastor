#ifndef _BIGTXN_CAPTURESERIAL_H
#define _BIGTXN_CAPTURESERIAL_H

typedef struct BIGTXN_CAPTURESERIAL_CALLBACK
{
    /* capture 获取timeline */
    TimeLineID (*bigtxn_parserstat_curtlid_get)(void* privdata);

} bigtxn_captureserial_callback;

/* 大事务序列化结构 */
typedef struct BIGTXN_CAPTURESERIAL
{
    /* 序列化主体结构 */
    serialstate                          base;

    /* 最近的txn */
    bigtxn*                              lasttxn;

    /* 大事务数据字典 */
    cache_sysdicts*                      dicts;

    /* 事务缓存，用于接收来自 increment_captureparser 线程的事务 */
    cache_txn*                           bigtxn2serial;

    /* padding */
    char                                        padding[CACHELINE_SIZE];

    /* 事务 hash */
    HTAB*                                       by_txns;

    /* padding */
    char                                        padding1[CACHELINE_SIZE];

    /* 存放上层指针 */
    void*                                       privdata;

    /* 获取时间线的回调函数 */
    bigtxn_captureserial_callback callback;
} bigtxn_captureserial;

/* 初始化 */
extern bigtxn_captureserial* bigtxn_captureserial_init(void);

/*
 * 逻辑处理主函数
*/
void* bigtxn_captureserial_main(void* args);

void bigtxn_captureserial_destroy(void* args);

#endif
