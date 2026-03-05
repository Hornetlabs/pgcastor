#ifndef _RIPPLE_INCREMENT_PUMPPARSERTRAIL_H
#define _RIPPLE_INCREMENT_PUMPPARSERTRAIL_H

typedef struct RIPPLE_INCREMENT_PUMPPARSERTRAIL_CALLBACK
{
    /* 设置 拆分trail状态*/
    void (*splittrail_state_set)(void* privdata, int state);

    /* 设置序列化状态*/
    void (*serialstate_state_set)(void* privdata, int state);

    /* 回调注册onlinerefresh节点 */
    void (*addonlinerefresh)(void* pumpstate, void* onlinerefresh);

    /* 解析线程回调添加refresh */
    void (*addrefresh)(void* privdata, void* refresh);

    /* refresh 是否完成，启动refresh检测是否有refresh在运行 */
    bool (*isrefreshdown)(void* privdata);

    /* onlinerefresh获取filetransfer节点 */
    void* (*filetransfernode_get)(void* privdata);

    /* 添加filetransfer节点 */
    void (*filetransfernode_add)(void* privdata, void* filetransfernode);

    /* 添加大事务节点 */
    void (*bigtxn_add)(void* privdata, FullTransactionId xid, ripple_recpos* pos);
    
    /* 设置大事务结束位置*/
    bool (*bigtxn_end)(void* privdata, FullTransactionId xid, ripple_recpos* pos);

    /* 设置未接受到end的大事务 状态为abandon*/
    bool (*bigtxn_setabandon)(void* privdata);

    /* 设置未接受到end的onlinerefresh 状态为abandon 并返回uuid链表*/
    bool (*onlinerefresh_setabandon)(void* privdata, void** list);

    /* 设置onlinerefresh结束位置*/
    bool (*onlinerefresh_end)(void* privdata, void* in_uuid, ripple_recpos* pos);

    /* 设置pumpstate重组后事务的 lsn */
    void (*setmetricloadlsn)(void* privdata, XLogRecPtr loadlsn);

    /* 设置pumpstate重组后事务的 lsn */
    void (*setmetricloadtimestamp)(void* privdata, TimestampTz loadtimestamp);

} ripple_increment_pumpparser_callback;

typedef struct RIPPLE_INCREMENT_PUMPPARSERTRAIL
{
    ripple_parsertrail                          parsertrail;
    int                                         state;
    ripple_queue*                               recordscache;
    void*                                       privdata;
    ripple_increment_pumpparser_callback        callback;
}ripple_increment_pumpparsertrail;

ripple_increment_pumpparsertrail* ripple_increment_pumpparsertrail_init(void);

/* 设置 state 的值 */
void ripple_increment_pumpparsertrail_state_set(ripple_increment_pumpparsertrail* parsertrail,int state);

void* ripple_increment_pumpparsertrail_main(void* args);

void ripple_increment_pumpparsertrail_free(ripple_increment_pumpparsertrail* parsertrail);

#endif

