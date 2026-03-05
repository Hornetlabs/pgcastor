#ifndef _RIPPLE_INCREMENT_PUMPSPLITTRAIL_H
#define _RIPPLE_INCREMENT_PUMPSPLITTRAIL_H


typedef struct RIPPLE_INCREMENT_PUMPSPLITTRAIL_CALLBACK
{
    /* 设置 parsertrail状态*/
    void (*parsertrail_state_set)(void* privdata, int state);

    /* 设置pumpstate在trail文件中读取的lsn*/
    void (*setmetricloadtrailstart)(void* privdata, uint64 loadtrailstart);

    /* 拆分线程添加filetransfer节点 */
    void (*pumpstate_filetransfernode_add)(void* privdata, void* filetransfernode);

} ripple_increment_pumpsplittrail_callback;

typedef struct RIPPLE_INCREMENT_PUMPSPLITTRAIL
{
    ripple_loadtrailrecords*                        loadrecords;
    bool                                            remote;                     /* 是否从服务器下载文件 */
    bool                                            filter;                     /* 标识是否过滤record */
    int                                             state;
    uint64                                          emitoffset;                 /* 网闸(gap) 使用       */
    void*                                           privdata;

    /* capture 的 data 目录 */
    char*                                           capturedata;
    char                                            jobname[128];
    ripple_queue*                                   recordscache;
    ripple_increment_pumpsplittrail_callback        callback;
}ripple_increment_pumpsplittrail;


/* refresh完成生成删除文件夹任务，加入到队列中 */
void ripple_increment_pumpsplittrail_deletedir_add(ripple_increment_pumpsplittrail* splittrail);

/* 重置解析的起点 */
void ripple_increment_pumpsplittrail_reset_position(ripple_increment_pumpsplittrail* splittrail, ripple_recpos* recpos);

ripple_increment_pumpsplittrail* ripple_increment_pumpsplittrail_init(void);

void ripple_increment_pumpsplittrail_state_set(ripple_increment_pumpsplittrail* splittrail, int state);

void* ripple_increment_pumpsplitrail_main(void* args);

void ripple_increment_pumpsplittrail_free(ripple_increment_pumpsplittrail* splitrail);

#endif
