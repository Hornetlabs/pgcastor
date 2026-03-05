#ifndef _RIPPLE_ONLINEREFRESH_SHARDINGNET_H
#define _RIPPLE_ONLINEREFRESH_SHARDINGNET_H

typedef struct RIPPLE_TASK_ONLINEREFRESH_SHARDINGNET_CALLBACK
{
    /* onlinerefresh--设置管理线程状态为RESET */
    void (*setreset)(void* privdata);

} ripple_onlinerefresh_shardingnet_callback;

typedef struct RIPPLE_TASK_ONLINEREFRESH_SHARDINGNET
{
    ripple_uuid_t                               onlinerefreshno;
    ripple_refresh_table_syncstats*             syncstats;
    ripple_queue*                               taskqueue;
    void*                                       privdata;
    ripple_onlinerefresh_shardingnet_callback   callback;               /* 在没有增量的情况下设置管理线程为reset */
} ripple_onlinerefresh_shardingnet;

ripple_onlinerefresh_shardingnet *ripple_onlinerefresh_shardingnet_init(void);

void* ripple_onlinerefresh_shardingnet_main(void* args);

void ripple_onlinerefresh_shardingnet_free(void* args);

#endif