#ifndef _RIPPLE_ONLINEREFRESH_PUMPNET_H
#define _RIPPLE_ONLINEREFRESH_PUMPNET_H

typedef struct RIPPLE_ONLINEREFRESH_PUMPNETSTATE_CALLBACK
{
    /* onlinerefresh--设置管理线程状态为RESET */
    void (*onlinerefresh_setreset)(void* privdata);

} ripple_onlinerefresh_pumpnetstate_callback;

typedef struct RIPPLE_ONLINEREFRESH_PUMPNETSTATE
{
    ripple_netclient        base;
    int                     state;                      /* 进程内线程状态变化 */
    ripple_uuid_t           onlinerefresh;
    ripple_recpos           recpos;
    ripple_recpos           crecpos;                    /* collector 文件起始编号 */
    ripple_file_buffers*    txn2filebuffer;
    void*                   privdata;
    ripple_onlinerefresh_pumpnetstate_callback callback;
} ripple_onlinerefresh_pumpnetstate;

typedef struct RIPPLE_TASK_ONLINEREFRESHPUMPWRITE
{
    ripple_onlinerefresh_pumpnetstate*      netstate;
} ripple_task_onlinerefreshpumpnet;


/* 初始化操作 */
extern ripple_onlinerefresh_pumpnetstate* ripple_onlinerefresh_pumpnetstate_init(void);

extern ripple_task_onlinerefreshpumpnet* ripple_onlinerefresh_pumpwrite_init(void);

/* 网络客户端 */
extern void* ripple_onlinerefresh_pumpnet_main(void* args);

extern void ripple_onlinerefresh_pumpnetstate_set_status(ripple_onlinerefresh_pumpnetstate* clientstate, int state);

extern void ripple_onlinerefresh_pumpnetstate_destroy(ripple_onlinerefresh_pumpnetstate* clientstate);

extern void ripple_onlinerefresh_pumpnet_free(void* args);

#endif

