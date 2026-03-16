#ifndef _RIPPLE_REFRESH_CAPTURE_H
#define _RIPPLE_REFRESH_CAPTURE_H

/* refresh 线程控制器结构体 */
typedef struct RIPPLE_REFRESH_CAPTURE
{
    int                         parallelcnt;
    char*                       conn_info;          /* 保存连接字符串 */
    char*                       snap_shot_name;     /* 保存快照名称 */
    char*                       refresh_path;       /* refresh文件夹路径 */
    ripple_thrsubmgr*           thrsmgr;
    PGconn*                     conn;               /* libpq连接句柄 */
    ripple_refresh_tables*      tables;             /* 待refresh表信息 */
    ripple_queue*               tqueue;             /* 任务队列 */
} ripple_refresh_capture;

extern ripple_refresh_capture *ripple_refresh_capture_init(void);

/* 设置快照名称 */
extern void ripple_refresh_capture_setsnapshotname(ripple_refresh_capture *rcapture, char *snapname);

extern void ripple_refresh_capture_setrefreshtables(ripple_refresh_tables* tables, ripple_refresh_capture *mgr);

extern void ripple_refresh_capture_setconn(PGconn* conn, ripple_refresh_capture *mgr);

extern void *ripple_refresh_capture_main(void* args);

extern void ripple_refresh_capture_free(void* privdata);

#endif
