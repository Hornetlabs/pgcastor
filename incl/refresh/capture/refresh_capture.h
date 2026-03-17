#ifndef _REFRESH_CAPTURE_H
#define _REFRESH_CAPTURE_H

/* refresh 线程控制器结构体 */
typedef struct REFRESH_CAPTURE
{
    int                         parallelcnt;
    char*                       conn_info;          /* 保存连接字符串 */
    char*                       snap_shot_name;     /* 保存快照名称 */
    char*                       refresh_path;       /* refresh文件夹路径 */
    thrsubmgr*           thrsmgr;
    PGconn*                     conn;               /* libpq连接句柄 */
    refresh_tables*      tables;             /* 待refresh表信息 */
    queue*               tqueue;             /* 任务队列 */
} refresh_capture;

extern refresh_capture *refresh_capture_init(void);

/* 设置快照名称 */
extern void refresh_capture_setsnapshotname(refresh_capture *rcapture, char *snapname);

extern void refresh_capture_setrefreshtables(refresh_tables* tables, refresh_capture *mgr);

extern void refresh_capture_setconn(PGconn* conn, refresh_capture *mgr);

extern void *refresh_capture_main(void* args);

extern void refresh_capture_free(void* privdata);

#endif
