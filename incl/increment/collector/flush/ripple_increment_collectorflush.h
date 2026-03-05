#ifndef _RIPPLE_INCREMENT_COLLECTORFLUSH_H
#define _RIPPLE_INCREMENT_COLLECTORFLUSH_H

typedef enum RIPPLE_INCREMENT_COLLECTORFLUSHNODE_STAT
{
    RIPPLE_INCREMENT_COLLECTORFLUSHNODE_STAT_NOP            = 0x00,
    RIPPLE_INCREMENT_COLLECTORFLUSHNODE_STAT_INIT           ,                   /* 等待启动 */
    RIPPLE_INCREMENT_COLLECTORFLUSHNODE_STAT_WORK           ,                   /* 启动后 */
}ripple_increment_collectornode_stat;

typedef struct RIPPLE_INCREMENT_COLLECTORFLUSH_CALLBACK
{
    /* 设置collectorstate写入到文件中的 lsn */
    void (*setmetricflushlsn)(void* privdata, char* pumpname, XLogRecPtr flushlsn);

    /* 设置collectorstate落盘的 trail 文件编号 */
    void (*setmetricflushtrailno)(void* privdata, char* pumpname, uint64 flushtrailno);

    /* 设置collectorstate落盘的 trail 文件内的偏移 */
    void (*setmetricflushtrailstart)(void* privdata, char* pumpname, uint64 flushtrailstart);

    /* 设置collectorstate落盘的 的时间戳 */
    void (*setmetricflushtimestamp)(void* privdata, char* pumpname, TimestampTz flushtimestamp);

    /* 设置collectorstate写入到文件中的 lsn */
    void (*collector_filetransfernode_add)(void* privdata, void* filetransfernode);
} ripple_increment_collectorflush_callback;

typedef struct RIPPLE_INCREMENT_COLLECTORFLUSH
{
    int                                         fd;
    int                                         basefd;                 /* base 对应的文件描述符 */
    uint64                                      maxsize;
    uint64                                      fileid;
    bool                                        upload;
    char                                        name[128];
    char                                        path[RIPPLE_MAXPATH];
    ripple_collectorbase                        collectorbase;
    void*                                       privdata;               /* 内容为: ripple_collectorstate*/
    ripple_file_buffers*                        netdata2filebuffer;
    ripple_increment_collectorflush_callback    callback;
} ripple_increment_collectorflush;

typedef struct RIPPLE_INCREMENT_COLLECTORFLUSHNODE
{
    char                                    name[128];
    ripple_increment_collectornode_stat     stat;
    ripple_increment_collectorflush*        flush;
}ripple_increment_collectorflushnode;

/*
 * 写主进程
*/
void* ripple_increment_collectorflush_main(void *args);

/* 初始化flushnode */
ripple_increment_collectorflushnode* ripple_increment_collectorflushnode_init(char* name);

/* 初始化 */
ripple_increment_collectorflush* ripple_increment_collectorflush_init(void);

/* 资源回收 */
void ripple_increment_collectorflush_destroy(ripple_increment_collectorflush* cflush);

/* flushnode资源回收 */
void ripple_increment_collectorflushnode_destroy(void* args);

#endif
