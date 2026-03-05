#ifndef _RIPPLE_INCREMENT_CAPTUREFLUSH_H
#define _RIPPLE_INCREMENT_CAPTUREFLUSH_H


typedef struct RIPPLE_INCREMENT_CAPTUREFLUSH_CALLBACK
{
    /* 设置capturestate的写入到文件中的 lsn */
    void (*setmetricflushlsn)(void* privdata, XLogRecPtr flushlsn);

    /* 设置状态线程的trail 文件编号 */
    void (*setmetrictrailno)(void* privdata, uint64 trailno);

    /* 设置状态线程的trail 文件内的偏移 */
    void (*setmetrictrailstart)(void* privdata, uint64 trailstart);

    /* 设置状态线程的时间戳 */
    void (*setmetricflushtimestamp)(void* privdata, TimestampTz flushtimestamp);

} ripple_increment_captureflush_callback;

typedef struct RIPPLE_INCREMENT_CAPTUREFLUSH
{
    int                                     fd;
    int                                     basefd;                 /* base 对应的文件描述符 */
    uint64                                  maxsize;
    uint64                                  fileid;                 /* 写入到的文件编号      */
    char                                    path[RIPPLE_MAXPATH];
    ripple_txnscontext*                     txnsctx;                /* 
                                                                     * 系统字典
                                                                     *  只使用了系统字典
                                                                     */
    ripple_capturebase                      base;
    ripple_file_buffers*                    txn2filebuffer;
    void*                                   privdata;               /* 内容为: ripple_increment_capture*/
    ripple_increment_captureflush_callback  callback;
} ripple_increment_captureflush;

/* 写数据 */
void* ripple_increment_captureflush_main(void *args);

ripple_increment_captureflush* ripple_increment_captureflush_init(void);

void ripple_increment_captureflush_destroy(ripple_increment_captureflush* wstates);

#endif
