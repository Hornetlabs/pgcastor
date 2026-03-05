#ifndef RIPPLE_FASTCOMPARE_TABLESLICETASK_H
#define RIPPLE_FASTCOMPARE_TABLESLICETASK_H

typedef struct RIPPLE_FASTCOMPARE_TABLESLICETASK
{
    ripple_task                             task;
    PGconn*                                 chunkconn;  /* 数据库链接, 生成chunk用   */
    PGconn*                                 dataconn;   /* 数据库链接, 查询详细数据用 */
    char*                                   conninfo;   /* 连接字串                 */
    ripple_queue*                           slicequeue; /* 分片队列                 */
    ripple_netclient                        client;     /* 网络客户端模块            */
    ripple_fastcompare_tablecomparecatalog *catalog;
    uint8                                  *netresult;
} ripple_fastcompare_tableslicetask;

/* 线程处理入口 */
extern void *ripple_fastcompare_tableslicetask_main(void* args);
extern void ripple_fastcompare_tableslicetask_free(void* privdata);
extern ripple_fastcompare_tableslicetask *ripple_fastcompare_tableslicetask_init(void);

#endif
