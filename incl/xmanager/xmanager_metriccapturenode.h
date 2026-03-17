#ifndef _XMANAGER_METRICCAPTURENODE_H_
#define _XMANAGER_METRICCAPTURENODE_H_

#define REFRESH_METRICCAPTURE_INFOCNT  10

typedef struct XMANAGER_METRICCAPTURENODE
{
    xmanager_metricnode              base;
    XLogRecPtr                              redolsn;
    XLogRecPtr                              restartlsn;
    XLogRecPtr                              confirmlsn;
    XLogRecPtr                              loadlsn;
    XLogRecPtr                              parselsn;
    XLogRecPtr                              flushlsn;
    uint64                                  trailno;
    uint64                                  trailstart;
    int64                                   parsetimestamp;
    int64                                   flushtimestamp;
} xmanager_metriccapturenode;


/* 初始化 node 节点 */
extern xmanager_metricnode* xmanager_metriccapturenode_init(void);

/* 清理 metric capture 节点内存 */
extern void xmanager_metriccapturenode_destroy(xmanager_metricnode* metricnode);

/* 将 capture node 节点序列化 */
extern bool xmanager_metriccapturenode_serial(xmanager_metricnode* metricnode,
                                                     uint8** blk,
                                                     int* blksize,
                                                     int* blkstart);

/* 反序列化为 capture node 节点 */
extern xmanager_metricnode* xmanager_metriccapturenode_deserial(uint8* blk, int* blkstart);

/* capture info 组装 */
extern void* xmanager_metricmsg_assemblecapture(xmanager_metricnode* pxmetricnode);

#endif
