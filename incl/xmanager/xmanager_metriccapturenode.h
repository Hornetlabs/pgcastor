#ifndef _RIPPLE_XMANAGER_METRICCAPTURENODE_H_
#define _RIPPLE_XMANAGER_METRICCAPTURENODE_H_

#define REFRESH_METRICCAPTURE_INFOCNT  10

typedef struct RIPPLE_XMANAGER_METRICCAPTURENODE
{
    ripple_xmanager_metricnode              base;
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
} ripple_xmanager_metriccapturenode;


/* 初始化 node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metriccapturenode_init(void);

/* 清理 metric capture 节点内存 */
extern void ripple_xmanager_metriccapturenode_destroy(ripple_xmanager_metricnode* metricnode);

/* 将 capture node 节点序列化 */
extern bool ripple_xmanager_metriccapturenode_serial(ripple_xmanager_metricnode* metricnode,
                                                     uint8** blk,
                                                     int* blksize,
                                                     int* blkstart);

/* 反序列化为 capture node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metriccapturenode_deserial(uint8* blk, int* blkstart);

/* capture info 组装 */
extern void* ripple_xmanager_metricmsg_assemblecapture(ripple_xmanager_metricnode* pxmetricnode);

#endif
