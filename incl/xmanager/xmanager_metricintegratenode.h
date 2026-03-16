#ifndef _RIPPLE_XMANAGER_METRICINTEGRATENODE_H_
#define _RIPPLE_XMANAGER_METRICINTEGRATENODE_H_


#define REFRESH_METRICINTEGRATE_INFOCNT  8

typedef struct RIPPLE_XMANAGER_METRICINTEGRATENODE
{
    ripple_xmanager_metricnode              base;
    XLogRecPtr                              loadlsn;
    XLogRecPtr                              synclsn;
    uint64                                  loadtrailno;
    uint64                                  loadtrailstart;
    uint64                                  synctrailno;
    uint64                                  synctrailstart;
    int64                                   loadtimestamp;
    int64                                   synctimestamp;
} ripple_xmanager_metricintegratenode;


/* 初始化 node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metricintegratenode_init(void);

/* 清理 metric integrate 节点内存 */
extern void ripple_xmanager_metricintegratenode_destroy(ripple_xmanager_metricnode* metricnode);

/* 将 integrate node 节点序列化 */
extern bool ripple_xmanager_metricintegratenode_serial(ripple_xmanager_metricnode* metricnode,
                                                     uint8** blk,
                                                     int* blksize,
                                                     int* blkstart);

/* 反序列化为 integrate node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metricintegratenode_deserial(uint8* blk, int* blkstart);

/* integrate info 组装 */
extern void* ripple_xmanager_metricmsg_assembleintegrate(ripple_xmanager_metricnode* pxmetricnode);
#endif
