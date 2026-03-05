#ifndef _RIPPLE_XMANAGER_METRICPUMPNODE_H_
#define _RIPPLE_XMANAGER_METRICPUMPNODE_H_

#define REFRESH_METRICPUMP_INFOCNT  8

typedef struct RIPPLE_XMANAGER_METRICPUMPNODE
{
    ripple_xmanager_metricnode              base;
    XLogRecPtr                              loadlsn;
    XLogRecPtr                              sendlsn;
    uint64                                  loadtrailno;
    uint64                                  loadtrailstart;
    uint64                                  sendtrailno;
    uint64                                  sendtrailstart;
    int64                                   loadtimestamp;
    int64                                   sendtimestamp;
} ripple_xmanager_metricpumpnode;

/* 初始化 node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metricpumpnode_init(void);

/* 清理 metric pump 节点内存 */
extern void ripple_xmanager_metricpumpnode_destroy(ripple_xmanager_metricnode* metricnode);

/* 将 pump node 节点序列化 */
extern bool ripple_xmanager_metricpumpnode_serial(ripple_xmanager_metricnode* metricnode,
                                                  uint8** blk,
                                                  int* blksize,
                                                  int* blkstart);

/* 反序列化为 pump node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metricpumpnode_deserial(uint8* blk, int* blkstart);

/* pump info 组装 */
extern void* ripple_xmanager_metricmsg_assemblepump(ripple_xmanager_metricnode* pxmetricnode);

#endif
