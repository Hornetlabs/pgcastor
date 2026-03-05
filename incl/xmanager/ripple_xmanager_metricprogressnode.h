#ifndef _RIPPLE_XMANAGER_METRICPROGRESSNODE_H_
#define _RIPPLE_XMANAGER_METRICPROGRESSNODE_H_


typedef struct RIPPLE_XMANAGER_METRICprogressNODE
{
    ripple_xmanager_metricnode              base;
    dlist*                                  progressjop;    /* job metricnode */
} ripple_xmanager_metricprogressnode;


/* 初始化 node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metricprogressnode_init(void);

/* 清理 metric progress 节点内存 */
extern void ripple_xmanager_metricprogressnode_destroy(ripple_xmanager_metricnode* metricnode);

/* 将 progress node 节点序列化 */
extern bool ripple_xmanager_metricprogressnode_serial(ripple_xmanager_metricnode* metricnode,
                                                      uint8** blk,
                                                      int* blksize,
                                                      int* blkstart);

/* 反序列化为 progress node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metricprogressnode_deserial(uint8* blk, int* blkstart);

/* progress info 组装 */
extern void* ripple_xmanager_metricmsg_assembleprogress(ripple_xmanager_metric* xmetric, ripple_xmanager_metricnode* pxmetricnode);

#endif
