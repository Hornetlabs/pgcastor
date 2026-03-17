#ifndef _XMANAGER_METRICPROGRESSNODE_H_
#define _XMANAGER_METRICPROGRESSNODE_H_


typedef struct XMANAGER_METRICprogressNODE
{
    xmanager_metricnode              base;
    dlist*                                  progressjop;    /* job metricnode */
} xmanager_metricprogressnode;


/* 初始化 node 节点 */
extern xmanager_metricnode* xmanager_metricprogressnode_init(void);

/* 清理 metric progress 节点内存 */
extern void xmanager_metricprogressnode_destroy(xmanager_metricnode* metricnode);

/* 将 progress node 节点序列化 */
extern bool xmanager_metricprogressnode_serial(xmanager_metricnode* metricnode,
                                                      uint8** blk,
                                                      int* blksize,
                                                      int* blkstart);

/* 反序列化为 progress node 节点 */
extern xmanager_metricnode* xmanager_metricprogressnode_deserial(uint8* blk, int* blkstart);

/* progress info 组装 */
extern void* xmanager_metricmsg_assembleprogress(xmanager_metric* xmetric, xmanager_metricnode* pxmetricnode);

#endif
