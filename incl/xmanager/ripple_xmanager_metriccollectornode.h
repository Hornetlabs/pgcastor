#ifndef _RIPPLE_XMANAGER_METRICCOLLECTORNODE_H_
#define _RIPPLE_XMANAGER_METRICCOLLECTORNODE_H_

#define REFRESH_METRICCOLLECTOR_INFOCNT  9

typedef struct RIPPLE_XMANAGER_METRICCOLLECTORINFO
{
    char                                    pumpname[128];
    XLogRecPtr                              recvlsn;
    XLogRecPtr                              flushlsn;
    uint64                                  recvtrailno;
    uint64                                  recvtrailstart;
    uint64                                  flushtrailno;
    uint64                                  flushtrailstart;
    int64                                   recvtimestamp;
    int64                                   flushtimestamp;
} ripple_xmanager_metriccollectorinfo;

typedef struct RIPPLE_XMANAGER_METRICCOLLECTORNODE
{
    ripple_xmanager_metricnode              base;
    dlist*                                  collectorinfo;
} ripple_xmanager_metriccollectornode;

/* 初始化 node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metriccollectornode_init(void);

/* 初始化 info */
extern ripple_xmanager_metriccollectorinfo* ripple_xmanager_metriccollectorinfo_init(void);

/* 清理 metric collector 节点内存 */
extern void ripple_xmanager_metriccollectornode_destroy(ripple_xmanager_metricnode* metricnode);

/* 清理 metric collectorinfo 内存 */
extern void ripple_xmanager_metriccollectorinfo_destroy(void* args);

extern int ripple_xmanager_metriccollectorinfo_cmp(void* s1, void* s2);

/* 将 collector node 节点序列化 */
extern bool ripple_xmanager_metriccollectornode_serial(ripple_xmanager_metricnode* metricnode,
                                                     uint8** blk,
                                                     int* blksize,
                                                     int* blkstart);

/* 反序列化为 collector node 节点 */
extern ripple_xmanager_metricnode* ripple_xmanager_metriccollectornode_deserial(uint8* blk, int* blkstart);

/* collector info 组装 */
extern void* ripple_xmanager_metricmsg_assemblecollector(ripple_xmanager_metricnode* pxmetricnode);

#endif
