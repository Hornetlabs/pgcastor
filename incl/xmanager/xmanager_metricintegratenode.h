#ifndef _XMANAGER_METRICINTEGRATENODE_H_
#define _XMANAGER_METRICINTEGRATENODE_H_

#define REFRESH_METRICINTEGRATE_INFOCNT 8

typedef struct XMANAGER_METRICINTEGRATENODE
{
    xmanager_metricnode base;
    XLogRecPtr          loadlsn;
    XLogRecPtr          synclsn;
    uint64              loadtrailno;
    uint64              loadtrailstart;
    uint64              synctrailno;
    uint64              synctrailstart;
    int64               loadtimestamp;
    int64               synctimestamp;
} xmanager_metricintegratenode;

/* Initialize node */
extern xmanager_metricnode* xmanager_metricintegratenode_init(void);

/* Clean up metric integrate node memory */
extern void xmanager_metricintegratenode_destroy(xmanager_metricnode* metricnode);

/* Serialize integrate node */
extern bool xmanager_metricintegratenode_serial(xmanager_metricnode* metricnode, uint8** blk,
                                                int* blksize, int* blkstart);

/* Deserialize to integrate node */
extern xmanager_metricnode* xmanager_metricintegratenode_deserial(uint8* blk, int* blkstart);

/* Assemble integrate info */
extern void* xmanager_metricmsg_assembleintegrate(xmanager_metricnode* pxmetricnode);
#endif
