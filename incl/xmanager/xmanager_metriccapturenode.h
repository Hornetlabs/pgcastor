#ifndef _XMANAGER_METRICCAPTURENODE_H_
#define _XMANAGER_METRICCAPTURENODE_H_

#define REFRESH_METRICCAPTURE_INFOCNT 10

typedef struct XMANAGER_METRICCAPTURENODE
{
    xmanager_metricnode base;
    XLogRecPtr          redolsn;
    XLogRecPtr          restartlsn;
    XLogRecPtr          confirmlsn;
    XLogRecPtr          loadlsn;
    XLogRecPtr          parselsn;
    XLogRecPtr          flushlsn;
    uint64              trailno;
    uint64              trailstart;
    int64               parsetimestamp;
    int64               flushtimestamp;
} xmanager_metriccapturenode;

/* Initialize node */
extern xmanager_metricnode* xmanager_metriccapturenode_init(void);

/* Clean up metric capture node memory */
extern void xmanager_metriccapturenode_destroy(xmanager_metricnode* metricnode);

/* Serialize capture node */
extern bool xmanager_metriccapturenode_serial(xmanager_metricnode* metricnode, uint8** blk,
                                              int* blksize, int* blkstart);

/* Deserialize to capture node */
extern xmanager_metricnode* xmanager_metriccapturenode_deserial(uint8* blk, int* blkstart);

/* Assemble capture info */
extern void* xmanager_metricmsg_assemblecapture(xmanager_metricnode* pxmetricnode);

#endif
