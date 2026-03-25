#ifndef _XMANAGER_METRICPROGRESSNODE_H_
#define _XMANAGER_METRICPROGRESSNODE_H_

typedef struct XMANAGER_METRICprogressNODE
{
    xmanager_metricnode base;
    dlist*              progressjop; /* job metricnode */
} xmanager_metricprogressnode;

/* Initialize node */
extern xmanager_metricnode* xmanager_metricprogressnode_init(void);

/* Clean up metric progress node memory */
extern void xmanager_metricprogressnode_destroy(xmanager_metricnode* metricnode);

/* Serialize progress node */
extern bool xmanager_metricprogressnode_serial(xmanager_metricnode* metricnode, uint8** blk,
                                               int* blksize, int* blkstart);

/* Deserialize to progress node */
extern xmanager_metricnode* xmanager_metricprogressnode_deserial(uint8* blk, int* blkstart);

/* Assemble progress info */
extern void* xmanager_metricmsg_assembleprogress(xmanager_metric*     xmetric,
                                                 xmanager_metricnode* pxmetricnode);

#endif
