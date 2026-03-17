#ifndef _XMANAGER_METRICXSCSCINODE_H_
#define _XMANAGER_METRICXSCSCINODE_H_

typedef struct XMANAGER_METRICXSCSCINODE
{
    xmanager_metricnode              base;
    uint64                                  number;
    xmanager_metricasyncmsgs*        asyncmsgs;
} xmanager_metricxscscinode;

extern void xmanager_metricxscscinode_destroy(xmanager_metricnode* metricnode);

extern xmanager_metricnode* xmanager_metricxscscinode_init(void);

extern int xmanager_metricxscscinode_cmp(void* s1, void* s2);

#endif
