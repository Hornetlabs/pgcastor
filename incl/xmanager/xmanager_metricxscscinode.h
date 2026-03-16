#ifndef _RIPPLE_XMANAGER_METRICXSCSCINODE_H_
#define _RIPPLE_XMANAGER_METRICXSCSCINODE_H_

typedef struct RIPPLE_XMANAGER_METRICXSCSCINODE
{
    ripple_xmanager_metricnode              base;
    uint64                                  number;
    ripple_xmanager_metricasyncmsgs*        asyncmsgs;
} ripple_xmanager_metricxscscinode;

extern void ripple_xmanager_metricxscscinode_destroy(ripple_xmanager_metricnode* metricnode);

extern ripple_xmanager_metricnode* ripple_xmanager_metricxscscinode_init(void);

extern int ripple_xmanager_metricxscscinode_cmp(void* s1, void* s2);

#endif
