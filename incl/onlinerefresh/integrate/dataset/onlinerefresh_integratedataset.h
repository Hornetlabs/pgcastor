#ifndef _RIPPLE_ONLINEREFRESH_INTEGRATEDATASET_H
#define _RIPPLE_ONLINEREFRESH_INTEGRATEDATASET_H


typedef struct RIPPLE_ONLINEREFRESH_INTEGRATEDATASETNODE
{
    ripple_uuid_t                   onlinerefreshno;
    FullTransactionId               txid;
    ripple_refresh_tables*          refreshtables;          /* 待同步的表 */
}ripple_onlinerefresh_integratedatasetnode;

typedef struct RIPPLE_ONLINEREFRESH_INTEGRATEDATASET
{
    dlist*                                          onlinerefresh;      /* 内容为 ripple_onlinerefresh_integratedatasetnode */
}ripple_onlinerefresh_integratedataset;

ripple_onlinerefresh_integratedatasetnode* ripple_onlinerefresh_integratedatasetnode_init(void);

void ripple_onlinerefresh_integratedatasetnode_no_set(ripple_onlinerefresh_integratedatasetnode* onlinerefreshnode, void* no);

void ripple_onlinerefresh_integratedatasetnode_txid_set(ripple_onlinerefresh_integratedatasetnode* onlinerefreshnode, FullTransactionId txid);

void ripple_onlinerefresh_integratedatasetnode_refreshtables_set(ripple_onlinerefresh_integratedatasetnode* onlinerefreshnode, ripple_refresh_tables* tables);

ripple_onlinerefresh_integratedataset* ripple_onlinerefresh_integratedataset_init(void);

bool ripple_onlinerefresh_integratedataset_add(ripple_onlinerefresh_integratedataset* dataset, void* node);

/* 复制onlinerefresh过滤集 */
ripple_onlinerefresh_integratedataset* ripple_onlinerefresh_integratedataset_copy(ripple_onlinerefresh_integratedataset* dataset);

ripple_onlinerefresh_integratedatasetnode* ripple_onlinerefresh_integratedataset_number_get(ripple_onlinerefresh_integratedataset* dataset, void* no);

ripple_onlinerefresh_integratedatasetnode* ripple_onlinerefresh_integratedataset_txid_get(ripple_onlinerefresh_integratedataset* dataset, FullTransactionId txid);

void ripple_onlinerefresh_integratedataset_delete(ripple_onlinerefresh_integratedataset* dataset, void* no);

void ripple_onlinerefresh_integratedataset_free(void* data);

#endif
