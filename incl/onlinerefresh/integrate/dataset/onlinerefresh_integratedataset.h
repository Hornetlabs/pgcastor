#ifndef _ONLINEREFRESH_INTEGRATEDATASET_H
#define _ONLINEREFRESH_INTEGRATEDATASET_H

typedef struct ONLINEREFRESH_INTEGRATEDATASETNODE
{
    uuid_t            onlinerefreshno;
    FullTransactionId txid;
    refresh_tables*   refreshtables; /* Tables to sync */
} onlinerefresh_integratedatasetnode;

typedef struct ONLINEREFRESH_INTEGRATEDATASET
{
    dlist* onlinerefresh; /* Content is onlinerefresh_integratedatasetnode */
} onlinerefresh_integratedataset;

onlinerefresh_integratedatasetnode* onlinerefresh_integratedatasetnode_init(void);

void onlinerefresh_integratedatasetnode_no_set(
    onlinerefresh_integratedatasetnode* onlinerefreshnode, void* no);

void onlinerefresh_integratedatasetnode_txid_set(
    onlinerefresh_integratedatasetnode* onlinerefreshnode, FullTransactionId txid);

void onlinerefresh_integratedatasetnode_refreshtables_set(
    onlinerefresh_integratedatasetnode* onlinerefreshnode, refresh_tables* tables);

onlinerefresh_integratedataset* onlinerefresh_integratedataset_init(void);

bool onlinerefresh_integratedataset_add(onlinerefresh_integratedataset* dataset, void* node);

/* Copy onlinerefresh filter set */
onlinerefresh_integratedataset* onlinerefresh_integratedataset_copy(
    onlinerefresh_integratedataset* dataset);

onlinerefresh_integratedatasetnode* onlinerefresh_integratedataset_number_get(
    onlinerefresh_integratedataset* dataset, void* no);

onlinerefresh_integratedatasetnode* onlinerefresh_integratedataset_txid_get(
    onlinerefresh_integratedataset* dataset, FullTransactionId txid);

void onlinerefresh_integratedataset_delete(onlinerefresh_integratedataset* dataset, void* no);

void onlinerefresh_integratedataset_free(void* data);

#endif
