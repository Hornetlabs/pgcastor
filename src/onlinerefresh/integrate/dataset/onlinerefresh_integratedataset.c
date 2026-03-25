#include "app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "refresh/refresh_tables.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"

/* Add content to onlinerefresh number */
bool onlinerefresh_integratedataset_add(onlinerefresh_integratedataset* dataset, void* node)
{
    dataset->onlinerefresh = dlist_put(dataset->onlinerefresh, node);
    return true;
}

onlinerefresh_integratedatasetnode* onlinerefresh_integratedatasetnode_init(void)
{
    onlinerefresh_integratedatasetnode* node = NULL;

    node = rmalloc0(sizeof(onlinerefresh_integratedatasetnode));
    if (NULL == node)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(node, 0, '\0', sizeof(onlinerefresh_integratedatasetnode));
    return node;
}

void onlinerefresh_integratedatasetnode_no_set(
    onlinerefresh_integratedatasetnode* onlinerefreshnode, void* no)
{
    rmemcpy1(onlinerefreshnode->onlinerefreshno.data, 0, no, UUID_LEN);
}

void onlinerefresh_integratedatasetnode_txid_set(
    onlinerefresh_integratedatasetnode* onlinerefreshnode, FullTransactionId txid)
{
    onlinerefreshnode->txid = txid;
}

void onlinerefresh_integratedatasetnode_refreshtables_set(
    onlinerefresh_integratedatasetnode* onlinerefreshnode, refresh_tables* tables)
{
    onlinerefreshnode->refreshtables = refresh_tables_copy(tables);
}

onlinerefresh_integratedataset* onlinerefresh_integratedataset_init(void)
{
    onlinerefresh_integratedataset* integratedataset = NULL;

    integratedataset = rmalloc0(sizeof(onlinerefresh_integratedataset));
    if (NULL == integratedataset)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(integratedataset, 0, '\0', sizeof(onlinerefresh_integratedataset));
    integratedataset->onlinerefresh = NULL;

    return integratedataset;
}

onlinerefresh_integratedataset* onlinerefresh_integratedataset_copy(
    onlinerefresh_integratedataset* dataset)
{
    dlistnode*                          dlnode = NULL;
    onlinerefresh_integratedataset*     result = NULL;
    onlinerefresh_integratedatasetnode* node = NULL;
    onlinerefresh_integratedatasetnode* new_node = NULL;

    result = onlinerefresh_integratedataset_init();
    if (NULL == result)
    {
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }

    for (dlnode = dataset->onlinerefresh->head; NULL != dlnode; dlnode = dlnode->next)
    {
        node = (onlinerefresh_integratedatasetnode*)dlnode->value;

        new_node = onlinerefresh_integratedatasetnode_init();
        onlinerefresh_integratedatasetnode_no_set(new_node, node->onlinerefreshno.data);
        onlinerefresh_integratedatasetnode_txid_set(new_node, node->txid);
        onlinerefresh_integratedatasetnode_refreshtables_set(new_node, node->refreshtables);
        onlinerefresh_integratedataset_add(result, new_node);
    }

    return result;
}

onlinerefresh_integratedatasetnode* onlinerefresh_integratedataset_number_get(
    onlinerefresh_integratedataset* dataset, void* no)
{
    dlistnode*                          dlnode = NULL;
    dlistnode*                          dlnodetmp = NULL;
    onlinerefresh_integratedatasetnode* node = NULL;
    if (NULL == dataset->onlinerefresh)
    {
        return NULL;
    }

    for (dlnode = dataset->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        node = (onlinerefresh_integratedatasetnode*)dlnode->value;
        if (0 == memcmp(node->onlinerefreshno.data, no, UUID_LEN))
        {
            break;
        }
    }
    return node;
}

onlinerefresh_integratedatasetnode* onlinerefresh_integratedataset_txid_get(
    onlinerefresh_integratedataset* dataset, FullTransactionId txid)
{
    dlistnode*                          dlnode = NULL;
    dlistnode*                          dlnodetmp = NULL;
    onlinerefresh_integratedatasetnode* node = NULL;
    if (NULL == dataset->onlinerefresh)
    {
        return NULL;
    }

    for (dlnode = dataset->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        node = (onlinerefresh_integratedatasetnode*)dlnode->value;
        if (txid == node->txid)
        {
            break;
        }
    }
    return node;
}

void onlinerefresh_integratedataset_delete(onlinerefresh_integratedataset* dataset, void* no)
{
    bool                                find = false;
    dlistnode*                          dlnode = NULL;
    dlistnode*                          dlnodetmp = NULL;
    onlinerefresh_integratedatasetnode* node = NULL;
    if (NULL == dataset->onlinerefresh)
    {
        return;
    }

    for (dlnode = dataset->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        node = (onlinerefresh_integratedatasetnode*)dlnode->value;
        if (0 == memcmp(node->onlinerefreshno.data, no, UUID_LEN))
        {
            find = true;
            break;
        }
    }
    if (true == find)
    {
        dlist_delete(dataset->onlinerefresh, dlnode, onlinerefresh_integratedataset_free);
    }

    return;
}

void onlinerefresh_integratedataset_free(void* data)
{
    onlinerefresh_integratedatasetnode* node = NULL;
    if (NULL == data)
    {
        return;
    }

    node = (onlinerefresh_integratedatasetnode*)data;
    refresh_freetables(node->refreshtables);

    rfree(data);

    return;
}
