#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/ripple_uuid.h"
#include "refresh/ripple_refresh_tables.h"
#include "onlinerefresh/integrate/dataset/ripple_onlinerefresh_integratedataset.h"


/* 向 onlinerefresh 编号内增加内容 */
bool ripple_onlinerefresh_integratedataset_add(ripple_onlinerefresh_integratedataset* dataset, void* node)
{
    dataset->onlinerefresh = dlist_put(dataset->onlinerefresh, node);
    return true;
}

ripple_onlinerefresh_integratedatasetnode* ripple_onlinerefresh_integratedatasetnode_init(void)
{
    ripple_onlinerefresh_integratedatasetnode* node = NULL;

    node = rmalloc0(sizeof(ripple_onlinerefresh_integratedatasetnode));
    if(NULL == node)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(node, 0, '\0', sizeof(ripple_onlinerefresh_integratedatasetnode));
    return node;
}

void ripple_onlinerefresh_integratedatasetnode_no_set(ripple_onlinerefresh_integratedatasetnode* onlinerefreshnode, void* no)
{
    rmemcpy1(onlinerefreshnode->onlinerefreshno.data, 0, no, RIPPLE_UUID_LEN);
}

void ripple_onlinerefresh_integratedatasetnode_txid_set(ripple_onlinerefresh_integratedatasetnode* onlinerefreshnode, FullTransactionId txid)
{
    onlinerefreshnode->txid = txid;
}

void ripple_onlinerefresh_integratedatasetnode_refreshtables_set(ripple_onlinerefresh_integratedatasetnode* onlinerefreshnode, ripple_refresh_tables* tables)
{
    onlinerefreshnode->refreshtables = ripple_refresh_tables_copy(tables);
}

ripple_onlinerefresh_integratedataset* ripple_onlinerefresh_integratedataset_init(void)
{
    ripple_onlinerefresh_integratedataset* integratedataset = NULL;

    integratedataset = rmalloc0(sizeof(ripple_onlinerefresh_integratedataset));
    if(NULL == integratedataset)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(integratedataset, 0, '\0', sizeof(ripple_onlinerefresh_integratedataset));
    integratedataset->onlinerefresh = NULL;

    return integratedataset;
}

ripple_onlinerefresh_integratedataset* ripple_onlinerefresh_integratedataset_copy(ripple_onlinerefresh_integratedataset* dataset)
{
    dlistnode* dlnode = NULL;
    ripple_onlinerefresh_integratedataset* result = NULL;
    ripple_onlinerefresh_integratedatasetnode* node = NULL;
    ripple_onlinerefresh_integratedatasetnode* new_node = NULL;

    result = ripple_onlinerefresh_integratedataset_init();
    if(NULL == result)
    {
        elog(RLOG_WARNING, "out of memory");
        return NULL;
    }

    for(dlnode = dataset->onlinerefresh->head; NULL != dlnode; dlnode = dlnode->next)
    {
        node = (ripple_onlinerefresh_integratedatasetnode*)dlnode->value;
        
        new_node = ripple_onlinerefresh_integratedatasetnode_init();
        ripple_onlinerefresh_integratedatasetnode_no_set(new_node, node->onlinerefreshno.data);
        ripple_onlinerefresh_integratedatasetnode_txid_set(new_node, node->txid);
        ripple_onlinerefresh_integratedatasetnode_refreshtables_set(new_node, node->refreshtables);
        ripple_onlinerefresh_integratedataset_add(result, new_node);
    }

    return result;
}

ripple_onlinerefresh_integratedatasetnode* ripple_onlinerefresh_integratedataset_number_get(ripple_onlinerefresh_integratedataset* dataset, void* no)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_onlinerefresh_integratedatasetnode* node = NULL;
    if(NULL == dataset->onlinerefresh)
    {
        return NULL;
    }

    for(dlnode = dataset->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        node = (ripple_onlinerefresh_integratedatasetnode*)dlnode->value;
        if (0 == memcmp(node->onlinerefreshno.data, no, RIPPLE_UUID_LEN))
        {
            break;
        }
    }
    return node;
}

ripple_onlinerefresh_integratedatasetnode* ripple_onlinerefresh_integratedataset_txid_get(ripple_onlinerefresh_integratedataset* dataset, FullTransactionId txid)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_onlinerefresh_integratedatasetnode* node = NULL;
    if(NULL == dataset->onlinerefresh)
    {
        return NULL;
    }

    for(dlnode = dataset->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        node = (ripple_onlinerefresh_integratedatasetnode*)dlnode->value;
        if (txid == node->txid)
        {
            break;
        }
    }
    return node;
}

void ripple_onlinerefresh_integratedataset_delete(ripple_onlinerefresh_integratedataset* dataset, void* no)
{
    bool find = false;
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    ripple_onlinerefresh_integratedatasetnode* node = NULL;
    if(NULL == dataset->onlinerefresh)
    {
        return;
    }

    for(dlnode = dataset->onlinerefresh->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        node = (ripple_onlinerefresh_integratedatasetnode*)dlnode->value;
        if (0 == memcmp(node->onlinerefreshno.data, no, RIPPLE_UUID_LEN))
        {
            find = true;
            break;
        }
    }
    if (true == find)
    {
        dlist_delete(dataset->onlinerefresh, dlnode, ripple_onlinerefresh_integratedataset_free);
    }

    return;
}

void ripple_onlinerefresh_integratedataset_free(void* data)
{
    ripple_onlinerefresh_integratedatasetnode* node = NULL;
    if(NULL == data)
    {
        return;
    }

    node = (ripple_onlinerefresh_integratedatasetnode*)data;
    ripple_refresh_freetables(node->refreshtables);
    
    rfree(data);

    return;
}

