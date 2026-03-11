#include "app_incl.h"
#include "utils/dlist/dlist.h"

static dlist* dlist_init(void)
{
    dlist* dl = rmalloc0(sizeof(dlist));
    if (NULL == dl)
    {
        elog(RLOG_WARNING, "dlist init error, out of memory.");
        return NULL;
    }
    rmemset0(dl, 0, '\0', sizeof(dlist));
    dl->head = NULL;
    dl->tail = NULL;
    dl->length = 0;
    return dl;
}

static dlistnode* dlist_node_init(void)
{
    dlistnode* dlnode = rmalloc0(sizeof(dlistnode));
    if (NULL == dlnode)
    {
        elog(RLOG_WARNING, "out of memory.");
        return NULL;
    }
    rmemset0(dlnode, 0, '\0', sizeof(dlistnode));
    dlnode->next = NULL;
    dlnode->prev = NULL;
    dlnode->value = NULL;
    return dlnode;
}

void dlist_setfree(dlist* dl, dlistvaluefree valuefree)
{
    if (NULL == dl)
    {
        return;
    }
    dl->free = valuefree;
}

/*
 * Free dlnode
 * Input:
 *  valuefree   value free function, can be NULL, when NULL, does not free dlistnode->value
 */
void dlist_node_free(dlistnode* dlnode, dlistvaluefree valuefree)
{
    if (NULL == dlnode)
    {
        return;
    }

    if (NULL != valuefree)
    {
        valuefree(dlnode->value);
    }
    rfree(dlnode);
}

/*
 * Put value at the tail of the linked list
 * When dl is empty, a new list will be created first
 */
dlist* dlist_put(dlist* dl, void* value)
{
    dlistnode* dlnode = NULL;
    if (NULL == dl)
    {
        dl = dlist_init();
    }

    dlnode = dlist_node_init();
    dlnode->value = value;

    if (NULL == dl->head)
    {
        dl->head = dlnode;
    }
    else
    {
        dl->tail->next = dlnode;
        dlnode->prev = dl->tail;
    }
    dl->tail = dlnode;
    dl->length++;
    return dl;
}

/*
 * Add node at head
 */
dlist* dlist_puthead(dlist* dl, void* value)
{
    dlistnode* dlnode = NULL;
    if (NULL == dl)
    {
        dl = dlist_init();
        if (NULL == dl)
        {
            elog(RLOG_WARNING, "put value 2 dlist head error");
            return NULL;
        }
    }

    dlnode = dlist_node_init();
    if (NULL == dlnode)
    {
        elog(RLOG_WARNING, "put value 2 dlist head error, init dlist node");
        return NULL;
    }
    dlnode->value = value;

    /* head is empty */
    dlnode->next = dl->head;
    if (NULL == dl->head)
    {
        dl->tail = dlnode;
    }
    else
    {
        dl->head->prev = dlnode;
    }
    dl->head = dlnode;
    dl->length++;

    return dl;
}

/* Get by value, not removed from the linked list */
void* dlist_get(dlist* dl, void* value, dlistvaluecmp valuecmp)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    if (NULL == dl)
    {
        return NULL;
    }

    for (dlnode = dl->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        if (0 != valuecmp(value, dlnode->value))
        {
            continue;
        }

        return dlnode->value;
    }

    return NULL;
}

/*
 * Get node from the head of the linked list, and remove the node from the list
 */
void* dlist_getvalue(dlist* dl)
{
    void*      value = NULL;
    dlistnode* dlnode = NULL;
    if (NULL == dl || NULL == dl->head)
    {
        return NULL;
    }

    dlnode = dl->head;
    dl->head = dlnode->next;
    if (NULL == dlnode->next)
    {
        dl->tail = NULL;
    }
    else
    {
        dlnode->next->prev = NULL;
    }
    dlnode->next = NULL;
    value = dlnode->value;
    dlist_node_free(dlnode, NULL);
    dl->length--;
    return value;
}

/*
 * Remove dlnode from dlist and free dlnode
 * Input:
 *  dl          linked list
 *  value       value to match
 *  valuecmp    value match function
 *  valuefree   value free function, can be NULL, when NULL, does not free dlistnode->value
 */
dlist* dlist_delete(dlist* dl, dlistnode* dlnode, dlistvaluefree valuefree)
{
    if (NULL == dl || NULL == dlnode)
    {
        return NULL;
    }

    /* Remove from list */
    if (NULL != dlnode->prev)
    {
        dlnode->prev->next = dlnode->next;
    }
    else
    {
        dl->head = dlnode->next;
    }

    if (NULL != dlnode->next)
    {
        dlnode->next->prev = dlnode->prev;
    }
    else
    {
        dl->tail = dlnode->prev;
    }

    dlist_node_free(dlnode, valuefree == NULL ? dl->free : valuefree);
    dl->length--;
    return dl;
}

/*
 * Match node in dlist by value and free dlnode
 * Input:
 *  dl          linked list
 *  value       value to match
 *  valuecmp    value match function
 *  valuefree   value free function, can be NULL, when NULL, does not free dlistnode->value
 */
dlist* dlist_deletebyvalue(dlist* dl, void* value, dlistvaluecmp valuecmp, dlistvaluefree valuefree)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    if (NULL == dl)
    {
        return NULL;
    }

    for (dlnode = dl->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        if (0 != valuecmp(value, dlnode->value))
        {
            continue;
        }

        dlist_delete(dl, dlnode, valuefree);
    }

    return dl;
}

/*
 * Match node in dlist by value and free dlnode/delete only first match
 * Input:
 *  dl          linked list
 *  value       value to match
 *  valuecmp    value match function
 *  valuefree   value free function, can be NULL, when NULL, does not free dlistnode->value
 */
dlist* dlist_deletebyvaluefirstmatch(dlist* dl, void* value, dlistvaluecmp valuecmp, dlistvaluefree valuefree)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    if (NULL == dl)
    {
        return NULL;
    }

    for (dlnode = dl->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        if (0 != valuecmp(value, dlnode->value))
        {
            continue;
        }

        dl = dlist_delete(dl, dlnode, valuefree);
        break;
    }

    return dl;
}

/*
 * Check if the value already exists in dlist and return value
 */
void* dlist_isexist(dlist* dl, void* value, dlistvaluecmp valuecmp)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;

    if (NULL == dl)
    {
        return NULL;
    }

    for (dlnode = dl->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        if (0 != valuecmp(value, dlnode->value))
        {
            continue;
        }
        return dlnode->value;
    }

    return NULL;
}

/*
 * Move the specified node to the head
 */
dlist* dlist_putnode2head(dlist* dl, dlistnode* dlnode)
{
    if (NULL == dl || NULL == dlnode)
    {
        return NULL;
    }

    /* remove from linked list */
    if (NULL != dlnode->prev)
    {
        dlnode->prev->next = dlnode->next;
    }
    else
    {
        dl->head = dlnode->next;
    }

    if (NULL != dlnode->next)
    {
        dlnode->next->prev = dlnode->prev;
    }
    else
    {
        dl->tail = dlnode->prev;
    }

    /* put at the head of linked list */
    dlnode->next = dlnode->prev = NULL;
    dlnode->next = dl->head;
    if (NULL == dl->head)
    {
        /* only one node */
        dl->tail = dlnode;
    }
    else
    {
        dl->head->prev = dlnode;
    }
    dl->head = dlnode;

    return dl;
}

/*
 * Free dlist
 * Input:
 *  valuefree   value free function, can be NULL, when NULL, does not free dlistnode->value
 */
void dlist_free(dlist* dl, dlistvaluefree valuefree)
{
    dlistnode* dlnode = NULL;
    if (NULL == dl)
    {
        return;
    }

    /* traverse and free nodes */
    for (dlnode = dl->head; NULL != dlnode; dlnode = dl->head)
    {
        dl->head = dlnode->next;
        dlist_node_free(dlnode, valuefree == NULL ? dl->free : valuefree);
    }
    rfree(dl);
}

/*
 * Free dlist
 * Input:
 *  valuefree   value free function, can be NULL, when NULL, does not free dlistnode->value
 */
void dlist_freevoid(void* args)
{
    dlist* dl = NULL;

    dl = (dlist*)args;
    dlist_free(dl, dl->free);
}

/*
 * Check if queue is empty
 */
bool dlist_isnull(dlist* dl)
{
    if (NULL == dl)
    {
        return true;
    }

    if (NULL == dl->head)
    {
        return true;
    }
    return false;
}

/*
 * Get count
 */
uint64 dlist_getcount(dlist* dl)
{
    return dl->length;
}

/*
 * Merge two linked lists into one
 *  Put dl2 after dl1, and free the original list
 */
dlist* dlist_concat(dlist* dl1, dlist* dl2)
{
    if (true == dlist_isnull(dl1))
    {
        dlist_free(dl1, NULL);
        return dl2;
    }

    if (true == dlist_isnull(dl2))
    {
        dlist_free(dl2, NULL);
        return dl1;
    }

    /* connect two lists together */
    dl2->head->prev = dl1->tail;
    dl1->tail->next = dl2->head;
    dl1->tail = dl2->tail;

    dl2->head = NULL;
    dl2->tail = NULL;
    dl1->length += dl2->length;

    dlist_free(dl2, NULL);
    return dl1;
}

/* Connect dlist and dlnode chain together */
bool dlist_append(dlist** pdl, dlistnode* dlnode)
{
    dlist*     dl = NULL;
    dlistnode* dlnodenext = NULL;
    if (NULL == dlnode)
    {
        return true;
    }
    dl = *pdl;

    if (NULL == dl)
    {
        dl = dlist_init();
        if (NULL == dl)
        {
            elog(RLOG_WARNING, "dlist append error, out of memory");
            return false;
        }
    }

    for (dlnodenext = dlnode; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        dl->length++;
        if (NULL == dl->head)
        {
            dl->head = dlnode;
            dl->tail = dlnode;
            continue;
        }

        dl->tail->next = dlnode;
        dlnode->prev = dl->tail;
        dl->tail = dlnode;
        dlnode->next = NULL;
    }

    *pdl = dl;
    return true;
}

/*
 * Truncate dlist, but not delete
 */
dlist* dlist_truncate(dlist* dl, dlistnode* dlnode)
{
    uint64     dlnodelength = 0;
    dlistnode* dlnodetmp = NULL;
    if (NULL == dlnode)
    {
        return dl;
    }

    /* head node */
    if (dl->head == dlnode)
    {
        dl->head = NULL;
        dl->tail = NULL;
        dl->length = 0;
        return dl;
    }

    dlnodetmp = dlnode;
    while (dlnodetmp)
    {
        dlnodelength++;
        dlnodetmp = dlnodetmp->next;
    }

    dl->tail = dlnode->prev;
    dl->tail->next = NULL;
    dlnode->prev = NULL;
    dl->length -= dlnodelength;
    return dl;
}
