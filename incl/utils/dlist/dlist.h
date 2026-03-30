#ifndef _DLIST_H
#define _DLIST_H

typedef int  (*dlistvaluecmp)(void* vala, void* valb);

typedef void (*dlistvaluefree)(void* value);

typedef struct DLISTNODE
{
    struct DLISTNODE* prev;
    struct DLISTNODE* next;
    void*             value;
} dlistnode;

typedef struct DLIST
{
    uint64         length;
    dlistnode*     head;
    dlistnode*     tail;
    dlistvaluefree free;
} dlist;

extern void dlist_setfree(dlist* dl, dlistvaluefree valuefree);

/*
 * free a dlnode
 * parameters:
 *  valuefree   value free function, may be NULL; if NULL, dlistnode->value is not freed
 */
extern void dlist_node_free(dlistnode* dlnode, dlistvaluefree valuefree);

/*
 * append value to list tail
 * if dl is NULL, a new list will be created
 */
extern dlist* dlist_put(dlist* dl, void* value);

/*
 * add node to list head
 */
extern dlist* dlist_puthead(dlist* dl, void* value);

/* get by value */
extern void* dlist_get(dlist* dl, void* value, dlistvaluecmp valuecmp);

/*
 * get node from list head and remove it from the list
 */
extern void* dlist_getvalue(dlist* dl);

/*
 * remove dlnode from dlist and free it
 * parameters:
 *  dl          linked list
 *  value       value to match
 *  valuecmp    value comparison function
 *  valuefree   value free function, may be NULL; if NULL, dlistnode->value is not freed
 */
extern dlist* dlist_delete(dlist* dl, dlistnode* dlnode, dlistvaluefree valuefree);

/*
 * find and delete dlist node by value
 * parameters:
 *  dl          linked list
 *  value       value to match
 *  valuecmp    value comparison function
 *  valuefree   value free function, may be NULL; if NULL, dlistnode->value is not freed
 */
extern dlist* dlist_deletebyvalue(dlist* dl, void* value, dlistvaluecmp valuecmp, dlistvaluefree valuefree);

/*
 * find and delete first matching dlist node by value
 * parameters:
 *  dl          linked list
 *  value       value to match
 *  valuecmp    value comparison function
 *  valuefree   value free function, may be NULL; if NULL, dlistnode->value is not freed
 */
extern dlist* dlist_deletebyvaluefirstmatch(dlist* dl, void* value, dlistvaluecmp valuecmp, dlistvaluefree valuefree);

/*
 * check if value already exists in dlist
 */
extern void* dlist_isexist(dlist* dl, void* value, dlistvaluecmp valuecmp);

/*
 * move specified node to list head
 */
extern dlist* dlist_putnode2head(dlist* dl, dlistnode* dlnode);

/*
 * free dlist
 * parameters:
 *  valuefree   value free function, may be NULL; if NULL, dlistnode->value is not freed
 */
extern void dlist_free(dlist* dl, dlistvaluefree valuefree);

/*
 * free dlist (void pointer version)
 * parameters:
 *  valuefree   value free function, may be NULL; if NULL, dlistnode->value is not freed
 */
extern void dlist_freevoid(void* args);

/*
 * check if list is empty
 */
extern bool dlist_isnull(dlist* dl);

/*
 * get item count
 */
extern uint64 dlist_getcount(dlist* dl);

/*
 * concatenate two lists into one
 *  appends dl2 after dl1 and clears the original list
 */
extern dlist* dlist_concat(dlist* dl1, dlist* dl2);

/* connect dlist and dlnode chain */
extern bool dlist_append(dlist** pdl, dlistnode* dlnode);

/*
 * truncate dlist without deleting
 */
extern dlist* dlist_truncate(dlist* dl, dlistnode* dlnode);

#endif
