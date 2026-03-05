#include "ripple_app_incl.h"
#include "utils/dlist/dlist.h"

static dlist* dlist_init(void)
{
    dlist* dl = rmalloc0(sizeof(dlist));
    if(NULL == dl)
    {
        elog(RLOG_WARNING, "dlist init error, out of memory.");
        return NULL;
    }
    rmemset0(dl, 0, '\0', sizeof(dlist));
    dl->head = NULL;
    dl->tail =NULL;
    dl->length = 0;
    return dl;
}

static dlistnode* dlist_node_init(void)
{
    dlistnode* dlnode = rmalloc0(sizeof(dlistnode));
    if(NULL == dlnode)
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
    if(NULL == dl)
    {
        return;
    }
    dl->free = valuefree;
}

/* 
 * 将dlnode释放
 * 入参:
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
void dlist_node_free(dlistnode* dlnode, dlistvaluefree valuefree)
{
    if(NULL == dlnode)
    {
        return;
    }

    if(NULL != valuefree)
    {
        valuefree(dlnode->value);
    }
    rfree(dlnode);
}

/*
 * 将 value 值放到链表的尾部
 * 当 dl 为空时会先生成新的链表
*/
dlist* dlist_put(dlist* dl, void* value)
{
    dlistnode* dlnode = NULL;
    if(NULL == dl)
    {
        dl = dlist_init();
    }

    dlnode = dlist_node_init();
    dlnode->value = value;

    if(NULL == dl->head)
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
 * 在头部添加节点
*/
dlist* dlist_puthead(dlist* dl, void* value)
{
    dlistnode* dlnode = NULL;
    if(NULL == dl)
    {
        dl = dlist_init();
        if(NULL == dl)
        {
            elog(RLOG_WARNING, "put value 2 dlist head error");
            return NULL;
        }
    }

    dlnode = dlist_node_init();
    if(NULL == dlnode)
    {
        elog(RLOG_WARNING, "put value 2 dlist head error, init dlist node");
        return NULL;
    }
    dlnode->value = value;

    /* head 为空 */
    dlnode->next = dl->head;
    if(NULL == dl->head)
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

/* 根据值获取, 不在链表中移除 */
void* dlist_get(dlist* dl, void* value, dlistvaluecmp valuecmp)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    if(NULL == dl)
    {
        return NULL;
    }

    for(dlnode = dl->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        if(0 != valuecmp(value, dlnode->value))
        {
            continue;
        }

        return dlnode->value;
    }

    return NULL;
}

/*
 * 在链表头部获取node,并将节点在链表中移除
*/
void* dlist_getvalue(dlist* dl)
{
    void* value = NULL;
    dlistnode* dlnode = NULL;
    if(NULL == dl || NULL == dl->head)
    {
        return NULL;
    }

    dlnode = dl->head;
    dl->head = dlnode->next;
    if(NULL == dlnode->next)
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
 * 将dlnode节点在dlist中移除并释放dlnode
 * 入参:
 *  dl          链表
 *  value       匹配的值
 *  valuecmp    值匹配函数
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
dlist* dlist_delete(dlist* dl, dlistnode* dlnode, dlistvaluefree valuefree)
{
    if(NULL == dl || NULL == dlnode)
    {
        return NULL;
    }

    /* 在链表中移除 */
    if(NULL != dlnode->prev)
    {
        dlnode->prev->next = dlnode->next;
    }
    else
    {
        dl->head = dlnode->next;
    }

    if(NULL != dlnode->next)
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
 * 根据值匹配dlist中的node节点并释放dlnode
 * 入参:
 *  dl          链表
 *  value       匹配的值
 *  valuecmp    值匹配函数
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
dlist* dlist_deletebyvalue(dlist* dl, void* value, dlistvaluecmp valuecmp, dlistvaluefree valuefree)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    if(NULL == dl)
    {
        return NULL;
    }

    for(dlnode = dl->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        if(0 != valuecmp(value, dlnode->value))
        {
            continue;
        }

        dlist_delete(dl, dlnode, valuefree);
    }

    return dl;
}


/* 
 * 根据值匹配dlist中的node节点并释放dlnode/仅删除第一个匹配的
 * 入参:
 *  dl          链表
 *  value       匹配的值
 *  valuecmp    值匹配函数
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
dlist* dlist_deletebyvaluefirstmatch(dlist* dl, void* value, dlistvaluecmp valuecmp, dlistvaluefree valuefree)
{
    dlistnode* dlnode = NULL;
    dlistnode* dlnodetmp = NULL;
    if(NULL == dl)
    {
        return NULL;
    }

    for(dlnode = dl->head; NULL != dlnode; dlnode = dlnodetmp)
    {
        dlnodetmp = dlnode->next;
        if(0 != valuecmp(value, dlnode->value))
        {
            continue;
        }

        dl = dlist_delete(dl, dlnode, valuefree);
        break;
    }

    return dl;
}

/*
 * 检测该值是否已经在 dlist 中存在,并返回 value
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
 * 将指定的节点转移到头部
*/
dlist* dlist_putnode2head(dlist* dl, dlistnode* dlnode)
{
    if(NULL == dl || NULL == dlnode)
    {
        return NULL;
    }

    /* 在链表中移除 */
    if(NULL != dlnode->prev)
    {
        dlnode->prev->next = dlnode->next;
    }
    else
    {
        dl->head = dlnode->next;
    }

    if(NULL != dlnode->next)
    {
        dlnode->next->prev = dlnode->prev;
    }
    else
    {
        dl->tail = dlnode->prev;
    }

    /* 放入到链表头上 */
    dlnode->next = dlnode->prev = NULL;
    dlnode->next = dl->head;
    if(NULL == dl->head)
    {
        /* 只有一个节点 */
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
 * 释放 dlist
 * 入参:
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
void dlist_free(dlist* dl, dlistvaluefree valuefree)
{
    dlistnode* dlnode = NULL;
    if(NULL == dl)
    {
        return;
    }

    /* 遍历释放节点 */
    for(dlnode = dl->head; NULL != dlnode; dlnode = dl->head)
    {
        dl->head = dlnode->next;
        dlist_node_free(dlnode, valuefree == NULL ? dl->free : valuefree);
    }
    rfree(dl);
}

/* 
 * 释放 dlist
 * 入参:
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
void dlist_freevoid(void* args)
{
    dlist* dl = NULL;

    dl = (dlist*)args;
    dlist_free(dl, dl->free);
}

/*
 * 查看队列是否为空
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
 * 获取个数
*/
uint64 dlist_getcount(dlist* dl)
{
    return dl->length;
}


/* 
 * 将两个链表合并为一个链表
 *  将 dl2 放在 dl1 的后面, 并将原链表清理掉
 */
dlist* dlist_concat(dlist* dl1, dlist* dl2)
{
    if(true == dlist_isnull(dl1))
    {
        dlist_free(dl1, NULL);
        return dl2;
    }

    if(true == dlist_isnull(dl2))
    {
        dlist_free(dl2, NULL);
        return dl1;
    }

    /* 两个链表串起来 */
    dl2->head->prev = dl1->tail;
    dl1->tail->next = dl2->head;
    dl1->tail = dl2->tail;

    dl2->head = NULL;
    dl2->tail = NULL;
    dl1->length += dl2->length;

    dlist_free(dl2, NULL);
    return dl1;
}

/* 将 dlist 和 dlnode 链连接起来 */
bool dlist_append(dlist** pdl, dlistnode* dlnode)
{
    dlist* dl = NULL;
    dlistnode* dlnodenext = NULL;
    if(NULL == dlnode)
    {
        return true;
    }
    dl = *pdl;

    if(NULL == dl)
    {
        dl = dlist_init();
        if(NULL == dl)
        {
            elog(RLOG_WARNING, "dlist append error, out of memory");
            return false;
        }
    }

    for(dlnodenext = dlnode; NULL != dlnode; dlnode = dlnodenext)
    {
        dlnodenext = dlnode->next;
        dl->length++;
        if(NULL == dl->head)
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
 * 将 dlist truncate 掉,但是不删除
*/
dlist* dlist_truncate(dlist* dl, dlistnode* dlnode)
{
    uint64 dlnodelength = 0;
    dlistnode* dlnodetmp = NULL;
    if(NULL == dlnode)
    {
        return dl;
    }

    /* 头部节点 */
    if(dl->head == dlnode)
    {
        dl->head = NULL;
        dl->tail = NULL;
        dl->length = 0;
        return dl;
    }

    dlnodetmp = dlnode;
    while(dlnodetmp)
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
