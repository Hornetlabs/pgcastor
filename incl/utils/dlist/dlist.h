#ifndef _DLIST_H
#define _DLIST_H

typedef int (*dlistvaluecmp)(void* vala, void* valb);

typedef void (*dlistvaluefree)(void* value);

typedef struct DLISTNODE
{
    struct DLISTNODE*   prev;
    struct DLISTNODE*   next;
    void*               value;
} dlistnode;

typedef struct DLIST
{
    uint64              length;
    dlistnode*          head;
    dlistnode*          tail;
    dlistvaluefree      free;
} dlist;

extern void dlist_setfree(dlist* dl, dlistvaluefree valuefree);

/* 
 * 将dlnode释放
 * 入参:
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
extern void dlist_node_free(dlistnode* dlnode, dlistvaluefree valuefree);

/*
 * 将 value 值放到链表的尾部
 * 当 dl 为空时会先生成新的链表
*/
extern dlist* dlist_put(dlist* dl, void* value);

/*
 * 在头部添加节点
*/
extern dlist* dlist_puthead(dlist* dl, void* value);

/* 根据值获取 */
extern void* dlist_get(dlist* dl, void* value, dlistvaluecmp valuecmp);

/*
 * 在链表头部获取node,并将节点在链表中移除
*/
extern void* dlist_getvalue(dlist* dl);

/* 
 * 将dlnode节点在dlist中移除并释放dlnode
 * 入参:
 *  dl          链表
 *  value       匹配的值
 *  valuecmp    值匹配函数
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
extern dlist* dlist_delete(dlist* dl, dlistnode* dlnode, dlistvaluefree valuefree);

/* 
 * 根据值匹配dlist中的node节点并释放dlnode
 * 入参:
 *  dl          链表
 *  value       匹配的值
 *  valuecmp    值匹配函数
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
extern dlist* dlist_deletebyvalue(dlist* dl, void* value, dlistvaluecmp valuecmp, dlistvaluefree valuefree);

/* 
 * 根据值匹配dlist中的node节点并释放dlnode/仅删除第一个匹配的
 * 入参:
 *  dl          链表
 *  value       匹配的值
 *  valuecmp    值匹配函数
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
extern dlist* dlist_deletebyvaluefirstmatch(dlist* dl, void* value, dlistvaluecmp valuecmp, dlistvaluefree valuefree);

/*
 * 检测该值是否已经在 dlist 中存在
*/
extern void* dlist_isexist(dlist* dl, void* value, dlistvaluecmp valuecmp);

/*
 * 将指定的节点转移到头部
*/
extern dlist* dlist_putnode2head(dlist* dl, dlistnode* dlnode);

/* 
 * 释放 dlist
 * 入参:
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
extern void dlist_free(dlist* dl, dlistvaluefree valuefree);

/* 
 * 释放 dlist
 * 入参:
 *  valuefree   value的释放函数,可为空, 为空时，不释放 dlistnode->value 值
 */
extern void dlist_freevoid(void* args);

/*
 * 查看队列是否为空
*/
extern bool dlist_isnull(dlist* dl);

/*
 * 获取个数
*/
extern uint64 dlist_getcount(dlist* dl);

/* 
 * 将两个链表合并为一个链表
 *  将 dl2 放在 dl1 的后面, 并将原链表清理掉
 */
extern dlist* dlist_concat(dlist* dl1, dlist* dl2);

/* 将 dlist 和 dlnode 链连接起来 */
extern bool dlist_append(dlist** pdl, dlistnode* dlnode);

/*
 * 将 dlist truncate 掉,但是不删除
*/
extern dlist* dlist_truncate(dlist* dl, dlistnode* dlnode);

#endif
