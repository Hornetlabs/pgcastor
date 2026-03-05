#ifndef _RIPPLE_QUEUE_H
#define _RIPPLE_QUEUE_H

typedef void (*queuedatafree)(void* value);

typedef struct RIPPLE_QUEUEITEM
{
    void*                           data;
    struct RIPPLE_QUEUEITEM*        prev;
    struct RIPPLE_QUEUEITEM*        next;
} ripple_queueitem;

typedef struct RIPPLE_QUEUE
{
    int                             waits;
    int                             error;
    uint64                          max;                /* 允许的 item 的最大数量 */
    uint64                          cnt;                /* 当前 item 的个数 */
    pthread_cond_t                  cond;
    pthread_mutex_t                 lock;
    ripple_queueitem*               head;
    ripple_queueitem*               tail;
} ripple_queue;

/* 队列初始化 */
ripple_queue* ripple_queue_init(void);

/* 设置 max */
void ripple_queue_setmax(ripple_queue* queue, uint64 max);

/*
 * 在队列头部加入
*/
bool ripple_queue_puthead(ripple_queue* queue, void* data);

/* 
 * 像队列中添加数据
 *  data 需要放入到队列中的数据
 */
bool ripple_queue_put(ripple_queue* queue, void* data);

void ripple_queueitem_free(ripple_queueitem* queueitem, queuedatafree datafree);

/* 批量获取 */
ripple_queueitem* ripple_queue_trygetbatch(ripple_queue* queue);

/* 
 * 在队列中获取数据
 *  返回为空时, 需要判断是否为超时
 */
void* ripple_queue_tryget(ripple_queue* queue);

/* 
 * 在队列中获取数据
 *  返回为空时, 需要判断是否为超时
 */
void* ripple_queue_get(ripple_queue* queue, int* timeout);

bool ripple_queue_isnull(ripple_queue* queue);

/* 清理队列中的内容 */
void ripple_queue_clear(ripple_queue* queue, queuedatafree datafree);

/*
 * 队列释放
 *  * 入参:
 *  datafree   data的释放函数,可为空, 为空时，不释放 queueitem->data 值
*/
void ripple_queue_destroy(ripple_queue* queue, queuedatafree datafree);

#endif
