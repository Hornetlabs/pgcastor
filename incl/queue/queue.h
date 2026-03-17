#ifndef _QUEUE_H
#define _QUEUE_H

typedef void (*queuedatafree)(void* value);

typedef struct QUEUEITEM
{
    void*                           data;
    struct QUEUEITEM*        prev;
    struct QUEUEITEM*        next;
} queueitem;

typedef struct QUEUE
{
    int                             waits;
    int                             error;
    uint64                          max;                /* 允许的 item 的最大数量 */
    uint64                          cnt;                /* 当前 item 的个数 */
    pthread_cond_t                  cond;
    pthread_mutex_t                 lock;
    queueitem*               head;
    queueitem*               tail;
} queue;

/* 队列初始化 */
queue* queue_init(void);

/* 设置 max */
void queue_setmax(queue* queue, uint64 max);

/*
 * 在队列头部加入
*/
bool queue_puthead(queue* queue, void* data);

/* 
 * 像队列中添加数据
 *  data 需要放入到队列中的数据
 */
bool queue_put(queue* queue, void* data);

void queueitem_free(queueitem* queueitem, queuedatafree datafree);

/* 批量获取 */
queueitem* queue_trygetbatch(queue* queue);

/* 
 * 在队列中获取数据
 *  返回为空时, 需要判断是否为超时
 */
void* queue_tryget(queue* queue);

/* 
 * 在队列中获取数据
 *  返回为空时, 需要判断是否为超时
 */
void* queue_get(queue* queue, int* timeout);

bool queue_isnull(queue* queue);

/* 清理队列中的内容 */
void queue_clear(queue* queue, queuedatafree datafree);

/*
 * 队列释放
 *  * 入参:
 *  datafree   data的释放函数,可为空, 为空时，不释放 queueitem->data 值
*/
void queue_destroy(queue* queue, queuedatafree datafree);

#endif
