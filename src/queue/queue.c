#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "queue/ripple_queue.h"

/* 初始化队列中的每个item */
static ripple_queueitem* ripple_queueitem_init(void)
{
    ripple_queueitem* queueitem = NULL;
    queueitem = (ripple_queueitem*)rmalloc0(sizeof(ripple_queueitem));
    if(NULL == queueitem)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rmemset0(queueitem, 0, 0, sizeof(ripple_queueitem));
    queueitem->prev = NULL;
    queueitem->next = NULL;
    queueitem->data = NULL;

    return queueitem;
}

/*
 * 队列Item释放
 *  * 入参:
 *  datafree   data的释放函数,可为空, 为空时，不释放 queueitem->data 值
*/
void ripple_queueitem_free(ripple_queueitem* queueitem, queuedatafree datafree)
{
    if(NULL == queueitem)
    {
        return;
    }

    if(NULL != datafree)
    {
        datafree(queueitem->data);
    }
    rfree(queueitem);
}


/* 队列初始化 */
ripple_queue* ripple_queue_init(void)
{
    ripple_queue* queue = NULL;
    queue = (ripple_queue*)rmalloc0(sizeof(ripple_queue));
    if(NULL == queue)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(queue, 0, 0, sizeof(ripple_queue));
    queue->max = 0;
    queue->cnt = 0;
    queue->head = NULL;
    queue->tail = NULL;
    queue->waits = 0;

    /* 初始化锁信息 */
    ripple_thread_mutex_init(&queue->lock, NULL);
    if(0 != ripple_thread_cond_init(&queue->cond, NULL))
    {
        elog(RLOG_WARNING, "can not init queue cond, %s", strerror(errno));
        return NULL;
    }

    return queue;
}

/* 设置 max */
void ripple_queue_setmax(ripple_queue* queue, uint64 max)
{
    queue->max = max;
}

/*
 * 在队列头部加入
*/
bool ripple_queue_puthead(ripple_queue* queue, void* data)
{
    int iret = 0;
    ripple_queueitem* qitem = NULL;

    queue->error = RIPPLE_ERROR_SUCCESS;
    if(0 < queue->max)
    {
        if(queue->cnt > queue->max)
        {
            queue->error = RIPPLE_ERROR_QUEUE_FULL;
            return false;
        }
    }

    qitem = ripple_queueitem_init();
    if(NULL == qitem)
    {
        elog(RLOG_WARNING, "queue put item init error");
        return false;
    }
    qitem->data = data;

    /* 加入到链表中 */
    iret = ripple_thread_lock(&queue->lock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    if (NULL == queue->head)
    {
        queue->tail = qitem;
    }
    else
    {
        qitem->next = queue->head;
    }
    queue->head = qitem;
    queue->cnt++;
    if(0 < queue->waits)
    {
        ripple_thread_cond_signal(&queue->cond);
    }

    ripple_thread_unlock(&queue->lock);
    return true;
}

/* 
 * 像队列中添加数据
 *  data 需要放入到队列中的数据
 */
bool ripple_queue_put(ripple_queue* queue, void* data)
{
    int iret = 0;
    ripple_queueitem* qitem = NULL;

    queue->error = RIPPLE_ERROR_SUCCESS;
    if(0 < queue->max)
    {
        if(queue->cnt > queue->max)
        {
            queue->error = RIPPLE_ERROR_QUEUE_FULL;
            return false;
        }
    }

    qitem = ripple_queueitem_init();
    if(NULL == qitem)
    {
        elog(RLOG_WARNING, "queue put item init error");
        return false;
    }
    qitem->data = data;

    /* 加入到链表中 */
    iret = ripple_thread_lock(&queue->lock);
    if(0 != iret)
    {
        elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
        return false;
    }

    if (NULL == queue->head)
    {
        queue->head = qitem;
    }
    else
    {
        queue->tail->next = qitem;
    }
    queue->tail = qitem;
    queue->cnt++;
    if(0 < queue->waits)
    {
        ripple_thread_cond_signal(&queue->cond);
    }

    ripple_thread_unlock(&queue->lock);
    return true;
}

/* 批量获取 */
ripple_queueitem* ripple_queue_trygetbatch(ripple_queue* queue)
{
    int iret = 0;
    ripple_queueitem* qitem = NULL;

    while(1)
    {
        iret = ripple_thread_lock(&queue->lock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
            break;
        }

        if(NULL == queue->head)
        {
            ripple_thread_unlock(&queue->lock);
            break;
        }

        qitem = queue->head;
        queue->head = NULL;
        queue->tail = NULL;
        queue->cnt = 0;

        /* 解锁 */
        ripple_thread_unlock(&queue->lock);
        return qitem;
    }

    return NULL;
}

/* 
 * 在队列中获取数据
 *  返回为空时, 需要判断是否为超时
 */
void* ripple_queue_tryget(ripple_queue* queue)
{
    void* data = NULL;
    ripple_queueitem* qitem = NULL;

    while(1)
    {
        ripple_thread_lock(&queue->lock);

        if(NULL == queue->head)
        {
            ripple_thread_unlock(&(queue->lock));
            return NULL;;
        }

        /* 获取数据 */
        qitem = queue->head;
        queue->head = qitem->next;
        qitem->next = NULL;

        if(NULL == queue->head)
        {
            queue->tail = NULL;
        }
        queue->cnt--;
        /* 解锁 */
        ripple_thread_unlock(&queue->lock);

        data = qitem->data;

        ripple_queueitem_free(qitem, NULL);
        return data;
    }

    return NULL;
}

/* 
 * 在队列中获取数据
 *  返回为空时, 需要判断是否为超时
 */
void* ripple_queue_get(ripple_queue* queue, int* timeout)
{
    int iret = 0;
    void* data = NULL;
    ripple_queueitem* qitem = NULL;
    struct timespec ts = { 0 };

    if(NULL != timeout)
    {
        *timeout = RIPPLE_ERROR_SUCCESS;
    }

    while(1)
    {
        iret = ripple_thread_lock(&queue->lock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
            return NULL;
        }

        if(NULL == queue->head)
        {
            /* 需要等待 */
            /* 设置超时时间 */
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;

            /* 设置标识，告知有线程等待 */
            queue->waits++;
            iret = ripple_thread_cond_timewait(&queue->cond, &queue->lock, &ts);
            if(0 != iret)
            {
                if(iret == ETIMEDOUT)
                {
                    queue->waits--;
                    ripple_thread_unlock(&(queue->lock));

                    if(NULL != timeout)
                    {
                        *timeout = RIPPLE_ERROR_TIMEOUT;
                    }
                    return NULL;
                }

                elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
                ripple_thread_unlock(&(queue->lock));
                return NULL;
            }
            queue->waits--;
            ripple_thread_unlock(&(queue->lock));
            continue;
        }

        /* 获取数据 */
        qitem = queue->head;
        queue->head = qitem->next;
        qitem->next = NULL;

        if(NULL == queue->head)
        {
            queue->tail = NULL;
        }
        queue->cnt--;
        /* 解锁 */
        iret = ripple_thread_unlock(&queue->lock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "unlock error:%s", strerror(errno));
            return NULL;
        }

        data = qitem->data;

        ripple_queueitem_free(qitem, NULL);
        return data;
    }

    return NULL;
}

/* 判断对列是否为空 */
bool ripple_queue_isnull(ripple_queue* queue)
{
    bool result = false;

    if(NULL == queue->head)
    {
        result = true;
    }

    return result;
}

/* 清理队列中的内容 */
void ripple_queue_clear(ripple_queue* queue, queuedatafree datafree)
{
    ripple_queueitem* qitem = NULL;
    if(NULL == queue)
    {
        return;
    }
    ripple_thread_lock(&queue->lock);

    for(qitem = queue->head; NULL != qitem; qitem = queue->head)
    {
        queue->head = qitem->next;
        ripple_queueitem_free(qitem, datafree);
    }

    queue->cnt = 0;
    queue->error = 0;
    queue->waits = 0;
    queue->tail = NULL;

    ripple_thread_unlock(&queue->lock);
}

/*
 * 队列释放
 *  * 入参:
 *  datafree   data的释放函数,可为空, 为空时，不释放 queueitem->data 值
*/
void ripple_queue_destroy(ripple_queue* queue, queuedatafree datafree)
{
    ripple_queueitem* qitem = NULL;

    elog(RLOG_DEBUG, "ripple_queue_destroy");

    if(NULL == queue)
    {
        return;
    }

    ripple_thread_mutex_destroy(&queue->lock);
    ripple_thread_cond_destroy(&queue->cond);

    for(qitem = queue->head; NULL != qitem; qitem = queue->head)
    {
        queue->head = qitem->next;
        ripple_queueitem_free(qitem, datafree);
    }

    rfree(queue);
}

