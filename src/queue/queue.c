#include "app_incl.h"
#include "port/thread/thread.h"
#include "queue/queue.h"

/* 初始化队列中的每个item */
static queueitem* queueitem_init(void)
{
    queueitem* queue_item = NULL;
    queue_item = (queueitem*)rmalloc0(sizeof(queueitem));
    if(NULL == queue_item)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return false;
    }
    rmemset0(queue_item, 0, 0, sizeof(queueitem));
    queue_item->prev = NULL;
    queue_item->next = NULL;
    queue_item->data = NULL;

    return queue_item;
}

/*
 * 队列Item释放
 *  * 入参:
 *  datafree   data的释放函数,可为空, 为空时，不释放 queueitem->data 值
*/
void queueitem_free(queueitem* queueitem, queuedatafree datafree)
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
queue* queue_init(void)
{
    queue* queue_ptr = NULL;
    queue_ptr = (queue*)rmalloc0(sizeof(queue));
    if(NULL == queue_ptr)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(queue_ptr, 0, 0, sizeof(queue));
    queue_ptr->max = 0;
    queue_ptr->cnt = 0;
    queue_ptr->head = NULL;
    queue_ptr->tail = NULL;
    queue_ptr->waits = 0;

    /* 初始化锁信息 */
    osal_thread_mutex_init(&queue_ptr->lock, NULL);
    if(0 != osal_thread_cond_init(&queue_ptr->cond, NULL))
    {
        elog(RLOG_WARNING, "can not init queue cond, %s", strerror(errno));
        return NULL;
    }

    return queue_ptr;
}

/* 设置 max */
void queue_setmax(queue* queue, uint64 max)
{
    queue->max = max;
}

/*
 * 在队列头部加入
*/
bool queue_puthead(queue* queue, void* data)
{
    int iret = 0;
    queueitem* qitem = NULL;

    queue->error = ERROR_SUCCESS;
    if(0 < queue->max)
    {
        if(queue->cnt > queue->max)
        {
            queue->error = ERROR_QUEUE_FULL;
            return false;
        }
    }

    qitem = queueitem_init();
    if(NULL == qitem)
    {
        elog(RLOG_WARNING, "queue put item init error");
        return false;
    }
    qitem->data = data;

    /* 加入到链表中 */
    iret = osal_thread_lock(&queue->lock);
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
        osal_thread_cond_signal(&queue->cond);
    }

    osal_thread_unlock(&queue->lock);
    return true;
}

/* 
 * 像队列中添加数据
 *  data 需要放入到队列中的数据
 */
bool queue_put(queue* queue, void* data)
{
    int iret = 0;
    queueitem* qitem = NULL;

    queue->error = ERROR_SUCCESS;
    if(0 < queue->max)
    {
        if(queue->cnt > queue->max)
        {
            queue->error = ERROR_QUEUE_FULL;
            return false;
        }
    }

    qitem = queueitem_init();
    if(NULL == qitem)
    {
        elog(RLOG_WARNING, "queue put item init error");
        return false;
    }
    qitem->data = data;

    /* 加入到链表中 */
    iret = osal_thread_lock(&queue->lock);
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
        osal_thread_cond_signal(&queue->cond);
    }

    osal_thread_unlock(&queue->lock);
    return true;
}

/* 批量获取 */
queueitem* queue_trygetbatch(queue* queue)
{
    int iret = 0;
    queueitem* qitem = NULL;

    while(1)
    {
        iret = osal_thread_lock(&queue->lock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
            break;
        }

        if(NULL == queue->head)
        {
            osal_thread_unlock(&queue->lock);
            break;
        }

        qitem = queue->head;
        queue->head = NULL;
        queue->tail = NULL;
        queue->cnt = 0;

        /* 解锁 */
        osal_thread_unlock(&queue->lock);
        return qitem;
    }

    return NULL;
}

/* 
 * 在队列中获取数据
 *  返回为空时, 需要判断是否为超时
 */
void* queue_tryget(queue* queue)
{
    void* data = NULL;
    queueitem* qitem = NULL;

    while(1)
    {
        osal_thread_lock(&queue->lock);

        if(NULL == queue->head)
        {
            osal_thread_unlock(&(queue->lock));
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
        osal_thread_unlock(&queue->lock);

        data = qitem->data;

        queueitem_free(qitem, NULL);
        return data;
    }

    return NULL;
}

/* 
 * 在队列中获取数据
 *  返回为空时, 需要判断是否为超时
 */
void* queue_get(queue* queue, int* timeout)
{
    int iret = 0;
    void* data = NULL;
    queueitem* qitem = NULL;
    struct timespec ts = { 0 };

    if(NULL != timeout)
    {
        *timeout = ERROR_SUCCESS;
    }

    while(1)
    {
        iret = osal_thread_lock(&queue->lock);
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
            iret = osal_thread_cond_timewait(&queue->cond, &queue->lock, &ts);
            if(0 != iret)
            {
                if(iret == ETIMEDOUT)
                {
                    queue->waits--;
                    osal_thread_unlock(&(queue->lock));

                    if(NULL != timeout)
                    {
                        *timeout = ERROR_TIMEOUT;
                    }
                    return NULL;
                }

                elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
                osal_thread_unlock(&(queue->lock));
                return NULL;
            }
            queue->waits--;
            osal_thread_unlock(&(queue->lock));
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
        iret = osal_thread_unlock(&queue->lock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "unlock error:%s", strerror(errno));
            return NULL;
        }

        data = qitem->data;

        queueitem_free(qitem, NULL);
        return data;
    }

    return NULL;
}

/* 判断对列是否为空 */
bool queue_isnull(queue* queue)
{
    bool result = false;

    if(NULL == queue->head)
    {
        result = true;
    }

    return result;
}

/* 清理队列中的内容 */
void queue_clear(queue* queue, queuedatafree datafree)
{
    queueitem* qitem = NULL;
    if(NULL == queue)
    {
        return;
    }
    osal_thread_lock(&queue->lock);

    for(qitem = queue->head; NULL != qitem; qitem = queue->head)
    {
        queue->head = qitem->next;
        queueitem_free(qitem, datafree);
    }

    queue->cnt = 0;
    queue->error = 0;
    queue->waits = 0;
    queue->tail = NULL;

    osal_thread_unlock(&queue->lock);
}

/*
 * 队列释放
 *  * 入参:
 *  datafree   data的释放函数,可为空, 为空时，不释放 queueitem->data 值
*/
void queue_destroy(queue* queue, queuedatafree datafree)
{
    queueitem* qitem = NULL;

    elog(RLOG_DEBUG, "queue_destroy");

    if(NULL == queue)
    {
        return;
    }

    osal_thread_mutex_destroy(&queue->lock);
    osal_thread_cond_destroy(&queue->cond);

    for(qitem = queue->head; NULL != qitem; qitem = queue->head)
    {
        queue->head = qitem->next;
        queueitem_free(qitem, datafree);
    }

    rfree(queue);
}

