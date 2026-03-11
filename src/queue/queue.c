#include "app_incl.h"
#include "port/thread/thread.h"
#include "queue/queue.h"

/* initialize each item in queue */
static queueitem* queueitem_init(void)
{
    queueitem* queue_item = NULL;
    queue_item = (queueitem*)rmalloc0(sizeof(queueitem));
    if (NULL == queue_item)
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
 * free queue item
 *  * parameters:
 *  datafree   data free function, may be NULL; if NULL, queueitem->data is not freed
 */
void queueitem_free(queueitem* queueitem, queuedatafree datafree)
{
    if (NULL == queueitem)
    {
        return;
    }

    if (NULL != datafree)
    {
        datafree(queueitem->data);
    }
    rfree(queueitem);
}

/* initialize queue */
queue* queue_init(void)
{
    queue* queue_ptr = NULL;
    queue_ptr = (queue*)rmalloc0(sizeof(queue));
    if (NULL == queue_ptr)
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

    /* initialize lock */
    osal_thread_mutex_init(&queue_ptr->lock, NULL);
    if (0 != osal_thread_cond_init(&queue_ptr->cond, NULL))
    {
        elog(RLOG_WARNING, "can not init queue cond, %s", strerror(errno));
        return NULL;
    }

    return queue_ptr;
}

/* set max */
void queue_setmax(queue* queue, uint64 max)
{
    queue->max = max;
}

/*
 * add to queue head
 */
bool queue_puthead(queue* queue, void* data)
{
    int        iret = 0;
    queueitem* qitem = NULL;

    queue->error = ERROR_SUCCESS;
    if (0 < queue->max)
    {
        if (queue->cnt > queue->max)
        {
            queue->error = ERROR_QUEUE_FULL;
            return false;
        }
    }

    qitem = queueitem_init();
    if (NULL == qitem)
    {
        elog(RLOG_WARNING, "queue put item init error");
        return false;
    }
    qitem->data = data;

    /* add to linked list */
    iret = osal_thread_lock(&queue->lock);
    if (0 != iret)
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
    if (0 < queue->waits)
    {
        osal_thread_cond_signal(&queue->cond);
    }

    osal_thread_unlock(&queue->lock);
    return true;
}

/*
 * add data to queue
 *  data: data to add to queue
 */
bool queue_put(queue* queue, void* data)
{
    int        iret = 0;
    queueitem* qitem = NULL;

    queue->error = ERROR_SUCCESS;
    if (0 < queue->max)
    {
        if (queue->cnt > queue->max)
        {
            queue->error = ERROR_QUEUE_FULL;
            return false;
        }
    }

    qitem = queueitem_init();
    if (NULL == qitem)
    {
        elog(RLOG_WARNING, "queue put item init error");
        return false;
    }
    qitem->data = data;

    /* add to linked list */
    iret = osal_thread_lock(&queue->lock);
    if (0 != iret)
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
    if (0 < queue->waits)
    {
        osal_thread_cond_signal(&queue->cond);
    }

    osal_thread_unlock(&queue->lock);
    return true;
}

/* batch get */
queueitem* queue_trygetbatch(queue* queue)
{
    int        iret = 0;
    queueitem* qitem = NULL;

    while (1)
    {
        iret = osal_thread_lock(&queue->lock);
        if (0 != iret)
        {
            elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
            break;
        }

        if (NULL == queue->head)
        {
            osal_thread_unlock(&queue->lock);
            break;
        }

        qitem = queue->head;
        queue->head = NULL;
        queue->tail = NULL;
        queue->cnt = 0;

        /* unlock */
        osal_thread_unlock(&queue->lock);
        return qitem;
    }

    return NULL;
}

/*
 * get data from queue
 *  returns NULL on timeout
 */
void* queue_tryget(queue* queue)
{
    void*      data = NULL;
    queueitem* qitem = NULL;

    while (1)
    {
        osal_thread_lock(&queue->lock);

        if (NULL == queue->head)
        {
            osal_thread_unlock(&(queue->lock));
            return NULL;
            ;
        }

        /* get data */
        qitem = queue->head;
        queue->head = qitem->next;
        qitem->next = NULL;

        if (NULL == queue->head)
        {
            queue->tail = NULL;
        }
        queue->cnt--;
        /* unlock */
        osal_thread_unlock(&queue->lock);

        data = qitem->data;

        queueitem_free(qitem, NULL);
        return data;
    }

    return NULL;
}

/*
 * get data from queue
 *  returns NULL on timeout
 */
void* queue_get(queue* queue, int* timeout)
{
    int             iret = 0;
    void*           data = NULL;
    queueitem*      qitem = NULL;
    struct timespec ts = {0};

    if (NULL != timeout)
    {
        *timeout = ERROR_SUCCESS;
    }

    while (1)
    {
        iret = osal_thread_lock(&queue->lock);
        if (0 != iret)
        {
            elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
            return NULL;
        }

        if (NULL == queue->head)
        {
            /* need to wait */
            /* set timeout */
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;

            /* set flag to indicate thread is waiting */
            queue->waits++;
            iret = osal_thread_cond_timewait(&queue->cond, &queue->lock, &ts);
            if (0 != iret)
            {
                if (iret == ETIMEDOUT)
                {
                    queue->waits--;
                    osal_thread_unlock(&(queue->lock));

                    if (NULL != timeout)
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

        /* get data */
        qitem = queue->head;
        queue->head = qitem->next;
        qitem->next = NULL;

        if (NULL == queue->head)
        {
            queue->tail = NULL;
        }
        queue->cnt--;
        /* unlock */
        iret = osal_thread_unlock(&queue->lock);
        if (0 != iret)
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

/* check if queue is empty */
bool queue_isnull(queue* queue)
{
    bool result = false;

    if (NULL == queue->head)
    {
        result = true;
    }

    return result;
}

/* clear queue contents */
void queue_clear(queue* queue, queuedatafree datafree)
{
    queueitem* qitem = NULL;
    if (NULL == queue)
    {
        return;
    }
    osal_thread_lock(&queue->lock);

    for (qitem = queue->head; NULL != qitem; qitem = queue->head)
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
 * destroy queue
 *  * parameters:
 *  datafree   data free function, may be NULL; if NULL, queueitem->data is not freed
 */
void queue_destroy(queue* queue, queuedatafree datafree)
{
    queueitem* qitem = NULL;

    elog(RLOG_DEBUG, "queue_destroy");

    if (NULL == queue)
    {
        return;
    }

    osal_thread_mutex_destroy(&queue->lock);
    osal_thread_cond_destroy(&queue->cond);

    for (qitem = queue->head; NULL != qitem; qitem = queue->head)
    {
        queue->head = qitem->next;
        queueitem_free(qitem, datafree);
    }

    rfree(queue);
}
