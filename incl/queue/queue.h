#ifndef _QUEUE_H
#define _QUEUE_H

typedef void (*queuedatafree)(void* value);

typedef struct QUEUEITEM
{
    void*             data;
    struct QUEUEITEM* prev;
    struct QUEUEITEM* next;
} queueitem;

typedef struct QUEUE
{
    int             waits;
    int             error;
    uint64          max; /* maximum allowed number of items */
    uint64          cnt; /* current number of items */
    pthread_cond_t  cond;
    pthread_mutex_t lock;
    queueitem*      head;
    queueitem*      tail;
} queue;

/* initialize queue */
queue* queue_init(void);

/* set max */
void queue_setmax(queue* queue, uint64 max);

/*
 * add to queue head
 */
bool queue_puthead(queue* queue, void* data);

/*
 * add data to queue
 *  data: data to add to queue
 */
bool queue_put(queue* queue, void* data);

void queueitem_free(queueitem* queueitem, queuedatafree datafree);

/* batch get */
queueitem* queue_trygetbatch(queue* queue);

/*
 * get data from queue (non-blocking)
 *  returns NULL on empty queue
 */
void* queue_tryget(queue* queue);

/*
 * get data from queue
 *  returns NULL on timeout
 */
void* queue_get(queue* queue, int* timeout);

bool queue_isnull(queue* queue);

/* clear queue contents */
void queue_clear(queue* queue, queuedatafree datafree);

/*
 * destroy queue
 *  * parameters:
 *  datafree   data free function, may be NULL; if NULL, queueitem->data is not freed
 */
void queue_destroy(queue* queue, queuedatafree datafree);

#endif
