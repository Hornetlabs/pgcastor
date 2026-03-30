#include "app_incl.h"
#include "utils/hash/hash_utils.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/guc/guc.h"
#include "port/thread/thread.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"

/*
 * Maintained jointly by parser thread and relay-write thread
 *   1. Parser thread transfers committed transaction content here
 *   2. Relay thread flushes committed data to disk or transfers to queuecache
 */

/* Add data */
void cache_txn_add(cache_txn* stmtcache, txn* txn)
{
    int iret = 0;
    if (NULL == txn)
    {
        return;
    }
    txn->cachenext = NULL;

stmtcache_add_retry:
    /* Add to linked list */
    iret = osal_thread_lock(&stmtcache->mutex_lock);
    if (0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    /* Check if exceeds transmaxnum size */
    if ((stmtcache->totalnum + 1) > g_transmaxnum)
    {
        /* Cache is full, unlock, wait for cache to be not full */
        /* Unlock, return */
        if (true == stmtcache->wsignal)
        {
            stmtcache->wsignal = false;
            osal_thread_cond_signal(&stmtcache->cond);
        }
        osal_thread_unlock(&stmtcache->mutex_lock);
        usleep(10000);
        elog(RLOG_DEBUG, "stmtcache full,waiting space, totalnum:%d, maxnum:%d", stmtcache->totalnum, g_transmaxnum);
        goto stmtcache_add_retry;
    }

    *stmtcache->tail = txn;
    stmtcache->tail = &(txn->cachenext);
    stmtcache->totalnum++;

    if (true == stmtcache->wsignal)
    {
        stmtcache->wsignal = false;
        osal_thread_cond_signal(&stmtcache->cond);
    }

    osal_thread_unlock(&stmtcache->mutex_lock);
}

/*
 * Get data
 *  extra
 * Used to set extra value information, in current scenario timeout is set, when exceeds specified
 * time, returns NULL and sets extra return code to timeout extra           0               Exit
 *      extra           1               Timeout
 */
txn* cache_txn_get(cache_txn* stmtcache, int* timeout)
{
    int             iret = 0;
    txn*            entry = NULL;
    struct timespec ts = {0};

    *timeout = 0;
    while (1)
    {
        iret = osal_thread_lock(&stmtcache->mutex_lock);
        if (0 != iret)
        {
            elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
            return NULL;
        }

        if (NULL == stmtcache->head)
        {
            /* Need to wait */
            /* Set timeout */
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;

            /* Set flag to notify waiting thread */
            stmtcache->wsignal = true;
            iret = osal_thread_cond_timewait(&stmtcache->cond, &stmtcache->mutex_lock, &ts);
            if (0 != iret)
            {
                if (iret == ETIMEDOUT)
                {
                    stmtcache->wsignal = false;
                    osal_thread_unlock(&(stmtcache->mutex_lock));

                    *timeout = ERROR_TIMEOUT;
                    return NULL;
                }

                elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
                return NULL;
            }
            stmtcache->wsignal = false;
        }

        /* Get data */
        entry = stmtcache->head;
        stmtcache->head = entry->cachenext;
        stmtcache->totalnum--;

        if (NULL == stmtcache->head)
        {
            stmtcache->tail = &(stmtcache->head);
        }

        /* Unlock */
        iret = osal_thread_unlock(&stmtcache->mutex_lock);
        if (0 != iret)
        {
            elog(RLOG_WARNING, "unlock error:%s", strerror(errno));
            return NULL;
        }
        entry->cachenext = NULL;
        return entry;
    }

    return NULL;
}

/*
 * Get batch data
 *  extra
 * Used to set extra value information, in current scenario timeout is set, when exceeds specified
 * time, returns NULL and sets extra return code to timeout extra           0               Exit
 *      extra           1               Timeout
 */
txn* cache_txn_getbatch(cache_txn* stmtcache, int* timeout)
{
    int             iret = 0;
    txn*            entry = NULL;
    struct timespec ts = {0};

    *timeout = 0;
    while (1)
    {
        iret = osal_thread_lock(&stmtcache->mutex_lock);
        if (0 != iret)
        {
            elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
        }

        if (NULL == stmtcache->head)
        {
            /* Need to wait */
            /* Set timeout */
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;

            /* Set flag to notify waiting thread */
            stmtcache->wsignal = true;
            iret = osal_thread_cond_timewait(&stmtcache->cond, &stmtcache->mutex_lock, &ts);
            if (0 != iret)
            {
                if (iret == ETIMEDOUT)
                {
                    stmtcache->wsignal = false;
                    osal_thread_unlock(&(stmtcache->mutex_lock));

                    *timeout = ERROR_TIMEOUT;
                    return NULL;
                }

                osal_thread_unlock(&(stmtcache->mutex_lock));
                return NULL;
            }
            stmtcache->wsignal = false;
        }

        /* Get data */
        stmtcache->totalnum = 0;
        entry = stmtcache->head;
        stmtcache->head = NULL;
        if (NULL == stmtcache->head)
        {
            stmtcache->tail = &(stmtcache->head);
        }

        /* Unlock */
        iret = osal_thread_unlock(&stmtcache->mutex_lock);
        if (0 != iret)
        {
            elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
        }
        return entry;
    }

    return NULL;
}

void cache_txn_clean(cache_txn* stmtcache)
{
    txn* txn = NULL;
    int  iret = 0;
    if (NULL == stmtcache)
    {
        return;
    }

    /* Lock */
    iret = osal_thread_lock(&stmtcache->mutex_lock);
    if (0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    /* Clean up stmtcache cache */
    for (txn = stmtcache->head; NULL != txn; txn = stmtcache->head)
    {
        stmtcache->head = txn->cachenext;

        txn_free(txn);
    }

    stmtcache->wsignal = false;
    stmtcache->totalnum = 0;
    stmtcache->head = NULL;
    stmtcache->tail = &(stmtcache->head);

    /* Unlock */
    iret = osal_thread_unlock(&stmtcache->mutex_lock);
    if (0 != iret)
    {
        elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
    }
}

void cache_txn_destroy(cache_txn* stmtcache)
{
    txn* txn = NULL;
    if (NULL == stmtcache)
    {
        return;
    }

    osal_thread_mutex_destroy(&stmtcache->mutex_lock);
    osal_thread_cond_destroy(&stmtcache->cond);

    /* Clean up stmtcache cache */
    for (txn = stmtcache->head; NULL != txn; txn = stmtcache->head)
    {
        stmtcache->head = txn->cachenext;

        txn_free(txn);
    }

    rfree(stmtcache);
    stmtcache = NULL;
}

/* Check if cache is empty */
bool cache_txn_isnull(cache_txn* stmtcache)
{
    if (NULL == stmtcache)
    {
        return true;
    }

    if (NULL == stmtcache->head)
    {
        return true;
    }
    return false;
}

/* stmt cache */
cache_txn* cache_txn_init(void)
{
    cache_txn* stmtcache = NULL;
    stmtcache = (cache_txn*)rmalloc1(sizeof(cache_txn));
    if (NULL == stmtcache)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(stmtcache, 0, '\0', sizeof(cache_txn));

    /* Initialize value information */
    stmtcache->wsignal = false;
    stmtcache->totalnum = 0;
    stmtcache->head = NULL;
    stmtcache->tail = &(stmtcache->head);

    /* Initialize lock and signal */
    osal_thread_mutex_init(&stmtcache->mutex_lock, NULL);

    if (0 != osal_thread_cond_init(&stmtcache->cond, NULL))
    {
        elog(RLOG_ERROR, "can not init pthread cond, %s", strerror(errno));
    }

    return stmtcache;
}
