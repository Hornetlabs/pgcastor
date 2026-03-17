#include "app_incl.h"
#include "utils/hash/hash_utils.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/guc/guc.h"
#include "port/thread/thread.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"

/*
 * parser 线程与中转线程-写线程  共同维护
 *   1、parser 线程将 commit 的事务内容转移到此处
 *   2、中转线程将 commit 后的数据进行落盘或转移至 queuecache 中
*/

/* 添加数据 */
void cache_txn_add(cache_txn* stmtcache, txn* txn)
{
    int iret = 0;
    if(NULL == txn)
    {
        return;
    }
    txn->cachenext = NULL;

stmtcache_add_retry:
    /* 加入到链表中 */
    iret = osal_thread_lock(&stmtcache->mutex_lock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    /* 检测超过 transmaxnum 大小 */
    if((stmtcache->totalnum + 1) > g_transmaxnum)
    {
        /* 缓存已满, 解锁，等缓存未满 */
        /* 解锁,返回 */
        if(true == stmtcache->wsignal)
        {
            stmtcache->wsignal = false;
            osal_thread_cond_signal(&stmtcache->cond);
        }
        osal_thread_unlock(&stmtcache->mutex_lock);
        usleep(10000);
        elog(RLOG_DEBUG, "stmtcache full,waiting space, totalnum:%d, maxnum:%d",
                            stmtcache->totalnum,
                            g_transmaxnum);
        goto stmtcache_add_retry;
    }

    *stmtcache->tail = txn;
    stmtcache->tail = &(txn->cachenext);
    stmtcache->totalnum++;

    if(true == stmtcache->wsignal)
    {
        stmtcache->wsignal = false;
        osal_thread_cond_signal(&stmtcache->cond);
    }

    osal_thread_unlock(&stmtcache->mutex_lock);
}

/* 
 * 获取数据
 *  extra 用于设置额外值信息，在当前的场景中设置了超时，当超过指定的时间后，返回NULL并设置extra的返回代码为超时
 *      extra           0               退出
 *      extra           1               超时
*/
txn* cache_txn_get(cache_txn* stmtcache, int* timeout)
{
    int iret = 0;
    txn* entry = NULL;
    struct timespec ts = { 0 };

    *timeout = 0;
    while(1)
    {
        iret = osal_thread_lock(&stmtcache->mutex_lock);
        if(0 != iret)
        {
            elog(RLOG_WARNING, "get lock error:%s", strerror(errno));
            return NULL;
        }

        if(NULL == stmtcache->head)
        {
            /* 需要等待 */
            /* 设置超时时间 */
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;

            /* 设置标识，告知有线程等待 */
            stmtcache->wsignal = true;
            iret = osal_thread_cond_timewait(&stmtcache->cond, &stmtcache->mutex_lock, &ts);
            if(0 != iret)
            {
                if(iret == ETIMEDOUT)
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

        /* 获取数据 */
        entry = stmtcache->head;
        stmtcache->head = entry->cachenext;
        stmtcache->totalnum--;

        if(NULL == stmtcache->head)
        {
            stmtcache->tail = &(stmtcache->head);
        }

        /* 解锁 */
        iret = osal_thread_unlock(&stmtcache->mutex_lock);
        if(0 != iret)
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
 * 获取批量数据
 *  extra 用于设置额外值信息，在当前的场景中设置了超时，当超过指定的时间后，返回NULL并设置extra的返回代码为超时
 *      extra           0               退出
 *      extra           1               超时
*/
txn* cache_txn_getbatch(cache_txn* stmtcache, int* timeout)
{
    int iret = 0;
    txn* entry = NULL;
    struct timespec ts = { 0 };

    *timeout = 0;
    while(1)
    {
        iret = osal_thread_lock(&stmtcache->mutex_lock);
        if(0 != iret)
        {
            elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
        }

        if(NULL == stmtcache->head)
        {
            /* 需要等待 */
            /* 设置超时时间 */
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;

            /* 设置标识，告知有线程等待 */
            stmtcache->wsignal = true;
            iret = osal_thread_cond_timewait(&stmtcache->cond, &stmtcache->mutex_lock, &ts);
            if(0 != iret)
            {
                if(iret == ETIMEDOUT)
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

        /* 获取数据 */
        stmtcache->totalnum = 0;
        entry = stmtcache->head;
        stmtcache->head = NULL;
        if(NULL == stmtcache->head)
        {
            stmtcache->tail = &(stmtcache->head);
        }

        /* 解锁 */
        iret = osal_thread_unlock(&stmtcache->mutex_lock);
        if(0 != iret)
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
    int iret = 0;
    if(NULL == stmtcache)
    {
        return;
    }

    /* 锁定 */
    iret = osal_thread_lock(&stmtcache->mutex_lock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "get lock error:%s", strerror(errno));
    }

    /* 清理 stmtcache 缓存 */
    for(txn = stmtcache->head; NULL != txn; txn = stmtcache->head)
    {
        stmtcache->head = txn->cachenext;

        txn_free(txn);
    }

    stmtcache->wsignal = false;
    stmtcache->totalnum = 0;
    stmtcache->head = NULL;
    stmtcache->tail = &(stmtcache->head);

    /* 解锁 */
    iret = osal_thread_unlock(&stmtcache->mutex_lock);
    if(0 != iret)
    {
        elog(RLOG_ERROR, "unlock error:%s", strerror(errno));
    }
}

void cache_txn_destroy(cache_txn* stmtcache)
{
    txn* txn = NULL;
    if(NULL == stmtcache)
    {
        return;
    }

    osal_thread_mutex_destroy(&stmtcache->mutex_lock);
    osal_thread_cond_destroy(&stmtcache->cond);

    /* 清理 stmtcache 缓存 */
    for(txn = stmtcache->head; NULL != txn; txn = stmtcache->head)
    {
        stmtcache->head = txn->cachenext;

        txn_free(txn);
    }

    rfree(stmtcache);
    stmtcache = NULL;
}

/* 缓存是否为空 */
bool cache_txn_isnull(cache_txn* stmtcache)
{
    if(NULL == stmtcache)
    {
        return true;
    }

    if (NULL == stmtcache->head)
    {
        return true;
    }
    return false;
}

/* stmt 缓存 */
cache_txn* cache_txn_init(void)
{
    cache_txn* stmtcache = NULL;
    stmtcache = (cache_txn*)rmalloc1(sizeof(cache_txn));
    if(NULL == stmtcache)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(stmtcache, 0, '\0', sizeof(cache_txn));

    /* 初始化值信息 */
    stmtcache->wsignal = false;
    stmtcache->totalnum = 0;
    stmtcache->head = NULL;
    stmtcache->tail = &(stmtcache->head);

    /* 锁和信号初始化 */
    osal_thread_mutex_init(&stmtcache->mutex_lock, NULL);

    if(0 != osal_thread_cond_init(&stmtcache->cond, NULL))
    {
        elog(RLOG_ERROR, "can not init pthread cond, %s", strerror(errno));
    }

    return stmtcache;
}
