#ifndef _RIPPLE_CACHE_TXN_H
#define _RIPPLE_CACHE_TXN_H

typedef struct RIPPLE_CACHE_TXN
{
    bool                            wsignal;
    int                             totalnum;
    pthread_cond_t                  cond;
    pthread_mutex_t                 mutex_lock;
    ripple_txn*                     head;
    ripple_txn**                    tail;
} ripple_cache_txn;

/* 添加数据 */
void ripple_cache_txn_add(ripple_cache_txn* stmtcache, ripple_txn* txn);

/* 获取数据 */
ripple_txn* ripple_cache_txn_get(ripple_cache_txn* stmtcache, int* timeout);

/* 获取批量数据 */
ripple_txn* ripple_cache_txn_getbatch(ripple_cache_txn* stmtcache, int* timeout);

void ripple_cache_txn_clean(ripple_cache_txn* stmtcache);

/* 初始化 */
ripple_cache_txn* ripple_cache_txn_init(void);

void ripple_cache_txn_destroy(ripple_cache_txn* stmtcache);

bool ripple_cache_txn_isnull(ripple_cache_txn* stmtcache);

#endif
