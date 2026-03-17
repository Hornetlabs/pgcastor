#ifndef _CACHE_TXN_H
#define _CACHE_TXN_H

typedef struct CACHE_TXN
{
    bool                            wsignal;
    int                             totalnum;
    pthread_cond_t                  cond;
    pthread_mutex_t                 mutex_lock;
    txn*                     head;
    txn**                    tail;
} cache_txn;

/* 添加数据 */
void cache_txn_add(cache_txn* stmtcache, txn* txn);

/* 获取数据 */
txn* cache_txn_get(cache_txn* stmtcache, int* timeout);

/* 获取批量数据 */
txn* cache_txn_getbatch(cache_txn* stmtcache, int* timeout);

void cache_txn_clean(cache_txn* stmtcache);

/* 初始化 */
cache_txn* cache_txn_init(void);

void cache_txn_destroy(cache_txn* stmtcache);

bool cache_txn_isnull(cache_txn* stmtcache);

#endif
