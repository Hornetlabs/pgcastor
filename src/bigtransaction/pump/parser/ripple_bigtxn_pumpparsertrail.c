#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "queue/ripple_queue.h"
#include "stmts/ripple_txnstmt.h"
#include "parser/trail/ripple_parsertrail.h"
#include "loadrecords/ripple_record.h"
#include "increment/pump/parser/ripple_increment_pumpparsertrail.h"
#include "bigtransaction/pump/parser/ripple_bigtxn_pumpparsertrail.h"

static bool ripple_bigtxn_pumpparsertrail_txns2queue(ripple_bigtxn_pumpparsertrail* bigtxnparser)
{
    FullTransactionId xid               = InvalidFullTransactionId;
    HTAB *tx_htab                       = NULL;
    ripple_txn* txn                     = NULL;
    dlistnode* dlnode                   = NULL;
    ripple_parsertrail* parsertrail     = NULL;

    parsertrail = (ripple_parsertrail*)bigtxnparser->pumpparsertrail;

    tx_htab = parsertrail->transcache->by_txns;

    for(dlnode = parsertrail->dtxns->head; NULL != dlnode; dlnode = parsertrail->dtxns->head)
    {
        parsertrail->dtxns->head = dlnode->next;
        txn = (ripple_txn*)dlnode->value;
        dlnode->value = NULL;
    
        if(RIPPLE_TXN_ISBIGTXN(txn->flag)
            && (NULL == hash_search(tx_htab, &xid, HASH_FIND, NULL)))
        {
            bigtxnparser->pumpparsertrail->state = RIPPLE_PUMP_STATUS_PARSER_STATE_EXIT;
            txn->commit = true;
        }

        ripple_cache_txn_add(parsertrail->parser2txn, txn);
        dlnode->value = NULL;
        dlist_node_free(dlnode, NULL);
    }

    if(NULL != parsertrail->lasttxn)
    {
        ripple_txn* copy_txn = NULL;

        xid = parsertrail->lasttxn->xid;

        /* 复制一个事务加到缓存中并在hash中清理事务 */
        copy_txn = ripple_txn_copy(parsertrail->lasttxn);
        copy_txn->sysdictHis = NULL;
        ripple_cache_txn_add(parsertrail->parser2txn, copy_txn);
        parsertrail->lasttxn->stmts = NULL;
        parsertrail->transcache->totalsize = 0;
        parsertrail->lasttxn->stmtsize = 4;

        if(RIPPLE_TXN_FLAG_NORMAL == parsertrail->lasttxn->flag)
        {
            RIPPLE_TXN_SET_BIGTXN(parsertrail->lasttxn->flag);
        }
    }

    return true;
}

/* 逻辑读取主线程 */
ripple_bigtxn_pumpparsertrail* ripple_bigtxn_pumpparsertrail_init(void)
{
    ripple_bigtxn_pumpparsertrail* traildecodecxt = NULL;

    traildecodecxt = (ripple_bigtxn_pumpparsertrail*)rmalloc0(sizeof(ripple_bigtxn_pumpparsertrail));
    if(NULL == traildecodecxt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(traildecodecxt, 0, 0, sizeof(ripple_bigtxn_pumpparsertrail));

    traildecodecxt->pumpparsertrail = ripple_increment_pumpparsertrail_init();
    return traildecodecxt;
}

void *ripple_bigtxn_pumpparsertrail_main(void *args)
{
    int timeout                                             = 0;
    ripple_thrnode* thrnode                                 = NULL;
    ripple_parsertrail* parsertrail                         = NULL;
    ripple_increment_pumpparsertrail* pumpparsertrail       = NULL;
    ripple_bigtxn_pumpparsertrail* bigtxn_traildecodecxt    = NULL;


    thrnode = (ripple_thrnode *)args;
    bigtxn_traildecodecxt = (ripple_bigtxn_pumpparsertrail*)thrnode->data;

    pumpparsertrail = bigtxn_traildecodecxt->pumpparsertrail;
    parsertrail = (ripple_parsertrail*)pumpparsertrail;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "pump bigtxn parsertrail stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;
    pumpparsertrail->state = RIPPLE_PUMP_STATUS_PARSER_STATE_WORK;

    while(1)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据, 超时退出 */
        parsertrail->records = ripple_queue_get(pumpparsertrail->recordscache, &timeout);
        if(NULL == parsertrail->records)
        {
            /* 需要退出，等待 worknode->status 变为 RIPPLE_WORK_STATUS_TERM 后退出*/
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            elog(RLOG_WARNING, "pump bigtxn parser get records from queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(false == ripple_parsertrail_parser(parsertrail))
        {
            elog(RLOG_WARNING, "bigtxn pump parser parser error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 事务处理 */
        if(true == dlist_isnull(parsertrail->dtxns))
        {
            continue;
        }

        if(false == ripple_bigtxn_pumpparsertrail_txns2queue(bigtxn_traildecodecxt))
        {
            elog(RLOG_WARNING, "bigtxn pump add txn 2 queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(RIPPLE_PUMP_STATUS_PARSER_STATE_EXIT == pumpparsertrail->state)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
    }

    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_bigtxn_pumpparsertrail_free(void *args)
{
    ripple_bigtxn_pumpparsertrail* traildecodecxt = NULL;

    traildecodecxt = (ripple_bigtxn_pumpparsertrail*)args;

    if (NULL == traildecodecxt)
    {
        return;
    }

    if (traildecodecxt->pumpparsertrail)
    {
        ripple_increment_pumpparsertrail_free(traildecodecxt->pumpparsertrail);
    }

    rfree(traildecodecxt);
}