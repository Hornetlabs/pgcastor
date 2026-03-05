#include "ripple_app_incl.h"
#include "port/thread/ripple_thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "queue/ripple_queue.h"
#include "threads/ripple_threads.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "loadrecords/ripple_record.h"
#include "storage/ripple_ff_detail.h"
#include "storage/ripple_file_buffer.h"
#include "storage/ripple_ffsmgr.h"
#include "storage/trail/ripple_fftrail.h"
#include "parser/trail/ripple_parsertrail.h"
#include "increment/integrate/parser/ripple_increment_integrateparsertrail.h"
#include "bigtransaction/integrate/parser/ripple_bigtxn_integrateparsertrail.h"


static bool ripple_bigtxn_integrateparsertrail_txns2queue(ripple_bigtxn_integrateparsertrail* bigtxnparser)
{
    FullTransactionId xid               = InvalidFullTransactionId;
    HTAB *tx_htab                       = NULL;
    ripple_txn* txn                     = NULL;
    dlistnode* dlnode                   = NULL;
    ripple_parsertrail* parsertrail     = NULL;

    parsertrail = (ripple_parsertrail*)bigtxnparser->decodingctx;

    tx_htab = parsertrail->transcache->by_txns;

    for(dlnode = parsertrail->dtxns->head; NULL != dlnode; dlnode = parsertrail->dtxns->head)
    {
        parsertrail->dtxns->head = dlnode->next;
        txn = (ripple_txn*)dlnode->value;
        dlnode->value = NULL;

        if(RIPPLE_TXN_ISBIGTXN(txn->flag)
            && (NULL == hash_search(tx_htab, &xid, HASH_FIND, NULL)))
        {
            bigtxnparser->decodingctx->state = RIPPLE_INTEGRATE_STATUS_PARSER_EXIT;
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

        /* 事务内数据量达到阈值 */
        if(NULL != parsertrail->lasttxn && parsertrail->transcache->totalsize > bigtxnparser->integrateparser_buffer)
        {
            /* 复制一个事务加到缓存中并在hash中清理事务 */
            copy_txn = ripple_txn_copy(parsertrail->lasttxn);
            copy_txn->sysdictHis = NULL;
            ripple_cache_txn_add(parsertrail->parser2txn, copy_txn);
            parsertrail->lasttxn->stmts = NULL;
            parsertrail->transcache->totalsize = 0;
            parsertrail->lasttxn->stmtsize = 4;
        }

        if(RIPPLE_TXN_FLAG_NORMAL == parsertrail->lasttxn->flag)
        {
            RIPPLE_TXN_SET_BIGTXN(parsertrail->lasttxn->flag);
        }
    }

    return true;
}

/* 逻辑读取主线程 */
ripple_bigtxn_integrateparsertrail* ripple_bigtxn_integrateparsertrail_init(void)
{
    ripple_bigtxn_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (ripple_bigtxn_integrateparsertrail*)rmalloc0(sizeof(ripple_bigtxn_integrateparsertrail));
    if(NULL == traildecodecxt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(traildecodecxt, 0, 0, sizeof(ripple_bigtxn_integrateparsertrail));

    traildecodecxt->decodingctx = ripple_increment_integrateparsertrail_init();

    traildecodecxt->integrateparser_buffer = RIPPLE_MB2BYTE(guc_getConfigOptionInt(RIPPLE_CFG_KEY_INTEGRATE_BUFFER));
    return traildecodecxt;
}

/* 解析 trail 文件主函数 */
void *ripple_bigtxn_integrateparsertrail_main(void* args)
{
    int timeout                                                 = 0;
    ripple_thrnode* thrnode                                     = NULL;
    ripple_increment_integrateparsertrail* traildecodecxt       = NULL;
    ripple_bigtxn_integrateparsertrail* bigtxn_traildecodecxt   = NULL;

    thrnode = (ripple_thrnode*)args;

    bigtxn_traildecodecxt = (ripple_bigtxn_integrateparsertrail*)thrnode->data;

    traildecodecxt = bigtxn_traildecodecxt->decodingctx;

    /* 查看状态 */
    if(RIPPLE_THRNODE_STAT_STARTING != thrnode->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn parsertrail stat exception, expected state is RIPPLE_THRNODE_STAT_STARTING");
        thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
        ripple_pthread_exit(NULL);
    }

    /* 设置为工作状态 */
    thrnode->stat = RIPPLE_THRNODE_STAT_WORK;

    traildecodecxt->state = RIPPLE_INTEGRATE_STATUS_PARSER_WORK;

    /* 进入工作 */
    while(1)
    {
        if(RIPPLE_THRNODE_STAT_TERM == thrnode->stat)
        {
            /* 序列化/落盘 */
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }

        /* 获取数据, 超时退出 */
        traildecodecxt->parsertrail.records = ripple_queue_get(traildecodecxt->recordscache, &timeout);

        if(true == dlist_isnull(traildecodecxt->parsertrail.records))
        {
            if(RIPPLE_ERROR_TIMEOUT == timeout)
            {
                /* 睡眠 10 毫秒 */
                usleep(10000);
                continue;
            }

             /* 出错了 */
            elog(RLOG_WARNING, "ripple_cache_txn_getbatch error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(false == ripple_parsertrail_parser((ripple_parsertrail *)traildecodecxt))
        {
            elog(RLOG_WARNING, "bigtxn integrate parser parser error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        /* 事务处理 */
        if(true == dlist_isnull(traildecodecxt->parsertrail.dtxns))
        {
            continue;
        }

        if(false == ripple_bigtxn_integrateparsertrail_txns2queue(bigtxn_traildecodecxt))
        {
            elog(RLOG_WARNING, "bigtxn integrate add txn 2 queue error");
            thrnode->stat = RIPPLE_THRNODE_STAT_ABORT;
            break;
        }

        if(RIPPLE_INTEGRATE_STATUS_PARSER_EXIT == bigtxn_traildecodecxt->decodingctx->state)
        {
            thrnode->stat = RIPPLE_THRNODE_STAT_EXIT;
            break;
        }
    }
    ripple_pthread_exit(NULL);
    return NULL;
}

void ripple_bigtxn_integrateparsertrail_free(void *args)
{
    ripple_bigtxn_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (ripple_bigtxn_integrateparsertrail*)args;

    if (traildecodecxt->decodingctx)
    {
        ripple_increment_integrateparsertrail_free(traildecodecxt->decodingctx);
    }

    rfree(traildecodecxt);
}
