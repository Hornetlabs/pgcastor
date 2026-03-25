#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "loadrecords/record.h"
#include "storage/ff_detail.h"
#include "storage/file_buffer.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"
#include "bigtransaction/integrate/parser/bigtxn_integrateparsertrail.h"

static bool bigtxn_integrateparsertrail_txns2queue(bigtxn_integrateparsertrail* bigtxnparser)
{
    FullTransactionId xid = InvalidFullTransactionId;
    HTAB*             tx_htab = NULL;
    txn*              txn_obj = NULL;
    dlistnode*        dlnode = NULL;
    parsertrail*      parsertrail_obj = NULL;

    parsertrail_obj = (parsertrail*)bigtxnparser->decodingctx;

    tx_htab = parsertrail_obj->transcache->by_txns;

    for (dlnode = parsertrail_obj->dtxns->head; NULL != dlnode;
         dlnode = parsertrail_obj->dtxns->head)
    {
        parsertrail_obj->dtxns->head = dlnode->next;
        txn_obj = (txn*)dlnode->value;
        dlnode->value = NULL;

        if (TXN_ISBIGTXN(txn_obj->flag) && (NULL == hash_search(tx_htab, &xid, HASH_FIND, NULL)))
        {
            bigtxnparser->decodingctx->state = INTEGRATE_STATUS_PARSER_EXIT;
            txn_obj->commit = true;
        }

        cache_txn_add(parsertrail_obj->parser2txn, txn_obj);
        dlnode->value = NULL;
        dlist_node_free(dlnode, NULL);
    }

    if (NULL != parsertrail_obj->lasttxn)
    {
        txn* copy_txn = NULL;

        xid = parsertrail_obj->lasttxn->xid;

        /* Transaction data volume reaches threshold */
        if (NULL != parsertrail_obj->lasttxn &&
            parsertrail_obj->transcache->totalsize > bigtxnparser->integrateparser_buffer)
        {
            /* Copy a transaction to cache and clean up transaction in hash */
            copy_txn = txn_copy(parsertrail_obj->lasttxn);
            copy_txn->sysdictHis = NULL;
            cache_txn_add(parsertrail_obj->parser2txn, copy_txn);
            parsertrail_obj->lasttxn->stmts = NULL;
            parsertrail_obj->transcache->totalsize = 0;
            parsertrail_obj->lasttxn->stmtsize = 4;
        }

        if (TXN_FLAG_NORMAL == parsertrail_obj->lasttxn->flag)
        {
            TXN_SET_BIGTXN(parsertrail_obj->lasttxn->flag);
        }
    }

    return true;
}

/* Logical read main thread */
bigtxn_integrateparsertrail* bigtxn_integrateparsertrail_init(void)
{
    bigtxn_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (bigtxn_integrateparsertrail*)rmalloc0(sizeof(bigtxn_integrateparsertrail));
    if (NULL == traildecodecxt)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(traildecodecxt, 0, 0, sizeof(bigtxn_integrateparsertrail));

    traildecodecxt->decodingctx = increment_integrateparsertrail_init();

    traildecodecxt->integrateparser_buffer =
        MB2BYTE(guc_getConfigOptionInt(CFG_KEY_INTEGRATE_BUFFER));
    return traildecodecxt;
}

/* Parse trail file main function */
void* bigtxn_integrateparsertrail_main(void* args)
{
    int                             timeout = 0;
    thrnode*                        thr_node = NULL;
    increment_integrateparsertrail* traildecodecxt = NULL;
    bigtxn_integrateparsertrail*    bigtxn_traildecodecxt = NULL;

    thr_node = (thrnode*)args;

    bigtxn_traildecodecxt = (bigtxn_integrateparsertrail*)thr_node->data;

    traildecodecxt = bigtxn_traildecodecxt->decodingctx;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(
            RLOG_WARNING,
            "integrate bigtxn parsertrail stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    traildecodecxt->state = INTEGRATE_STATUS_PARSER_WORK;

    /* Enter work */
    while (1)
    {
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialization/flush to disk */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data, timeout exit */
        traildecodecxt->parsertrail.records = queue_get(traildecodecxt->recordscache, &timeout);

        if (true == dlist_isnull(traildecodecxt->parsertrail.records))
        {
            if (ERROR_TIMEOUT == timeout)
            {
                /* Sleep 10 milliseconds */
                usleep(10000);
                continue;
            }

            /* Error occurred */
            elog(RLOG_WARNING, "cache_txn_getbatch error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if (false == parsertrail_parser((parsertrail*)traildecodecxt))
        {
            elog(RLOG_WARNING, "bigtxn integrate parser parser error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Transaction processing */
        if (true == dlist_isnull(traildecodecxt->parsertrail.dtxns))
        {
            continue;
        }

        if (false == bigtxn_integrateparsertrail_txns2queue(bigtxn_traildecodecxt))
        {
            elog(RLOG_WARNING, "bigtxn integrate add txn 2 queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if (INTEGRATE_STATUS_PARSER_EXIT == bigtxn_traildecodecxt->decodingctx->state)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
    }
    pthread_exit(NULL);
    return NULL;
}

void bigtxn_integrateparsertrail_free(void* args)
{
    bigtxn_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (bigtxn_integrateparsertrail*)args;

    if (traildecodecxt->decodingctx)
    {
        increment_integrateparsertrail_free(traildecodecxt->decodingctx);
    }

    rfree(traildecodecxt);
}
