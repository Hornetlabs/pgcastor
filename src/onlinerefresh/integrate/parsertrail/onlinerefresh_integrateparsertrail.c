#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "parser/trail/parsertrail.h"
#include "increment/integrate/parser/increment_integrateparsertrail.h"
#include "onlinerefresh/integrate/parsertrail/onlinerefresh_integrateparsertrail.h"

/* Logic read main thread */
onlinerefresh_integrateparsertrail* onlinerefresh_integrateparsertrail_init(void)
{
    onlinerefresh_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (onlinerefresh_integrateparsertrail*)rmalloc0(sizeof(onlinerefresh_integrateparsertrail));
    if (NULL == traildecodecxt)
    {
        elog(RLOG_WARNING, "onlinerefresh integrateparsertrail malloc out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(traildecodecxt, 0, 0, sizeof(onlinerefresh_integrateparsertrail));

    traildecodecxt->decodingctx = increment_integrateparsertrail_init();
    return traildecodecxt;
}

/*
 * false    processing error
 * true     processing success
 *
 * Input params:
 * bexit    true: can exit
 *          false: cannot exit yet
 */
static bool onlinerefresh_integrateparsertrail_txns2queue(increment_integrateparsertrail* parser, bool* bexit)
{
    txn*       txn_ptr = NULL;
    dlistnode* dlnode = NULL;

    for (dlnode = parser->parsertrail.dtxns->head; NULL != dlnode; dlnode = parser->parsertrail.dtxns->head)
    {
        parser->parsertrail.dtxns->head = dlnode->next;
        txn_ptr = (txn*)dlnode->value;
        dlnode->value = NULL;

        if (TXN_FLAG_NORMAL == txn_ptr->flag)
        {
            /* Normal transaction */
            cache_txn_add(parser->parsertrail.parser2txn, txn_ptr);
            dlnode->value = NULL;
            dlist_node_free(dlnode, NULL);
            continue;
        }
        else if (TXN_ISBIGTXN(txn_ptr->flag))
        {
            elog(RLOG_WARNING, "in integrate onlinerefresh increment parse bigtxn, logical error");
            return false;
        }
        else if (TXN_ISONLINEREFRESHTXN(txn_ptr->flag))
        {
            /* Incremental transaction */
            if (TXN_TYPE_ONLINEREFRESH_INC_END == txn_ptr->type)
            {
                /* Incremental processing completed, can exit */
                cache_txn_add(parser->parsertrail.parser2txn, txn_ptr);
                dlnode->value = NULL;
                dlist_node_free(dlnode, NULL);
                *bexit = true;
                return true;
            }
            else
            {
                elog(RLOG_WARNING, "in integrate onlinerefresh increment unknown txn flag:%u", txn_ptr->flag);
                return false;
            }
        }
        else
        {
            elog(RLOG_WARNING, "in integrate onlinerefresh increment unknown txn flag:%u", txn_ptr->flag);
            return false;
        }
    }

    return true;
}

/* Parse trail file main function */
void* onlinerefresh_integrateparsertrail_main(void* args)
{
    bool                                bexit = false;
    int                                 timeout = 0;
    thrnode*                            thr_node = NULL;
    parsertrail*                        parser_trail = NULL;
    increment_integrateparsertrail*     traildecodecxt = NULL;
    onlinerefresh_integrateparsertrail* oliparsertrail = NULL;

    thr_node = (thrnode*)args;

    oliparsertrail = (onlinerefresh_integrateparsertrail*)thr_node->data;

    traildecodecxt = oliparsertrail->decodingctx;
    parser_trail = (parsertrail*)traildecodecxt;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "onlinerefresh integrate parser trail stat exception, expected state is "
             "THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    /* Enter work */
    while (1)
    {
        /* First check if exit signal is received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data, timeout exit */
        traildecodecxt->parsertrail.records = queue_get(traildecodecxt->recordscache, &timeout);
        if (true == dlist_isnull(traildecodecxt->parsertrail.records))
        {
            if (ERROR_TIMEOUT == timeout)
            {
                continue;
            }
            elog(RLOG_WARNING, "onlinerefresh integrate get records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if (false == parsertrail_parser(parser_trail))
        {
            elog(RLOG_WARNING, "onlinerefresh integrate increment parse records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Transaction processing */
        if (true == dlist_isnull(parser_trail->dtxns))
        {
            continue;
        }

        if (false == onlinerefresh_integrateparsertrail_txns2queue(traildecodecxt, &bexit))
        {
            elog(RLOG_WARNING, "onlinerefresh integrate increment add txn 2 queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Parsed onlinerefresh end, thread can exit */
        if (true == bexit)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_integrateparsertrail_free(void* args)
{
    onlinerefresh_integrateparsertrail* traildecodecxt = NULL;

    traildecodecxt = (onlinerefresh_integrateparsertrail*)args;

    if (NULL == traildecodecxt)
    {
        return;
    }

    if (traildecodecxt->decodingctx)
    {
        increment_integrateparsertrail_free(traildecodecxt->decodingctx);
    }

    rfree(traildecodecxt);
}
