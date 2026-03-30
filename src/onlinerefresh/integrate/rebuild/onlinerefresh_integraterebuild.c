#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_updaterewind.h"
#include "rebuild/rebuild.h"
#include "onlinerefresh/integrate/rebuild/onlinerefresh_integraterebuild.h"

/* Only used to update parsed position */
static bool onlinerefresh_integraterebuild_updaterewindstmt_set(onlinerefresh_integraterebuild* rebuild_obj, txn* txn)
{
    txnstmt*              stmtnode = NULL;
    txnstmt_updaterewind* updaterewind = NULL;

    /* Allocate space */
    stmtnode = txnstmt_init();
    if (NULL == stmtnode)
    {
        return false;
    }
    rmemset0(stmtnode, 0, '\0', sizeof(txnstmt));

    updaterewind = txnstmt_updaterewind_init();
    if (NULL == updaterewind)
    {
        rfree(stmtnode);
        return false;
    }

    updaterewind->rewind.trail.fileid = txn->segno;
    updaterewind->rewind.trail.offset = txn->end.trail.offset;

    stmtnode->type = TXNSTMT_TYPE_UPDATEREWIND;
    stmtnode->stmt = (void*)updaterewind;

    txn->stmts = lappend(txn->stmts, stmtnode);

    return true;
}

/* Initialize */
onlinerefresh_integraterebuild* onlinerefresh_integraterebuild_init(void)
{
    char*                           burst = NULL;
    onlinerefresh_integraterebuild* rebuild_obj = NULL;

    rebuild_obj = rmalloc0(sizeof(onlinerefresh_integraterebuild));
    if (NULL == rebuild_obj)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rebuild_obj, 0, '\0', sizeof(onlinerefresh_integraterebuild));
    rebuild_reset(&rebuild_obj->rebuild);
    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;
    rebuild_obj->mergetxn = (0 == guc_getConfigOptionInt(CFG_KEY_MERGETXN)) ? false : true;
    rebuild_obj->txbundlesize = guc_getConfigOptionInt(CFG_KEY_TXBUNDLESIZE);
    /* Set integrate_method */
    burst = guc_getConfigOption(CFG_KEY_INTEGRATE_METHOD);
    if (burst != NULL && '\0' != burst[0] && 0 == strcmp(burst, "burst"))
    {
        rebuild_obj->burst = true;
    }
    return rebuild_obj;
}

/* Work */
void* onlinerefresh_integraterebuild_main(void* args)
{
    int                             timeout = 0;
    int                             txbundlesize = 0;
    txn*                            txns = NULL;
    txn*                            ntxn = NULL;
    txn*                            txnnode = NULL;
    thrnode*                        thr_node = NULL;
    onlinerefresh_integraterebuild* rebuild_obj = NULL;

    thr_node = (thrnode*)args;

    rebuild_obj = (onlinerefresh_integraterebuild*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "onlinerefresh integrate rebuild stat exception, expected state is "
             "THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (1)
    {
        /* First check if exit signal is received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data from cache */
        txns = cache_txn_getbatch(rebuild_obj->parser2rebuild, &timeout);
        if (NULL == txns)
        {
            /* Continue on timeout */
            if (ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            /* Processing failed, exit */
            elog(RLOG_WARNING, "onlinerefrese integrate rebuild cache_txn_getbatch error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Traverse txns and reorganize statements */
        for (txnnode = txns; NULL != txnnode; txnnode = txns)
        {
            /* special is used to mark specified transactions:
             * refresh/onlinerefreshbegin/onlinerefreshend */
            txns = txnnode->cachenext;

            /* Transaction type is incremental end, directly add to cache to ensure status table
             * content is correct */
            if (TXN_TYPE_ONLINEREFRESH_INC_END == txnnode->type)
            {
                if (NULL != ntxn)
                {
                    onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
                    cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }
                /* Put transaction into cache */
                onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                break;
            }

            if (true == rebuild_obj->burst)
            {
                /* burst reorganization */
                if (false == rebuild_txnburst(&rebuild_obj->rebuild, txnnode))
                {
                    elog(RLOG_WARNING, "onlinerefrese integrate rebuild_txnburst error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
            }
            else
            {
                /* Reorganize */
                if (false == rebuild_prepared(&rebuild_obj->rebuild, txnnode))
                {
                    elog(RLOG_WARNING, "onlinerefrese integrate rebuild_prepared error");
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
            }

            if (NULL == txnnode->stmts)
            {
                txn_free(txnnode);
                rfree(txnnode);
                continue;
            }

            if (false == rebuild_obj->mergetxn)
            {
                /* Put transaction into cache */
                onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                continue;
            }

            if (txnnode->stmts->length > rebuild_obj->txbundlesize)
            {
                if (NULL != ntxn)
                {
                    onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
                    cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }
                onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                continue;
            }

            /* Merge transaction processing */
            /* If NULL, allocate space */
            if (NULL == ntxn)
            {
                ntxn = (txn*)rmalloc0(sizeof(txn));
                if (NULL == ntxn)
                {
                    elog(RLOG_WARNING,
                         "onlinerefrese integrate rebuild_obj malloc txn out of memory, %s",
                         strerror(errno));
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
                rmemset0(ntxn, 0, '\0', sizeof(txn));
            }

            /* Copy transaction info */
            ntxn->xid = txnnode->xid;
            ntxn->flag = txnnode->flag;
            ntxn->segno = txnnode->segno;
            ntxn->debugno = txnnode->debugno;
            ntxn->start = txnnode->start;
            ntxn->end = txnnode->end;
            ntxn->redo = txnnode->redo;
            ntxn->restart = txnnode->restart;
            ntxn->confirm = txnnode->confirm;
            txbundlesize += txnnode->stmts->length;

            /* Add stmts to new transaction */
            ntxn->stmts = list_concat(ntxn->stmts, txnnode->stmts);
            if (ntxn->stmts != txnnode->stmts)
            {
                rfree(txnnode->stmts);
            }
            txnnode->stmts = NULL;

            /* Free entry */
            txn_free(txnnode);
            rfree(txnnode);

            /* Last transaction or exceeds merged transaction size */
            if (NULL != txns && NULL != txns->stmts &&
                ((txbundlesize + txns->stmts->length) < rebuild_obj->txbundlesize))
            {
                continue;
            }
            onlinerefresh_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
            ntxn = NULL;
            txbundlesize = 0;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_integraterebuild_free(void* args)
{
    onlinerefresh_integraterebuild* rebuild_obj = NULL;

    rebuild_obj = (onlinerefresh_integraterebuild*)args;

    if (NULL == rebuild_obj)
    {
        return;
    }

    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;

    rebuild_destroy(&rebuild_obj->rebuild);

    rfree(rebuild_obj);
}
