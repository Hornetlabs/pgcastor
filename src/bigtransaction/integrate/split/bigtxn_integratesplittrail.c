#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "increment/integrate/split/increment_integratesplittrail.h"
#include "bigtransaction/integrate/split/bigtxn_integratesplittrail.h"

/* Add records to queue */
static bool bigtxn_integratesplittrail_addrecords2queue(increment_integratesplittrail* splittrail, thrnode* thrnode)
{
    /* Add to queue */
    while (THRNODE_STAT_WORK == thrnode->stat)
    {
        if (false == queue_put(splittrail->recordscache, splittrail->loadrecords->records))
        {
            if (ERROR_QUEUE_FULL == splittrail->recordscache->error)
            {
                usleep(50000);
                continue;
            }
            elog(RLOG_WARNING, "integrate add records 2 queue error");
            break;
        }
        splittrail->loadrecords->records = NULL;
        return true;
    }

    return false;
}

/* Logical read main thread */
bigtxn_integratesplittrail* bigtxn_integratesplittrail_init(void)
{
    bigtxn_integratesplittrail* stctx = NULL;

    stctx = (bigtxn_integratesplittrail*)rmalloc0(sizeof(bigtxn_integratesplittrail));
    if (NULL == stctx)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stctx, 0, 0, sizeof(bigtxn_integratesplittrail));

    stctx->splittrailctx = increment_integratesplittrail_init();
    stctx->splittrailctx->state = INTEGRATE_STATUS_SPLIT_WAITSET;
    return stctx;
}

/* Logical read main thread */
void* bigtxn_integratesplittrail_main(void* args)
{
    uint64                         fileid = 0;
    thrnode*                       thr_node = NULL;
    increment_integratesplittrail* stctx = NULL;
    bigtxn_integratesplittrail*    bigtxn_stctx = NULL;

    thr_node = (thrnode*)args;

    bigtxn_stctx = (bigtxn_integratesplittrail*)thr_node->data;

    stctx = bigtxn_stctx->splittrailctx;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "integrate bigtxn splittrail stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (1)
    {
        /*
         * 1. Open file. When opening file, encountered scenarios:
         *    File does not exist, then wait for file to exist. While waiting for file, also need to
         * check if exit signal received.
         * 2. Check if exit signal received, if received then exit.
         * 3. Calculate offset based on blockid, read data based on this content.
         */
        /* Open file */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialization/flush to disk */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
        fileid = stctx->loadrecords->fileid;

        if (false == loadtrailrecords_load(stctx->loadrecords))
        {
            elog(RLOG_WARNING, "bigtxn load trail records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if (true == dlist_isnull(stctx->loadrecords->records))
        {
            /*
             * No data read, caught up to latest, so need to re-read this block
             */
            /* Need to exit, wait for thr_node->stat to become TERM then exit */
            if (THRNODE_STAT_TERM != thr_node->stat)
            {
                /* Sleep 10 milliseconds */
                usleep(10000);
                /* Restarting repeated file sending will rename file, need to close file descriptor
                 */
                loadtrailrecords_fileclose(stctx->loadrecords);
                continue;
            }
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Whether filtering is needed, if not needed then add to queue */
        if (false == stctx->filter)
        {
            /* Add to queue */
            if (false == bigtxn_integratesplittrail_addrecords2queue(stctx, thr_node))
            {
                elog(RLOG_WARNING, "integrate bigtxn add records 2 queue error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if (false == loadtrailrecords_filterremainmetadata(stctx->loadrecords, fileid, stctx->emitoffset))
        {
            stctx->filter = false;
        }

        if (true == dlist_isnull(stctx->loadrecords->records))
        {
            /*
             * No data read, caught up to latest, so need to re-read this block
             */
            continue;
        }

        /* Add to queue */
        if (false == bigtxn_integratesplittrail_addrecords2queue(stctx, thr_node))
        {
            elog(RLOG_WARNING, "integrate bigtxn add records 2 queue error");
            break;
        }
        /* TODO checkpoint logic */
    }

    pthread_exit(NULL);
    return NULL;
}

void bigtxn_integratesplittrail_free(void* args)
{
    bigtxn_integratesplittrail* stctx = NULL;

    stctx = (bigtxn_integratesplittrail*)args;

    if (stctx->splittrailctx)
    {
        increment_integratesplittrail_free(stctx->splittrailctx);
    }

    rfree(stctx);
}
