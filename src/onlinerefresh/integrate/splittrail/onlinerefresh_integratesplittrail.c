#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
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
#include "onlinerefresh/integrate/splittrail/onlinerefresh_integratesplittrail.h"

/* Logic read main thread */
onlinerefresh_integratesplittrail* onlinerefresh_integratesplittrail_init(void)
{
    onlinerefresh_integratesplittrail* stctx = NULL;

    stctx = (onlinerefresh_integratesplittrail*)rmalloc0(sizeof(onlinerefresh_integratesplittrail));
    if (NULL == stctx)
    {
        elog(RLOG_WARNING, "onlinerefresh integratesplittrail malloc out of memory, %s",
             strerror(errno));
        return NULL;
    }
    rmemset0(stctx, 0, 0, sizeof(onlinerefresh_integratesplittrail));

    stctx->splittrailctx = increment_integratesplittrail_init();
    stctx->splittrailctx->state = INTEGRATE_STATUS_SPLIT_WAITSET;
    return stctx;
}

/* Add records to queue */
static bool onlinerefresh_integratesplittrail_addrecords2queue(
    increment_integratesplittrail* splittrail, thrnode* thrnode)
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

/* Logic read main thread */
void* onlinerefresh_integratesplittrail_main(void* args)
{
    uint64                             fileid = 0;
    thrnode*                           thr_node = NULL;
    increment_integratesplittrail*     splittrail = NULL;
    onlinerefresh_integratesplittrail* olintegratesplittrail = NULL;

    thr_node = (thrnode*)args;

    olintegratesplittrail = (onlinerefresh_integratesplittrail*)thr_node->data;

    splittrail = olintegratesplittrail->splittrailctx;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "onlinerefresh integrate spliittrail stat exception, expected state is "
             "THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (1)
    {
        /*
         * 1、Open file, scenarios encountered when opening file:
         * File does not exist, wait for file to exist, also check if exit signal is received during
         * waiting 2、Check if exit signal is received, if yes, exit 3、Calculate offset based on
         * blockid, read data according to this content
         */
        /* First check if exit signal is received */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Load records */
        /* Pre-reserve fileid, when loadrecords, file will be automatically switched */
        fileid = splittrail->loadrecords->fileid;
        if (false == loadtrailrecords_load(splittrail->loadrecords))
        {
            elog(RLOG_WARNING, "load trail records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        if (true == dlist_isnull(splittrail->loadrecords->records))
        {
            /*
             * No data read, caught up to latest, need to re-read this block
             */
            usleep(10000);
            continue;
        }

        /* Need to filter or not, if not, add to queue */
        if (false == splittrail->filter)
        {
            /* Add to queue */
            if (false == onlinerefresh_integratesplittrail_addrecords2queue(splittrail, thr_node))
            {
                elog(RLOG_WARNING, "integrate add records 2 queue error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if (false == loadtrailrecords_filterremainmetadata(splittrail->loadrecords, fileid,
                                                           splittrail->emitoffset))
        {
            /* Filtering complete */
            splittrail->filter = false;
        }

        if (true == dlist_isnull(splittrail->loadrecords->records))
        {
            /*
             * No data read, caught up to latest, need to re-read this block
             */
            continue;
        }

        if (false == onlinerefresh_integratesplittrail_addrecords2queue(splittrail, thr_node))
        {
            elog(RLOG_WARNING, "integrate add records 2 queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_integratesplittrail_free(void* args)
{
    onlinerefresh_integratesplittrail* stctx = NULL;

    stctx = (onlinerefresh_integratesplittrail*)args;

    if (NULL == stctx)
    {
        return;
    }

    if (stctx->splittrailctx)
    {
        increment_integratesplittrail_free(stctx->splittrailctx);
    }

    rfree(stctx);
}
