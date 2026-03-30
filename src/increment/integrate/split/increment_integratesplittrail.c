#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/mpage/mpage.h"
#include "utils/hash/hash_search.h"
#include "queue/queue.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "storage/trail/fftrail.h"
#include "threads/threads.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadtrailrecords.h"
#include "increment/integrate/split/increment_integratesplittrail.h"

/* Set the state of the integrate split thread */
void increment_integratesplittrail_state_set(increment_integratesplittrail* splittrail, int state)
{
    splittrail->state = state;
}

/*
 * Parameter description:
 *    fileid set to loadtrail->fileid, loadtrail->offset set to 0
 *    emitoffset set to emitoffset
 */
void increment_integratesplittrail_emit_set(increment_integratesplittrail* splittrail, uint64 fileid, uint64 emitoffset)
{
    /* Reset filtering start point */
    splittrail->loadrecords->fileid = fileid;
    splittrail->loadrecords->foffset = 0;
    splittrail->emitoffset = emitoffset;

    /* Reset parsing and filtering start points */
    loadtrailrecords_setloadposition(splittrail->loadrecords,
                                     splittrail->loadrecords->fileid,
                                     splittrail->loadrecords->foffset);
}

static bool increment_integratesplittrail_state_checkandset(increment_integratesplittrail* splittrail)
{
    if (INTEGRATE_STATUS_SPLIT_WORKING == splittrail->state)
    {
        return true;
    }
    return false;
}

/* Initialize info, including setting basic info in loadtrail */
increment_integratesplittrail* increment_integratesplittrail_init(void)
{
    char*                          cdata = NULL;
    increment_integratesplittrail* splittrail = NULL;

    splittrail = (increment_integratesplittrail*)rmalloc1(sizeof(increment_integratesplittrail));
    if (NULL == splittrail)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(splittrail, 0, '\0', sizeof(increment_integratesplittrail));

    /* Set loadtrail info based on config file */
    splittrail->capturedata = rmalloc0(MAXPGPATH);
    if (NULL == splittrail->capturedata)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(splittrail->capturedata, 0, '\0', MAXPGPATH);
    cdata = guc_getConfigOption(CFG_KEY_TRAIL_DIR);
    snprintf(splittrail->capturedata, MAXPGPATH, "%s/%s", cdata, STORAGE_TRAIL_DIR);

    /*------------------------load record module initialization begin---------------------------*/
    splittrail->loadrecords = loadtrailrecords_init();
    if (NULL == splittrail->loadrecords)
    {
        elog(RLOG_WARNING, "integrate increment load records error");
        return NULL;
    }

    if (false == loadtrailrecords_setloadpageroutine(splittrail->loadrecords, LOADPAGE_TYPE_FILE))
    {
        elog(RLOG_WARNING, "integrate increment set load page error");
        return NULL;
    }

    if (false == loadtrailrecords_setloadsource(splittrail->loadrecords, splittrail->capturedata))
    {
        elog(RLOG_WARNING, "integrate increment set capture data error");
        return NULL;
    }
    loadtrailrecords_setloadposition(splittrail->loadrecords, 0, 0);
    /*------------------------load record module initialization   end---------------------------*/

    /* Set state to waiting for set */
    increment_integratesplittrail_state_set(splittrail, INTEGRATE_STATUS_SPLIT_WAITSET);
    splittrail->filter = true;
    return splittrail;
}

/* Add records to queue */
static bool increment_integratesplitrail_addrecords2queue(increment_integratesplittrail* splittrail)
{
    /* Add to queue */
    while (INTEGRATE_STATUS_SPLIT_WORKING == splittrail->state)
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

/* Main processing entry */
void* increment_integratesplitrail_main(void* args)
{
    uint64                         fileid = 0;
    thrnode*                       thr_node = NULL;
    increment_integratesplittrail* splittrail = NULL;

    thr_node = (thrnode*)args;
    /* Parameter conversion */
    splittrail = (increment_integratesplittrail*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "increment integrate splittrail exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (true)
    {
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialize/flush to disk */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        if (!increment_integratesplittrail_state_checkandset(splittrail))
        {
            /* Sleep 10 milliseconds */
            usleep(10000);
            continue;
        }

        /* Load records */
        /* Pre-reserve fileid, file will auto-switch during loadrecords */
        fileid = splittrail->loadrecords->fileid;
        if (false == loadtrailrecords_load(splittrail->loadrecords))
        {
            elog(RLOG_WARNING, "load trail records error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        splittrail->callback.setmetricloadtrailno(splittrail->privdata, splittrail->loadrecords->fileid);
        splittrail->callback.setmetricloadtrailstart(splittrail->privdata, splittrail->loadrecords->foffset);

        if (true == dlist_isnull(splittrail->loadrecords->records))
        {
            /*
             * No data read, caught up to latest, so need to re-read this block
             */
            /* Need to exit, wait for thr_node->stat to become TERM then exit*/
            if (THRNODE_STAT_TERM != thr_node->stat)
            {
                /* Sleep 10 milliseconds */
                usleep(10000);
                continue;
            }
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Whether filtering is needed, if not needed then add to queue */
        if (false == splittrail->filter)
        {
            /* Add to queue */
            if (false == increment_integratesplitrail_addrecords2queue(splittrail))
            {
                elog(RLOG_WARNING, "integrate add records 2 queue error");
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }
            continue;
        }

        if (false == loadtrailrecords_filterremainmetadata(splittrail->loadrecords, fileid, splittrail->emitoffset))
        {
            splittrail->filter = false;
        }

        if (true == dlist_isnull(splittrail->loadrecords->records))
        {
            /*
             * All data filtered out, continue to get data
             */
            continue;
        }

        /* Add to queue */
        if (false == increment_integratesplitrail_addrecords2queue(splittrail))
        {
            elog(RLOG_WARNING, "integrate add records 2 queue error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void increment_integratesplittrail_free(increment_integratesplittrail* splitrail)
{
    if (NULL == splitrail)
    {
        return;
    }

    if (splitrail->capturedata)
    {
        rfree(splitrail->capturedata);
    }

    if (NULL != splitrail->loadrecords)
    {
        loadtrailrecords_free(splitrail->loadrecords);
    }
    splitrail->recordscache = NULL;
    rfree(splitrail);
}
