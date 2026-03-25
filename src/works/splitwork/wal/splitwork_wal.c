#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/hash/hash_search.h"
#include "utils/mpage/mpage.h"
#include "threads/threads.h"
#include "misc/misc_stat.h"
#include "misc/misc_control.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "catalog/control.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "works/splitwork/wal/splitwork_wal.h"
#include "works/splitwork/wal/wal_define.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "works/parserwork/wal/parserwork_wal.h"
#include "task/task_slot.h"
#include "onlinerefresh/capture/loadrecord/onlinerefresh_captureloadrecord.h"

typedef struct HISTORY_TIMELINE_ENDLSN
{
    TimeLineID timeline;
    XLogRecPtr endlsn;
} history_timeline_endlsn;

typedef struct HISTORY_TIMELINE_FILE
{
    uint32_t                 len;
    history_timeline_endlsn* vector;
} history_timeline_file;

static void wal_usleep(long microsec)
{
    if (microsec > 0)
    {
        struct timeval delay;
        delay.tv_sec = microsec / 1000000L;
        delay.tv_usec = microsec % 1000000L;
        (void)select(0, NULL, NULL, NULL, &delay);
    }
}

static history_timeline_file* splitwork_wal_history_file_parser(char* buffer, size_t len)
{
    char*                  start_ptr = buffer;
    TimeLineID             timeline = 0;
    uint32_t               hi = 0;
    uint32_t               low = 0;
    uint32_t               vector_len = 0;
    history_timeline_file* result = NULL;
    uint32_t               index_vector = 0;

    /* First pass, count number of lines */
    while (*start_ptr)
    {
        start_ptr = strstr(start_ptr, "\n");
        vector_len += 1;

        while (*start_ptr == '\n' && ((start_ptr - buffer) < len))
        {
            start_ptr++;
        }

        if (start_ptr - buffer >= len)
        {
            break;
        }
    }

    /* Reset pointer */
    start_ptr = buffer;

    result = rmalloc0(sizeof(history_timeline_file));
    if (!result)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result, 0, 0, sizeof(history_timeline_file));

    result->len = vector_len;
    result->vector = rmalloc0(sizeof(history_timeline_endlsn) * vector_len);
    if (!result->vector)
    {
        elog(RLOG_ERROR, "oom");
    }
    rmemset0(result->vector, 0, 0, sizeof(history_timeline_endlsn) * vector_len);

    /* Get data */
    while (*start_ptr)
    {
        sscanf(start_ptr, "%u\t%X/%X\t", &timeline, &hi, &low);

        result->vector[index_vector].timeline = timeline;
        result->vector[index_vector].endlsn = (uint64_t)((uint64_t)hi << 32 | low);
        index_vector++;

        start_ptr = strstr(start_ptr, "\n");

        while (*start_ptr == '\n' && ((start_ptr - buffer) < len))
        {
            start_ptr++;
        }

        if (start_ptr - buffer >= len)
        {
            break;
        }
    }

    return result;
}

static TimeLineID splitwork_wal_get_timelineid_from_file(char* buffer, size_t len, XLogRecPtr lsn)
{
    history_timeline_file* history = splitwork_wal_history_file_parser(buffer, len);
    TimeLineID             result = 0;
    uint32_t               left = 0;
    uint32_t               right = history->len;

    /* Binary search */
    while (left < right)
    {
        uint32_t mid = left + (right - left) / 2;

        if (lsn < history->vector[mid].endlsn)
        {
            right = mid;
        }
        else
        {
            left = mid + 1;
        }
    }

    /* If left == history->len, it is the last timeline */
    if (left == history->len)
    {
        result = history->vector[history->len - 1].timeline + 1;
    }
    else
    {
        result = history->vector[history->len - 1].timeline;
    }

    /* Cleanup */
    rfree(history->vector);
    rfree(history);

    return result;
}

static bool splitwork_wal_history_file_read(char** buffer, size_t* len, char* dpath,
                                            uint32_t timeline)
{
    int  fd = -1;
    char fpath[MAXPGPATH];

    snprintf(fpath, MAXPGPATH, "%s/%08X.history", dpath, timeline);

    fd = osal_file_open(fpath, O_RDONLY, 0);

    /* Return false if history file does not exist */
    if (fd < 0)
    {
        return false;
    }

    *len = osal_file_size(fd);
    *buffer = rmalloc0(*len);
    rmemset0(*buffer, 0, 0, *len);
    lseek(fd, 0, SEEK_SET);

    /* Return false if cannot read file */
    if (osal_file_read(fd, *buffer, *len) < 0)
    {
        osal_file_close(fd);
        return false;
    }

    osal_file_close(fd);
    return true;
}

static void tryUpdateTimeLine(loadwalrecords* readCtl)
{
    char*             his_buffer = NULL;
    size_t            len = 0;
    TimeLineID        history_num = readCtl->timeline;
    TimeLineID        timeline = 0;
    loadpagefromfile* loadpage = NULL;
    bool              rewind = (readCtl->startptr && readCtl->endptr) ? true : false;

    loadpage = (loadpagefromfile*)readCtl->loadpage;

    if (!rewind)
    {
        /* If not in rewind state, try to find the next timeline's history file */
        history_num += 1;
    }

    /* Get history file */
    if (splitwork_wal_history_file_read(&his_buffer, &len, loadpage->fdir, history_num))
    {
        timeline = splitwork_wal_get_timelineid_from_file(his_buffer, len, readCtl->startptr);
        readCtl->timeline = timeline;
        /* Close file */
        readCtl->loadpageroutine->loadpageclose(readCtl->loadpage);
    }

    if (his_buffer)
    {
        rfree(his_buffer);
    }
}

static void splitwork_wal_freerecorddlist(void* dlist_v)
{
    dlist* list = (dlist*)dlist_v;

    /* Free record doubly-linked list memory */
    dlist_free(list, (dlistvaluefree)record_free);
}

/* Add records to queue */
static bool splitwork_wal_addrecords2queue(splitwalcontext* walctx, thrnode* thrnode)
{
    /* Add to queue */
    while (THRNODE_STAT_WORK == thrnode->stat)
    {
        if (false == queue_put(walctx->recordqueue, walctx->loadrecords->records))
        {
            if (ERROR_QUEUE_FULL == walctx->recordqueue->error)
            {
                usleep(50000);
                continue;
            }
            elog(RLOG_WARNING, "capture split thread add records 2 queue error");
            break;
        }
        walctx->loadrecords->records = NULL;
        return true;
    }

    return false;
}

void* splitwork_wal_main(void* args)
{
    bool             delay = false;
    uint32           waitCount = 0;
    thrnode*         thrnode_ptr = NULL;
    splitwalcontext* walctx = NULL;
    loadwalrecords*  loadrecords = NULL;

    thrnode_ptr = (thrnode*)args;
    walctx = (splitwalcontext*)thrnode_ptr->data;

    loadrecords = walctx->loadrecords;
    /* Check state */
    if (THRNODE_STAT_STARTING != thrnode_ptr->stat)
    {
        elog(RLOG_WARNING,
             "increment capture split stat exception, expected state is THRNODE_STAT_STARTING");
        thrnode_ptr->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thrnode_ptr->stat = THRNODE_STAT_WORK;

    while (1)
    {
        if (THRNODE_STAT_TERM == thrnode_ptr->stat)
        {
            /* Parser exiting */
            thrnode_ptr->stat = THRNODE_STAT_EXIT;
            break;
        }

        if (SPLITWORK_WAL_STATUS_INIT == walctx->status)
        {
            /* Sleep 20ms */
            wal_usleep(20 * 1000);
            continue;
        }
        else if (SPLITWORK_WAL_STATUS_REWIND == walctx->status)
        {
            /* If start ptr exceeds endlsn, reassign start and end */
            if (!loadwalrecords_checkend(loadrecords->startptr, loadrecords))
            {
                /* Close file descriptor */
                loadrecords->loadpageroutine->loadpageclose(loadrecords->loadpage);

                /* Reposition startptr to the beginning of the previous WAL log.
                 * If startptr is at the beginning of the next WAL log,
                 * position it to the beginning of the one before that. */
                if (walctx->rewind_start)
                {
                    loadrecords->startptr = GetLastXlogSegmentBegin(
                        (walctx->rewind_start + loadrecords->loadpage->filesize),
                        loadrecords->loadpage->filesize);
                    loadrecords->endptr = loadrecords->startptr + loadrecords->loadpage->filesize;
                    walctx->rewind_start = InvalidXLogRecPtr;
                }
                else
                {
                    loadrecords->startptr = GetLastXlogSegmentBegin(
                        loadrecords->startptr, loadrecords->loadpage->filesize);
                    loadrecords->endptr = loadrecords->startptr + loadrecords->loadpage->filesize;
                }
                loadrecords->prev = InvalidXLogRecPtr;
            }
        }

        if (walctx->change)
        {
            /* Clear queue cache first */
            queue_clear(walctx->recordqueue, splitwork_wal_freerecorddlist);

            walctx->callback.parserwal_rewindstat_setemiting(walctx->privdata);
            walctx->change = false;

            /* Reset loadrecord */
            loadwalrecords_clean(loadrecords);

            /* Set start point to new start */
            loadrecords->startptr = walctx->change_startptr;
            loadrecords->endptr = InvalidFullTransactionId;
            elog(RLOG_DEBUG, "rewind finish, start lsn:%lu", loadrecords->startptr);

            walctx->status = SPLITWORK_WAL_STATUS_NORMAL;
            /* Re-fetch timeline */
            tryUpdateTimeLine(loadrecords);
            continue;
        }

        delay = false;

        /* Get a block of data from WAL file based on known information */
        if (!loadwalrecords_load(loadrecords))
        {
            delay = true;
        }

        /* When splitting reaches endlsn */
        if (SPLITWORK_WAL_STATUS_REWIND == walctx->status &&
            loadrecords->startptr >= loadrecords->endptr)
        {
            /* The first incomplete record at the beginning of the next file exists */
            if (loadrecords->seg_first_incomplete_next)
            {
                /* Try to merge */
                loadwalrecords_merge_seg_last_record(loadrecords);
            }
            /* After merge, there should be no incomplete record at the end of the page
             * and incomplete record at the start of the next file */
            if (loadrecords->seg_first_incomplete_next && loadrecords->page_last_record_incomplete)
            {
                elog(RLOG_ERROR, "have incomplete record when split rewinding");
            }

            /* This may happen on the first time, subsequent records won't be read, just clean up */
            if (loadrecords->page_last_record_incomplete)
            {
                /* Free directly */
                recordcross_free(loadrecords->page_last_record_incomplete);
                /* Set to NULL */
                loadrecords->page_last_record_incomplete = NULL;
            }

            /* Merge done, move the first incomplete record of current file to
             * seg_first_incomplete_next */
            loadrecords->seg_first_incomplete_next = loadrecords->seg_first_incomplete;

            /* Set to NULL */
            loadrecords->seg_first_incomplete = NULL;
        }

        /* Check if there are completed split records */
        if (loadrecords->records)
        {
            /* Add to queue */
            splitwork_wal_addrecords2queue(walctx, thrnode_ptr);
        }

        if (delay)
        {
            waitCount++;
            /* wait 500ms */
            wal_usleep(500 * 1000);
        }
        else
        {
            waitCount = 0;
        }

        if (waitCount >= 10)
        {
            tryUpdateTimeLine(loadrecords);
            waitCount = 0;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void splitwal_destroy(splitwalcontext* split_wal_ctx)
{
    if (NULL == split_wal_ctx)
    {
        return;
    }

    /* Free loadwalrecords */
    if (NULL != split_wal_ctx->loadrecords)
    {
        loadwalrecords_free(split_wal_ctx->loadrecords);
    }

    split_wal_ctx->privdata = NULL;
    split_wal_ctx->recordqueue = NULL;

    rfree(split_wal_ctx);
    split_wal_ctx = NULL;

    return;
}

splitwalcontext* splitwal_init(void)
{
    splitwalcontext* split_wal_ctx = NULL;

    split_wal_ctx = rmalloc0(sizeof(splitwalcontext));
    if (NULL == split_wal_ctx)
    {
        elog(RLOG_ERROR, "out of memory");
    }
    rmemset0(split_wal_ctx, 0, 0, sizeof(splitwalcontext));

    /* Initialize loadrecords, loadpage related also initialized here */
    split_wal_ctx->loadrecords = loadwalrecords_init();

    split_wal_ctx->rewind_start = InvalidXLogRecPtr;

    return split_wal_ctx;
}

void* onlinerefresh_captureloadrecord_main(void* args)
{
    uint32                           waitCount = 0;
    thrnode*                         thrnode_ptr = NULL;
    splitwalcontext*                 walctx = NULL;
    loadwalrecords*                  loadrecords = NULL;
    recpos                           pos = {{'\0'}};
    onlinerefresh_captureloadrecord* split_task = NULL;
    bool                             delay = false;

    thrnode_ptr = (thrnode*)args;
    split_task = (onlinerefresh_captureloadrecord*)thrnode_ptr->data;
    walctx = split_task->splitwalctx;
    loadrecords = walctx->loadrecords;
    pos.wal.type = RECPOS_TYPE_WAL;

    /* Check state */
    if (THRNODE_STAT_STARTING != thrnode_ptr->stat)
    {
        elog(RLOG_WARNING,
             "onlinerefresh capture loadrecord stat exception, expected state is "
             "THRNODE_STAT_STARTING");
        thrnode_ptr->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }
    thrnode_ptr->stat = THRNODE_STAT_WORK;

    while (1)
    {
        /* First check if exit signal is received */
        if (THRNODE_STAT_TERM == thrnode_ptr->stat)
        {
            thrnode_ptr->stat = THRNODE_STAT_EXIT;
            break;
        }

        pos.wal.lsn = loadrecords->startptr;
        pos.wal.timeline = loadrecords->timeline;

        loadrecords->loadpageroutine->loadpagesetstartpos(loadrecords->loadpage, pos);

        delay = false;

        /* Get a block of data from WAL file based on known information */
        if (!loadwalrecords_load(loadrecords))
        {
            delay = true;
        }

        if (loadrecords->records)
        {
            /* When splitting reaches endlsn */
            if (walctx->status == SPLITWORK_WAL_STATUS_REWIND &&
                loadrecords->startptr >= loadrecords->endptr)
            {
                /* The first incomplete record at the beginning of the next file exists */
                if (loadrecords->seg_first_incomplete_next)
                {
                    /* Try to merge */
                    loadwalrecords_merge_seg_last_record(loadrecords);
                }
                /* After merge, there should be no incomplete records at all */
                if (loadrecords->seg_first_incomplete_next &&
                    loadrecords->page_last_record_incomplete)
                {
                    elog(RLOG_WARNING, "have incomplete record when split rewinding");
                    thrnode_ptr->stat = THRNODE_STAT_ABORT;
                    break;
                }

                /* This may happen on the first time, subsequent records won't be read, just clean
                 * up */
                if (loadrecords->page_last_record_incomplete)
                {
                    /* Free directly */
                    recordcross_free(loadrecords->page_last_record_incomplete);
                    /* Set to NULL */
                    loadrecords->page_last_record_incomplete = NULL;
                }

                /* Merge done, move the first incomplete record of current file to
                 * seg_first_incomplete_next
                 */
                loadrecords->seg_first_incomplete_next = loadrecords->seg_first_incomplete;

                /* Set to NULL */
                loadrecords->seg_first_incomplete = NULL;
            }

            /* Add to queue */
            splitwork_wal_addrecords2queue(walctx, thrnode_ptr);
        }

        if (delay)
        {
            waitCount++;
            /* wait 500ms */
            wal_usleep(500 * 1000);
        }
        else
        {
            waitCount = 0;
        }

        if (waitCount >= 10)
        {
            tryUpdateTimeLine(loadrecords);
            waitCount = 0;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void onlinerefresh_captureloadrecord_free(void* args)
{
    splitwalcontext*                 split_wal_ctx = NULL;
    onlinerefresh_captureloadrecord* split_task = NULL;

    split_task = (onlinerefresh_captureloadrecord*)args;
    if (NULL == split_task)
    {
        return;
    }

    split_wal_ctx = split_task->splitwalctx;
    if (NULL == split_wal_ctx)
    {
        rfree(split_task);
        return;
    }

    /* Free loadwalrecords */
    if (NULL != split_wal_ctx->loadrecords)
    {
        loadwalrecords_free(split_wal_ctx->loadrecords);
    }

    split_wal_ctx->privdata = NULL;
    split_wal_ctx->recordqueue = NULL;

    rfree(split_wal_ctx);
    split_wal_ctx = NULL;

    rfree(split_task);
    return;
}
