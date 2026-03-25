#include "app_incl.h"
#include "libpq-fe.h"
#include "port/file/fd.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/conn/conn.h"
#include "utils/string/stringinfo.h"
#include "threads/threads.h"
#include "sync/sync.h"
#include "queue/queue.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "works/syncwork/refresh_integratesync.h"
#include "refresh/sharding2db/refresh_sharding2db.h"
#include "refresh/integrate/refresh_integrate.h"

/*
 * refresh file table info length
 * cnt + completecnt + tablestat + oid + schema + table + stat
 *  4  +      4      +     4     +  4  +   64   +  64   + cnt * 1
 */
#define REFRESH_FILE_TABLE_LEN \
    (sizeof(int) + sizeof(int) + sizeof(int) + sizeof(Oid) + NAMEDATALEN + NAMEDATALEN)

typedef enum REFRESH_INTEGRATEJOB_STAT
{
    REFRESH_INTEGRATE_STAT_JOBNOP = 0x00,
    REFRESH_INTEGRATE_STAT_JOBSTARTING,   /* worker thread starting */
    REFRESH_INTEGRATE_STAT_JOBWORKING,    /* worker thread working */
    REFRESH_INTEGRATE_STAT_JOBWAITINGDONE /* waiting for worker thread to complete */
} refresh_integratejob_stat;

refresh_integrate* refresh_integrate_init(void)
{
    refresh_integrate* rintegrate = NULL;

    rintegrate = (refresh_integrate*)rmalloc0(sizeof(refresh_integrate));
    if (NULL == rintegrate)
    {
        elog(RLOG_WARNING, "refresh integrate init out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rintegrate, 0, 0, sizeof(refresh_integrate));

    rintegrate->parallelcnt = guc_getConfigOptionInt(CFG_KEY_MAX_WORK_PER_REFRESH);
    if (0 == rintegrate->parallelcnt)
    {
        elog(RLOG_WARNING, "guc parameter  max_work_per_refresh configuration error");
        return NULL;
    }

    rintegrate->conn_info = guc_getConfigOption(CFG_KEY_URL);

    rintegrate->refresh_path = (char*)rmalloc0(MAXPGPATH);
    if (NULL == rintegrate->refresh_path)
    {
        elog(RLOG_WARNING, "refresh_path out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rintegrate->refresh_path, 0, 0, MAXPGPATH);
    sprintf(rintegrate->refresh_path, "%s", guc_getConfigOption(CFG_KEY_TRAIL_DIR));

    rintegrate->sync_stats = NULL;

    rintegrate->stat = REFRESH_INTEGRATE_STAT_NOP;
    rintegrate->tqueue = queue_init();
    return rintegrate;
}

/* refresh data to sync status table and truncate existing tables */
static bool refresh_integrate_setsynctable(refresh_integrate* rintegrate, thrnode* thrnode)
{
    int        index = 0;
    PGconn*    conn = NULL;
    PGresult*  res = NULL;
    StringInfo sql = NULL;
    char*      catalog_schema = NULL;

refresh_integrate_setsynctableretry:
    sleep(1);
    if (THRNODE_STAT_TERM == thrnode->stat)
    {
        return false;
    }

    conn = conn_get(rintegrate->conn_info);
    if (NULL == conn)
    {
        elog(RLOG_WARNING, "connect database failed");
        goto refresh_integrate_setsynctableretry;
    }

    if (false == conn_begin(conn))
    {
        elog(RLOG_WARNING, "Execute begin failed");
        PQfinish(conn);
        goto refresh_integrate_setsynctableretry;
    }
    sql = makeStringInfo();
    catalog_schema = guc_getConfigOption(CFG_KEY_CATALOGSCHEMA);

    for (index = 0; index < rintegrate->parallelcnt; index++)
    {
        resetStringInfo(sql);
        appendStringInfo(
            sql,
            "INSERT INTO \"%s\".\"%s\" \n"
            "(name, type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) "
            "\n"
            "VALUES (\'%s%d\', %hd, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (name) DO UPDATE SET \n"
            "(type, stat, rewind_fileid, rewind_offset, emit_fileid, emit_offset, xid, lsn) =  \n"
            "(EXCLUDED.type, EXCLUDED.stat, EXCLUDED.rewind_fileid, EXCLUDED.rewind_offset, "
            "EXCLUDED.emit_fileid, EXCLUDED.emit_offset, EXCLUDED.xid, EXCLUDED.lsn); ",
            catalog_schema, SYNC_STATUSTABLE_NAME, REFRESH_REFRESH, index, 1);
        res = conn_exec(conn, sql->data);
        if (NULL == res)
        {
            elog(RLOG_WARNING, "Execute commit failed");
            deleteStringInfo(sql);
            goto refresh_integrate_setsynctableretry;
        }
        PQclear(res);
    }

    /* truncate table info */
    if (1 == guc_getConfigOptionInt(CFG_KEY_TRUNCATETABLE))
    {
        /* if truncate fails, reconnect to database and retry */
        if (false == refresh_table_syncstats_truncatetable_fromsyncstats(rintegrate->sync_stats,
                                                                         (void*)conn))
        {
            res = conn_exec(conn, "ROLLBACK");
            if (NULL == res)
            {
                elog(RLOG_WARNING, "Execute rollback failed");
                deleteStringInfo(sql);
                goto refresh_integrate_setsynctableretry;
            }
            PQclear(res);
            PQfinish(conn);
            sleep(1);
            goto refresh_integrate_setsynctableretry;
        }
    }

    if (false == conn_commit(conn))
    {
        deleteStringInfo(sql);
        PQfinish(conn);
        goto refresh_integrate_setsynctableretry;
    }
    PQfinish(conn);
    deleteStringInfo(sql);

    return true;
}

/* start worker threads */
static bool refresh_integrate_startjobs(refresh_integrate* rintegrate)
{
    int                  index = 0;
    refresh_sharding2db* shard2db = NULL;
    elog(RLOG_DEBUG, "integrate refresh, work thread num: %d", rintegrate->parallelcnt);

    /* allocate space for each thread */
    for (index = 0; index < rintegrate->parallelcnt; index++)
    {
        /* allocate space and initialize */
        shard2db = refresh_sharding2db_init();
        if (NULL == shard2db)
        {
            return false;
        }
        shard2db->name = (char*)rmalloc0(NAMEDATALEN);
        if (NULL == shard2db->name)
        {
            elog(RLOG_WARNING, "malloc sharding2dbname out of memory");
            refresh_sharding2db_free(shard2db);
            return false;
        }
        rmemset0(shard2db->name, 0, '\0', NAMEDATALEN);
        sprintf(shard2db->name, "%s%d", REFRESH_REFRESH, index);
        shard2db->syncstats->base.conn = NULL;
        shard2db->refresh_path = rintegrate->refresh_path;
        shard2db->syncstats->base.conninfo = rintegrate->conn_info;
        shard2db->syncstats->tablesyncstats = rintegrate->sync_stats;
        shard2db->syncstats->queue = rintegrate->tqueue;

        /* register worker thread */
        if (false == threads_addjobthread(rintegrate->thrsmgr->parents,
                                          THRNODE_IDENTITY_INTEGRATE_REFRESH_JOB,
                                          rintegrate->thrsmgr->submgrref.no, (void*)shard2db,
                                          refresh_sharding2db_free, NULL, refresh_sharding2db_work))
        {
            elog(RLOG_WARNING, "refresh integrate start job error");
            return false;
        }
    }

    return true;
}

void* refresh_integrate_main(void* args)
{
    int                       jobcnt = 0;
    refresh_integratejob_stat jobstat = REFRESH_INTEGRATE_STAT_JOBNOP;
    thrnode*                  thr_node = NULL;
    refresh_integrate*        rintegrate = NULL;

    thr_node = (thrnode*)args;
    rintegrate = (refresh_integrate*)thr_node->data;

    /* check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING,
             "refresh integrate stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    /* can exit if there are no tables to sync */
    if (!rintegrate->sync_stats->tablesyncall || !rintegrate->sync_stats->tablesyncing)
    {
        elog(RLOG_INFO, "There are no tables to be synchronized, so no refresh is needed");
        thr_node->stat = THRNODE_STAT_EXIT;
        rintegrate->stat = REFRESH_INTEGRATE_STAT_DONE;
        pthread_exit(NULL);
        return NULL;
    }
    elog(RLOG_DEBUG, "integrate refresh start");

    rintegrate->stat = REFRESH_INTEGRATE_STAT_WORK;

    /* add data to sync status table, cleanup existing tables */
    if (false == refresh_integrate_setsynctable(rintegrate, thr_node))
    {
        elog(RLOG_INFO, "integrate refresh setsynctable error");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    /* get configured number of existing worker threads and initialize management structure */
    if (false == refresh_integrate_startjobs(rintegrate))
    {
        elog(RLOG_INFO, "integrate refresh start job threads error");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
        return NULL;
    }

    jobstat = REFRESH_INTEGRATE_STAT_JOBSTARTING;
    while (true)
    {
        /*
         * first check if exit signal received
         *  for sub-manager thread, there are two scenarios for receiving TERM signal:
         *  1. parent persistent thread of sub-manager thread exits
         *  2. exit flag received
         *
         * in both scenarios, sub-manager thread does not need to set worker threads to FREE state
         */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* wait for all worker threads to start successfully */
        if (REFRESH_INTEGRATE_STAT_JOBSTARTING == jobstat)
        {
            /* check if already started successfully */
            jobcnt = 0;
            if (false == threads_countsubmgrjobthredsabovework(rintegrate->thrsmgr->parents,
                                                               rintegrate->thrsmgr->childthrrefs,
                                                               &jobcnt))
            {
                elog(RLOG_WARNING, "integrate refresh count job thread above work stat error");
                thr_node->stat = THRNODE_STAT_ABORT;
                goto refresh_integrate_main_done;
            }

            if (jobcnt != rintegrate->thrsmgr->childthrrefs->length)
            {
                continue;
            }
            jobstat = REFRESH_INTEGRATE_STAT_JOBWORKING;
            continue;
        }
        else if (REFRESH_INTEGRATE_STAT_JOBWORKING == jobstat)
        {
            /* worker threads started, add work tasks to queue */
            if (false == refresh_table_syncstat_genqueue(rintegrate->sync_stats,
                                                         (void*)rintegrate->tqueue,
                                                         rintegrate->refresh_path))
            {
                /* failed to add tasks to queue, manager thread exits, main thread handles
                 * sub-thread cleanup */
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }

            /* first check if tasks and sync tables exist */
            if (NULL == rintegrate->sync_stats->tablesyncing)
            {
                jobstat = REFRESH_INTEGRATE_STAT_JOBWAITINGDONE;
            }
            continue;
        }
        else if (REFRESH_INTEGRATE_STAT_JOBWAITINGDONE == jobstat)
        {
            /*
             * waiting for task completion has two parts
             *  1. task queue is empty
             *  2. sub-threads have fully exited
             */
            if (false == queue_isnull(rintegrate->tqueue))
            {
                continue;
            }

            /* set idle threads to exit and count exited threads */
            jobcnt = rintegrate->thrsmgr->childthrrefs->length;
            if (false ==
                threads_setsubmgrjobthredstermandcountexit(
                    rintegrate->thrsmgr->parents, rintegrate->thrsmgr->childthrrefs, 0, &jobcnt))
            {
                elog(RLOG_WARNING, "integrate refresh set job threads term in idle error");
                thr_node->stat = THRNODE_STAT_ABORT;
                goto refresh_integrate_main_done;
            }

            /* not fully exited, continue waiting */
            if (jobcnt != rintegrate->thrsmgr->childthrrefs->length)
            {
                continue;
            }

            /* all threads have exited, set sub-thread state to FREE */
            threads_setsubmgrjobthredsfree(rintegrate->thrsmgr->parents,
                                           rintegrate->thrsmgr->childthrrefs, 0,
                                           rintegrate->parallelcnt);

            /* set this thread to exit */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }
    }

refresh_integrate_main_done:
    rintegrate->stat = REFRESH_INTEGRATE_STAT_DONE;
    pthread_exit(NULL);
    return NULL;
}

/* write existing task info and status to file */
bool refresh_integrate_write(refresh_integrate* rintegrate)
{
    int                     fd = -1;
    uint64                  tbcnt = 0;
    uint64                  tbsize = 0;
    uint64                  offset = 0;
    uint64                  bufferoffset = 0;
    uint8*                  buffer_tb = NULL;
    char                    path[MAXPATH] = {'\0'};
    char                    temp_path[MAXPATH] = {'\0'};
    refresh_table_syncstat* table = NULL;

    /* generate path */
    snprintf(path, MAXPATH, "%s/%s", REFRESH_REFRESH, REFRESH_STATUS);
    snprintf(temp_path, MAXPATH, "%s/%s.tmp", REFRESH_REFRESH, REFRESH_STATUS);

    /* delete temporary file */
    unlink(temp_path);

    /* open temporary file */
    fd = osal_basic_open_file(temp_path, O_RDWR | O_CREAT | BINARY);
    if (fd < 0)
    {
        elog(RLOG_WARNING, "open file %s error %s", temp_path, strerror(errno));
        return false;
    }

    /* number of existing tables */
    offset += 8;

    refresh_table_syncstats_lock(rintegrate->sync_stats);

    table = rintegrate->sync_stats ? rintegrate->sync_stats->tablesyncing : NULL;
    for (; NULL != table; table = table->next)
    {
        bufferoffset = 0;
        tbsize = (REFRESH_FILE_TABLE_LEN + table->cnt * 1);
        buffer_tb = rmalloc0(tbsize);
        if (!buffer_tb)
        {
            refresh_table_syncstats_unlock(rintegrate->sync_stats);
            elog(RLOG_ERROR, "oom");
        }
        rmemset0(buffer_tb, 0, 0, tbsize);
        rmemcpy0(buffer_tb, bufferoffset, &table->cnt, sizeof(table->cnt));
        bufferoffset += sizeof(table->cnt);
        rmemcpy0(buffer_tb, bufferoffset, &table->completecnt, sizeof(table->completecnt));
        bufferoffset += sizeof(table->completecnt);
        rmemcpy0(buffer_tb, bufferoffset, &table->tablestat, sizeof(table->tablestat));
        bufferoffset += sizeof(table->tablestat);
        rmemcpy0(buffer_tb, bufferoffset, table->schema, strlen(table->schema));
        bufferoffset += NAMEDATALEN;
        rmemcpy0(buffer_tb, bufferoffset, table->table, strlen(table->table));
        bufferoffset += NAMEDATALEN;
        /* no sharding or not yet added to tasks */
        if (0 != table->cnt && NULL != table->stat)
        {
            rmemcpy0(buffer_tb, bufferoffset, table->stat, (table->cnt * sizeof(int8_t)));
            bufferoffset += (table->cnt * sizeof(int8_t));
        }
        osal_file_pwrite(fd, (char*)buffer_tb, bufferoffset, offset);
        osal_file_sync(fd);
        offset += bufferoffset;
        rfree(buffer_tb);
        buffer_tb = NULL;
        tbcnt++;
    }

    refresh_table_syncstats_unlock(rintegrate->sync_stats);
    osal_file_pwrite(fd, (char*)&tbcnt, sizeof(tbcnt), 0);
    osal_file_sync(fd);
    osal_file_close(fd);

    /* rename file */
    if (osal_durable_rename(temp_path, path, RLOG_DEBUG))
    {
        elog(RLOG_WARNING, "Error renaming file %s to %s", temp_path, path);
        return false;
    }

    return true;
}

/* read refresh file and generate refresh tasks */
bool refresh_integrate_read(refresh_integrate** refresh)
{
    struct stat        st;
    int                fd = -1;
    int                read_size = 0;
    int                index_stat = 0;
    int                table_index = 0;
    uint64             tbcnt = 0;
    uint64             offset = 0;
    uint64             bufferoffset = 0;
    uint8*             buffer = NULL;
    char               path[MAXPATH] = {'\0'};
    char               synctable[REFRESH_FILE_TABLE_LEN] = {'\0'};
    refresh_integrate* rintegrate = NULL;

    /* generate path */
    snprintf(path, MAXPATH, "%s/%s", REFRESH_REFRESH, REFRESH_STATUS);

    /* check if file exists, on first startup file doesn't exist, so simply return allocated struct
     */
    if (0 != stat(path, &st))
    {
        return true;
    }

    // todo: error handling
    rintegrate = refresh_integrate_init();
    if (rintegrate == NULL)
    {
        elog(RLOG_WARNING, "malloc refresh integrate error");
        return false;
    }

    rintegrate->sync_stats = refresh_table_syncstats_init();
    if (rintegrate == NULL)
    {
        elog(RLOG_WARNING, "malloc refresh integrate sync_stats error ");
        return false;
    }
    rintegrate->stat = REFRESH_INTEGRATE_STAT_INIT;

    /* open file in read-only mode */
    fd = osal_basic_open_file(path, O_RDONLY | BINARY);
    if (fd < 0)
    {
        elog(RLOG_WARNING, "open integrate refresh file %s error %s", path, strerror(errno));
        return false;
    }

    /* read file, get persist rewind info and count from file start */
    read_size = osal_file_pread(fd, (char*)&tbcnt, sizeof(tbcnt), 0);
    if (read_size <= 0)
    {
        elog(RLOG_WARNING, "try read file %s head, read 0, error %s", path, strerror(errno));
        osal_file_close(fd);
        return false;
    }

    offset += sizeof(tbcnt);

    for (table_index = 0; table_index < tbcnt; table_index++)
    {
        char                    table[NAMEDATALEN] = {'\0'};
        char                    schema[NAMEDATALEN] = {'\0'};
        refresh_table_syncstat* new_syncstat = refresh_table_syncstat_init();

        bufferoffset = 0;

        read_size = osal_file_pread(fd, synctable, REFRESH_FILE_TABLE_LEN, offset);
        if (read_size <= 0)
        {
            elog(RLOG_WARNING, "try read file %s table, read 0, error %s", path, strerror(errno));
            osal_file_close(fd);
            return false;
        }

        buffer = (uint8*)synctable;

        rmemcpy1(&new_syncstat->cnt, 0, buffer + bufferoffset, sizeof(new_syncstat->cnt));
        bufferoffset += sizeof(new_syncstat->cnt);
        rmemcpy1(&new_syncstat->completecnt, 0, buffer + bufferoffset,
                 sizeof(new_syncstat->completecnt));
        bufferoffset += sizeof(new_syncstat->completecnt);
        rmemcpy1(&new_syncstat->tablestat, 0, buffer + bufferoffset,
                 sizeof(new_syncstat->tablestat));
        bufferoffset += sizeof(new_syncstat->tablestat);
        rmemcpy1(&new_syncstat->oid, 0, buffer + bufferoffset, sizeof(new_syncstat->oid));
        bufferoffset += sizeof(new_syncstat->oid);

        rmemcpy1(schema, 0, buffer + bufferoffset, NAMEDATALEN);
        bufferoffset += NAMEDATALEN;
        rmemcpy1(table, 0, buffer + bufferoffset, NAMEDATALEN);

        offset += REFRESH_FILE_TABLE_LEN;

        // copy table info
        refresh_table_syncstat_schema_set(schema, new_syncstat);
        refresh_table_syncstat_table_set(table, new_syncstat);

        if (0 < new_syncstat->cnt)
        {
            new_syncstat->stat = (int8_t*)rmalloc0(new_syncstat->cnt * sizeof(int8_t));
            if (NULL == new_syncstat->stat)
            {
                elog(RLOG_WARNING, "malloc tablestat error");
                osal_file_close(fd);
                return false;
            }
            rmemset0(new_syncstat->stat, 0, '\0', new_syncstat->cnt * sizeof(int8_t));

            read_size = osal_file_pread(fd, (char*)new_syncstat->stat,
                                        (new_syncstat->cnt * sizeof(int8_t)), offset);
            if (read_size <= 0)
            {
                elog(RLOG_WARNING, "try read file %s tablestat, read 0, error %s", path,
                     strerror(errno));
                refresh_table_syncstat_free(new_syncstat);
                osal_file_close(fd);
                return false;
            }

            /* stats->stat */
            for (index_stat = 0; index_stat < new_syncstat->cnt; index_stat++)
            {
                if (REFRESH_TABLE_SYNCS_SHARD_STAT_SYNCING == new_syncstat->stat[index_stat])
                {
                    new_syncstat->stat[index_stat] = REFRESH_TABLE_SYNCS_SHARD_STAT_INIT;
                }
            }

            offset += (new_syncstat->cnt * sizeof(int8_t));
        }

        new_syncstat->next = rintegrate->sync_stats->tablesyncing;
        if (rintegrate->sync_stats->tablesyncing)
        {
            rintegrate->sync_stats->tablesyncing->prev = new_syncstat;
        }

        rintegrate->sync_stats->tablesyncing = new_syncstat;
    }

    if (NULL != rintegrate->sync_stats->tablesyncing)
    {
        refresh_table_syncstats_tablesyncing2tablesyncall(rintegrate->sync_stats);
    }

    /* processing complete, close file */
    osal_file_close(fd);

    *refresh = (void*)rintegrate;
    return true;
}

/* free resources */
void refresh_integrate_free(void* args)
{
    refresh_integrate* rintegrate = NULL;

    rintegrate = (refresh_integrate*)args;

    if (!rintegrate)
    {
        return;
    }

    if (rintegrate->sync_stats)
    {
        refresh_table_syncstats_free(rintegrate->sync_stats);
    }

    if (rintegrate->refresh_path)
    {
        rfree(rintegrate->refresh_path);
    }

    if (rintegrate->tqueue)
    {
        queue_destroy(rintegrate->tqueue, NULL);
    }

    rfree(rintegrate);
}

/* free refresh linked list */
void refresh_integrate_listfree(void* args)
{
    List*              refresh = NULL;
    ListCell*          lc = NULL;
    refresh_integrate* rintegrate = NULL;

    refresh = (List*)args;

    if (!refresh)
    {
        return;
    }

    foreach (lc, refresh)
    {
        rintegrate = (refresh_integrate*)lfirst(lc);
        refresh_integrate_free((void*)rintegrate);
    }
    list_free(refresh);
}
