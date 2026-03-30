#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/guc/guc.h"
#include "utils/dlist/dlist.h"
#include "utils/list/list_func.h"
#include "utils/hash/hash_search.h"
#include "utils/uuid/uuid.h"
#include "utils/string/stringinfo.h"
#include "queue/queue.h"
#include "threads/threads.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "stmts/txnstmt.h"
#include "stmts/txnstmt_prepared.h"
#include "rebuild/rebuild.h"
#include "refresh/refresh_tables.h"
#include "refresh/refresh_table_sharding.h"
#include "refresh/refresh_table_syncstats.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "stmts/txnstmt_updaterewind.h"
#include "stmts/txnstmt_updatesynctable.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratedataset.h"
#include "onlinerefresh/integrate/dataset/onlinerefresh_integratefilterdataset.h"
#include "onlinerefresh/onlinerefresh_persist.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/integrate/bigtxn_integratemanager.h"
#include "onlinerefresh/integrate/onlinerefresh_integrate.h"
#include "increment/integrate/rebuild/increment_integraterebuild.h"

/* Initialization */
increment_integraterebuild* increment_integraterebuild_init(void)
{
    char*                       burst = NULL;
    increment_integraterebuild* rebuild_obj = rmalloc0(sizeof(increment_integraterebuild));
    if (NULL == rebuild_obj)
    {
        elog(RLOG_WARNING, "out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(rebuild_obj, 0, '\0', sizeof(increment_integraterebuild));
    rebuild_reset((rebuild*)rebuild_obj);
    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;
    rebuild_obj->filterlsn = InvalidXLogRecPtr;
    rebuild_obj->stat = INCREMENT_INTEGRATEREBUILD_STAT_NOP;
    rebuild_obj->mergetxn = (0 == guc_getConfigOptionInt(CFG_KEY_MERGETXN)) ? false : true;
    rebuild_obj->txbundlesize = guc_getConfigOptionInt(CFG_KEY_TXBUNDLESIZE);

    /* Set integrate_method */
    burst = guc_getConfigOption(CFG_KEY_INTEGRATE_METHOD);
    if (burst != NULL && '\0' != burst[0] && 0 == strcmp(burst, "burst"))
    {
        rebuild_obj->burst = true;
    }

    rebuild_obj->txnpersist = bigtxn_persist_init();

    rebuild_obj->honlinerefreshfilterdataset = onlinerefresh_integratefilterdataset_init();

    rebuild_obj->onlinerefreshdataset = onlinerefresh_integratedataset_init();
    return rebuild_obj;
}

static bool increment_integraterebuild_canwork(increment_integraterebuild* rebuild)
{
    if (rebuild->stat == INCREMENT_INTEGRATEREBUILD_STAT_WORK)
    {
        return true;
    }
    else if (rebuild->stat == INCREMENT_INTEGRATEREBUILD_STAT_READY)
    {
        rebuild->stat = INCREMENT_INTEGRATEREBUILD_STAT_WORK;
        return true;
    }
    else if (rebuild->stat == INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM)
    {
        cache_txn_clean(rebuild->parser2rebuild);
        return false;
    }
    return false;
}

static txn* increment_integraterebuild_updatesynctabletxn_set(increment_integraterebuild* rebuild_obj, txn* txn_obj)
{
    txn*                     cur_txn = NULL;
    txnstmt*                 stmtnode = NULL;
    txnstmt_updatesynctable* updatesynctable = NULL;

    cur_txn = txn_init(txn_obj->xid, txn_obj->start.wal.lsn, InvalidXLogRecPtr);
    if (NULL == cur_txn)
    {
        elog(RLOG_WARNING, "cur_txn out of memory, %s", strerror(errno));
        return NULL;
    }

    /* Allocate space */
    stmtnode = txnstmt_init();
    if (NULL == stmtnode)
    {
        rfree(cur_txn);
        elog(RLOG_WARNING, "stmtnode out of memory, %s", strerror(errno));
        return NULL;
    }
    rmemset0(stmtnode, 0, '\0', sizeof(txnstmt));

    updatesynctable = txnstmt_updatesynctable_init();
    if (NULL == updatesynctable)
    {
        rfree(cur_txn);
        rfree(stmtnode);
        return NULL;
    }

    stmtnode->type = TXNSTMT_TYPE_UPDATESYNCTABLE;
    stmtnode->stmt = (void*)updatesynctable;

    cur_txn->stmts = lappend(cur_txn->stmts, stmtnode);
    cur_txn->confirm.wal.lsn = txn_obj->confirm.wal.lsn;
    cur_txn->end.trail.offset = txn_obj->end.trail.offset;
    cur_txn->endtimestamp = txn_obj->endtimestamp;
    cur_txn->segno = txn_obj->segno;
    cur_txn->xid = txn_obj->xid;
    return cur_txn;
}

static bool increment_integraterebuild_updaterewindstmt_set(increment_integraterebuild* rebuild_obj, txn* txn_obj)
{
    recpos                pos = {{'\0'}};
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

    pos.trail.fileid = txn_obj->segno;
    pos.trail.offset = txn_obj->end.trail.offset;
    bigtxn_persist_electionrewind(rebuild_obj->txnpersist, &pos);

    updaterewind->rewind = rebuild_obj->txnpersist->rewind;

    stmtnode->type = TXNSTMT_TYPE_UPDATEREWIND;
    stmtnode->stmt = (void*)updaterewind;

    txn_obj->stmts = lappend(txn_obj->stmts, stmtnode);

    return true;
}

/* Clean up filter sets corresponding to abandoned onlinerefresh */
static void increment_integraterebuild_delonlinerefresdataset(increment_integraterebuild* rebuild_obj,
                                                              thrnode*                    thr_node,
                                                              void*                       abandon)
{
    List*                               luuid = NULL;
    ListCell*                           lc = NULL;
    uuid_t*                             uuid = NULL;
    onlinerefresh_integratedatasetnode* endnode = NULL;

    if (NULL == abandon)
    {
        return;
    }

    luuid = (List*)abandon;

    foreach (lc, luuid)
    {
        uuid = (uuid_t*)lfirst(lc);

        endnode = onlinerefresh_integratedataset_number_get(rebuild_obj->onlinerefreshdataset, uuid->data);
        while (true)
        {
            if (THRNODE_STAT_TERM == thr_node->stat)
            {
                return;
            }

            /* Wait for onlinerefresh to finish */
            if (rebuild_obj->callback.isonlinerefreshdone(rebuild_obj->privdata, uuid->data))
            {
                onlinerefresh_persist_statesetbyuuid(rebuild_obj->olpersist,
                                                     &endnode->onlinerefreshno,
                                                     ONLINEREFRESH_PERSISTNODE_STAT_DONE);
                /* Clean up persist existing tables after onlinerefresh ends */
                onlinerefresh_persist_removerefreshtbsbyuuid(rebuild_obj->olpersist, &endnode->onlinerefreshno);
                onlinerefresh_persist_electionrewindbyuuid(rebuild_obj->olpersist, &endnode->onlinerefreshno);
                onlinerefresh_integratefilterdataset_delete(rebuild_obj->honlinerefreshfilterdataset,
                                                            endnode->refreshtables,
                                                            endnode->txid);
                onlinerefresh_integratedataset_delete(rebuild_obj->onlinerefreshdataset, uuid->data);
                break;
            }
            usleep(50000);
        }
    }
    return;
}

/* Add big transaction */
static void increment_integraterebuild_addbigtxnpersist(increment_integraterebuild* rebuild_obj, txn* txn_obj)
{
    bool                exist = false;
    recpos              pos = {{'\0'}};
    dlistnode*          dnode = NULL;
    dlistnode*          dlnodenext = NULL;
    bigtxn_persist*     persist = NULL;
    bigtxn_persistnode* persistnode = NULL;

    persist = rebuild_obj->txnpersist;

    /* Ensure existence */
    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    pos.trail.fileid = txn_obj->segno;
    pos.trail.offset = txn_obj->end.trail.offset;

    /* Iterate through persist, check if same transaction exists */
    for (; dnode; dnode = dlnodenext)
    {
        dlnodenext = dnode->next;
        persistnode = (bigtxn_persistnode*)dnode->value;

        /* Check if a big transaction with the same transaction ID exists */
        if (txn_obj->xid == persistnode->xid)
        {
            /* Exists */
            exist = true;
            bigtxn_persistnode_set_begin(persistnode, &pos);
            bigtxn_persistnode_set_xid(persistnode, txn_obj->xid);
        }
    }

    /* After iteration, check if exists, only add if not exists */
    if (!exist)
    {
        bigtxn_persistnode* pernode = NULL;

        /* Build persist node and initialize */
        pernode = bigtxn_persist_node_init();
        bigtxn_persistnode_set_begin(pernode, &pos);
        bigtxn_persistnode_set_xid(pernode, txn_obj->xid);
        pernode->stat = BIGTXN_PERSISTNODE_STAT_INPROCESS;
        persist->dpersistnodes = dlist_put(persist->dpersistnodes, pernode);
        persist->count += 1;
    }
}

/* Add onlinerefresh persist */
static void increment_integraterebuild_addonlinerefreshpersist(increment_integraterebuild* rebuild_obj,
                                                               onlinerefresh_integrate*    olrintegrate)
{
    bool                       exist = false;
    dlistnode*                 dnode = NULL;
    dlistnode*                 dlnodenext = NULL;
    onlinerefresh_persist*     persist = NULL;
    onlinerefresh_persistnode* persistnode = NULL;

    persist = rebuild_obj->olpersist;

    /* Ensure existence */
    dnode = persist->dpersistnodes ? persist->dpersistnodes->head : NULL;

    /* Iterate through persist, check if same transaction exists */
    for (; dnode; dnode = dlnodenext)
    {
        dlnodenext = dnode->next;
        persistnode = (onlinerefresh_persistnode*)dnode->value;

        /* Check if a big transaction with the same transaction ID exists */
        if (0 == memcmp(olrintegrate->no.data, persistnode->uuid.data, UUID_LEN))
        {
            /* Exists */
            exist = true;
            onlinerefresh_persistnode_beginset(persistnode, olrintegrate->begin);
        }
    }

    /* After iteration, check if exists, only add if not exists */
    if (!exist)
    {
        onlinerefresh_persistnode* pernode = NULL;

        /* Build persist node and initialize */
        pernode = onlinerefresh_persistnode_init();
        onlinerefresh_persistnode_statset(pernode, ONLINEREFRESH_PERSISTNODE_STAT_INIT);
        onlinerefresh_persistnode_uuidset(pernode, &olrintegrate->no);
        onlinerefresh_persistnode_incrementset(pernode, olrintegrate->increment);
        onlinerefresh_persistnode_beginset(pernode, olrintegrate->begin);
        onlinerefresh_persistnode_txidset(pernode, olrintegrate->txid);

        pernode->refreshtbs = olrintegrate->tablesyncstats;
        if (true == dlist_isnull(persist->dpersistnodes))
        {
            persist->rewind.trail.fileid = olrintegrate->begin.trail.fileid;
            persist->rewind.trail.offset = olrintegrate->begin.trail.offset;
        }
        persist->dpersistnodes = dlist_put(persist->dpersistnodes, pernode);
    }
    onlinerefresh_persist_write(persist);
}

static bool increment_integraterebuild_isspecialtxn(txn* txn_obj)
{
    if (NULL == txn_obj->stmts)
    {
        return true;
    }

    /* refresh */
    if (TXN_TYPE_METADATA < txn_obj->type)
    {
        return true;
    }
    else
    {
        return false;
    }
}

/*
 * Choose transaction rebuild method based on rebuild->burst
 *
 * Return value: true success, false failure
 */
static bool increment_integraterebuild_rebuildtxn(increment_integraterebuild* rebuild_obj, txn* txn_obj)
{
    if (true == rebuild_obj->burst)
    {
        /* Rebuild */
        if (false == rebuild_txnburst((rebuild*)rebuild_obj, txn_obj))
        {
            elog(RLOG_WARNING, "increment_integraterebuild_txnburst error");
            return false;
        }
    }
    else
    {
        /* Rebuild */
        if (false == rebuild_prepared((rebuild*)rebuild_obj, txn_obj))
        {
            elog(RLOG_WARNING, "increment_integraterebuild_prepared error");
            return false;
        }
    }

    return true;
}

/*
 * integate rebuild applies system catalog, cleans up index and attribute before applying
 */
static void increment_integraterebuild_transcatalog2transcache(increment_integraterebuild* rebuild_obj, txn* txn_obj)
{
    bool              equal = false;
    ListCell*         stmtlc = NULL;
    ListCell*         catraloglc = NULL;
    txnstmt*          stmtnode = NULL;
    txnstmt_metadata* metadatastmt = NULL;

    if (NULL == txn_obj->stmts)
    {
        return;
    }

    foreach (stmtlc, txn_obj->stmts)
    {
        stmtnode = (txnstmt*)lfirst(stmtlc);

        if (TXNSTMT_TYPE_METADATA == stmtnode->type)
        {
            metadatastmt = (txnstmt_metadata*)stmtnode->stmt;
            catraloglc = metadatastmt->begin;
            /* Clean up previous index and attribute based on class->oid */
            cache_sysdicts_clearsysdicthisbyclass(rebuild_obj->rebuild.sysdicts, catraloglc);

            while (1)
            {
                /* Apply to system catalog */
                cache_sysdicts_txnsysdicthisitem2cache(rebuild_obj->rebuild.sysdicts, (void*)catraloglc);

                /* Only one */
                if (catraloglc == metadatastmt->end || true == equal)
                {
                    break;
                }

                /* Check if reached the last one */
                catraloglc = catraloglc->next;
                if (catraloglc == metadatastmt->end)
                {
                    equal = true;
                }
            }
        }
    }
    return;
}

/* Only returns false on big transaction abnormal exit and term signal */
static bool increment_integraterebuild_specialtxn(increment_integraterebuild* rebuild_obj,
                                                  thrnode*                    thr_node,
                                                  txn*                        txn_obj,
                                                  txn**                       ntxn,
                                                  int*                        txbundlesize)
{
    recpos                              pos = {{'\0'}};
    txn*                                cur_txn = NULL;
    txnstmt*                            stmtnode = NULL;
    txnstmt_onlinerefresh*              onlinerefresh = NULL;
    onlinerefresh_integratedatasetnode* datasetnode = NULL;
    onlinerefresh_integratedatasetnode* endnode = NULL;

    if (NULL == txn_obj->stmts)
    {
        return true;
    }

    stmtnode = (txnstmt*)lfirst(list_head(txn_obj->stmts));

    /* refresh */
    if (TXNSTMT_TYPE_REFRESH == stmtnode->type)
    {
        cur_txn = NULL;
        while (true)
        {
            if (THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }

            if (true == rebuild_obj->callback.isrefreshdown(rebuild_obj->privdata))
            {
                break;
            }
            usleep(50000);
        }
        /* Update status table info, does not include rewind info */
        cur_txn = increment_integraterebuild_updatesynctabletxn_set(rebuild_obj, txn_obj);
        if (cur_txn)
        {
            cur_txn->segno = 1;
            cur_txn->end.trail.offset = 0;
            /* Rewind info is not stored in transaction, calculated rewind is passed to apply thread
             * via stmt */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, cur_txn);
            cache_txn_add(rebuild_obj->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
    }
    else if (TXNSTMT_TYPE_ONLINEREFRESH_BEGIN == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if (false == increment_integraterebuild_rebuildtxn(rebuild_obj, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* Rewind info is not stored in transaction, calculated rewind is passed to apply thread
             * via stmt */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, *ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        /* Convert type */
        onlinerefresh = (txnstmt_onlinerefresh*)stmtnode->stmt;

        /* Wait for cached transactions to finish applying */
        while (true)
        {
            if (THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }

            if (txn_obj->segno == rebuild_obj->olpersist->rewind.trail.fileid &&
                txn_obj->end.trail.offset == rebuild_obj->olpersist->rewind.trail.offset)
            {
                break;
            }

            /* Cache apply complete and sync thread idle */
            if (cache_txn_isnull(rebuild_obj->rebuild2sync) &&
                true == rebuild_obj->callback.issyncidle(rebuild_obj->privdata))
            {
                refresh_tables*          tables = NULL;
                refresh_table_syncstats* tablesyncstats = NULL;
                onlinerefresh_integrate* integrateolrefresh = NULL;

                /* Register onlinerefresh node */
                tables = refresh_tables_copy(onlinerefresh->refreshtables);

                tablesyncstats = refresh_table_syncstats_init();
                refresh_table_syncstats_tablesyncing_set(tables, tablesyncstats);
                refresh_table_syncstats_tablesyncall_set(tables, tablesyncstats);

                integrateolrefresh = onlinerefresh_integrate_init(onlinerefresh->increment);

                integrateolrefresh->stat = ONLINEREFRESH_INTEGRATE_INIT;
                rmemcpy1(integrateolrefresh->no.data, 0, onlinerefresh->no->data, UUID_LEN);
                integrateolrefresh->increment = onlinerefresh->increment;
                integrateolrefresh->tablesyncstats = tablesyncstats;
                integrateolrefresh->txid = onlinerefresh->txid;
                integrateolrefresh->begin.trail.fileid = txn_obj->segno;
                integrateolrefresh->begin.trail.offset = txn_obj->end.trail.offset;

                increment_integraterebuild_addonlinerefreshpersist(rebuild_obj, integrateolrefresh);
                rebuild_obj->callback.addonlinerefresh(rebuild_obj->privdata, integrateolrefresh);

                refresh_freetables(tables);
                break;
            }
            usleep(50000);
        }

        while (true)
        {
            if (THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }
            /* Wait for existing onlinerefresh stock to finish */
            if (true == rebuild_obj->callback.isolrrefreshdone(rebuild_obj->privdata, onlinerefresh->no->data))
            {
                break;
            }
            usleep(50000);
        }

        /* Create onlinerefresh filter dataset */
        onlinerefresh_integratefilterdataset_add(rebuild_obj->honlinerefreshfilterdataset,
                                                 onlinerefresh->refreshtables,
                                                 onlinerefresh->txid);
        datasetnode = onlinerefresh_integratedatasetnode_init();
        onlinerefresh_integratedatasetnode_no_set(datasetnode, onlinerefresh->no->data);
        onlinerefresh_integratedatasetnode_txid_set(datasetnode, onlinerefresh->txid);
        onlinerefresh_integratedatasetnode_refreshtables_set(datasetnode, onlinerefresh->refreshtables);
        onlinerefresh_integratedataset_add(rebuild_obj->onlinerefreshdataset, datasetnode);
    }
    else if (TXNSTMT_TYPE_ONLINEREFRESH_END == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if (false == increment_integraterebuild_rebuildtxn(rebuild_obj, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            cache_txn_add(rebuild_obj->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        elog(RLOG_DEBUG, "get TXNSTMT_TYPE_ONLINEREFRESH_END");
        /* Get onlinerefresh number */
        endnode = onlinerefresh_integratedataset_number_get(rebuild_obj->onlinerefreshdataset, stmtnode->stmt);

        while (true)
        {
            if (THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }
            /* Wait for onlinerefresh to finish */
            if (rebuild_obj->callback.isonlinerefreshdone(rebuild_obj->privdata, endnode->onlinerefreshno.data))
            {
                onlinerefresh_persist_statesetbyuuid(rebuild_obj->olpersist,
                                                     &endnode->onlinerefreshno,
                                                     ONLINEREFRESH_PERSISTNODE_STAT_DONE);
                /* Clean up persist existing tables after onlinerefresh ends */
                onlinerefresh_persist_removerefreshtbsbyuuid(rebuild_obj->olpersist, &endnode->onlinerefreshno);
                onlinerefresh_persist_electionrewindbyuuid(rebuild_obj->olpersist, &endnode->onlinerefreshno);
                onlinerefresh_integratefilterdataset_delete(rebuild_obj->honlinerefreshfilterdataset,
                                                            endnode->refreshtables,
                                                            endnode->txid);
                onlinerefresh_integratedataset_delete(rebuild_obj->onlinerefreshdataset, stmtnode->stmt);
                rebuild_obj->callback.setonlinerefreshfree(rebuild_obj->privdata, stmtnode->stmt);
                break;
            }
            usleep(50000);
        }

        cur_txn = increment_integraterebuild_updatesynctabletxn_set(rebuild_obj, txn_obj);
        if (cur_txn)
        {
            /* Rewind info is not stored in transaction, calculated rewind is passed to apply thread
             * via stmt */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, cur_txn);
            cache_txn_add(rebuild_obj->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
    }
    else if (TXNSTMT_TYPE_RESET == stmtnode->type)
    {
        if (NULL != *ntxn)
        {
            if (false == increment_integraterebuild_rebuildtxn(rebuild_obj, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* Rewind info is not stored in transaction, calculated rewind is passed to apply thread
             * via stmt */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, *ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        bigtxn_integratepersist_cleannotdone(rebuild_obj->txnpersist);
    }
    else if (TXNSTMT_TYPE_ABANDON == stmtnode->type)
    {
        bigtxn_persist_removebyxid(rebuild_obj->txnpersist, txn_obj->xid);
        pos.trail.fileid = txn_obj->segno;
        pos.trail.offset = txn_obj->end.trail.offset;
        bigtxn_persist_electionrewind(rebuild_obj->txnpersist, &pos);
    }
    else if (TXNSTMT_TYPE_BIGTXN_BEGIN == stmtnode->type)
    {
        /*
         * todo: delete persist, big transaction starts when receiving end, won't update rewind
         * point until complete, will always start big transaction if not ended
         */
        increment_integraterebuild_addbigtxnpersist(rebuild_obj, txn_obj);
    }
    else if (TXNSTMT_TYPE_BIGTXN_END == stmtnode->type)
    {
        bigtxn_end_stmt*         end_stmt = NULL;
        bigtxn_integratemanager* bigtxn = NULL;
        if (NULL != *ntxn)
        {
            if (false == increment_integraterebuild_rebuildtxn(rebuild_obj, *ntxn))
            {
                elog(RLOG_WARNING, "increment integraterebuild specialtxn rebuildtxn error");
                return false;
            }
            /* Rewind info is not stored in transaction, calculated rewind is passed to apply thread
             * via stmt */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, *ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, *ntxn);
            *ntxn = NULL;
            *txbundlesize = 0;
        }
        end_stmt = (bigtxn_end_stmt*)stmtnode->stmt;
        /* Wait for cached transactions to finish applying */
        while (true)
        {
            if (THRNODE_STAT_TERM == thr_node->stat)
            {
                return false;
            }

            /* Cache apply complete and sync thread idle */
            if (cache_txn_isnull(rebuild_obj->rebuild2sync) &&
                true == rebuild_obj->callback.issyncidle(rebuild_obj->privdata))
            {
                if (true == end_stmt->commit)
                {
                    bigtxn = bigtxn_integratemanager_init();
                    bigtxn->xid = end_stmt->xid;
                    /* Copy onlinerefresh filter set for big transaction internal filtering */
                    bigtxn->honlinerefreshfilterdataset =
                        onlinerefresh_integratefilterdataset_copy(rebuild_obj->honlinerefreshfilterdataset);
                    if (false == dlist_isnull(rebuild_obj->onlinerefreshdataset->onlinerefresh))
                    {
                        bigtxn->onlinerefreshdataset =
                            onlinerefresh_integratedataset_copy(rebuild_obj->onlinerefreshdataset);
                    }
                    bigtxn_integratemanager_stat_set(bigtxn, BIGTXN_INTEGRATEMANAGER_STAT_INIT);
                    rebuild_obj->callback.addbigtxn(rebuild_obj->privdata, (void*)bigtxn);
                }
                break;
            }
            usleep(50000);
        }

        if (true == end_stmt->commit)
        {
            while (true)
            {
                if (THRNODE_STAT_TERM == thr_node->stat)
                {
                    return false;
                }

                if (true == rebuild_obj->callback.isbigtxnsigterm(rebuild_obj->privdata, end_stmt->xid))
                {
                    return false;
                }

                if (true == rebuild_obj->callback.isbigtxndown(rebuild_obj->privdata, end_stmt->xid))
                {
                    break;
                }
                usleep(50000);
            }
        }

        bigtxn_persist_removebyxid(rebuild_obj->txnpersist, end_stmt->xid);

        cur_txn = increment_integraterebuild_updatesynctabletxn_set(rebuild_obj, txn_obj);
        if (cur_txn)
        {
            cur_txn->xid = end_stmt->xid;
            /* Rewind info is not stored in transaction, calculated rewind is passed to apply thread
             * via stmt */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, cur_txn);
            cache_txn_add(rebuild_obj->rebuild2sync, cur_txn);
            cur_txn = NULL;
        }
    }
    else if (TXNSTMT_TYPE_ONLINEREFRESHABANDON == stmtnode->type)
    {
        increment_integraterebuild_delonlinerefresdataset(rebuild_obj, thr_node, stmtnode->stmt);
    }

    return true;
}

/* Work */
void* increment_integraterebuild_main(void* args)
{
    bool                                  find = false;
    int                                   timeout = 0;
    int                                   txbundlesize = 0;
    Oid                                   relid = INVALIDOID;
    ListCell*                             filterlc = NULL;
    txn*                                  txns = NULL;
    txn*                                  ntxn = NULL;
    txn*                                  txnnode = NULL;
    thrnode*                              thr_node = NULL;
    txnstmt*                              stmtnode = NULL;
    increment_integraterebuild*           rebuild_obj = NULL;
    pg_parser_translog_tbcol_values*      values = NULL;
    pg_parser_translog_tbcolbase*         tbcolbase = NULL;
    pg_parser_translog_tbcol_nvalues*     nvalues = NULL;
    onlinerefresh_integratedatasetnode*   node = NULL;
    onlinerefresh_integratefilterdataset* filterdatasetentry = NULL;

    thr_node = (thrnode*)args;
    /* Parameter conversion */
    rebuild_obj = (increment_integraterebuild*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "increment integrate rebuild exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        pthread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    while (1)
    {
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialize/flush to disk */
            thr_node->stat = THRNODE_STAT_EXIT;
            cache_txn_clean(rebuild_obj->parser2rebuild);
            break;
        }

        if (false == increment_integraterebuild_canwork(rebuild_obj))
        {
            usleep(50000);
            continue;
        }

        /* Get data from cache */
        txns = cache_txn_getbatch(rebuild_obj->parser2rebuild, &timeout);
        if (NULL == txns)
        {
            /* Timeout, retry */
            if (ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }

            elog(RLOG_WARNING, "get file buffer error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Iterate through txns and rebuild statements */
        for (txnnode = txns; NULL != txnnode; txnnode = txns)
        {
            /* special used to mark specified transactions:
             * refresh/onlinerefreshbegin/onlinerefreshend */
            txns = txnnode->cachenext;

            /* Filter transactions */
            if (txnnode->confirm.wal.lsn <= rebuild_obj->filterlsn)
            {
                /* Apply system catalog data */
                increment_integraterebuild_transcatalog2transcache(rebuild_obj, txnnode);
                txn_free(txnnode);
                rfree(txnnode);
                continue;
            }
            if (MAX_LSN > txnnode->confirm.wal.lsn)
            {
                rebuild_obj->filterlsn = txnnode->confirm.wal.lsn;
            }

            /* Used to handle specified transactions: refresh/onlinerefreshbegin/onlinerefreshend */
            if (true == increment_integraterebuild_isspecialtxn(txnnode))
            {
                if (false ==
                    increment_integraterebuild_specialtxn(rebuild_obj, thr_node, txnnode, &ntxn, &txbundlesize))
                {
                    /* term exit or other independent thread exit, return to upper while loop
                     * waiting for term */
                    rebuild_obj->stat = INCREMENT_INTEGRATEREBUILD_STAT_WAITTERM;
                    elog(RLOG_WARNING, "Received term or other independent thread exit");
                    break;
                }

                if (txnnode)
                {
                    txn_free(txnnode);
                    rfree(txnnode);
                }
                continue;
            }

            if (false == dlist_isnull(rebuild_obj->onlinerefreshdataset->onlinerefresh))
            {
                /* Filter data already applied in onlinerefresh */
                List* tmpstmt = NULL;
                stmtnode = NULL;
                find = false;
                foreach (filterlc, txnnode->stmts)
                {
                    stmtnode = (txnstmt*)lfirst(filterlc);
                    if (stmtnode->type != TXNSTMT_TYPE_DML)
                    {
                        tmpstmt = lappend(tmpstmt, stmtnode);
                        continue;
                    }

                    tbcolbase = (pg_parser_translog_tbcolbase*)stmtnode->stmt;

                    if (PG_PARSER_TRANSLOG_DMLTYPE_MULTIINSERT == tbcolbase->m_dmltype)
                    {
                        nvalues = (pg_parser_translog_tbcol_nvalues*)stmtnode->stmt;
                        relid = nvalues->m_relid;
                    }
                    else
                    {
                        values = (pg_parser_translog_tbcol_values*)stmtnode->stmt;
                        relid = values->m_relid;
                    }

                    filterdatasetentry =
                        hash_search(rebuild_obj->honlinerefreshfilterdataset, &relid, HASH_FIND, &find);
                    if (false == find)
                    {
                        tmpstmt = lappend(tmpstmt, stmtnode);
                        continue;
                    }

                    if (txnnode->xid < filterdatasetentry->txid)
                    {
                        txnstmt_free(stmtnode);
                        continue;
                    }

                    node = onlinerefresh_integratedataset_txid_get(rebuild_obj->onlinerefreshdataset,
                                                                   filterdatasetentry->txid);
                    while (false ==
                           rebuild_obj->callback.isonlinerefreshdone(rebuild_obj->privdata, node->onlinerefreshno.data))
                    {
                        if (THRNODE_STAT_TERM == thr_node->stat)
                        {
                            thr_node->stat = THRNODE_STAT_EXIT;
                            pthread_exit(NULL);
                            return NULL;
                        }
                        usleep(50000);
                        continue;
                    }

                    tmpstmt = lappend(tmpstmt, stmtnode);
                    continue;
                }
                list_free(txnnode->stmts);
                txnnode->stmts = tmpstmt;
                tmpstmt = NULL;

                /* No processing needed, todo update status table */
                if (NULL == txnnode->stmts)
                {
                    txn* cur_txn = NULL;
                    cur_txn = increment_integraterebuild_updatesynctabletxn_set(rebuild_obj, txnnode);
                    if (cur_txn)
                    {
                        /* Rewind info is not stored in transaction, calculated rewind is passed to
                         * apply thread via stmt
                         */
                        increment_integraterebuild_updaterewindstmt_set(rebuild_obj, cur_txn);
                        cache_txn_add(rebuild_obj->rebuild2sync, cur_txn);
                        cur_txn = NULL;
                    }
                    else
                    {
                        elog(RLOG_WARNING, "increment_integraterebuild_updatesynctabletxn_set error");
                        thr_node->stat = THRNODE_STAT_ABORT;
                        break;
                    }
                    txn_free(txnnode);
                    rfree(txnnode);
                    continue;
                }
            }

            if (NULL == txnnode->stmts)
            {
                txn_free(txnnode);
                rfree(txnnode);
                continue;
            }

            /* Do not execute transaction merging */
            if (false == rebuild_obj->mergetxn)
            {
                /* Put transaction into cache */
                if (txnnode->end.wal.lsn != InvalidXLogRecPtr)
                {
                    rebuild_obj->filterlsn = txnnode->end.wal.lsn;
                }

                if (false == increment_integraterebuild_rebuildtxn(rebuild_obj, txnnode))
                {
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }

                /* Rewind info is not stored in transaction, calculated rewind is passed to apply
                 * thread via stmt */
                increment_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                continue;
            }

            if (txnnode->stmts->length > rebuild_obj->txbundlesize)
            {
                if (NULL != ntxn)
                {
                    if (false == increment_integraterebuild_rebuildtxn(rebuild_obj, ntxn))
                    {
                        thr_node->stat = THRNODE_STAT_ABORT;
                        break;
                    }
                    /* Rewind info is not stored in transaction, calculated rewind is passed to
                     * apply thread via stmt */
                    increment_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
                    cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
                    ntxn = NULL;
                    txbundlesize = 0;
                }

                if (false == increment_integraterebuild_rebuildtxn(rebuild_obj, txnnode))
                {
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }

                /* Rewind info is not stored in transaction, calculated rewind is passed to apply
                 * thread via stmt */
                increment_integraterebuild_updaterewindstmt_set(rebuild_obj, txnnode);
                cache_txn_add(rebuild_obj->rebuild2sync, txnnode);
                continue;
            }

            /* Merge transaction processing */
            /* If empty, allocate space */
            if (NULL == ntxn)
            {
                ntxn = (txn*)rmalloc0(sizeof(txn));
                if (NULL == ntxn)
                {
                    elog(RLOG_WARNING, "integrate rebuild txn init out of memory, %s", strerror(errno));
                    thr_node->stat = THRNODE_STAT_ABORT;
                    break;
                }
                rmemset0(ntxn, 0, '\0', sizeof(txn));
            }

            /* Copy transaction info */
            ntxn->xid = txnnode->xid;
            ntxn->segno = txnnode->segno;
            ntxn->debugno = txnnode->debugno;
            ntxn->start = txnnode->start;
            ntxn->end = txnnode->end;
            ntxn->redo = txnnode->redo;
            ntxn->restart = txnnode->restart;
            ntxn->confirm = txnnode->confirm;
            ntxn->endtimestamp = txnnode->endtimestamp;
            txbundlesize += txnnode->stmts->length;

            /* Add sysdictHis to new transaction */
            if (NULL != ntxn->sysdictHis || NULL != txnnode->sysdictHis)
            {
                ntxn->sysdictHis = list_concat(ntxn->sysdictHis, txnnode->sysdictHis);
                if (ntxn->sysdictHis != txnnode->sysdictHis && NULL != txnnode->sysdictHis)
                {
                    rfree(txnnode->sysdictHis);
                }
                txnnode->sysdictHis = NULL;
            }

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

            if (false == increment_integraterebuild_rebuildtxn(rebuild_obj, ntxn))
            {
                thr_node->stat = THRNODE_STAT_ABORT;
                break;
            }

            /* Rewind info is not stored in transaction, calculated rewind is passed to apply thread
             * via stmt */
            increment_integraterebuild_updaterewindstmt_set(rebuild_obj, ntxn);
            cache_txn_add(rebuild_obj->rebuild2sync, ntxn);
            ntxn = NULL;
            txbundlesize = 0;
        }
    }

    pthread_exit(NULL);
    return NULL;
}

void increment_integraterebuild_free(increment_integraterebuild* rebuild_obj)
{
    if (NULL == rebuild_obj)
    {
        return;
    }

    rebuild_obj->parser2rebuild = NULL;
    rebuild_obj->rebuild2sync = NULL;

    rebuild_destroy((rebuild*)rebuild_obj);

    if (rebuild_obj->honlinerefreshfilterdataset)
    {
        hash_destroy(rebuild_obj->honlinerefreshfilterdataset);
    }

    if (rebuild_obj->onlinerefreshdataset)
    {
        dlist_free(rebuild_obj->onlinerefreshdataset->onlinerefresh, onlinerefresh_integratedataset_free);
        rfree(rebuild_obj->onlinerefreshdataset);
    }

    if (rebuild_obj->olpersist)
    {
        onlinerefresh_persist_free(rebuild_obj->olpersist);
    }

    if (rebuild_obj->txnpersist)
    {
        bigtxn_persist_free(rebuild_obj->txnpersist);
    }

    rfree(rebuild_obj);
}
