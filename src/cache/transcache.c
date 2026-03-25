#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/hash/hash_utils.h"
#include "utils/hash/hash_search.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/uuid/uuid.h"
#include "utils/regex/regex.h"
#include "port/thread/thread.h"
#include "misc/misc_control.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/ffsmgr.h"
#include "cache/cache_sysidcts.h"
#include "catalog/catalog.h"
#include "catalog/attribute.h"
#include "catalog/class.h"
#include "catalog/database.h"
#include "catalog/type.h"
#include "catalog/namespace.h"
#include "catalog/constraint.h"
#include "catalog/proc.h"
#include "catalog/index.h"
#include "stmts/txnstmt.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "cache/fpwcache.h"
#include "cache/toastcache.h"
#include "utils/mpage/mpage.h"
#include "queue/queue.h"
#include "loadrecords/record.h"
#include "loadrecords/loadpage.h"
#include "loadrecords/loadpageam.h"
#include "loadrecords/loadpagefromfile.h"
#include "loadrecords/loadrecords.h"
#include "loadrecords/loadwalrecords.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "utils/guc/guc.h"
#include "refresh/refresh_tables.h"
#include "stmts/txnstmt_refresh.h"
#include "stmts/txnstmt_onlinerefresh.h"
#include "works/parserwork/wal/onlinerefresh.h"
#include "strategy/filter_dataset.h"
/*
 * Transactions are caches that are being processed.
 * Processing cache is set as a hash table for fast lookup
 *  Processing cache does not need locking during ENTER/REMOVE/FIND
 */

static void transcache_dlist_append(decodingcontext* ctx, txn* txn)
{
    if (NULL == txn)
    {
        return;
    }

    if (NULL == ctx->trans_cache->transdlist->head)
    {
        ctx->trans_cache->transdlist->head = txn;
        ctx->trans_cache->transdlist->tail = txn;
    }
    else
    {
        ctx->trans_cache->transdlist->tail->next = txn;
        txn->prev = ctx->trans_cache->transdlist->tail;
        ctx->trans_cache->transdlist->tail = txn;
    }
}

/*
 * Remove transaction from doubly linked list
 * Parameters:
 *  in_ctx          Parsing context
 *  txn             Transaction to be removed
 *  brestart        Output parameter, indicates whether restartlsn is updated
 *                      true        Updated
 *                      false       Not updated
 *  bconfirm        Output parameter, indicates whether confirmlsn is updated
 *                      true        Updated
 *                      false       Not updated
 *  bset            Input parameter, indicates whether to update restartlsn and confirmlsn when
 * calling this function true        Need to update false       No need to update
 */
void transcache_dlist_remove(void* in_ctx, txn* txn, bool* brestart, XLogRecPtr* restartlsn,
                             bool* bconfirm, XLogRecPtr* confirmlsn, bool bset)
{
    decodingcontext* ctx = NULL;
    if (NULL == txn)
    {
        return;
    }

    *brestart = false;
    *bconfirm = false;
    ctx = (decodingcontext*)in_ctx;

    if (NULL == txn->prev)
    {
        /* Set new restartlsn */
        if (true == bset && txn->start.wal.lsn > ctx->base.restartlsn)
        {
            ctx->base.restartlsn = txn->start.wal.lsn;
            *brestart = true;
            if (NULL != restartlsn)
            {
                *restartlsn = txn->start.wal.lsn;
            }
            elog(RLOG_DEBUG, "restartlsn update by:xid:%lu, %X/%X", txn->xid,
                 (uint32)(txn->start.wal.lsn >> 32), (uint32)(txn->start.wal.lsn));
        }

        if (NULL == txn->next)
        {
            /* Empty linked list */
            if (true == *brestart)
            {
                ctx->base.restartlsn = txn->end.wal.lsn;
                if (NULL != restartlsn)
                {
                    *restartlsn = txn->end.wal.lsn;
                }
            }

            ctx->trans_cache->transdlist->head = NULL;
            ctx->trans_cache->transdlist->tail = NULL;
        }
        else
        {
            ctx->trans_cache->transdlist->head = txn->next;
            txn->next->prev = NULL;
        }
    }
    else
    {
        if (NULL == txn->next)
        {
            ctx->trans_cache->transdlist->tail = txn->prev;
            txn->prev->next = NULL;
        }
        else
        {
            txn->prev->next = txn->next;
            txn->next->prev = txn->prev;
        }
    }

    if (true == bset && ctx->base.confirmedlsn < txn->end.wal.lsn)
    {
        ctx->base.confirmedlsn = txn->end.wal.lsn;
        *bconfirm = true;
        if (NULL != confirmlsn)
        {
            *confirmlsn = txn->end.wal.lsn;
        }
    }

    txn->prev = NULL;
    txn->next = NULL;
}

txn* transcache_getTXNByXid(void* in_ctx, uint64_t xid)
{
    bool             find = false;
    HTAB*            tx_htab = NULL;
    decodingcontext* ctx = NULL;
    txn*             txn_entry = NULL;

    /* Invalid transaction, no need to maintain in hash */
    if (InvalidFullTransactionId == xid)
    {
        return NULL;
    }

    ctx = (decodingcontext*)in_ctx;
    tx_htab = ctx->trans_cache->by_txns;

    txn_entry = (txn*)hash_search(tx_htab, &xid, HASH_ENTER, &find);
    if (!find)
    {
        /* First capture of this transaction */
        if (ctx->onlinerefresh)
        {
            /* onlinerefresh node is not empty */
            dlistnode*     dlnode = ctx->onlinerefresh->head;
            onlinerefresh* olnode = NULL;
            for (; dlnode; dlnode = dlnode->next)
            {
                olnode = (onlinerefresh*)dlnode->value;
                if (!olnode->increment)
                {
                    /* Skip when incremental is not needed */
                    continue;
                }

                if (xid < olnode->snapshot->xmin)
                {
                    /* No need to add to xids */
                    continue;
                }
                if (xid > olnode->txid)
                {
                    if (olnode->state != ONLINEREFRESH_STATE_FULLSNAPSHOT)
                    {
                        onlinerefresh_state_setfullsnapshot(olnode);
                    }
                    continue;
                }
                else
                {
                    /* There is a case where xmin = xmax, so first exclude xid = xmin */
                    if (xid != olnode->snapshot->xmin && xid >= olnode->snapshot->xmax)
                    {
                        onlinerefresh_xids_append(olnode, xid);
                    }
                }
            }
        }
        /* Initialize */
        txn_initset(txn_entry, xid, ctx->decode_record->start.wal.lsn);
        TXN_SET_TRANS_INHASH(txn_entry->flag);

        /* Add transaction to doubly linked list */
        transcache_dlist_append(ctx, txn_entry);
    }

    return txn_entry;
}

txn* transcache_getTXNByXidFind(transcache* transcache, uint64_t xid)
{
    txn* txn_entry = NULL;
    txn_entry = (txn*)hash_search(transcache->by_txns, &xid, HASH_FIND, NULL);
    return txn_entry;
}

/* Delete */
/* Modify input parameters */
void transcache_removeTXNByXid(transcache* in_transcache, uint64_t xid)
{
    bool find = false;
    txn* txn_entry = NULL;

    txn_entry = hash_search(in_transcache->by_txns, &xid, HASH_REMOVE, &find);
    if (false == find)
    {
        elog(RLOG_WARNING, "ripple logical error");
    }
    else
    {
        elog(RLOG_DEBUG, "txn lsn info :restart:%X/%X, confirm:%X/%X, redo:%X/%X,",
             (uint32)(txn_entry->restart.wal.lsn >> 32), (uint32)(txn_entry->restart.wal.lsn),
             (uint32)(txn_entry->confirm.wal.lsn >> 32), (uint32)(txn_entry->confirm.wal.lsn),
             (uint32)(txn_entry->redo.wal.lsn >> 32), (uint32)(txn_entry->redo.wal.lsn));
        rmemset1(txn_entry, 0, '\0', sizeof(txn));
    }
    // elog(RLOG_INFO, "remove txn, found:%d, xid:%lu", find, xid);
    return;
}

/* Put converted sysdict results from transaction into sysdicthis */
void transcache_sysdict2his(txn* txn)
{
    ListCell*         lc = NULL;
    txn_sysdict*      sysdict = NULL;
    catalogdata*      catalogdata = NULL;
    txnstmt*          stmt = NULL;
    bool              first_foreach = true;
    txnstmt_metadata* metadata = NULL;

    if (NULL == txn || NULL == txn->sysdict)
    {
        return;
    }

    /* Add stmt as system catalog segment identifier */
    stmt = rmalloc1(sizeof(txnstmt));
    rmemset0(stmt, 0, 0, sizeof(txnstmt));
    metadata = rmalloc1(sizeof(txnstmt_metadata));
    rmemset0(metadata, 0, 0, sizeof(txnstmt_metadata));

    stmt->type = TXNSTMT_TYPE_METADATA;

    foreach (lc, txn->sysdict)
    {
        sysdict = (txn_sysdict*)lfirst(lc);

        catalogdata = catalog_colvalued2catalog(g_idbtype, g_idbversion, sysdict->colvalues);
        if (NULL == catalogdata)
        {
            continue;
        }

        /* Set transaction startlsn, used for filtering when flushing dictionary table */
        catalogdata->lsn.wal.lsn = txn->start.wal.lsn;

        txn->sysdictHis = lappend(txn->sysdictHis, catalogdata);

        /* Execute on first time, when the tail of the list is the start */
        if (first_foreach)
        {
            first_foreach = false;
            metadata->begin = list_tail(txn->sysdictHis);
        }
    }
    if (false == first_foreach)
    {
        /* After linked list assembly complete, when the tail of the list is the end */
        metadata->end = list_tail(txn->sysdictHis);
        stmt->stmt = (void*)metadata;
        txn->stmts = lappend(txn->stmts, stmt);
    }
    else
    {
        rfree(metadata);
        rfree(stmt);
    }
}

/* Update parsing node lsn information * */
bool transcache_refreshlsn(void* in_ctx, txn* txn)
{
    decodingcontext* ctx = NULL;

    if (NULL == txn || NULL == in_ctx)
    {
        elog(RLOG_WARNING, "ctx or txn is null");
        return false;
    }

    ctx = (decodingcontext*)in_ctx;

    if (TXN_CHECK_TRANS_INHASH(txn->flag))
    {
        if (NULL == txn->prev)
        {
            /* Set new restartlsn */

            if (txn->start.wal.lsn > ctx->base.restartlsn)
            {
                ctx->base.restartlsn = txn->start.wal.lsn;
                txn->restart.wal.lsn = txn->start.wal.lsn;
                elog(RLOG_DEBUG, "restartlsn update by:xid:%lu, %X/%X", txn->xid,
                     (uint32)(txn->start.wal.lsn >> 32), (uint32)(txn->start.wal.lsn));
            }

            if (NULL == txn->next)
            {
                if (txn->end.wal.lsn > ctx->base.restartlsn)
                {
                    ctx->base.restartlsn = txn->end.wal.lsn;
                }
            }
        }
    }
    else
    {
        if (NULL == ctx->trans_cache->transdlist->head)
        {
            ctx->base.restartlsn = txn->end.wal.lsn;
        }

        ctx->base.confirmedlsn = txn->end.wal.lsn;
    }

    if (txn->end.wal.lsn > ctx->base.confirmedlsn)
    {
        ctx->base.confirmedlsn = txn->end.wal.lsn;
    }

    fpwcache_calcredolsnbyrestartlsn(ctx->trans_cache, ctx->base.restartlsn, &(ctx->base.redolsn));

    txn->restart.wal.lsn = ctx->base.restartlsn;
    txn->confirm.wal.lsn = ctx->base.confirmedlsn;
    txn->redo.wal.lsn = ctx->base.redolsn;
    if (NULL != ctx->callback.setmetricsynclsn)
    {
        ctx->callback.setmetricsynclsn(ctx->privdata, ctx->base.redolsn, ctx->base.restartlsn,
                                       ctx->base.confirmedlsn);
    }

    return true;
}

/* Delete txn in decodingcontext */
bool transcache_deletetxn(void* in_ctx, txn* txn)
{
    decodingcontext* ctx = NULL;

    if (NULL == txn || NULL == in_ctx)
    {
        elog(RLOG_WARNING, "ctx or txn is null");
        return false;
    }

    ctx = (decodingcontext*)in_ctx;

    /* Remove transaction from ctx->trans_cache->transdlist */
    if (NULL == txn->prev)
    {
        if (NULL == txn->next)
        {
            ctx->trans_cache->transdlist->head = NULL;
            ctx->trans_cache->transdlist->tail = NULL;
        }
        else
        {
            ctx->trans_cache->transdlist->head = txn->next;
            txn->next->prev = NULL;
        }
    }
    else
    {
        if (NULL == txn->next)
        {
            ctx->trans_cache->transdlist->tail = txn->prev;
            txn->prev->next = NULL;
        }
        else
        {
            txn->prev->next = txn->next;
            txn->next->prev = txn->prev;
        }
    }
    txn->prev = NULL;
    txn->next = NULL;

    /* Remove transaction from hash */
    transcache_removeTXNByXid(ctx->trans_cache, txn->xid);

    return true;
}

void transcache_sysdict_free(txn* txn)
{
    ListCell* cell = NULL;
    List*     sysdict_List = txn->sysdict;

    if (!sysdict_List)
    {
        return;
    }

    foreach (cell, sysdict_List)
    {
        txn_sysdict* dict = (txn_sysdict*)lfirst(cell);
        if (dict->convert_colvalues)
        {
            cache_sysdicts_catalogdatafreevoid(dict->convert_colvalues);
        }
        heap_free_trans_result((pg_parser_translog_tbcolbase*)dict->colvalues);
        rfree(dict);
    }
    list_free(sysdict_List);
    txn->sysdict = NULL;
}

/* Delete transcache */
void transcache_free(transcache* transcache)
{
    HASH_SEQ_STATUS status;
    ListCell*       lc = NULL;
    txn*            txn = NULL;
    checkpointnode* chkptnode = NULL;
    if (NULL == transcache)
    {
        return;
    }

    /* Release txn hash table */
    if (NULL != transcache->transdlist)
    {
        for (txn = transcache->transdlist->head; NULL != txn; txn = transcache->transdlist->head)
        {
            transcache->transdlist->head = txn->next;

            txn_free(txn);
        }
        rfree(transcache->transdlist);
    }

    if (NULL != transcache->by_txns)
    {
        hash_destroy(transcache->by_txns);
    }

    /* Delete hash table */
    /* Delete class table */
    if (NULL != transcache->sysdicts)
    {
        /* Clean up relfilenode cache */
        if (NULL != transcache->sysdicts->by_relfilenode)
        {
            hash_destroy(transcache->sysdicts->by_relfilenode);
        }

        if (NULL != transcache->sysdicts->by_class)
        {
            catalog_class_value* catalogclassentry;
            hash_seq_init(&status, transcache->sysdicts->by_class);
            while (NULL != (catalogclassentry = hash_seq_search(&status)))
            {
                if (NULL != catalogclassentry->class)
                {
                    rfree(catalogclassentry->class);
                }
            }

            hash_destroy(transcache->sysdicts->by_class);
        }

        /* Delete attributes table */
        if (NULL != transcache->sysdicts->by_attribute)
        {
            catalog_attribute_value* catalogattrentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_attribute);
            while (NULL != (catalogattrentry = hash_seq_search(&status)))
            {
                if (NULL != catalogattrentry->attrs)
                {
                    foreach (lc, catalogattrentry->attrs)
                    {
                        rfree(lfirst(lc));
                    }
                    list_free(catalogattrentry->attrs);
                }
            }

            hash_destroy(transcache->sysdicts->by_attribute);
        }

        /* Delete type table */
        if (NULL != transcache->sysdicts->by_type)
        {
            catalog_type_value* catalogtypeentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_type);
            while (NULL != (catalogtypeentry = hash_seq_search(&status)))
            {
                if (NULL != catalogtypeentry->type)
                {
                    rfree(catalogtypeentry->type);
                }
            }

            hash_destroy(transcache->sysdicts->by_type);
        }

        /* Delete proc table */
        if (NULL != transcache->sysdicts->by_proc)
        {
            catalog_proc_value* catalogprocentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_proc);
            while (NULL != (catalogprocentry = hash_seq_search(&status)))
            {
                if (NULL != catalogprocentry->proc)
                {
                    rfree(catalogprocentry->proc);
                }
            }

            hash_destroy(transcache->sysdicts->by_proc);
        }

        /* Delete tablespace table */
        if (NULL != transcache->sysdicts->by_tablespace)
        {
            /* tablespace table is not used in current program */
            hash_destroy(transcache->sysdicts->by_tablespace);
        }

        /* Delete namespace table */
        if (NULL != transcache->sysdicts->by_namespace)
        {
            catalog_namespace_value* catalognamespaceentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_namespace);
            while (NULL != (catalognamespaceentry = hash_seq_search(&status)))
            {
                if (NULL != catalognamespaceentry->namespace)
                {
                    rfree(catalognamespaceentry->namespace);
                }
            }
            hash_destroy(transcache->sysdicts->by_namespace);
        }

        /* Delete range table */
        if (NULL != transcache->sysdicts->by_range)
        {
            catalog_range_value* catalograngeentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_range);
            while (NULL != (catalograngeentry = hash_seq_search(&status)))
            {
                if (NULL != catalograngeentry->range)
                {
                    rfree(catalograngeentry->range);
                }
            }
            hash_destroy(transcache->sysdicts->by_range);
        }

        /* Delete enum table */
        if (NULL != transcache->sysdicts->by_enum)
        {
            catalog_enum_value* catalogenumentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_enum);
            while (NULL != (catalogenumentry = hash_seq_search(&status)))
            {
                if (NULL != catalogenumentry->enums)
                {
                    foreach (lc, catalogenumentry->enums)
                    {
                        rfree(lfirst(lc));
                    }
                    list_free(catalogenumentry->enums);
                }
            }

            hash_destroy(transcache->sysdicts->by_enum);
        }

        /* Delete operator table */
        if (NULL != transcache->sysdicts->by_operator)
        {
            catalog_operator_value* catalogoperatorentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_operator);
            while (NULL != (catalogoperatorentry = hash_seq_search(&status)))
            {
                if (NULL != catalogoperatorentry->operator)
                {
                    rfree(catalogoperatorentry->operator);
                }
            }

            hash_destroy(transcache->sysdicts->by_operator);
        }

        /* by_authid */
        if (NULL != transcache->sysdicts->by_authid)
        {
            catalog_authid_value* catalogauthidentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_authid);
            while (NULL != (catalogauthidentry = hash_seq_search(&status)))
            {
                if (NULL != catalogauthidentry->authid)
                {
                    rfree(catalogauthidentry->authid);
                }
            }

            hash_destroy(transcache->sysdicts->by_authid);
        }

        if (NULL != transcache->sysdicts->by_constraint)
        {
            catalog_constraint_value* catalogconentry;
            hash_seq_init(&status, transcache->sysdicts->by_constraint);
            while (NULL != (catalogconentry = hash_seq_search(&status)))
            {
                if (NULL != catalogconentry->constraint)
                {
                    if (0 != catalogconentry->constraint->conkeycnt)
                    {
                        rfree(catalogconentry->constraint->conkey);
                    }
                    rfree(catalogconentry->constraint);
                }
            }

            hash_destroy(transcache->sysdicts->by_constraint);
        }

        /*by_database*/
        if (NULL != transcache->sysdicts->by_database)
        {
            catalog_database_value* catalogdatabaseentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_database);
            while (NULL != (catalogdatabaseentry = hash_seq_search(&status)))
            {
                if (NULL != catalogdatabaseentry->database)
                {
                    rfree(catalogdatabaseentry->database);
                }
            }

            hash_destroy(transcache->sysdicts->by_database);
        }

        /* by_datname2oid */
        if (NULL != transcache->sysdicts->by_datname2oid)
        {
            hash_destroy(transcache->sysdicts->by_datname2oid);
            transcache->sysdicts->by_datname2oid = NULL;
        }

        /* by_index */
        if (NULL != transcache->sysdicts->by_index)
        {
            catalog_index_value*      index = NULL;
            catalog_index_hash_entry* catalogindexentry = NULL;
            hash_seq_init(&status, transcache->sysdicts->by_index);
            while (NULL != (catalogindexentry = hash_seq_search(&status)))
            {
                if (NULL != catalogindexentry->index_list)
                {
                    foreach (lc, catalogindexentry->index_list)
                    {
                        index = (catalog_index_value*)lfirst(lc);
                        if (index->index)
                        {
                            if (index->index->indkey)
                            {
                                rfree(index->index->indkey);
                            }
                            rfree(index->index);
                        }
                        rfree(index);
                    }
                    list_free(catalogindexentry->index_list);
                }
            }
            hash_destroy(transcache->sysdicts->by_index);
        }

        rfree(transcache->sysdicts);
        transcache->sysdicts = NULL;
    }

    if (NULL != transcache->addtablepattern)
    {
        filter_dataset_listpairsfree(transcache->addtablepattern);
    }

    if (NULL != transcache->tableexcludes)
    {
        filter_dataset_listpairsfree(transcache->tableexcludes);
    }

    if (NULL != transcache->tableincludes)
    {
        filter_dataset_listpairsfree(transcache->tableincludes);
    }

    /* Clean up fpw cache */
    if (NULL != transcache->by_fpwtuples)
    {
        ReorderBufferFPWEntry* fpwentry = NULL;
        hash_seq_init(&status, transcache->by_fpwtuples);
        while (NULL != (fpwentry = hash_seq_search(&status)))
        {
            if (NULL != fpwentry->data)
            {
                rfree(fpwentry->data);
            }
        }

        hash_destroy(transcache->by_fpwtuples);
    }

    if (NULL != transcache->fpwtupleslist)
    {
        list_free_deep(transcache->fpwtupleslist);
    }

    if (NULL != transcache->hsyncdataset)
    {
        hash_destroy(transcache->hsyncdataset);
        transcache->hsyncdataset = NULL;
    }

    if (NULL != transcache->htxnfilterdataset)
    {
        hash_destroy(transcache->htxnfilterdataset);
        transcache->htxnfilterdataset = NULL;
    }

    /* Release chkpt */
    if (NULL != transcache->chkpts)
    {
        for (chkptnode = transcache->chkpts->head; NULL != chkptnode;
             chkptnode = transcache->chkpts->head)
        {
            transcache->chkpts->head = chkptnode->next;
            rfree(chkptnode);
            chkptnode = NULL;
        }

        rfree(transcache->chkpts);
        transcache->chkpts = NULL;
    }
}

/* Get database identifier */
Oid transcache_getdboid(void* in_transcache)
{
    transcache* trans_cache = NULL;

    trans_cache = (transcache*)in_transcache;
    return database_getdbid(trans_cache->sysdicts->by_database);
}

/* Get database name */
char* transcache_getdbname(Oid dbid, void* in_transcache)
{
    transcache* trans_cache = NULL;
    trans_cache = (transcache*)in_transcache;
    return database_getdbname(dbid, trans_cache->sysdicts->by_database);
}

/* Get namespace data */
void* transcache_getnamespace(Oid oid, void* in_transcache)
{
    transcache* trans_cache = NULL;
    trans_cache = (transcache*)in_transcache;
    return namespace_getbyoid(oid, trans_cache->sysdicts->by_namespace);
}

/* Get class data */
void* transcache_getclass(Oid oid, void* in_transcache)
{
    transcache* trans_cache = NULL;
    trans_cache = (transcache*)in_transcache;
    return class_getbyoid(oid, trans_cache->sysdicts->by_class);
}

/* Get attribute data */
void* transcache_getattributes(Oid oid, void* in_transcache)
{
    transcache* trans_cache = NULL;
    trans_cache = (transcache*)in_transcache;
    return attribute_getbyoid(oid, trans_cache->sysdicts->by_attribute);
}

/* Get index list */
void* transcache_getindex(Oid oid, void* in_transcache)
{
    transcache* trans_cache = NULL;
    trans_cache = (transcache*)in_transcache;
    return index_getbyoid(oid, trans_cache->sysdicts->by_index);
}

/* Get type data */
void* transcache_gettype(Oid oid, void* in_transcache)
{
    transcache* trans_cache = NULL;
    trans_cache = (transcache*)in_transcache;
    return type_getbyoid(oid, trans_cache->sysdicts->by_type);
}
