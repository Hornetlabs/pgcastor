#include "app_incl.h"
#include "port/thread/thread.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "threads/threads.h"
#include "queue/queue.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "storage/file_buffer.h"
#include "storage/ff_detail.h"
#include "storage/smgr.h"
#include "storage/ffsmgr.h"
#include "cache/cache_sysidcts.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/transcache.h"
#include "serial/serial.h"
#include "bigtransaction/persist/bigtxn_persist.h"
#include "bigtransaction/bigtxn.h"
#include "bigtransaction/capture/serial/bigtxn_captureserial.h"
#include "misc/misc_control.h"
#include "works/parserwork/wal/decode_heap_util.h"
#include "catalog/catalog.h"
#include "stmts/txnstmt.h"

/*---------------------------callback begin----------------------------------*/

/* Get database name, first search cache within transaction, then search global cache */
static char* bigtxn_captureserial_getdbname(void* serial, Oid oid)
{
    bigtxn_captureserial*         cserial = NULL;
    pg_parser_sysdict_pgdatabase* database = NULL;

    cserial = (bigtxn_captureserial*)serial;

    database = catalog_get_database_sysdict(cserial->dicts->by_database, NULL, cserial->lasttxn->txndicts, oid);
    if (!database)
    {
        elog(RLOG_ERROR, "can't find database by oid: %u", oid);
    }
    return database->datname.data;
}

/* Get namespace by oid */
static void* bigtxn_captureserial_getnamespace(void* serial, Oid oid)
{
    /*
     * First get from current transaction, if not found in current transaction, get from dicts
     */
    bigtxn_captureserial* cserial = NULL;
    void*                 namespace = NULL;

    cserial = (bigtxn_captureserial*)serial;

    namespace = catalog_get_namespace_sysdict(cserial->dicts->by_namespace, NULL, cserial->lasttxn->txndicts, oid);

    if (!namespace)
    {
        elog(RLOG_ERROR, "can't find namespace by oid: %u", oid);
    }

    return namespace;
}

/* Get class (table) by oid */
static void* bigtxn_captureserial_getclass(void* serial, Oid oid)
{
    /*
     * First get from current transaction, if not found in current transaction, get from dicts
     */

    bigtxn_captureserial* cserial = NULL;
    void*                 class = NULL;

    cserial = (bigtxn_captureserial*)serial;

    class = catalog_get_class_sysdict(cserial->dicts->by_class, NULL, cserial->lasttxn->txndicts, oid);

    if (!class)
    {
        elog(RLOG_ERROR, "can't find class by oid: %u", oid);
    }

    return class;
}

/* Get index information by oid for big transaction, return as linked list */
static void* bigtxn_captureserial_getindex(void* serial, Oid oid)
{
    bigtxn_captureserial* cserial = NULL;
    void*                 index = NULL;

    cserial = (bigtxn_captureserial*)serial;

    index = catalog_get_index_sysdict_list(cserial->dicts->by_index, NULL, cserial->lasttxn->txndicts, oid);

    return index;
}

/* Get column attributes by oid */
static void* bigtxn_captureserial_getatrrs(void* serial, Oid oid)
{
    /*
     * To ensure accuracy, his has higher priority than global cache, returned list needs to be
     * freed after use
     */
    bigtxn_captureserial*      cserial = NULL;
    pg_parser_sysdict_pgclass* class = NULL;
    int                        index_attrs = 0;
    int                        natts = 0;
    List*                      result = NULL;

    cserial = (bigtxn_captureserial*)serial;

    /* Search for pg_class */
    class = bigtxn_captureserial_getclass(serial, oid);
    natts = class->relnatts;

    for (index_attrs = 0; index_attrs < natts; index_attrs++)
    {
        void* temp_att = NULL;
        temp_att = catalog_get_attribute_sysdict(cserial->dicts->by_attribute,
                                                 NULL,
                                                 cserial->lasttxn->txndicts,
                                                 oid,
                                                 index_attrs + 1);
        if (!temp_att)
        {
            elog(RLOG_ERROR, "can't find pg_attribute relation");
        }
        result = lappend(result, temp_att);
    }

    return result;
}

/* Get type by oid */
static void* bigtxn_captureserial_gettype(void* serial, Oid oid)
{
    /*
     * First get from current transaction, if not found in current transaction, get from dicts
     */

    bigtxn_captureserial* cserial = NULL;
    void*                 type = NULL;

    cserial = (bigtxn_captureserial*)serial;

    type = catalog_get_type_sysdict(cserial->dicts->by_type, NULL, cserial->lasttxn->txndicts, oid);

    if (!type)
    {
        elog(RLOG_ERROR, "can't find type by oid: %u", oid);
    }

    return type;
}

/* Apply txnmetadata system catalog */
static void bigtxn_captureserial_transcatalog2transcache(void* serial, void* catalog)
{
    /* Maintain the intra-transaction sysdict linked list for big transactions here */
    List*                 dict = NULL;
    catalogdata*          catalog_data = NULL;
    bigtxn_captureserial* cserial = NULL;

    cserial = (bigtxn_captureserial*)serial;
    catalog_data = (catalogdata*)lfirst((ListCell*)catalog);
    dict = cserial->lasttxn->txndicts;

    /* Copy system catalog */
    dict = lappend(dict, catalog_copy(catalog_data));

    cserial->lasttxn->txndicts = dict;
}

/* Apply sysdicthis system catalog */
static void bigtxn_captureserial_sysdicthis2sysdict(bigtxn_captureserial* cserial, List* his)
{
    if (NULL == his)
    {
        return;
    }

    /* Can be simply wrapped once, internally only uses sysdict and relfilenode, and relfilenode has
     * null value check */
    cache_sysdicts_txnsysdicthis2cache(cserial->dicts, his);
}

/*---------------------------callback end -----------------------------------*/

/* Initialize */
bigtxn_captureserial* bigtxn_captureserial_init(void)
{
    int                   mbytes = 0;
    uint64                bytes = 0;
    bigtxn_captureserial* cserial = NULL;
    HASHCTL               hash_ctl;

    cserial = (bigtxn_captureserial*)rmalloc0(sizeof(bigtxn_captureserial));
    if (NULL == cserial)
    {
        elog(RLOG_ERROR, "big transaction capture serial out of memory, %s", strerror(errno));
    }
    rmemset0(cserial, 0, '\0', sizeof(bigtxn_captureserial));
    cserial->lasttxn = NULL;
    cserial->bigtxn2serial = cache_txn_init();
    cserial->by_txns = NULL;
    cserial->dicts = NULL;

    /* Serialization initialization */
    serialstate_init(&cserial->base);

    cserial->base.txn2filebuffer = file_buffer_init();

    /* Create transaction hash */
    rmemset1(&hash_ctl, 0, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(FullTransactionId);
    hash_ctl.entrysize = sizeof(bigtxn);
    cserial->by_txns = hash_create("transaction hash", 8192, &hash_ctl, HASH_ELEM | HASH_BLOBS);

    cserial->base.ffsmgrstate->status = FFSMGR_STATUS_NOP;
    cserial->base.ffsmgrstate->compatibility = guc_getConfigOptionInt(CFG_KEY_COMPATIBILITY);

    /* Convert file size */
    mbytes = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    bytes = MB2BYTE(mbytes);
    cserial->base.ffsmgrstate->maxbufid = (bytes / FILE_BUFFER_SIZE);
    ffsmgr_init(FFSMG_IF_TYPE_TRAIL, cserial->base.ffsmgrstate);
    return cserial;
}

/* Get dboid from system catalog */
static Oid bigtxn_captureserial_getdboid(void* inserial)
{
    return misc_controldata_database_get(inserial);
}

static void bigtxn_captureserial_freeattributes(void* attrs)
{
    List* list = (List*)attrs;

    /* Only free list, don't care about content */
    list_free(list);
}

/* Write entry data to disk */
static void bigtxn_captureserial_txn2disk(serialstate* serialstate, txn* txn)
{
    bool       first = true;
    bool       txnformetadata = true; /* Used to indicate current transaction only contains metadata */
    ListCell*  lc = NULL;
    ff_txndata txndata = {{0}};

    /*
     * Assemble transaction information
     */
    if (NULL == txn->stmts)
    {
        return;
    }

    /* Call formatting interface for formatting processing */
    /* When a transaction only contains metadata, this transaction doesn't need to be flushed to
     * disk */
    foreach (lc, txn->stmts)
    {
        txnstmt* rstmt = (txnstmt*)lfirst(lc);
        rmemset1(&txndata, 0, '\0', sizeof(ff_txndata));
        txndata.data = rstmt;
        rstmt->database = serialstate->database;
        txndata.header.type = FF_DATA_TYPE_TXN;
        txndata.header.transid = txn->xid;

    bigtxn_captureserial_txn2disk_reset:
        if (false == txnformetadata)
        {
            if (1 == list_length(txn->stmts))
            {
                if (TXN_TYPE_BIGTXN_BEGIN == txn->type)
                {
                    first = false;
                    txndata.header.transind = FF_DATA_TRANSIND_START;
                }
                else if (FROZEN_TXNID == txn->xid)
                {
                    txndata.header.transind = (FF_DATA_TRANSIND_START | FF_DATA_TRANSIND_IN);
                }
                else
                {
                    txndata.header.transind = FF_DATA_TRANSIND_IN;
                }
            }
            else
            {
                if (true == first && TXN_TYPE_BIGTXN_BEGIN == txn->type)
                {
                    first = false;
                    txndata.header.transind = FF_DATA_TRANSIND_START;
                }
                else
                {
                    txndata.header.transind = FF_DATA_TRANSIND_IN;
                }
            }
        }
        else
        {
            if (TXNSTMT_TYPE_METADATA == rstmt->type)
            {
                /* metadata is marked as start, so no commit will be generated afterwards */
                txndata.header.transind = FF_DATA_TRANSIND_START;
            }
            else
            {
                txnformetadata = false;
                goto bigtxn_captureserial_txn2disk_reset;
            }
        }
        serialstate->ffsmgrstate->ffsmgr->ffsmgr_serial(FFTRAIL_CXT_TYPE_DATA,
                                                        (void*)&txndata,
                                                        serialstate->ffsmgrstate);
    }
}

/*
 * Put buffer into flush queue, need to set buffer's flag before putting
 */
static void capture_serial_buffer2waitflush(bigtxn_captureserial* cserial, txn* txn)
{
    /*
     * 1. Get new cache
     * 2. Set new cache's identifier information
     * 3. Set old cache's identifier information based on lsn info in wstate
     */
    int          oldflag = 0;
    int          bufid = 0;
    int          timeout = 0;

    ff_fileinfo* finfo = NULL;
    file_buffer* fbuffer = NULL;
    file_buffer* foldbuffer = NULL;
    serialstate* serialstate = NULL;
    serialstate = &cserial->base;

    foldbuffer = file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    if (0 == foldbuffer->start)
    {
        return;
    }
    oldflag = foldbuffer->flag;

    /* Get new buffer cache */
    while (1)
    {
        bufid = file_buffer_get(serialstate->txn2filebuffer, &timeout);
        if (INVALID_BUFFERID == bufid)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            return;
        }
        break;
    }

    fbuffer = file_buffer_getbybufid(serialstate->txn2filebuffer, bufid);
    if (NULL == fbuffer->privdata)
    {
        finfo = (ff_fileinfo*)rmalloc0(sizeof(ff_fileinfo));
        if (NULL == finfo)
        {
            elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
        }
        rmemset0(finfo, 0, '\0', sizeof(ff_fileinfo));
        fbuffer->privdata = (void*)finfo;
    }
    else
    {
        finfo = (ff_fileinfo*)fbuffer->privdata;
    }

    rmemcpy0(fbuffer->data, 0, foldbuffer->data, foldbuffer->start);
    fbuffer->start = foldbuffer->start;

    /* Set other information for new buffer */
    rmemcpy0(finfo, 0, (ff_fileinfo*)foldbuffer->privdata, sizeof(ff_fileinfo));

    /* Set information for old buffer */
    foldbuffer->extra.rewind.fileaddr.trail.fileid = finfo->fileid;
    foldbuffer->extra.rewind.fileaddr.trail.offset = (((finfo->blknum - 1) * FILE_BUFFER_SIZE) + fbuffer->start);
    foldbuffer->extra.rewind.curtlid = cserial->callback.bigtxn_parserstat_curtlid_get(cserial->privdata);
    /* Add old buffer to pending flush queue */
    rmemcpy1(&fbuffer->extra, 0, &foldbuffer->extra, sizeof(file_buffer_extra));
    file_buffer_waitflush_add(serialstate->txn2filebuffer, foldbuffer);

    fbuffer->flag = oldflag;
    serialstate->ffsmgrstate->bufid = bufid;
    return;
}

static void bigtxn_captureserial_set_callback(bigtxn_captureserial* cserial)
{
    cserial->base.ffsmgrstate->callback.getdboid = bigtxn_captureserial_getdboid;
    cserial->base.ffsmgrstate->callback.getdbname = bigtxn_captureserial_getdbname;
    cserial->base.ffsmgrstate->callback.getfilebuffer = serialstate_getfilebuffer;
    cserial->base.ffsmgrstate->callback.getclass = bigtxn_captureserial_getclass;
    cserial->base.ffsmgrstate->callback.getindex = bigtxn_captureserial_getindex;
    cserial->base.ffsmgrstate->callback.getnamespace = bigtxn_captureserial_getnamespace;
    cserial->base.ffsmgrstate->callback.getattributes = bigtxn_captureserial_getatrrs;
    cserial->base.ffsmgrstate->callback.gettype = bigtxn_captureserial_gettype;
    cserial->base.ffsmgrstate->callback.catalog2transcache = bigtxn_captureserial_transcatalog2transcache;
    cserial->base.ffsmgrstate->callback.freeattributes = bigtxn_captureserial_freeattributes;
    cserial->base.ffsmgrstate->callback.setonlinerefreshdataset = NULL;
    cserial->base.ffsmgrstate->callback.setredosysdicts = NULL;
    cserial->base.ffsmgrstate->callback.setdboid = NULL;
    cserial->base.ffsmgrstate->callback.getrecords = NULL;
    cserial->base.ffsmgrstate->callback.getparserstate = NULL;
}

/* Fill ffsmgrstate information */
static void bigtxn_captureserial_initserial(serialstate* serialstate, int serialtype)
{
    int    mbytes = 0;
    uint64 bytes = 0;

    serialstate->ffsmgrstate->status = FFSMGR_STATUS_NOP;
    serialstate->ffsmgrstate->compatibility = guc_getConfigOptionInt(CFG_KEY_COMPATIBILITY);

    /* Convert file size */
    mbytes = guc_getConfigOptionInt(CFG_KEY_TRAIL_MAX_SIZE);
    bytes = MB2BYTE(mbytes);
    serialstate->ffsmgrstate->maxbufid = (bytes / FILE_BUFFER_SIZE);
}

/* Initialize big transaction in hash */
static bool bigtxn_captureserial_initbigtxn(bigtxn_captureserial* cserial, txn* txn)
{
    bool         found = false;
    bigtxn*      htxn = NULL;
    file_buffer* fbuffer = NULL;

    /* Start of big transaction, check if it exists */
    htxn = hash_search(cserial->by_txns, &txn->xid, HASH_ENTER, &found);
    if (true == found)
    {
        /* Need to reset flag */
        elog(RLOG_WARNING, "big transaction capture serial txn already in the hash, %lu", htxn->xid);
        return false;
    }
    htxn->xid = txn->xid;
    if (false == bigtxn_reset(htxn))
    {
        /* Need to reset flag */
        elog(RLOG_WARNING, "big transaction capture reset error, %lu", htxn->xid);
        return false;
    }

    /* Call initialization interface */
    cserial->base.ffsmgrstate->fdata = NULL;
    cserial->base.ffsmgrstate->ffsmgr->ffsmgr_init(FFSMGR_IF_OPTYPE_SERIAL, cserial->base.ffsmgrstate);
    htxn->fdata = cserial->base.ffsmgrstate->fdata;

    /* Reset buffer at start of big transaction, first put old buffer into free queue */
    if (INVALID_BUFFERID != cserial->base.ffsmgrstate->bufid)
    {
        /* Do switch */
        fbuffer = file_buffer_getbybufid(cserial->base.txn2filebuffer, cserial->base.ffsmgrstate->bufid);

        /* Do copy */
        if (NULL != cserial->lasttxn)
        {
            file_buffer_copy(fbuffer, &cserial->lasttxn->fbuffer);
            file_buffer_free(cserial->base.txn2filebuffer, fbuffer);
            cserial->lasttxn = NULL;
        }
    }

    /* Set starting from 0, 0, this function resets ffsmgrstate->bufid */
    serialstate_fbuffer_set(&cserial->base, 0, 0, txn->xid);
    cserial->lasttxn = htxn;
    return true;
}

/* Switch transaction */
static bool bigtxn_captureserial_shiftbigtxn(bigtxn_captureserial* cserial, txn* txn)
{
    bool         found = false;
    int          timeout = 0;
    bigtxn*      htxn = NULL;
    file_buffer* fbuffer = NULL;

    if (NULL != cserial->lasttxn)
    {
        if (txn->xid == cserial->lasttxn->xid)
        {
            htxn = cserial->lasttxn;
            return true;
        }

        /* Preserve previous transaction information */
        fbuffer = file_buffer_getbybufid(cserial->base.txn2filebuffer, cserial->base.ffsmgrstate->bufid);

        /* Do copy */
        file_buffer_copy(fbuffer, &cserial->lasttxn->fbuffer);
        file_buffer_free(cserial->base.txn2filebuffer, fbuffer);
        cserial->lasttxn = NULL;
    }

    /* Search in big transactions */
    htxn = hash_search(cserial->by_txns, &txn->xid, HASH_FIND, &found);
    if (false == found)
    {
        /* Need to reset flag */
        elog(RLOG_WARNING, "big transaction capture serial txn %lu not in the hash", txn->xid);
        return false;
    }

    /*
     * Set as new
     *  Get a new free bufferid
     *  Set this buffer's information
     */
    while (1)
    {
        htxn->fbuffer.bufid = file_buffer_get(cserial->base.txn2filebuffer, &timeout);
        if (INVALID_BUFFERID == htxn->fbuffer.bufid)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                usleep(10000);
                continue;
            }
            elog(RLOG_WARNING, "capture big txn serial get buffer error");
            return false;
        }
        break;
    }

    /* Get new fbuffer */
    fbuffer = file_buffer_getbybufid(cserial->base.txn2filebuffer, htxn->fbuffer.bufid);
    file_buffer_copy(&htxn->fbuffer, fbuffer);
    cserial->base.ffsmgrstate->bufid = htxn->fbuffer.bufid;
    cserial->base.ffsmgrstate->fdata = htxn->fdata;

    /* Save htxn to lasttxn */
    cserial->lasttxn = htxn;

    return true;
}

/* Big transaction end processing */
static bool bigtxn_captureserial_endbigtxn(bigtxn_captureserial* cserial, txn* txn)
{
    int          flag = 0;
    file_buffer* fbuffer = NULL;
    serialstate* serialstate = NULL;
    serialstate = &cserial->base;
    if (TXN_TYPE_BIGTXN_END_COMMIT != txn->type && TXN_TYPE_BIGTXN_END_ABORT != txn->type)
    {
        return true;
    }

    /* Call release interface */
    cserial->base.ffsmgrstate->ffsmgr->ffsmgr_free(FFSMGR_IF_OPTYPE_SERIAL, cserial->base.ffsmgrstate);

    if (TXN_TYPE_BIGTXN_END_COMMIT == txn->type)
    {
        /* Big transaction end, commit, system catalog exchange */
        bigtxn_captureserial_sysdicthis2sysdict(cserial, cserial->lasttxn->txndicts);
    }
    flag = FILE_BUFFER_FLAG_BIGTXNEND;

    /* Get fbuffer */
    fbuffer = file_buffer_getbybufid(serialstate->txn2filebuffer, serialstate->ffsmgrstate->bufid);
    fbuffer->flag |= flag;
    file_buffer_waitflush_add(serialstate->txn2filebuffer, fbuffer);
    bigtxn_clean(cserial->lasttxn);
    hash_search(cserial->by_txns, &txn->xid, HASH_REMOVE, NULL);
    cserial->lasttxn = NULL;
    serialstate->ffsmgrstate->bufid = InvalidFullTransactionId;
    return true;
}

/*
 * Main logic processing function
 */
void* bigtxn_captureserial_main(void* args)
{
    int                   timeout = 0;
    txn*                  txn = NULL;
    bigtxn*               htxn = NULL;
    thrnode*              thr_node = NULL;
    bigtxn_captureserial* cserial = NULL;

    thr_node = (thrnode*)args;
    cserial = (bigtxn_captureserial*)thr_node->data;

    /* Check status */
    if (THRNODE_STAT_STARTING != thr_node->stat)
    {
        elog(RLOG_WARNING, "capture bigtxn serial stat exception, expected state is THRNODE_STAT_STARTING");
        thr_node->stat = THRNODE_STAT_ABORT;
        osal_thread_exit(NULL);
    }

    /* Set to working state */
    thr_node->stat = THRNODE_STAT_WORK;

    /* Load data dictionary */
    cache_sysdictsload((void**)&cserial->dicts);

    /* Set callback functions */
    bigtxn_captureserial_set_callback(cserial);

    /* Serialization content settings */
    bigtxn_captureserial_initserial(&cserial->base, FFSMG_IF_TYPE_TRAIL);

    /* Set main structure used for callback */
    cserial->base.ffsmgrstate->privdata = cserial;

    while (1)
    {
        /*
         * Processing flow
         *  1. Get transaction from queue
         *  2. Determine if transaction is big transaction
         *      2.1 Not big transaction, only apply system catalog to dicts
         *      2.2 Is big transaction, serialize big transaction
         */
        if (THRNODE_STAT_TERM == thr_node->stat)
        {
            /* Serialization/flush to disk */
            thr_node->stat = THRNODE_STAT_EXIT;
            break;
        }

        /* Get data */
        txn = cache_txn_get(cserial->bigtxn2serial, &timeout);
        if (NULL == txn)
        {
            if (ERROR_TIMEOUT == timeout)
            {
                continue;
            }

            elog(RLOG_WARNING, "capture big transaction get txn error");
            thr_node->stat = THRNODE_STAT_ABORT;
            break;
        }

        /* Not big transaction, only apply system catalog data */
        if (!TXN_ISBIGTXN(txn->flag))
        {
            /* Only apply system catalog data */
            if (txn->sysdictHis)
            {
                bigtxn_captureserial_sysdicthis2sysdict(cserial, txn->sysdictHis);
            }

            /* Free txn content */
            txn_free(txn);
            rfree(txn);
            continue;
        }

        /* Big transaction data */
        if (TXN_TYPE_BIGTXN_BEGIN == txn->type)
        {
            if (false == bigtxn_captureserial_initbigtxn(cserial, txn))
            {
                elog(RLOG_WARNING, "capture big txn init big txn error");
                goto bigtxn_captureserial_main_done;
            }
        }
        else
        {
            if (false == bigtxn_captureserial_shiftbigtxn(cserial, txn))
            {
                elog(RLOG_WARNING, "capture big txn shift lasttxn error");
                goto bigtxn_captureserial_main_done;
            }
        }
        htxn = cserial->lasttxn;

        /* Serialize */
        bigtxn_captureserial_txn2disk(&cserial->base, txn);

        /* Serialization complete, force flush */
        capture_serial_buffer2waitflush(cserial, txn);

        /* Save sysdicthis */
        if (htxn->txndicts)
        {
            cache_sysdicts_txnsysdicthisfree(htxn->txndicts);
            list_free(htxn->txndicts);
            htxn->txndicts = NULL;
        }
        htxn->txndicts = txn->sysdictHis;
        txn->sysdictHis = NULL;

        if (false == bigtxn_captureserial_endbigtxn(cserial, txn))
        {
            elog(RLOG_WARNING, "big txn capture serial end big transaction error");
            goto bigtxn_captureserial_main_done;
        }

        /* End, free txn content */
        txn_free(txn);
        rfree(txn);
    }

bigtxn_captureserial_main_done:
    osal_thread_exit(NULL);
    return NULL;
}

/* Resource cleanup */
void bigtxn_captureserial_destroy(void* args)
{
    HASH_SEQ_STATUS       status;
    ListCell*             lc = NULL;
    bigtxn_captureserial* cserial = NULL;

    cserial = (bigtxn_captureserial*)args;
    if (NULL == cserial)
    {
        return;
    }

    file_buffer_destroy(cserial->base.txn2filebuffer);

    cache_txn_destroy(cserial->bigtxn2serial);

    serialstate_destroy((serialstate*)cserial);

    if (NULL != cserial->dicts)
    {
        if (NULL != cserial->dicts->by_class)
        {
            catalog_class_value* catalogclassentry;
            hash_seq_init(&status, cserial->dicts->by_class);
            while (NULL != (catalogclassentry = hash_seq_search(&status)))
            {
                if (NULL != catalogclassentry->class)
                {
                    rfree(catalogclassentry->class);
                }
            }

            hash_destroy(cserial->dicts->by_class);
        }

        /* Delete attributes table */
        if (NULL != cserial->dicts->by_attribute)
        {
            catalog_attribute_value* catalogattrentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_attribute);
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

            hash_destroy(cserial->dicts->by_attribute);
        }

        /* Delete type table */
        if (NULL != cserial->dicts->by_type)
        {
            catalog_type_value* catalogtypeentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_type);
            while (NULL != (catalogtypeentry = hash_seq_search(&status)))
            {
                if (NULL != catalogtypeentry->type)
                {
                    rfree(catalogtypeentry->type);
                }
            }

            hash_destroy(cserial->dicts->by_type);
        }

        /* Delete proc table */
        if (NULL != cserial->dicts->by_proc)
        {
            catalog_proc_value* catalogprocentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_proc);
            while (NULL != (catalogprocentry = hash_seq_search(&status)))
            {
                if (NULL != catalogprocentry->proc)
                {
                    rfree(catalogprocentry->proc);
                }
            }

            hash_destroy(cserial->dicts->by_proc);
        }

        /* Delete tablespace table */
        if (NULL != cserial->dicts->by_tablespace)
        {
            /* tablespace table is not used in current program */
            hash_destroy(cserial->dicts->by_tablespace);
        }

        /* Delete namespace table */
        if (NULL != cserial->dicts->by_namespace)
        {
            catalog_namespace_value* catalognamespaceentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_namespace);
            while (NULL != (catalognamespaceentry = hash_seq_search(&status)))
            {
                if (NULL != catalognamespaceentry->namespace)
                {
                    rfree(catalognamespaceentry->namespace);
                }
            }
            hash_destroy(cserial->dicts->by_namespace);
        }

        /* Delete range table */
        if (NULL != cserial->dicts->by_range)
        {
            catalog_range_value* catalograngeentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_range);
            while (NULL != (catalograngeentry = hash_seq_search(&status)))
            {
                if (NULL != catalograngeentry->range)
                {
                    rfree(catalograngeentry->range);
                }
            }
            hash_destroy(cserial->dicts->by_range);
        }

        /* Delete enum table */
        if (NULL != cserial->dicts->by_enum)
        {
            catalog_enum_value* catalogenumentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_enum);
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

            hash_destroy(cserial->dicts->by_enum);
        }

        /* Delete operator table */
        if (NULL != cserial->dicts->by_operator)
        {
            catalog_operator_value* catalogoperatorentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_operator);
            while (NULL != (catalogoperatorentry = hash_seq_search(&status)))
            {
                if (NULL != catalogoperatorentry->operator)
                {
                    rfree(catalogoperatorentry->operator);
                }
            }

            hash_destroy(cserial->dicts->by_operator);
        }

        /* by_authid */
        if (NULL != cserial->dicts->by_authid)
        {
            catalog_authid_value* catalogauthidentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_authid);
            while (NULL != (catalogauthidentry = hash_seq_search(&status)))
            {
                if (NULL != catalogauthidentry->authid)
                {
                    rfree(catalogauthidentry->authid);
                }
            }

            hash_destroy(cserial->dicts->by_authid);
        }

        if (NULL != cserial->dicts->by_constraint)
        {
            catalog_constraint_value* catalogconentry;
            hash_seq_init(&status, cserial->dicts->by_constraint);
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

            hash_destroy(cserial->dicts->by_constraint);
        }

        /*by_database*/
        if (NULL != cserial->dicts->by_database)
        {
            catalog_database_value* catalogdatabaseentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_database);
            while (NULL != (catalogdatabaseentry = hash_seq_search(&status)))
            {
                if (NULL != catalogdatabaseentry->database)
                {
                    rfree(catalogdatabaseentry->database);
                }
            }

            hash_destroy(cserial->dicts->by_database);
        }

        /* by_datname2oid */
        if (NULL != cserial->dicts->by_datname2oid)
        {
            hash_destroy(cserial->dicts->by_datname2oid);
            cserial->dicts->by_datname2oid = NULL;
        }

        /* by_index */
        if (NULL != cserial->dicts->by_index)
        {
            catalog_index_value*      index = NULL;
            catalog_index_hash_entry* catalogindexentry = NULL;
            hash_seq_init(&status, cserial->dicts->by_index);
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
            hash_destroy(cserial->dicts->by_index);
        }

        rfree(cserial->dicts);
        cserial->dicts = NULL;
    }

    /* Big transaction table deletion */
    if (NULL != cserial->by_txns)
    {
        bigtxn* txnentry = NULL;
        hash_seq_init(&status, cserial->by_txns);
        while (NULL != (txnentry = hash_seq_search(&status)))
        {
            bigtxn_clean(txnentry);
        }
        hash_destroy(cserial->by_txns);
    }

    cserial->privdata = NULL;

    rfree(cserial);
    cserial = NULL;
}
