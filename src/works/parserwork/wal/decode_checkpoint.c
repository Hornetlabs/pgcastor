#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/sinval/sinval.h"
#include "port/thread/thread.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_errnodef.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
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
#include "works/parserwork/wal/decode_xact.h"
#include "works/parserwork/wal/decode_ddl.h"
#include "works/parserwork/wal/decode_relmap.h"
#include "works/parserwork/wal/decode_checkpoint.h"

void decode_chkpt_init(decodingcontext* ctx, XLogRecPtr redolsn)
{
    checkpointnode* chkptnode = NULL;

    /* Add redolsn to linked list */
    chkptnode = (checkpointnode*)rmalloc0(sizeof(checkpointnode));
    if (NULL == chkptnode)
    {
        elog(RLOG_DEBUG, "out of memory, %s", strerror(errno));
    }
    rmemset0(chkptnode, 0, '\0', sizeof(checkpointnode));
    chkptnode->prev = NULL;
    chkptnode->next = NULL;
    chkptnode->xid = 0;
    chkptnode->redolsn = redolsn;

    ctx->trans_cache->chkpts = rmalloc0(sizeof(checkpoints));
    if (NULL == ctx->trans_cache->chkpts)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(ctx->trans_cache->chkpts, 0, '\0', sizeof(checkpoints));
    ctx->trans_cache->chkpts->head = NULL;
    ctx->trans_cache->chkpts->tail = NULL;
    ctx->trans_cache->chkpts->head = chkptnode;
    ctx->trans_cache->chkpts->tail = chkptnode;

    elog(RLOG_INFO, "ripple redolsn from %X/%X", (uint32)(redolsn >> 32), (uint32)redolsn);
}

/* checkpoint commit */
void decode_chkpt(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    txn*                              txn = NULL;
    checkpointnode*                   chkptnode = NULL;
    pg_parser_translog_pre_transchkp* prechkpt = NULL;

    prechkpt = (pg_parser_translog_pre_transchkp*)pbase;

    /* Add redolsn to linked list */
    chkptnode = (checkpointnode*)rmalloc0(sizeof(checkpointnode));
    if (NULL == chkptnode)
    {
        elog(RLOG_DEBUG, "out of memory, %s", strerror(errno));
    }
    rmemset0(chkptnode, 0, '\0', sizeof(checkpointnode));
    chkptnode->prev = NULL;
    chkptnode->next = NULL;
    chkptnode->xid = prechkpt->m_base.m_xid;
    chkptnode->redolsn = prechkpt->m_redo_lsn;

    /* Hook it on */
    if (NULL == ctx->trans_cache->chkpts->head)
    {
        ctx->trans_cache->chkpts->head = chkptnode;
        ctx->trans_cache->chkpts->tail = chkptnode;
    }
    else
    {
        chkptnode->prev = ctx->trans_cache->chkpts->tail;
        ctx->trans_cache->chkpts->tail->next = chkptnode;
        ctx->trans_cache->chkpts->tail = chkptnode;
    }
    elog(RLOG_DEBUG,
         "add checkpoint:%X/%X",
         (uint32)(chkptnode->redolsn >> 32),
         (uint32)(chkptnode->redolsn));

    /* Check if timeline has changed */
    if (prechkpt->m_this_timeline != prechkpt->m_prev_timeline)
    {
        /* Timeline changed, create a transaction with xid = 0 */
        txn = txn_init(InvalidTransactionId,
                       ctx->decode_record->start.wal.lsn,
                       ctx->decode_record->end.wal.lsn);

        /* Set timeline */
        txn->type = TXN_TYPE_TIMELINE;
        txn->curtlid = prechkpt->m_this_timeline;
        ctx->base.curtlid = prechkpt->m_this_timeline;

        /* Add to queue */
        cache_txn_add(ctx->parser2txns, txn);
    }

    if (InvalidTransactionId == chkptnode->xid &&
        ctx->decode_record->start.wal.lsn > ctx->base.confirmedlsn)
    {
        txn = txn_init(InvalidTransactionId,
                       ctx->decode_record->start.wal.lsn,
                       ctx->decode_record->end.wal.lsn);

        /* Update redo/restart/confirm lsn based on transaction startlsn/endlsn */
        transcache_refreshlsn((void*)ctx, txn);

        /* Add to queue */
        cache_txn_add(ctx->parser2txns, txn);
    }
}

void decode_recovery(decodingcontext* ctx, pg_parser_translog_pre_base* pbase)
{
    txn*                                txn = NULL;
    pg_parser_translog_pre_endrecovery* recovery = NULL;

    if (recovery->m_this_timeline != recovery->m_prev_timeline)
    {
        /* Timeline changed, create a transaction with xid = 0 */
        txn = txn_init(InvalidTransactionId,
                       ctx->decode_record->start.wal.lsn,
                       ctx->decode_record->end.wal.lsn);

        /* Set timeline */
        txn->type = TXN_TYPE_TIMELINE;
        txn->curtlid = recovery->m_this_timeline;
        ctx->base.curtlid = recovery->m_this_timeline;

        /* Add to queue */
        cache_txn_add(ctx->parser2txns, txn);
    }
}
