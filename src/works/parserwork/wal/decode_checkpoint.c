#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/sinval/sinval.h"
#include "port/thread/ripple_thread.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "cache/ripple_txn.h"
#include "cache/ripple_cache_txn.h"
#include "cache/ripple_cache_sysidcts.h"
#include "cache/ripple_transcache.h"
#include "utils/mpage/mpage.h"
#include "queue/ripple_queue.h"
#include "loadrecords/ripple_record.h"
#include "loadrecords/ripple_loadpage.h"
#include "loadrecords/ripple_loadpageam.h"
#include "loadrecords/ripple_loadpagefromfile.h"
#include "loadrecords/ripple_loadrecords.h"
#include "loadrecords/ripple_loadwalrecords.h"
#include "snapshot/ripple_snapshot.h"
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_xact.h"
#include "works/parserwork/wal/ripple_decode_ddl.h"
#include "works/parserwork/wal/ripple_decode_relmap.h"
#include "works/parserwork/wal/ripple_decode_checkpoint.h"

void ripple_decode_chkpt_init(ripple_decodingcontext* ctx, XLogRecPtr redolsn)
{
    ripple_checkpointnode* chkptnode = NULL;

    /* 将 redolsn 加入到链表中 */
    chkptnode = (ripple_checkpointnode*)rmalloc0(sizeof(ripple_checkpointnode));
    if(NULL == chkptnode)
    {
        elog(RLOG_DEBUG, "out of memory, %s", strerror(errno));
    }
    rmemset0(chkptnode, 0, '\0', sizeof(ripple_checkpointnode));
    chkptnode->prev = NULL;
    chkptnode->next = NULL;
    chkptnode->xid = 0;
    chkptnode->redolsn = redolsn;

    ctx->transcache->chkpts = rmalloc0(sizeof(ripple_checkpoints));
    if(NULL == ctx->transcache->chkpts)
    {
        elog(RLOG_ERROR, "out of memory, %s", strerror(errno));
    }
    rmemset0(ctx->transcache->chkpts, 0, '\0', sizeof(ripple_checkpoints));
    ctx->transcache->chkpts->head = NULL;
    ctx->transcache->chkpts->tail = NULL;
    ctx->transcache->chkpts->head = chkptnode;
    ctx->transcache->chkpts->tail = chkptnode;

    elog(RLOG_INFO, "ripple redolsn from %X/%X",
                    (uint32)(redolsn>>32), (uint32)redolsn);
}

/* checkpoint 提交 */
void ripple_decode_chkpt(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    ripple_txn *txn = NULL;
    ripple_checkpointnode* chkptnode = NULL;
    xk_pg_parser_translog_pre_transchkp* prechkpt = NULL;

    prechkpt = (xk_pg_parser_translog_pre_transchkp*)pbase;

    /* 将 redolsn 加入到链表中 */
    chkptnode = (ripple_checkpointnode*)rmalloc0(sizeof(ripple_checkpointnode));
    if(NULL == chkptnode)
    {
        elog(RLOG_DEBUG, "out of memory, %s", strerror(errno));
    }
    rmemset0(chkptnode, 0, '\0', sizeof(ripple_checkpointnode));
    chkptnode->prev = NULL;
    chkptnode->next = NULL;
    chkptnode->xid = prechkpt->m_base.m_xid;
    chkptnode->redolsn = prechkpt->m_redo_lsn;

    /* 挂上去 */
    if(NULL == ctx->transcache->chkpts->head)
    {
        ctx->transcache->chkpts->head = chkptnode;
        ctx->transcache->chkpts->tail = chkptnode;
    }
    else
    {
        chkptnode->prev = ctx->transcache->chkpts->tail;
        ctx->transcache->chkpts->tail->next = chkptnode;
        ctx->transcache->chkpts->tail = chkptnode;
    }
    elog(RLOG_DEBUG, "add checkpoint:%X/%X", 
                        (uint32)(chkptnode->redolsn>>32),
                        (uint32)(chkptnode->redolsn));

    /* 检查时间线是否发生变化 */
    if (prechkpt->m_this_timeline != prechkpt->m_prev_timeline)
    {
        /* 时间线发生变化, 创建一个xid = 0的事物 */
        txn = ripple_txn_init(InvalidTransactionId, 
                            ctx->decode_record->start.wal.lsn,
                            ctx->decode_record->end.wal.lsn);

        /* 设置时间线 */
        txn->type = RIPPLE_TXN_TYPE_TIMELINE;
        txn->curtlid = prechkpt->m_this_timeline;
        ctx->base.curtlid = prechkpt->m_this_timeline;

        /* 加入到队列中 */
        ripple_cache_txn_add(ctx->parser2txns, txn);
    }

    if(InvalidTransactionId == chkptnode->xid 
        && ctx->decode_record->start.wal.lsn > ctx->base.confirmedlsn)
    {
        txn = ripple_txn_init(InvalidTransactionId, 
                            ctx->decode_record->start.wal.lsn,
                            ctx->decode_record->end.wal.lsn);

        /* 根据事务startlsn/endlsn更新redo/restart/confirm lsn */
        ripple_transcache_refreshlsn((void*)ctx, txn);

        /* 加入到队列中 */
        ripple_cache_txn_add(ctx->parser2txns, txn);
    }
}

void ripple_decode_recovery(ripple_decodingcontext* ctx, xk_pg_parser_translog_pre_base* pbase)
{
    ripple_txn *txn = NULL;
    xk_pg_parser_translog_pre_endrecovery* recovery = NULL;

    if (recovery->m_this_timeline != recovery->m_prev_timeline)
    {
        /* 时间线发生变化, 创建一个xid = 0的事物 */
        txn = ripple_txn_init(InvalidTransactionId, 
                            ctx->decode_record->start.wal.lsn,
                            ctx->decode_record->end.wal.lsn);

        /* 设置时间线 */
        txn->type = RIPPLE_TXN_TYPE_TIMELINE;
        txn->curtlid = recovery->m_this_timeline;
        ctx->base.curtlid = recovery->m_this_timeline;

        /* 加入到队列中 */
        ripple_cache_txn_add(ctx->parser2txns, txn);
    }
}
