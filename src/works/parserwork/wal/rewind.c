#include "ripple_app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "misc/ripple_misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_errnodef.h"
#include "common/xk_pg_parser_translog.h"
#include "misc/ripple_misc_control.h"
#include "snapshot/ripple_snapshot.h"
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
#include "works/parserwork/wal/ripple_rewind.h"
#include "works/parserwork/wal/ripple_parserwork_decode.h"
#include "works/parserwork/wal/ripple_decode_heap.h"
#include "works/parserwork/wal/ripple_decode_xact.h"
#include "works/parserwork/wal/ripple_decode_relmap.h"
#include "works/parserwork/wal/ripple_decode_checkpoint.h"
#include "works/parserwork/wal/ripple_decode_seq.h"

typedef void (*rewind_prefunc)(ripple_decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);

typedef struct RIPPLE_REWIND_PREMGR
{
    int                 type;
    char*               name;
    rewind_prefunc      func;
} ripple_rewind_premgr;

static void ripple_rewind_find_checkpoint(ripple_decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);

static ripple_rewind_premgr m_rewindpremgr[] =
{
    { XK_PG_PARSER_TRANSLOG_INVALID,               "INVALID"        , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_INSERT,           "INSERT"         , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_UPDATE,           "UPDATE"         , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE,       "HOT UPDATE"     , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_DELETE,           "DELETE"         , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT,    "MULTI INSERT"   , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_COMMIT,           "COMMIT"         , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_ABORT,            "ABORT"          , NULL },
    { XK_PG_PARSER_TRANSLOG_XLOG_SWITCH,           "SWITCH"         , NULL },
    { XK_PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE,       "ONLINE"         , ripple_rewind_find_checkpoint },
    { XK_PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN,     "SHUTDOWN"       , ripple_rewind_find_checkpoint },
    { XK_PG_PARSER_TRANSLOG_FPW_TUPLE,             "FPW_TUPLE"      , NULL },
    { XK_PG_PARSER_TRANSLOG_RELMAP,                "RELMAP"         , NULL },
    { XK_PG_PARSER_TRANSLOG_RUNNING_XACTS,         "RUNNING_XACTS"  , NULL },
    { XK_PG_PARSER_TRANSLOG_XLOG_RECOVERY,         "RECOVERY"       , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE,   "COMMIT_PREPARE" , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE,    "ABORT_PREPARE"  , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_ASSIGNMENT,       "ASSIGNMENT"     , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_PREPARE,          "PREPARE"        , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_TRUNCATE,         "TRUNCATE"       , NULL },
    {XK_PG_PARSER_TRANSLOG_SEQ,                    "SEQUENCE"       , NULL }
};

static ripple_rewind_premgr m_emitpremgr[] =
{
    { XK_PG_PARSER_TRANSLOG_INVALID,               "INVALID"        , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_INSERT,           "INSERT"         , ripple_decode_heap_emit },
    { XK_PG_PARSER_TRANSLOG_HEAP_UPDATE,           "UPDATE"         , ripple_decode_heap_emit },
    { XK_PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE,       "HOT UPDATE"     , ripple_decode_heap_emit },
    { XK_PG_PARSER_TRANSLOG_HEAP_DELETE,           "DELETE"         , ripple_decode_heap_emit },
    { XK_PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT,    "MULTI INSERT"   , ripple_decode_heap_emit },
    { XK_PG_PARSER_TRANSLOG_XACT_COMMIT,           "COMMIT"         , ripple_decode_xact_commit_emit },
    { XK_PG_PARSER_TRANSLOG_XACT_ABORT,            "ABORT"          , ripple_decode_xact_abort_emit },
    { XK_PG_PARSER_TRANSLOG_XLOG_SWITCH,           "SWITCH"         , NULL },
    { XK_PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE,       "ONLINE"         , ripple_decode_chkpt },
    { XK_PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN,     "SHUTDOWN"       , ripple_decode_chkpt },
    { XK_PG_PARSER_TRANSLOG_FPW_TUPLE,             "FPW_TUPLE"      , ripple_heap_fpw_tuples },
    { XK_PG_PARSER_TRANSLOG_RELMAP,                "RELMAP"         , ripple_decode_relmap },
    { XK_PG_PARSER_TRANSLOG_RUNNING_XACTS,         "RUNNING_XACTS"  , NULL },
    { XK_PG_PARSER_TRANSLOG_XLOG_RECOVERY,         "RECOVERY"       , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE,   "COMMIT_PREPARE" , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE,    "ABORT_PREPARE"  , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_ASSIGNMENT,       "ASSIGNMENT"     , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_PREPARE,          "PREPARE"        , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_TRUNCATE,         "TRUNCATE"       , NULL },
    {XK_PG_PARSER_TRANSLOG_SEQ,                    "SEQUENCE"       , NULL }
};

static int              m_precnt = (sizeof(m_rewindpremgr))/(sizeof(ripple_rewind_premgr));

#define ripple_EpochFromFullTransactionId(x)   ((uint32) ((x) >> 32))
#define ripple_XidFromFullTransactionId(x)     ((uint32) (x))

static void ripple_rewind_find_checkpoint(ripple_decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase)
{
    xk_pg_parser_translog_pre_transchkp *ckp = (xk_pg_parser_translog_pre_transchkp *)pbase;
    if (ripple_XidFromFullTransactionId(ckp->m_nextid) <= decodingctx->rewind->strategy.xmin)
    {
        decodingctx->rewind->redolsn = ckp->m_redo_lsn;
        ripple_rewind_stat_setrewinding(decodingctx->rewind);
    }
}

bool ripple_rewind_fastrewind(ripple_decodingcontext *decodingctx)
{
    int32 rippleerrno = 0;
    xk_pg_parser_translog_pre_base* preparserresutl = NULL;

    decodingctx->walpre.m_record = decodingctx->decode_record->data;

    /* 调用预解析，根据预解析内容，分发处理 */
    if(false == xk_pg_parser_trans_preTrans(&decodingctx->walpre, &preparserresutl, &rippleerrno))
    {
        elog(RLOG_ERROR, "xk_pg_parser_trans_preTrans error, %08X, %s",
                            rippleerrno, xk_pg_parser_errno_getErrInfo(rippleerrno));
        return false;
    }

    /* 调用分发函数 */
    if(m_precnt <= preparserresutl->m_type)
    {
        elog(RLOG_ERROR, "xk_pg_parser_trans_preTrans unknown type:%u", preparserresutl->m_type);
        return false;
    }

    if(NULL == m_rewindpremgr[preparserresutl->m_type].func)
    {
        /* 释放无用的pre */
        xk_pg_parser_trans_preTrans_free(preparserresutl);
        return true;
    }

    m_rewindpremgr[preparserresutl->m_type].func(decodingctx, preparserresutl);
    xk_pg_parser_trans_preTrans_free(preparserresutl);
    return true;
}

bool ripple_rewind_fastrewind_emit(ripple_decodingcontext *decodingctx)
{
    int32 rippleerrno = 0;
    xk_pg_parser_translog_pre_base* preparserresutl = NULL;

    decodingctx->walpre.m_record = decodingctx->decode_record->data;

    /* 调用预解析，根据预解析内容，分发处理 */
    if(false == xk_pg_parser_trans_preTrans(&decodingctx->walpre, &preparserresutl, &rippleerrno))
    {
        elog(RLOG_ERROR, "xk_pg_parser_trans_preTrans error, %08X, %s",
                            rippleerrno, xk_pg_parser_errno_getErrInfo(rippleerrno));
        return false;
    }

    /* 调用分发函数 */
    if(m_precnt <= preparserresutl->m_type)
    {
        elog(RLOG_ERROR, "xk_pg_parser_trans_preTrans unknown type:%u", preparserresutl->m_type);
        return false;
    }

    if(NULL == m_emitpremgr[preparserresutl->m_type].func)
    {
        /* 释放无用的pre */
        xk_pg_parser_trans_preTrans_free(preparserresutl);
        return true;
    }

    m_emitpremgr[preparserresutl->m_type].func(decodingctx, preparserresutl);
    xk_pg_parser_trans_preTrans_free(preparserresutl);
    return true;
}


void ripple_rewind_strategy_setfastrewind(ripple_snapshot* snapshot, ripple_decodingcontext* decoingctx)
{
    ripple_rewind *rewind = NULL;

    if (!decoingctx->rewind)
    {
        decoingctx->rewind = rmalloc0(sizeof(ripple_rewind));
        rmemset0(decoingctx->rewind, 0, 0, sizeof(ripple_rewind));
    }

    rewind = decoingctx->rewind;
    rewind->stat = RIPPLE_REWIND_INIT;
    rewind->strategy.xmin = snapshot->xmin;
    rewind->strategy.xmax = snapshot->xmax;
    rewind->strategy.xips = snapshot->xids;

    decoingctx->stat = RIPPLE_DECODINGCONTEXT_REWIND;
}

void ripple_rewind_stat_setsearchcheckpoint(ripple_rewind* rewind)
{
    if (rewind == NULL)
    {
        elog(RLOG_ERROR, "rewind ptr is NULL");
    }
    rewind->stat = RIPPLE_REWIND_SEARCHCHECKPOINT;
}

void ripple_rewind_stat_setrewinding(ripple_rewind* rewind)
{
    if (rewind == NULL)
    {
        elog(RLOG_ERROR, "rewind ptr is NULL");
    }
    rewind->stat = RIPPLE_REWIND_REWINDING;
    g_xsynchstat = RIPPLE_XSYNCHSTAT_REWINDING;
}

void ripple_rewind_stat_setemiting(ripple_rewind* rewind)
{
    if (rewind == NULL)
    {
        elog(RLOG_ERROR, "rewind ptr is NULL");
    }
    rewind->stat = RIPPLE_REWIND_EMITING;
}

void ripple_rewind_stat_setemited(ripple_rewind* rewind)
{
    if (rewind == NULL)
    {
        elog(RLOG_ERROR, "rewind ptr is NULL");
    }

    rewind->stat = RIPPLE_REWIND_EMITED;
    ripple_misc_controldata_stat_setrunning();
    g_xsynchstat = RIPPLE_XSYNCHSTAT_RUNNING;

    ripple_misc_controldata_flush();
}

/* 当处于rewind时, 只有处于以下两种状态时可以从cache中获取record */
bool ripple_rewind_check_stat_allow_get_entry(ripple_rewind* rewind)
{
    bool result = false;
    if (rewind == NULL)
    {
        elog(RLOG_ERROR, "rewind ptr is NULL");
    }
    result = rewind->stat == RIPPLE_REWIND_SEARCHCHECKPOINT
            || rewind->stat == RIPPLE_REWIND_EMITING
            || rewind->stat == RIPPLE_REWIND_INIT;
    return result;
}
