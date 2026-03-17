#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/guc/guc.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_errnodef.h"
#include "common/pg_parser_translog.h"
#include "misc/misc_control.h"
#include "snapshot/snapshot.h"
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
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_heap.h"
#include "works/parserwork/wal/decode_xact.h"
#include "works/parserwork/wal/decode_relmap.h"
#include "works/parserwork/wal/decode_checkpoint.h"
#include "works/parserwork/wal/decode_seq.h"

typedef void (*rewind_ptr_prefunc)(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase);

typedef struct REWIND_PREMGR
{
    int                 type;
    char*               name;
    rewind_ptr_prefunc      func;
} rewind_ptr_premgr;

static void rewind_ptr_find_checkpoint(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase);

static rewind_ptr_premgr m_rewind_ptrpremgr[] =
{
    { PG_PARSER_TRANSLOG_INVALID,               "INVALID"        , NULL },
    { PG_PARSER_TRANSLOG_HEAP_INSERT,           "INSERT"         , NULL },
    { PG_PARSER_TRANSLOG_HEAP_UPDATE,           "UPDATE"         , NULL },
    { PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE,       "HOT UPDATE"     , NULL },
    { PG_PARSER_TRANSLOG_HEAP_DELETE,           "DELETE"         , NULL },
    { PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT,    "MULTI INSERT"   , NULL },
    { PG_PARSER_TRANSLOG_XACT_COMMIT,           "COMMIT"         , NULL },
    { PG_PARSER_TRANSLOG_XACT_ABORT,            "ABORT"          , NULL },
    { PG_PARSER_TRANSLOG_XLOG_SWITCH,           "SWITCH"         , NULL },
    { PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE,       "ONLINE"         , rewind_ptr_find_checkpoint },
    { PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN,     "SHUTDOWN"       , rewind_ptr_find_checkpoint },
    { PG_PARSER_TRANSLOG_FPW_TUPLE,             "FPW_TUPLE"      , NULL },
    { PG_PARSER_TRANSLOG_RELMAP,                "RELMAP"         , NULL },
    { PG_PARSER_TRANSLOG_RUNNING_XACTS,         "RUNNING_XACTS"  , NULL },
    { PG_PARSER_TRANSLOG_XLOG_RECOVERY,         "RECOVERY"       , NULL },
    { PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE,   "COMMIT_PREPARE" , NULL },
    { PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE,    "ABORT_PREPARE"  , NULL },
    { PG_PARSER_TRANSLOG_XACT_ASSIGNMENT,       "ASSIGNMENT"     , NULL },
    { PG_PARSER_TRANSLOG_XACT_PREPARE,          "PREPARE"        , NULL },
    { PG_PARSER_TRANSLOG_HEAP_TRUNCATE,         "TRUNCATE"       , NULL },
    {PG_PARSER_TRANSLOG_SEQ,                    "SEQUENCE"       , NULL }
};

static rewind_ptr_premgr m_emitpremgr[] =
{
    { PG_PARSER_TRANSLOG_INVALID,               "INVALID"        , NULL },
    { PG_PARSER_TRANSLOG_HEAP_INSERT,           "INSERT"         , decode_heap_emit },
    { PG_PARSER_TRANSLOG_HEAP_UPDATE,           "UPDATE"         , decode_heap_emit },
    { PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE,       "HOT UPDATE"     , decode_heap_emit },
    { PG_PARSER_TRANSLOG_HEAP_DELETE,           "DELETE"         , decode_heap_emit },
    { PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT,    "MULTI INSERT"   , decode_heap_emit },
    { PG_PARSER_TRANSLOG_XACT_COMMIT,           "COMMIT"         , decode_xact_commit_emit },
    { PG_PARSER_TRANSLOG_XACT_ABORT,            "ABORT"          , decode_xact_abort_emit },
    { PG_PARSER_TRANSLOG_XLOG_SWITCH,           "SWITCH"         , NULL },
    { PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE,       "ONLINE"         , decode_chkpt },
    { PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN,     "SHUTDOWN"       , decode_chkpt },
    { PG_PARSER_TRANSLOG_FPW_TUPLE,             "FPW_TUPLE"      , heap_fpw_tuples },
    { PG_PARSER_TRANSLOG_RELMAP,                "RELMAP"         , decode_relmap },
    { PG_PARSER_TRANSLOG_RUNNING_XACTS,         "RUNNING_XACTS"  , NULL },
    { PG_PARSER_TRANSLOG_XLOG_RECOVERY,         "RECOVERY"       , NULL },
    { PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE,   "COMMIT_PREPARE" , NULL },
    { PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE,    "ABORT_PREPARE"  , NULL },
    { PG_PARSER_TRANSLOG_XACT_ASSIGNMENT,       "ASSIGNMENT"     , NULL },
    { PG_PARSER_TRANSLOG_XACT_PREPARE,          "PREPARE"        , NULL },
    { PG_PARSER_TRANSLOG_HEAP_TRUNCATE,         "TRUNCATE"       , NULL },
    {PG_PARSER_TRANSLOG_SEQ,                    "SEQUENCE"       , NULL }
};

static int              m_precnt = (sizeof(m_rewind_ptrpremgr))/(sizeof(rewind_ptr_premgr));

#define EpochFromFullTransactionId(x)   ((uint32) ((x) >> 32))
#define XidFromFullTransactionId(x)     ((uint32) (x))

static void rewind_ptr_find_checkpoint(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase)
{
    pg_parser_translog_pre_transchkp *ckp = (pg_parser_translog_pre_transchkp *)pbase;
    if (XidFromFullTransactionId(ckp->m_nextid) <= decodingctx->rewind_ptr->strategy.xmin)
    {
        decodingctx->rewind_ptr->redolsn = ckp->m_redo_lsn;
        rewind_stat_setrewinding(decodingctx->rewind_ptr);
    }
}

bool rewind_fastrewind(decodingcontext *decodingctx)
{
    int32 rippleerrno = 0;
    pg_parser_translog_pre_base* preparserresutl = NULL;

    decodingctx->walpre.m_record = decodingctx->decode_record->data;

    /* 调用预解析，根据预解析内容，分发处理 */
    if(false == pg_parser_trans_preTrans(&decodingctx->walpre, &preparserresutl, &rippleerrno))
    {
        elog(RLOG_ERROR, "pg_parser_trans_preTrans error, %08X, %s",
                            rippleerrno, pg_parser_errno_getErrInfo(rippleerrno));
        return false;
    }

    /* 调用分发函数 */
    if(m_precnt <= preparserresutl->m_type)
    {
        elog(RLOG_ERROR, "pg_parser_trans_preTrans unknown type:%u", preparserresutl->m_type);
        return false;
    }

    if(NULL == m_rewind_ptrpremgr[preparserresutl->m_type].func)
    {
        /* 释放无用的pre */
        pg_parser_trans_preTrans_free(preparserresutl);
        return true;
    }

    m_rewind_ptrpremgr[preparserresutl->m_type].func(decodingctx, preparserresutl);
    pg_parser_trans_preTrans_free(preparserresutl);
    return true;
}

bool rewind_fastrewind_emit(decodingcontext *decodingctx)
{
    int32 rippleerrno = 0;
    pg_parser_translog_pre_base* preparserresutl = NULL;

    decodingctx->walpre.m_record = decodingctx->decode_record->data;

    /* 调用预解析，根据预解析内容，分发处理 */
    if(false == pg_parser_trans_preTrans(&decodingctx->walpre, &preparserresutl, &rippleerrno))
    {
        elog(RLOG_ERROR, "pg_parser_trans_preTrans error, %08X, %s",
                            rippleerrno, pg_parser_errno_getErrInfo(rippleerrno));
        return false;
    }

    /* 调用分发函数 */
    if(m_precnt <= preparserresutl->m_type)
    {
        elog(RLOG_ERROR, "pg_parser_trans_preTrans unknown type:%u", preparserresutl->m_type);
        return false;
    }

    if(NULL == m_emitpremgr[preparserresutl->m_type].func)
    {
        /* 释放无用的pre */
        pg_parser_trans_preTrans_free(preparserresutl);
        return true;
    }

    m_emitpremgr[preparserresutl->m_type].func(decodingctx, preparserresutl);
    pg_parser_trans_preTrans_free(preparserresutl);
    return true;
}


void rewind_strategy_setfastrewind(snapshot* snapshot, decodingcontext* decoingctx)
{
    rewind_info *rewind_ptr = NULL;

    if (!decoingctx->rewind_ptr)
    {
        decoingctx->rewind_ptr = rmalloc0(sizeof(rewind_info));
        rmemset0(decoingctx->rewind_ptr, 0, 0, sizeof(rewind_info));
    }
    rewind_ptr = decoingctx->rewind_ptr;
    rewind_ptr->stat = REWIND_INIT;
    rewind_ptr->strategy.xmin = snapshot->xmin;
    rewind_ptr->strategy.xmax = snapshot->xmax;
    rewind_ptr->strategy.xips = snapshot->xids;

    decoingctx->stat = DECODINGCONTEXT_REWIND;
}

void rewind_stat_setsearchcheckpoint(rewind_info* rewind_ptr)
{
    if (rewind_ptr == NULL)
    {
        elog(RLOG_ERROR, "rewind_ptr ptr is NULL");
    }
    rewind_ptr->stat = REWIND_SEARCHCHECKPOINT;
}

void rewind_stat_setrewinding(rewind_info* rewind_ptr)
{
    if (rewind_ptr == NULL)
    {
        elog(RLOG_ERROR, "rewind_ptr ptr is NULL");
    }
    rewind_ptr->stat = REWIND_REWINDING;
    g_xsynchstat = XSYNCHSTAT_REWINDING;
}

void rewind_stat_setemiting(rewind_info* rewind_ptr)
{
    if (rewind_ptr == NULL)
    {
        elog(RLOG_ERROR, "rewind_ptr ptr is NULL");
    }
    rewind_ptr->stat = REWIND_EMITING;
}

void rewind_stat_setemited(rewind_info* rewind_ptr)
{
    if (rewind_ptr == NULL)
    {
        elog(RLOG_ERROR, "rewind_ptr ptr is NULL");
    }

    rewind_ptr->stat = REWIND_EMITED;
    misc_controldata_stat_setrunning();
    g_xsynchstat = XSYNCHSTAT_RUNNING;

    misc_controldata_flush();
}

/* 当处于rewind_ptr时, 只有处于以下两种状态时可以从cache中获取record */
bool rewind_check_stat_allow_get_entry(rewind_info* rewind_ptr)
{
    bool result = false;
    if (rewind_ptr == NULL)
    {
        elog(RLOG_ERROR, "rewind_ptr ptr is NULL");
    }
    result = rewind_ptr->stat == REWIND_SEARCHCHECKPOINT
            || rewind_ptr->stat == REWIND_EMITING
            || rewind_ptr->stat == REWIND_INIT;
    return result;
}
