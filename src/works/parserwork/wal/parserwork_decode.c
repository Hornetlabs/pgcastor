#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "misc/misc_stat.h"
#include "common/pg_parser_define.h"
#include "common/pg_parser_translog.h"
#include "common/pg_parser_errnodef.h"
#include "loadrecords/record.h"
#include "queue/queue.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "snapshot/snapshot.h"
#include "works/parserwork/wal/rewind.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_xact.h"
#include "works/parserwork/wal/decode_heap.h"
#include "works/parserwork/wal/decode_relmap.h"
#include "works/parserwork/wal/decode_checkpoint.h"
#include "works/parserwork/wal/decode_seq.h"

typedef void (*decode_prefunc)(decodingcontext* decodingctx, pg_parser_translog_pre_base* pbase);

typedef struct DECODE_PREMGR
{
    int                 type;
    char*               name;
    decode_prefunc      func;
} decode_premgr;

static decode_premgr m_decodepremgr[] =
{
    { PG_PARSER_TRANSLOG_INVALID,               "INVALID"        , NULL },
    { PG_PARSER_TRANSLOG_HEAP_INSERT,           "INSERT"         , decode_heap },
    { PG_PARSER_TRANSLOG_HEAP_UPDATE,           "UPDATE"         , decode_heap },
    { PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE,       "HOT UPDATE"     , decode_heap },
    { PG_PARSER_TRANSLOG_HEAP_DELETE,           "DELETE"         , decode_heap },
    { PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT,    "MULTI INSERT"   , decode_heap },
    { PG_PARSER_TRANSLOG_XACT_COMMIT,           "COMMIT"         , decode_xact_commit },
    { PG_PARSER_TRANSLOG_XACT_ABORT,            "ABORT"          , decode_xact_abort },
    { PG_PARSER_TRANSLOG_XLOG_SWITCH,           "SWITCH"         , NULL },
    { PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE,       "ONLINE"         , decode_chkpt },
    { PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN,     "SHUTDOWN"       , decode_chkpt },
    { PG_PARSER_TRANSLOG_FPW_TUPLE,             "FPW_TUPLE"      , heap_fpw_tuples },
    { PG_PARSER_TRANSLOG_RELMAP,                "RELMAP"         , decode_relmap },
    { PG_PARSER_TRANSLOG_RUNNING_XACTS,         "RUNNING_XACTS"  , NULL },
    { PG_PARSER_TRANSLOG_XLOG_RECOVERY,         "RECOVERY"       , decode_recovery },
    { PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE,   "COMMIT_PREPARE" , NULL },
    { PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE,    "ABORT_PREPARE"  , NULL },
    { PG_PARSER_TRANSLOG_XACT_ASSIGNMENT,       "ASSIGNMENT"     , NULL },
    { PG_PARSER_TRANSLOG_XACT_PREPARE,          "PREPARE"        , NULL },
    { PG_PARSER_TRANSLOG_HEAP_TRUNCATE,         "TRUNCATE"       , heap_truncate },
    {PG_PARSER_TRANSLOG_SEQ,                    "SEQUENCE"       , decode_seq }
};

static int              m_precnt = (sizeof(m_decodepremgr))/(sizeof(decode_premgr));
static XLogRecPtr       m_parserlsn = 0;


void parserwork_waldecode(decodingcontext* decodingctx)
{
    int32 rippleerrno = 0;
    pg_parser_translog_pre_base* preparserresutl = NULL;

    decodingctx->walpre.m_record = decodingctx->decode_record->data;

    m_parserlsn = decodingctx->decode_record->start.wal.lsn;
    /* 调用预解析，根据预解析内容，分发处理 */
    if(false == pg_parser_trans_preTrans(&decodingctx->walpre, &preparserresutl, &rippleerrno))
    {
        elog(RLOG_ERROR, "pg_parser_trans_preTrans error, %08X, %s",
                            rippleerrno, pg_parser_errno_getErrInfo(rippleerrno));
    }

    /* 调用分发函数 */
    if(m_precnt <= preparserresutl->m_type)
    {
        elog(RLOG_ERROR, "pg_parser_trans_preTrans unknown type:%u", preparserresutl->m_type);
    }

    if(NULL == m_decodepremgr[preparserresutl->m_type].func)
    {
        /* 查看事务号是否有效，无效则手动添加一个事务号，并放入到缓存中处理 */
        pg_parser_trans_preTrans_free(preparserresutl);
        return;
    }

    m_decodepremgr[preparserresutl->m_type].func(decodingctx, preparserresutl);
    pg_parser_trans_preTrans_free(preparserresutl);
}

void decode_getparserlsn(XLogRecPtr* plsn)
{
    *plsn = m_parserlsn;
}

