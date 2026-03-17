#include "app_incl.h"
#include "libpq-fe.h"
#include "utils/list/list_func.h"
#include "utils/dlist/dlist.h"
#include "utils/hash/hash_search.h"
#include "utils/hash/hash_utils.h"
#include "misc/misc_stat.h"
#include "common/xk_pg_parser_define.h"
#include "common/xk_pg_parser_translog.h"
#include "common/xk_pg_parser_errnodef.h"
#include "cache/txn.h"
#include "cache/cache_txn.h"
#include "cache/cache_sysidcts.h"
#include "cache/transcache.h"
#include "catalog/catalog.h"
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
#include "works/parserwork/wal/decode_relmap.h"
#include "works/parserwork/wal/decode_checkpoint.h"
#include "works/parserwork/wal/parserwork_decode.h"
#include "works/parserwork/wal/decode_heap.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode_xact.h"
#include "onlinerefresh/capture/parserwal/onlinerefresh_capture_decode_heap.h"

typedef void (*decode_prefunc_onlinerefresh)(decodingcontext* decodingctx, xk_pg_parser_translog_pre_base* pbase);

typedef struct DECODE_PREMGR_ONLINEREFRESH
{
    int                 type;
    char*               name;
    decode_prefunc_onlinerefresh      func;
} decode_premgr_onlinerefresh;

static decode_premgr_onlinerefresh m_decodepremgr_onlinerefresh[] =
{
    { XK_PG_PARSER_TRANSLOG_INVALID,               "INVALID"        , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_INSERT,           "INSERT"         , onlinerefresh_decode_heap },
    { XK_PG_PARSER_TRANSLOG_HEAP_UPDATE,           "UPDATE"         , onlinerefresh_decode_heap },
    { XK_PG_PARSER_TRANSLOG_HEAP_HOT_UPDATE,       "HOT UPDATE"     , onlinerefresh_decode_heap },
    { XK_PG_PARSER_TRANSLOG_HEAP_DELETE,           "DELETE"         , onlinerefresh_decode_heap },
    { XK_PG_PARSER_TRANSLOG_HEAP2_MULTI_INSERT,    "MULTI INSERT"   , onlinerefresh_decode_heap },
    { XK_PG_PARSER_TRANSLOG_XACT_COMMIT,           "COMMIT"         , onlinerefresh_decode_xact_commit },
    { XK_PG_PARSER_TRANSLOG_XACT_ABORT,            "ABORT"          , onlinerefresh_decode_xact_abort },
    { XK_PG_PARSER_TRANSLOG_XLOG_SWITCH,           "SWITCH"         , NULL },
    { XK_PG_PARSER_TRANSLOG_XLOG_CKP_ONLINE,       "ONLINE"         , decode_chkpt },
    { XK_PG_PARSER_TRANSLOG_XLOG_CKP_SHUTDOWN,     "SHUTDOWN"       , decode_chkpt },
    { XK_PG_PARSER_TRANSLOG_FPW_TUPLE,             "FPW_TUPLE"      , heap_fpw_tuples },
    { XK_PG_PARSER_TRANSLOG_RELMAP,                "RELMAP"         , decode_relmap },
    { XK_PG_PARSER_TRANSLOG_RUNNING_XACTS,         "RUNNING_XACTS"  , NULL },
    { XK_PG_PARSER_TRANSLOG_XLOG_RECOVERY,         "RECOVERY"       , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_COMMIT_PREPARE,   "COMMIT_PREPARE" , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_ABORT_PREPARE,    "ABORT_PREPARE"  , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_ASSIGNMENT,       "ASSIGNMENT"     , NULL },
    { XK_PG_PARSER_TRANSLOG_XACT_PREPARE,          "PREPARE"        , NULL },
    { XK_PG_PARSER_TRANSLOG_HEAP_TRUNCATE,         "TRUNCATE"       , NULL },
    {XK_PG_PARSER_TRANSLOG_SEQ,                    "SEQUENCE"       , NULL }
};

static int              m_precnt_onlinerefresh = (sizeof(m_decodepremgr_onlinerefresh))/(sizeof(decode_premgr_onlinerefresh));
static XLogRecPtr       m_parserlsn = 0;


void parserwork_waldecode_onlinerefresh(decodingcontext* decodingctx)
{
    int32 rippleerrno = 0;
    xk_pg_parser_translog_pre_base* preparserresutl = NULL;
    decodingctx->walpre.m_record = decodingctx->decode_record->data;

    m_parserlsn = decodingctx->decode_record->start.wal.lsn;
    /* 调用预解析，根据预解析内容，分发处理 */
    if(false == xk_pg_parser_trans_preTrans(&decodingctx->walpre, &preparserresutl, &rippleerrno))
    {
        elog(RLOG_ERROR, "xk_pg_parser_trans_preTrans error, %08X, %s",
                            rippleerrno, xk_pg_parser_errno_getErrInfo(rippleerrno));
    }

    /* 调用分发函数 */
    if(m_precnt_onlinerefresh <= preparserresutl->m_type)
    {
        elog(RLOG_ERROR, "xk_pg_parser_trans_preTrans unknown type:%u", preparserresutl->m_type);
    }

    if(NULL == m_decodepremgr_onlinerefresh[preparserresutl->m_type].func)
    {
        /* 查看事务号是否有效，无效则手动添加一个事务号，并放入到缓存中处理 */
        xk_pg_parser_trans_preTrans_free(preparserresutl);
        return;
    }

    m_decodepremgr_onlinerefresh[preparserresutl->m_type].func(decodingctx, preparserresutl);
    xk_pg_parser_trans_preTrans_free(preparserresutl);
}
